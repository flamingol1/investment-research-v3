"""采集调度引擎 - 核心采集逻辑"""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from sqlalchemy.orm import Session

from ..models.db_models import CollectionLog, CollectionTask, IntelArchive
from ..repository.collection_repo import CollectionTaskRepository, CollectionLogRepository
from ..repository.archive_repo import ArchiveRepository
from ..repository.source_repo import SourceRepository
from ..sources.base import CollectionResult, SUPPORTED_DATA_TYPES
from ..sources.registry import SourceRegistry
from .tasks import ALL_DATA_TYPES

from investresearch.core.logging import get_logger

logger = get_logger("intel_hub.engine")


class CollectionEngine:
    """采集调度引擎

    职责:
    1. 按任务配置选择数据源
    2. 执行采集并记录日志
    3. 将采集结果归档
    """

    def __init__(
        self,
        session: Session,
        registry: SourceRegistry,
    ) -> None:
        self._session = session
        self._registry = registry
        self._task_repo = CollectionTaskRepository(session)
        self._log_repo = CollectionLogRepository(session)
        self._archive_repo = ArchiveRepository(session)
        self._source_repo = SourceRepository(session)

    def collect_stock(self, stock_code: str, data_types: list[str] | None = None) -> list[CollectionResult]:
        """采集指定股票的多种数据

        Args:
            stock_code: 股票代码
            data_types: 要采集的数据类型列表，None 表示全部

        Returns:
            各类型的采集结果列表
        """
        if data_types is None:
            data_types = ALL_DATA_TYPES

        results: list[CollectionResult] = []
        for dt in data_types:
            result = self.collect_one(stock_code, dt)
            results.append(result)

        return results

    def collect_one(self, stock_code: str, data_type: str, **kwargs: Any) -> CollectionResult:
        """采集单个数据类型

        自动选择最优数据源，失败后回退到备源。
        采集结果自动归档。
        """
        start_time = time.time()

        # 选择数据源并采集
        result = self._registry.collect_with_fallback(
            data_type=data_type,
            target=stock_code,
            **kwargs,
        )

        elapsed_ms = int((time.time() - start_time) * 1000)
        result.duration_ms = elapsed_ms

        # 记录采集日志
        source = self._source_repo.get_by_name(result.source_name)
        source_id = source.id if source else None

        log = self._log_repo.create(
            task_id=None,
            source_id=source_id,
            source_name=result.source_name,
            target=stock_code,
            data_type=data_type,
            status=result.status,
            records_fetched=result.records_fetched,
            records_stored=0,  # 稍后更新
            error_message=result.error or "",
            duration_ms=elapsed_ms,
        )

        # 归档成功的采集结果
        stored = 0
        if result.status in ("success", "partial"):
            stored = self._archive_result(stock_code, data_type, result, log.id, source_id)

        # 更新日志中的存储记录数
        self._log_repo.complete_log(
            log_id=log.id,
            status=result.status,
            records_stored=stored,
            error=result.error or "",
        )

        # 更新数据源健康状态
        if result.status == "failed":
            self._source_repo.update_health(result.source_name, "degraded", result.error or "")
        else:
            self._source_repo.update_health(result.source_name, "healthy")

        self._session.commit()

        logger.info(
            f"采集完成 | target={stock_code} type={data_type} "
            f"source={result.source_name} status={result.status} "
            f"fetched={result.records_fetched} stored={stored} "
            f"duration={elapsed_ms}ms"
        )

        return result

    def run_task(self, task_id: int) -> list[CollectionResult]:
        """执行指定的采集任务"""
        task = self._task_repo.get_by_id(task_id)
        if task is None:
            raise ValueError(f"任务不存在: {task_id}")
        if task.status == "running":
            raise RuntimeError(f"任务正在运行中: {task_id}")

        self._task_repo.mark_running(task_id)

        # 确定要采集的数据类型
        data_types = [task.task_type] if task.task_type != "all" else ALL_DATA_TYPES

        results: list[CollectionResult] = []
        try:
            for dt in data_types:
                source_name = None
                if task.source_id:
                    source = self._source_repo.get_by_id(task.source_id)
                    source_name = source.name if source else None

                result = self._registry.collect_with_fallback(
                    data_type=dt,
                    target=task.target,
                    preferred_source=source_name,
                )
                results.append(result)

                # 记录日志
                source = self._source_repo.get_by_name(result.source_name)
                source_id = source.id if source else task.source_id

                log = self._log_repo.create(
                    task_id=task_id,
                    source_id=source_id,
                    source_name=result.source_name,
                    target=task.target,
                    data_type=dt,
                    status=result.status,
                    records_fetched=result.records_fetched,
                    records_stored=0,
                    error_message=result.error or "",
                    duration_ms=result.duration_ms,
                )

                # 归档
                stored = 0
                if result.status in ("success", "partial"):
                    stored = self._archive_result(
                        task.target, dt, result, log.id, source_id
                    )

                self._log_repo.complete_log(
                    log.id, result.status, stored, result.error or ""
                )

            all_success = all(r.status == "success" for r in results)
            self._task_repo.mark_completed(task_id, all_success)
            self._session.commit()

        except Exception as e:
            self._task_repo.mark_completed(task_id, False)
            self._session.commit()
            raise

        return results

    def _archive_result(
        self,
        stock_code: str,
        data_type: str,
        result: CollectionResult,
        log_id: int,
        source_id: int | None,
    ) -> int:
        """将采集结果归档到 IntelArchive 表"""
        try:
            data = result.data

            # 构建归档记录
            title = f"{stock_code} {data_type}"
            summary = json.dumps(data, ensure_ascii=False, default=str)[:2000]
            content = json.dumps(data, ensure_ascii=False, default=str)

            # 如果数据是列表，为每条记录创建单独的归档
            items = data.get("items", data) if isinstance(data, dict) else data
            if isinstance(items, list) and len(items) > 1:
                stored = 0
                for i, item in enumerate(items):
                    item_summary = json.dumps(item, ensure_ascii=False, default=str)[:1000]
                    item_content = json.dumps(item, ensure_ascii=False, default=str)
                    self._archive_repo.create(
                        stock_code=stock_code,
                        stock_name="",
                        category=data_type,
                        source_name=result.source_name,
                        data_date=None,
                        title=f"{stock_code} {data_type} #{i+1}",
                        summary=item_summary,
                        content_json=item_content,
                        tags=f"{data_type},{result.source_name}",
                        collection_log_id=log_id,
                        source_id=source_id,
                    )
                    stored += 1
                return stored
            else:
                self._archive_repo.create(
                    stock_code=stock_code,
                    stock_name="",
                    category=data_type,
                    source_name=result.source_name,
                    data_date=None,
                    title=title,
                    summary=summary,
                    content_json=content,
                    tags=f"{data_type},{result.source_name}",
                    collection_log_id=log_id,
                    source_id=source_id,
                )
                return 1
        except Exception as e:
            logger.warning(f"归档失败: {e}")
            return 0
