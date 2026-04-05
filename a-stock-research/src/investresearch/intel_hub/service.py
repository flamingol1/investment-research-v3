"""情报中心门面服务 (Facade)

统一入口，对外提供简洁的 API。
内部协调 数据源管理、采集引擎、知识库、归档管理 四个子系统。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from sqlalchemy.orm import Session

from .config import IntelHubConfig
from .models.db_models import (
    IntelArchive,
    IntelSource,
    CollectionTask,
    CollectionLog,
)
from .models.schemas import (
    SourceCreate,
    SourceUpdate,
    SourceRead,
    TaskCreate,
    TaskUpdate,
    TaskRead,
    ArchiveRead,
    ArchiveSearchQuery,
    CollectionResult,
    KnowledgeSearchResult,
)
from .repository import (
    Database,
    init_db,
    get_session,
    SourceRepository,
    CollectionTaskRepository,
    CollectionLogRepository,
    ArchiveRepository,
)
from .sources import SourceRegistry
from .sources.base import SUPPORTED_DATA_TYPES
from .collectors import CollectionEngine, TASK_TYPE_DEFINITIONS
from .collectors.tasks import ALL_DATA_TYPES

from investresearch.core.logging import get_logger

logger = get_logger("intel_hub.service")


class IntelligenceHub:
    """情报中心 - 统一门面服务

    用法::

        hub = IntelligenceHub()
        hub.initialize()

        # 采集数据
        results = hub.collect_stock("300358")

        # 搜索归档
        archives, total = hub.search_archives(stock_code="300358")

        # 获取历史行情
        archives, _ = hub.search_archives(
            stock_code="300358",
            category="daily_prices",
        )
    """

    def __init__(
        self,
        config: IntelHubConfig | None = None,
        db_url: str | None = None,
    ) -> None:
        self._config = config or IntelHubConfig()
        self._db: Database | None = None
        self._registry: SourceRegistry | None = None
        self._engine: CollectionEngine | None = None

        if db_url:
            self._config.db_path = db_url

    def initialize(self) -> None:
        """初始化情报中心"""
        # 初始化数据库
        db_url = f"sqlite:///{self._config.db_path}"
        self._db = init_db(db_url)

        # 初始化数据源注册中心
        self._registry = SourceRegistry.create_default_registry()

        # 同步数据源到数据库
        self._sync_sources_to_db()

        # 初始化采集引擎
        session = self._db.get_session()
        self._engine = CollectionEngine(session, self._registry)

        logger.info("情报中心初始化完成")

    def close(self) -> None:
        """关闭并释放资源"""
        if self._db:
            self._db.dispose()
            self._db = None

    # ================================================================
    # 数据采集
    # ================================================================

    def collect_stock(
        self,
        stock_code: str,
        data_types: list[str] | None = None,
        preferred_source: str | None = None,
    ) -> list[CollectionResult]:
        """采集指定股票的数据

        Args:
            stock_code: 股票代码
            data_types: 要采集的数据类型，None=全部
            preferred_source: 首选数据源名称

        Returns:
            各类型采集结果列表
        """
        self._ensure_initialized()

        if data_types is None:
            data_types = ALL_DATA_TYPES

        results: list[CollectionResult] = []
        for dt in data_types:
            start_time = __import__("time").time()

            # 选择数据源并采集
            result = self._registry.collect_with_fallback(
                data_type=dt,
                target=stock_code,
                preferred_source=preferred_source,
            )
            result.duration_ms = int((__import__("time").time() - start_time) * 1000)

            # 归档成功的采集结果
            session = self._db.get_session()
            try:
                if result.status in ("success", "partial"):
                    self._archive_collection(session, stock_code, dt, result)

                # 记录采集日志
                self._log_collection(session, stock_code, dt, result)

                # 更新数据源健康状态
                self._update_source_health(session, result)

                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

            results.append(result)

        return results

    def collect_type(
        self,
        stock_code: str,
        data_type: str,
        preferred_source: str | None = None,
    ) -> CollectionResult:
        """采集指定股票的单个数据类型"""
        results = self.collect_stock(stock_code, [data_type], preferred_source)
        return results[0] if results else CollectionResult(
            target=stock_code,
            data_type=data_type,
            source_name="none",
            status="failed",
            error="采集失败",
        )

    # ================================================================
    # 数据源管理
    # ================================================================

    def list_sources(self) -> list[SourceRead]:
        """列出所有数据源"""
        self._ensure_initialized()
        session = self._db.get_session()
        try:
            repo = SourceRepository(session)
            sources = repo.list_all()
            return [SourceRead.model_validate(s) for s in sources]
        finally:
            session.close()

    def get_source(self, name: str) -> SourceRead | None:
        """获取数据源详情"""
        self._ensure_initialized()
        session = self._db.get_session()
        try:
            repo = SourceRepository(session)
            source = repo.get_by_name(name)
            return SourceRead.model_validate(source) if source else None
        finally:
            session.close()

    def update_source(self, name: str, data: SourceUpdate) -> SourceRead | None:
        """更新数据源配置"""
        self._ensure_initialized()
        session = self._db.get_session()
        try:
            repo = SourceRepository(session)
            source = repo.update(name, data)
            if source:
                session.commit()
                return SourceRead.model_validate(source)
            return None
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def check_source_health(self, name: str) -> dict[str, Any]:
        """检查数据源健康状态"""
        self._ensure_initialized()
        adapter = self._registry.get(name)
        if adapter is None:
            return {"name": name, "status": "unknown", "error": "数据源未注册"}

        health = adapter.health_check()

        # 更新数据库
        session = self._db.get_session()
        try:
            repo = SourceRepository(session)
            repo.update_health(name, health.status, health.error or "")
            session.commit()
        finally:
            session.close()

        return health.model_dump()

    def check_all_health(self) -> dict[str, dict[str, Any]]:
        """检查所有数据源健康状态"""
        self._ensure_initialized()
        results = self._registry.health_check_all()

        # 批量更新数据库
        session = self._db.get_session()
        try:
            repo = SourceRepository(session)
            for name, health in results.items():
                repo.update_health(name, health.status, health.error or "")
            session.commit()
        finally:
            session.close()

        return {name: h.model_dump() for name, h in results.items()}

    # ================================================================
    # 采集任务管理
    # ================================================================

    def list_tasks(self) -> list[TaskRead]:
        """列出所有采集任务"""
        self._ensure_initialized()
        session = self._db.get_session()
        try:
            repo = CollectionRepository(session)
            tasks = repo.list_all()
            return [TaskRead.model_validate(t) for t in tasks]
        finally:
            session.close()

    def create_task(self, data: TaskCreate) -> TaskRead:
        """创建采集任务"""
        self._ensure_initialized()
        session = self._db.get_session()
        try:
            repo = CollectionRepository(session)
            source_repo = SourceRepository(session)

            source_id = None
            if data.source_name:
                source = source_repo.get_by_name(data.source_name)
                source_id = source.id if source else None

            task = repo.create(data, source_id)
            session.commit()
            return TaskRead.model_validate(task)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def run_task(self, task_id: int) -> list[CollectionResult]:
        """执行采集任务"""
        self._ensure_initialized()
        session = self._db.get_session()
        try:
            engine = CollectionEngine(session, self._registry)
            return engine.run_task(task_id)
        finally:
            session.close()

    # ================================================================
    # 归档与知识库
    # ================================================================

    def search_archives(
        self,
        stock_code: str | None = None,
        category: str | None = None,
        keyword: str | None = None,
        source_name: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[ArchiveRead], int]:
        """搜索归档资料"""
        self._ensure_initialized()
        session = self._db.get_session()
        try:
            repo = ArchiveRepository(session)
            query = ArchiveSearchQuery(
                stock_code=stock_code,
                category=category,
                keyword=keyword,
                source_name=source_name,
                page=page,
                page_size=page_size,
            )
            results, total = repo.search(query)
            return [ArchiveRead.model_validate(a) for a in results], total
        finally:
            session.close()

    def get_archive_content(self, archive_id: int) -> dict[str, Any] | None:
        """获取归档资料的完整内容"""
        self._ensure_initialized()
        session = self._db.get_session()
        try:
            repo = ArchiveRepository(session)
            return repo.get_content(archive_id)
        finally:
            session.close()

    def get_archive_stats(self) -> dict[str, Any]:
        """获取归档统计"""
        self._ensure_initialized()
        session = self._db.get_session()
        try:
            repo = ArchiveRepository(session)
            return repo.get_stats()
        finally:
            session.close()

    def get_collection_logs(
        self,
        target: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """获取采集日志"""
        self._ensure_initialized()
        session = self._db.get_session()
        try:
            repo = CollectionLogRepository(session)
            if target:
                logs = repo.list_by_target(target, limit)
            else:
                logs = session.query(CollectionLog).order_by(
                    CollectionLog.started_at.desc()
                ).limit(limit).all()

            return [
                {
                    "id": log.id,
                    "source_name": log.source_name,
                    "target": log.target,
                    "data_type": log.data_type,
                    "status": log.status,
                    "records_fetched": log.records_fetched,
                    "records_stored": log.records_stored,
                    "error_message": log.error_message,
                    "duration_ms": log.duration_ms,
                    "started_at": str(log.started_at) if log.started_at else None,
                }
                for log in logs
            ]
        finally:
            session.close()

    def get_supported_data_types(self) -> dict[str, dict[str, str]]:
        """获取所有支持的采集数据类型"""
        return {
            name: {
                "display_name": defn.display_name,
                "description": defn.description,
                "category": defn.category,
            }
            for name, defn in TASK_TYPE_DEFINITIONS.items()
        }

    # ================================================================
    # 内部方法
    # ================================================================

    def _ensure_initialized(self) -> None:
        if self._db is None or self._registry is None:
            self.initialize()

    def _sync_sources_to_db(self) -> None:
        """将注册中心的数据源同步到数据库"""
        session = self._db.get_session()
        try:
            repo = SourceRepository(session)
            for adapter in self._registry.list_all():
                existing = repo.get_by_name(adapter.name)
                if existing is None:
                    repo.create(SourceCreate(
                        name=adapter.name,
                        display_name=adapter.display_name,
                        description=adapter.display_name,
                        enabled=True,
                        priority=adapter.priority,
                    ))
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _archive_collection(
        self,
        session: Session,
        stock_code: str,
        data_type: str,
        result: CollectionResult,
    ) -> int:
        """归档采集结果"""
        repo = ArchiveRepository(session)
        data = result.data
        content = json.dumps(data, ensure_ascii=False, default=str)
        summary = content[:2000]

        items = data.get("items", data) if isinstance(data, dict) else data
        if isinstance(items, list) and len(items) > 1:
            stored = 0
            for i, item in enumerate(items):
                item_content = json.dumps(item, ensure_ascii=False, default=str)
                repo.create(
                    stock_code=stock_code,
                    stock_name="",
                    category=data_type,
                    source_name=result.source_name,
                    data_date=None,
                    title=f"{stock_code} {data_type} #{i+1}",
                    summary=item_content[:1000],
                    content_json=item_content,
                    tags=f"{data_type},{result.source_name}",
                )
                stored += 1
            return stored
        else:
            repo.create(
                stock_code=stock_code,
                stock_name="",
                category=data_type,
                source_name=result.source_name,
                data_date=None,
                title=f"{stock_code} {data_type}",
                summary=summary,
                content_json=content,
                tags=f"{data_type},{result.source_name}",
            )
            return 1

    def _log_collection(
        self,
        session: Session,
        stock_code: str,
        data_type: str,
        result: CollectionResult,
    ) -> None:
        """记录采集日志"""
        repo = CollectionLogRepository(session)
        repo.create(
            task_id=None,
            source_id=None,
            source_name=result.source_name,
            target=stock_code,
            data_type=data_type,
            status=result.status,
            records_fetched=result.records_fetched,
            records_stored=0,
            error_message=result.error or "",
            duration_ms=result.duration_ms,
        )

    def _update_source_health(
        self,
        session: Session,
        result: CollectionResult,
    ) -> None:
        """更新数据源健康状态"""
        repo = SourceRepository(session)
        if result.status == "failed":
            repo.update_health(result.source_name, "degraded", result.error or "")
        else:
            repo.update_health(result.source_name, "healthy")
