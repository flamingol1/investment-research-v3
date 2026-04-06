"""归档管理 - 采集结果 → 归档资料的转换和存储"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from ..models.db_models import IntelArchive
from ..repository.archive_repo import ArchiveRepository
from ..sources.base import CollectionResult

from investresearch.core.logging import get_logger

logger = get_logger("intel_hub.archive")


class ArchiveManager:
    """归档管理器

    将采集结果转换为结构化的归档记录。
    支持智能拆分：列表数据按条目归档，单条数据整体归档。
    """

    def __init__(self, session: Session) -> None:
        self._repo = ArchiveRepository(session)
        self._session = session

    def archive_result(
        self,
        stock_code: str,
        stock_name: str,
        data_type: str,
        result: CollectionResult,
        collection_log_id: int | None = None,
        source_id: int | None = None,
    ) -> int:
        """归档采集结果，返回归档数量"""
        data = result.data

        if isinstance(data, dict) and "items" in data:
            items = data["items"]
        elif isinstance(data, list):
            items = data
        else:
            items = None

        # 列表数据：按条目归档
        if isinstance(items, list) and len(items) > 1:
            return self._archive_items(
                stock_code, stock_name, data_type, items,
                result.source_name, collection_log_id, source_id,
            )

        # 单条/空数据：整体归档
        return self._archive_whole(
            stock_code, stock_name, data_type, data,
            result.source_name, collection_log_id, source_id,
        )

    def archive_custom(
        self,
        stock_code: str,
        stock_name: str,
        category: str,
        source_name: str,
        title: str,
        summary: str,
        content: dict[str, Any] | str,
        data_date: datetime | None = None,
        tags: str = "",
    ) -> int:
        """自定义归档"""
        self._repo.create(
            stock_code=stock_code,
            stock_name=stock_name,
            category=category,
            source_name=source_name,
            data_date=data_date,
            title=title,
            summary=summary,
            content_json=content,
            tags=tags,
        )
        self._session.flush()
        return 1

    def _archive_items(
        self,
        stock_code: str,
        stock_name: str,
        data_type: str,
        items: list[Any],
        source_name: str,
        log_id: int | None,
        source_id: int | None,
    ) -> int:
        stored = 0
        for i, item in enumerate(items):
            item_content = json.dumps(item, ensure_ascii=False, default=str)
            item_summary = item_content[:1000]
            self._repo.create(
                stock_code=stock_code,
                stock_name=stock_name,
                category=data_type,
                source_name=source_name,
                data_date=None,
                title=f"{stock_code} {data_type} #{i + 1}",
                summary=item_summary,
                content_json=item_content,
                tags=f"{data_type},{source_name}",
                collection_log_id=log_id,
                source_id=source_id,
            )
            stored += 1
        self._session.flush()
        logger.info(f"归档 {stored} 条 {data_type} 数据 ({stock_code})")
        return stored

    def _archive_whole(
        self,
        stock_code: str,
        stock_name: str,
        data_type: str,
        data: Any,
        source_name: str,
        log_id: int | None,
        source_id: int | None,
    ) -> int:
        content = json.dumps(data, ensure_ascii=False, default=str)
        summary = content[:2000]
        self._repo.create(
            stock_code=stock_code,
            stock_name=stock_name,
            category=data_type,
            source_name=source_name,
            data_date=None,
            title=f"{stock_code} {data_type}",
            summary=summary,
            content_json=content,
            tags=f"{data_type},{source_name}",
            collection_log_id=log_id,
            source_id=source_id,
        )
        self._session.flush()
        logger.info(f"归档 {data_type} 整体数据 ({stock_code})")
        return 1
