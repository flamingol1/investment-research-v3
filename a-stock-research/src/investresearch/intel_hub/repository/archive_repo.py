"""归档资料 CRUD"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Sequence

from sqlalchemy import select, func, and_
from sqlalchemy.orm import Session

from ..models.db_models import IntelArchive
from ..models.schemas import ArchiveSearchQuery


class ArchiveRepository:
    """归档资料仓储"""

    def __init__(self, session: Session) -> None:
        self._session = session

    def create(
        self,
        stock_code: str,
        stock_name: str,
        category: str,
        source_name: str,
        data_date: datetime | None,
        title: str,
        summary: str,
        content_json: dict[str, Any] | str,
        tags: str = "",
        file_path: str = "",
        collection_log_id: int | None = None,
        source_id: int | None = None,
    ) -> IntelArchive:
        if isinstance(content_json, dict):
            content_json = json.dumps(content_json, ensure_ascii=False, default=str)

        archive = IntelArchive(
            stock_code=stock_code,
            stock_name=stock_name,
            category=category,
            source_name=source_name,
            data_date=data_date,
            title=title,
            summary=summary,
            content_json=content_json,
            tags=tags,
            file_path=file_path,
            collection_log_id=collection_log_id,
            source_id=source_id,
        )
        self._session.add(archive)
        self._session.flush()
        return archive

    def get_by_id(self, archive_id: int) -> IntelArchive | None:
        return self._session.get(IntelArchive, archive_id)

    def get_content(self, archive_id: int) -> dict[str, Any] | None:
        """获取归档的完整内容"""
        archive = self.get_by_id(archive_id)
        if archive is None:
            return None
        try:
            return json.loads(archive.content_json) if archive.content_json else {}
        except json.JSONDecodeError:
            return {}

    def delete(self, archive_id: int) -> bool:
        archive = self.get_by_id(archive_id)
        if archive is None:
            return False
        self._session.delete(archive)
        self._session.flush()
        return True

    def mark_indexed(self, archive_id: int) -> None:
        """标记已入向量库"""
        archive = self.get_by_id(archive_id)
        if archive:
            archive.indexed = True
            self._session.flush()

    def search(self, query: ArchiveSearchQuery) -> tuple[Sequence[IntelArchive], int]:
        """搜索归档资料，返回 (结果列表, 总数)"""
        conditions = []

        if query.stock_code:
            conditions.append(IntelArchive.stock_code == query.stock_code)
        if query.category:
            conditions.append(IntelArchive.category == query.category)
        if query.source_name:
            conditions.append(IntelArchive.source_name == query.source_name)
        if query.keyword:
            keyword_cond = (
                IntelArchive.title.contains(query.keyword)
                | IntelArchive.summary.contains(query.keyword)
                | IntelArchive.tags.contains(query.keyword)
            )
            conditions.append(keyword_cond)
        if query.date_from:
            dt_from = datetime.combine(query.date_from, datetime.min.time())
            conditions.append(IntelArchive.data_date >= dt_from)
        if query.date_to:
            dt_to = datetime.combine(query.date_to, datetime.max.time())
            conditions.append(IntelArchive.data_date <= dt_to)

        where_clause = and_(*conditions) if conditions else True

        # 计算总数
        count_stmt = select(func.count(IntelArchive.id)).where(where_clause)
        total = self._session.execute(count_stmt).scalar() or 0

        # 分页查询
        offset = (query.page - 1) * query.page_size
        data_stmt = (
            select(IntelArchive)
            .where(where_clause)
            .order_by(IntelArchive.created_at.desc())
            .offset(offset)
            .limit(query.page_size)
        )
        results = self._session.execute(data_stmt).scalars().all()

        return results, total

    def get_unindexed(self, limit: int = 100) -> Sequence[IntelArchive]:
        """获取未入向量库的归档"""
        stmt = (
            select(IntelArchive)
            .where(IntelArchive.indexed.is_(False))
            .order_by(IntelArchive.created_at)
            .limit(limit)
        )
        return self._session.execute(stmt).scalars().all()

    def get_stats(self) -> dict[str, Any]:
        """归档统计"""
        total = self._session.execute(
            select(func.count(IntelArchive.id))
        ).scalar() or 0

        by_category = dict(
            self._session.execute(
                select(IntelArchive.category, func.count(IntelArchive.id))
                .group_by(IntelArchive.category)
            ).all()
        )

        indexed_count = self._session.execute(
            select(func.count(IntelArchive.id))
            .where(IntelArchive.indexed.is_(True))
        ).scalar() or 0

        return {
            "total": total,
            "by_category": by_category,
            "indexed": indexed_count,
            "unindexed": total - indexed_count,
        }
