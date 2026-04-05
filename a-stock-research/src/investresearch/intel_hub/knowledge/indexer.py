"""知识索引构建 - 将归档资料索引到 ChromaDB 向量库"""

from __future__ import annotations

import json
from typing import Any, Sequence

from sqlalchemy.orm import Session

from ..repository.archive_repo import ArchiveRepository
from ..models.db_models import IntelArchive

from investresearch.core.logging import get_logger

logger = get_logger("intel_hub.indexer")


class KnowledgeIndexer:
    """知识索引构建器

    将归档资料同步到 ChromaDB 向量知识库。
    """

    def __init__(self, session: Session, chroma_dir: str = "data/chroma") -> None:
        self._session = session
        self._repo = ArchiveRepository(session)
        self._chroma_dir = chroma_dir
        self._store = None

    def _get_store(self):
        """延迟加载 ChromaDB 存储"""
        if self._store is None:
            try:
                from investresearch.knowledge_base.chroma_store import ChromaKnowledgeStore
                self._store = ChromaKnowledgeStore(persist_dir=self._chroma_dir)
            except ImportError:
                logger.warning("ChromaDB 知识库不可用，跳过向量索引")
                return None
        return self._store

    def index_unindexed(self, limit: int = 100) -> int:
        """将未索引的归档资料批量索引到 ChromaDB"""
        store = self._get_store()
        if store is None:
            return 0

        archives = self._repo.get_unindexed(limit)
        if not archives:
            return 0

        indexed = 0
        for archive in archives:
            try:
                self._index_one(store, archive)
                self._repo.mark_indexed(archive.id)
                indexed += 1
            except Exception as e:
                logger.warning(f"索引归档 {archive.id} 失败: {e}")

        self._session.commit()
        logger.info(f"索引完成: {indexed}/{len(archives)} 条")
        return indexed

    def _index_one(self, store: Any, archive: IntelArchive) -> None:
        """将单条归档索引到 ChromaDB"""
        # 构建文档文本
        doc_text = f"{archive.title}\n{archive.summary}"
        if doc_text.strip() == "":
            return

        # 使用归档标题 + 摘要作为文档内容
        metadata = {
            "archive_id": archive.id,
            "stock_code": archive.stock_code,
            "stock_name": archive.stock_name,
            "category": archive.category,
            "source_name": archive.source_name,
            "tags": archive.tags,
        }

        # 直接使用 ChromaDB 的 add 方法
        collection_name = self._get_collection_name(archive.category)
        if collection_name is None:
            return

        collection = store._client.get_or_create_collection(collection_name)
        collection.add(
            documents=[doc_text],
            metadatas=[metadata],
            ids=[f"archive_{archive.id}"],
        )

    def _get_collection_name(self, category: str) -> str | None:
        """根据数据类型映射到 ChromaDB collection"""
        mapping = {
            "stock_info": "raw_data_archive",
            "daily_prices": "raw_data_archive",
            "realtime_quote": "raw_data_archive",
            "financials": "raw_data_archive",
            "valuation": "raw_data_archive",
            "announcements": "raw_data_archive",
            "governance": "raw_data_archive",
            "research_reports": "research_report",
            "shareholders": "raw_data_archive",
            "industry": "industry_analysis",
            "valuation_pct": "raw_data_archive",
            "news": "raw_data_archive",
        }
        return mapping.get(category, "raw_data_archive")

    def rebuild_all(self) -> dict[str, int]:
        """重建全部向量索引"""
        store = self._get_store()
        if store is None:
            return {"indexed": 0, "failed": 0}

        # 获取所有归档（包括已索引的）
        from sqlalchemy import select
        stmt = select(IntelArchive).order_by(IntelArchive.id)
        archives = self._session.execute(stmt).scalars().all()

        indexed = 0
        failed = 0
        for archive in archives:
            try:
                self._index_one(store, archive)
                self._repo.mark_indexed(archive.id)
                indexed += 1
            except Exception as e:
                logger.warning(f"重建索引 {archive.id} 失败: {e}")
                failed += 1

        self._session.commit()
        return {"indexed": indexed, "failed": failed}
