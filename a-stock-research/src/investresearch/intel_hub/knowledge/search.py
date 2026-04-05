"""知识检索服务 - 统一检索入口（结构化查询 + 语义检索）"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from ..models.schemas import ArchiveSearchQuery, KnowledgeSearchResult
from ..repository.archive_repo import ArchiveRepository

from investresearch.core.logging import get_logger

logger = get_logger("intel_hub.search")


class KnowledgeSearchService:
    """知识检索服务

    提供两层检索:
    1. 结构化查询 — 通过 SQLite 按字段精确筛选
    2. 语义检索 — 通过 ChromaDB 向量相似度搜索
    """

    def __init__(self, session: Session, chroma_dir: str = "data/chroma") -> None:
        self._session = session
        self._repo = ArchiveRepository(session)
        self._chroma_dir = chroma_dir

    def search(
        self,
        keyword: str | None = None,
        stock_code: str | None = None,
        category: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[KnowledgeSearchResult], int]:
        """统一检索入口"""
        # 结构化查询
        query = ArchiveSearchQuery(
            keyword=keyword,
            stock_code=stock_code,
            category=category,
            page=page,
            page_size=page_size,
        )
        archives, total = self._repo.search(query)

        results = [
            KnowledgeSearchResult(
                archive_id=a.id,
                stock_code=a.stock_code,
                category=a.category,
                title=a.title,
                summary=a.summary[:200] if a.summary else "",
                source_name=a.source_name,
                data_date=a.data_date,
            )
            for a in archives
        ]

        return results, total

    def semantic_search(
        self,
        query_text: str,
        category: str | None = None,
        n_results: int = 10,
    ) -> list[KnowledgeSearchResult]:
        """语义检索 — 通过 ChromaDB 向量搜索"""
        try:
            from investresearch.knowledge_base.chroma_store import ChromaKnowledgeStore
            store = ChromaKnowledgeStore(persist_dir=self._chroma_dir)

            # 确定搜索的 collection
            if category:
                collection_name = self._category_to_collection(category)
                results = self._search_collection(store, collection_name, query_text, n_results)
            else:
                # 搜索所有 collection
                results = []
                for coll_name in self._all_collections():
                    results.extend(
                        self._search_collection(store, coll_name, query_text, n_results)
                    )
                results.sort(key=lambda r: r.relevance_score, reverse=True)
                results = results[:n_results]

            return results

        except ImportError:
            logger.warning("ChromaDB 不可用，回退到结构化搜索")
            archives, _ = self._repo.search(ArchiveSearchQuery(
                keyword=query_text, page=1, page_size=n_results,
            ))
            return [
                KnowledgeSearchResult(
                    archive_id=a.id,
                    stock_code=a.stock_code,
                    category=a.category,
                    title=a.title,
                    summary=a.summary[:200] if a.summary else "",
                    source_name=a.source_name,
                )
                for a in archives
            ]

    def _search_collection(
        self,
        store: Any,
        collection_name: str,
        query_text: str,
        n_results: int,
    ) -> list[KnowledgeSearchResult]:
        """搜索单个 ChromaDB collection"""
        results = []
        try:
            collection = store._client.get_or_create_collection(collection_name)
            query_result = collection.query(
                query_texts=[query_text],
                n_results=min(n_results, 10),
            )

            if query_result and query_result.get("documents"):
                documents = query_result["documents"][0]
                metadatas = query_result.get("metadatas", [[]])[0]
                distances = query_result.get("distances", [[]])[0]

                for doc, meta, dist in zip(documents, metadatas, distances):
                    results.append(KnowledgeSearchResult(
                        archive_id=meta.get("archive_id"),
                        stock_code=meta.get("stock_code"),
                        category=meta.get("category"),
                        title=meta.get("stock_code", "") + " " + meta.get("category", ""),
                        summary=str(doc)[:200] if doc else "",
                        content_snippet=str(doc)[:500] if doc else "",
                        relevance_score=round(1.0 - dist, 4) if dist else 0.0,
                        source_name=meta.get("source_name", ""),
                    ))
        except Exception as e:
            logger.warning(f"搜索 {collection_name} 失败: {e}")

        return results

    @staticmethod
    def _category_to_collection(category: str) -> str:
        mapping = {
            "research_reports": "research_report",
            "industry": "industry_analysis",
        }
        return mapping.get(category, "raw_data_archive")

    @staticmethod
    def _all_collections() -> list[str]:
        return [
            "stock_research",
            "industry_analysis",
            "macro_environment",
            "research_report",
            "risk_analysis",
            "investment_decision",
            "raw_data_archive",
        ]
