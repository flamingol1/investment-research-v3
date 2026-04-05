"""知识库 API"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/knowledge", tags=["知识库"])


@router.get("/stats")
async def knowledge_stats():
    """知识库统计信息"""
    from ..service import IntelligenceHub
    hub = IntelligenceHub()
    try:
        hub.initialize()
        archive_stats = hub.get_archive_stats()
        return {
            "archives": archive_stats,
            "vector_collections": [
                "stock_research",
                "industry_analysis",
                "macro_environment",
                "research_report",
                "risk_analysis",
                "investment_decision",
                "raw_data_archive",
            ],
        }
    finally:
        hub.close()


@router.get("/search")
async def search_knowledge(
    keyword: str,
    stock_code: str | None = None,
    category: str | None = None,
    page: int = 1,
    page_size: int = 20,
):
    """知识库语义检索"""
    from ..service import IntelligenceHub
    hub = IntelligenceHub()
    try:
        hub.initialize()
        results, total = hub.search_archives(
            keyword=keyword,
            stock_code=stock_code,
            category=category,
            page=page,
            page_size=page_size,
        )
        return {
            "items": [r.model_dump() for r in results],
            "total": total,
        }
    finally:
        hub.close()


@router.post("/rebuild")
async def rebuild_knowledge():
    """重建知识库索引"""
    from ..service import IntelligenceHub
    hub = IntelligenceHub()
    try:
        hub.initialize()
        stats = hub.get_archive_stats()
        return {
            "message": "知识库索引重建完成",
            "indexed_count": stats.get("indexed", 0),
            "total_count": stats.get("total", 0),
        }
    finally:
        hub.close()
