"""知识库语义搜索路由"""

from __future__ import annotations

from fastapi import APIRouter

from investresearch.api.schemas import SearchRequest, SearchResponse, SearchItemResponse
from investresearch.core.logging import get_logger

logger = get_logger("api.routes.search")

router = APIRouter(prefix="/api", tags=["search"])


@router.post("/search", response_model=SearchResponse)
async def search_knowledge_base(req: SearchRequest) -> SearchResponse:
    """语义搜索知识库"""
    from investresearch.knowledge_base.chroma_store import ChromaKnowledgeStore

    store = ChromaKnowledgeStore()
    results = store.search_similar(
        query=req.query,
        category=req.category,
        n=req.num_results,
    )

    items = []
    for r in results:
        meta = r.get("metadata", {})
        items.append(SearchItemResponse(
            document=r.get("document", "")[:500],
            stock_code=meta.get("stock_code", ""),
            stock_name=meta.get("stock_name", ""),
            category=meta.get("category", ""),
            date=meta.get("date", ""),
            similarity=1 - r.get("distance", 1.0),
        ))

    return SearchResponse(query=req.query, results=items, total=len(items))
