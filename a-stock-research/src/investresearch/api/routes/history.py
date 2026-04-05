"""研究历史路由"""

from __future__ import annotations

from fastapi import APIRouter

from investresearch.api.schemas import HistoryResponse, HistoryEntryResponse
from investresearch.core.logging import get_logger

logger = get_logger("api.routes.history")

router = APIRouter(prefix="/api", tags=["history"])


@router.get("/history/{stock_code}", response_model=HistoryResponse)
async def get_history(stock_code: str) -> HistoryResponse:
    """获取某股票的研究历史"""
    from investresearch.knowledge_base.chroma_store import ChromaKnowledgeStore

    store = ChromaKnowledgeStore()
    history = store.get_research_history(stock_code)

    if not history:
        return HistoryResponse(stock_code=stock_code, entries=[])

    entries: list[HistoryEntryResponse] = []
    stock_name = ""
    for entry in history:
        if hasattr(entry, "model_dump"):
            # Pydantic model
            data = entry.model_dump(mode="json")
        elif isinstance(entry, dict):
            data = entry
        else:
            continue

        rd = data.get("research_date", "")
        date_str = str(rd)[:16] if rd else ""
        stock_name = data.get("stock_name", stock_name)

        entries.append(HistoryEntryResponse(
            stock_code=data.get("stock_code", stock_code),
            stock_name=stock_name,
            research_date=date_str,
            depth=data.get("depth", "standard"),
            recommendation=data.get("recommendation"),
            risk_level=data.get("risk_level"),
            target_price_low=data.get("target_price_low"),
            target_price_high=data.get("target_price_high"),
            current_price=data.get("current_price"),
            agents_completed=data.get("agents_completed", []),
        ))

    return HistoryResponse(stock_code=stock_code, stock_name=stock_name, entries=entries)
