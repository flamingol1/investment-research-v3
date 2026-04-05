"""监控列表路由"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from investresearch.api.schemas import (
    WatchAddRequest,
    WatchListResponse,
    WatchListItemResponse,
    ApiResponse,
    UpdateResponse,
)
from investresearch.core.logging import get_logger

logger = get_logger("api.routes.watch")

router = APIRouter(prefix="/api", tags=["watch"])


def _get_manager():
    from investresearch.knowledge_base.watch_list import WatchListManager
    return WatchListManager()


@router.get("/watch", response_model=WatchListResponse)
async def get_watch_list() -> WatchListResponse:
    """获取监控列表"""
    mgr = _get_manager()
    wl = mgr.get_all()
    items = [
        WatchListItemResponse(
            stock_code=item.stock_code,
            stock_name=item.stock_name,
            recommendation=item.recommendation,
            added_at=item.added_at,
            last_updated_at=item.last_updated_at,
            last_report_date=item.last_report_date,
            status=item.status,
            notes=item.notes,
        )
        for item in wl.items
    ]
    return WatchListResponse(items=items, total=len(items), updated_at=wl.updated_at)


@router.post("/watch", response_model=ApiResponse)
async def add_to_watch(req: WatchAddRequest) -> ApiResponse:
    """添加到监控列表"""
    mgr = _get_manager()
    added = mgr.add(req.stock_code, stock_name=req.stock_name)
    if not added:
        return ApiResponse(success=True, message=f"{req.stock_code} 已在监控列表中")
    return ApiResponse(success=True, message=f"已添加 {req.stock_code} 到监控列表")


@router.delete("/watch/{stock_code}", response_model=ApiResponse)
async def remove_from_watch(stock_code: str) -> ApiResponse:
    """从监控列表移除"""
    mgr = _get_manager()
    removed = mgr.remove(stock_code)
    if not removed:
        raise HTTPException(status_code=404, detail=f"{stock_code} 不在监控列表中")
    return ApiResponse(success=True, message=f"已移除 {stock_code}")


@router.post("/update/{stock_code}", response_model=UpdateResponse)
async def trigger_update(stock_code: str) -> UpdateResponse:
    """触发增量更新"""
    import asyncio
    from investresearch.knowledge_base.updater import IncrementalUpdaterAgent
    from investresearch.core.models import AgentInput

    updater = IncrementalUpdaterAgent()
    output = await updater.safe_run(AgentInput(stock_code=stock_code))

    if output.status.value == "failed":
        return UpdateResponse(
            stock_code=stock_code,
            status="failed",
            errors=output.errors,
        )

    data = output.data
    return UpdateResponse(
        stock_code=stock_code,
        status="success",
        changes=data.get("changes", {}),
        duration_seconds=data.get("duration_seconds", 0),
    )


@router.post("/track", response_model=ApiResponse)
async def trigger_batch_track() -> ApiResponse:
    """触发批量动态跟踪"""
    import asyncio
    from investresearch.knowledge_base.tracker import DynamicTrackerAgent
    from investresearch.core.models import AgentInput

    tracker = DynamicTrackerAgent()
    output = await tracker.safe_run(AgentInput(stock_code="TRACK_ALL"))

    alerts = output.data.get("alerts", [])
    checked = output.data.get("checked_count", 0)

    return ApiResponse(
        success=True,
        message=f"跟踪完成: 检查 {checked} 个标的，发现 {len(alerts)} 个预警",
        data={"alerts": alerts, "checked_count": checked},
    )
