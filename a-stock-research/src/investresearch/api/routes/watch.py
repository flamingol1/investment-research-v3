"""Watch list routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from investresearch.api.schemas import (
    ApiResponse,
    UpdateResponse,
    WatchAddRequest,
    WatchListItemResponse,
    WatchListResponse,
)
from investresearch.core.logging import get_logger
from investresearch.core.models import AgentInput, AgentStatus

logger = get_logger("api.routes.watch")

router = APIRouter(prefix="/api", tags=["watch"])


def _get_manager():
    from investresearch.knowledge_base.watch_list import WatchListManager

    return WatchListManager()


def _build_update_message(stock_code: str, changes: dict[str, int], errors: list[str], summary: str) -> str:
    if summary:
        return summary

    change_parts = []
    labels = {
        "new_prices": "行情",
        "new_financials": "财报",
        "new_valuation": "估值",
    }

    for key, value in changes.items():
        if value > 0:
            change_parts.append(f"{labels.get(key, key)} +{value}")

    if change_parts:
        message = f"{stock_code} 增量更新完成：{', '.join(change_parts)}"
    else:
        message = f"{stock_code} 数据已是最新，无需更新"

    if errors:
        message += f"；另有 {len(errors)} 项更新失败"

    return message


@router.get("/watch", response_model=WatchListResponse)
async def get_watch_list() -> WatchListResponse:
    """Get the current watch list."""
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
    """Add a stock into the watch list."""
    mgr = _get_manager()
    added = mgr.add(req.stock_code, stock_name=req.stock_name)
    if not added:
        return ApiResponse(success=True, message=f"{req.stock_code} 已在监控列表中")
    return ApiResponse(success=True, message=f"已添加 {req.stock_code} 到监控列表")


@router.delete("/watch/{stock_code}", response_model=ApiResponse)
async def remove_from_watch(stock_code: str) -> ApiResponse:
    """Remove a stock from the watch list."""
    mgr = _get_manager()
    removed = mgr.remove(stock_code)
    if not removed:
        raise HTTPException(status_code=404, detail=f"{stock_code} 不在监控列表中")
    return ApiResponse(success=True, message=f"已移除 {stock_code}")


@router.post("/update/{stock_code}", response_model=UpdateResponse)
async def trigger_update(stock_code: str) -> UpdateResponse:
    """Trigger an incremental update."""
    from investresearch.knowledge_base.updater import IncrementalUpdaterAgent

    updater = IncrementalUpdaterAgent()
    mgr = _get_manager()

    try:
        output = await updater.safe_run(AgentInput(stock_code=stock_code))
    except Exception as exc:
        logger.error(f"增量更新失败 {stock_code}: {exc}", exc_info=True)
        mgr.update_status(stock_code, "warning")
        mgr.update_last_checked(stock_code)
        mgr.save()
        return UpdateResponse(
            stock_code=stock_code,
            status="failed",
            message=f"增量更新失败: {exc}",
            errors=[str(exc)],
        )

    if output.status == AgentStatus.FAILED:
        mgr.update_status(stock_code, "warning")
        mgr.update_last_checked(stock_code)
        mgr.save()
        return UpdateResponse(
            stock_code=stock_code,
            status="failed",
            message=output.summary or f"{stock_code} 增量更新失败",
            errors=output.errors,
        )

    data = output.data
    changes = {
        key: value
        for key, value in data.get("changes", {}).items()
        if isinstance(value, int)
    }
    errors = list(output.errors)

    mgr.update_status(stock_code, "warning" if errors else "normal")
    mgr.update_last_checked(stock_code)
    mgr.save()

    return UpdateResponse(
        stock_code=stock_code,
        status="success",
        message=_build_update_message(stock_code, changes, errors, output.summary),
        changes=changes,
        duration_seconds=float(data.get("duration_seconds", 0.0) or 0.0),
        errors=errors,
    )


@router.post("/track", response_model=ApiResponse)
async def trigger_batch_track() -> ApiResponse:
    """Trigger batch dynamic tracking."""
    from investresearch.knowledge_base.tracker import DynamicTrackerAgent

    tracker = DynamicTrackerAgent()
    output = await tracker.safe_run(AgentInput(stock_code="TRACK_ALL"))

    alerts = output.data.get("alerts", [])
    checked = output.data.get("checked_count", 0)

    return ApiResponse(
        success=True,
        message=f"跟踪完成: 检查 {checked} 个标的，发现 {len(alerts)} 个预警",
        data={"alerts": alerts, "checked_count": checked},
    )
