"""研究相关路由 - 发起研究、查询状态、WebSocket进度"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException

from investresearch.api.deps import get_task_manager, get_progress_callback
from investresearch.api.schemas import (
    ResearchRequest,
    ResearchStatusResponse,
    ReportSummary,
    ReportDetailResponse,
    ApiResponse,
)
from investresearch.core.logging import get_logger

logger = get_logger("api.routes.research")

router = APIRouter(prefix="/api", tags=["research"])


@router.post("/research", response_model=ApiResponse)
async def start_research(req: ResearchRequest) -> ApiResponse:
    """发起研究任务（异步后台执行）"""
    mgr = get_task_manager()
    task_id = mgr.create_task(req.stock_code, req.depth)

    # 后台启动研究流程
    asyncio.create_task(_run_research_background(task_id, req.stock_code, req.depth))

    return ApiResponse(
        success=True,
        message="研究任务已启动",
        data={"task_id": task_id, "stock_code": req.stock_code, "depth": req.depth},
    )


@router.get("/research/{task_id}", response_model=ResearchStatusResponse)
async def get_research_status(task_id: str) -> ResearchStatusResponse:
    """查询研究任务状态"""
    mgr = get_task_manager()
    task = mgr.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")

    report_summary = None
    if task.get("report") and task["status"] == "completed":
        r = task["report"]
        report_summary = ReportSummary(
            stock_code=r.stock_code,
            stock_name=r.stock_name,
            report_date=r.report_date.strftime("%Y%m%d") if hasattr(r, "report_date") else "",
            depth=r.depth,
            recommendation=r.conclusion.recommendation if r.conclusion else "",
            risk_level=r.conclusion.risk_level if r.conclusion else "",
            target_price_low=r.conclusion.target_price_low if r.conclusion else None,
            target_price_high=r.conclusion.target_price_high if r.conclusion else None,
            current_price=r.conclusion.current_price if r.conclusion else None,
            upside_pct=r.conclusion.upside_pct if r.conclusion else None,
            has_full_report=bool(r.markdown),
        )

    started = task.get("started_at")
    completed = task.get("completed_at")

    return ResearchStatusResponse(
        task_id=task_id,
        stock_code=task["stock_code"],
        status=task["status"],
        progress=task["progress"],
        stage=task["stage"],
        current_agent=task.get("current_agent", ""),
        started_at=started if isinstance(started, datetime) else None,
        completed_at=completed if isinstance(completed, datetime) else None,
        report=report_summary,
        errors=task.get("errors", []),
    )


@router.websocket("/ws/research/{task_id}")
async def research_websocket(websocket: WebSocket, task_id: str) -> None:
    """WebSocket实时推送研究进度"""
    mgr = get_task_manager()
    task = mgr.get_task(task_id)

    await websocket.accept()

    if not task:
        await websocket.send_json({"error": f"任务 {task_id} 不存在"})
        await websocket.close()
        return

    queue = mgr.subscribe_ws(task_id)
    try:
        while True:
            # 发送当前状态
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=30.0)
                await websocket.send_json(msg)

                # 任务完成或失败时关闭连接
                if msg.get("status") in ("completed", "failed"):
                    break
            except asyncio.TimeoutError:
                # 心跳
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    finally:
        mgr.unsubscribe_ws(task_id, queue)


@router.get("/reports", response_model=list[ReportSummary])
async def list_reports() -> list[ReportSummary]:
    """列出所有研究报告"""
    reports_dir = Path("output/reports")
    if not reports_dir.exists():
        return []

    results: list[ReportSummary] = []

    # 扫描结论文件
    for f in sorted(reports_dir.glob("*_conclusion.json"), reverse=True):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            stock_code = f.stem.replace("_conclusion", "")
            date_str = ""

            # 查找对应的报告文件确定日期
            md_files = sorted(reports_dir.glob(f"{stock_code}_*.md"), reverse=True)
            if md_files:
                name_parts = md_files[0].stem.split("_")
                if len(name_parts) >= 2:
                    date_str = name_parts[-1]

            c = data.get("conclusion", data)  # 兼容不同结构
            results.append(ReportSummary(
                stock_code=stock_code,
                stock_name="",
                report_date=date_str,
                recommendation=c.get("recommendation", ""),
                risk_level=c.get("risk_level", ""),
                target_price_low=c.get("target_price_low"),
                target_price_high=c.get("target_price_high"),
                current_price=c.get("current_price"),
                upside_pct=c.get("upside_pct"),
                has_full_report=len(md_files) > 0,
            ))
        except Exception as e:
            logger.warning(f"解析结论文件失败 {f}: {e}")

    return results


@router.get("/reports/{stock_code}/{date}", response_model=ReportDetailResponse)
async def get_report(stock_code: str, date: str) -> ReportDetailResponse:
    """获取研究报告详情"""
    reports_dir = Path("output/reports")

    # 查找Markdown报告
    md_file = reports_dir / f"{stock_code}_{date}.md"
    markdown = ""
    if md_file.exists():
        markdown = md_file.read_text(encoding="utf-8")

    # 查找结论
    conclusion_file = reports_dir / f"{stock_code}_conclusion.json"
    conclusion = None
    if conclusion_file.exists():
        try:
            data = json.loads(conclusion_file.read_text(encoding="utf-8"))
            from investresearch.core.models import InvestmentConclusion
            conclusion = InvestmentConclusion(**data)
        except Exception as e:
            logger.warning(f"解析结论失败: {e}")

    return ReportDetailResponse(
        stock_code=stock_code,
        report_date=date,
        markdown=markdown,
        conclusion=conclusion,
    )


# ============================================================
# 后台任务
# ============================================================


async def _run_research_background(task_id: str, stock_code: str, depth: str) -> None:
    """后台执行研究流程"""
    mgr = get_task_manager()

    try:
        mgr.update_task(
            task_id,
            status="running",
            started_at=datetime.now(),
            stage="init",
            progress=0.0,
            last_message="初始化研究流程...",
        )

        from investresearch.decision_layer.coordinator import ResearchCoordinator

        progress_cb = get_progress_callback(task_id)
        coordinator = ResearchCoordinator(progress_callback=progress_cb)
        report = await coordinator.run_research(stock_code, depth=depth)

        # 保存输出文件
        out_path = Path("output/reports")
        out_path.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")

        if report.markdown:
            (out_path / f"{stock_code}_{date_str}.md").write_text(report.markdown, encoding="utf-8")
        if report.conclusion:
            c_data = report.conclusion.model_dump(mode="json")
            (out_path / f"{stock_code}_conclusion.json").write_text(
                json.dumps(c_data, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        mgr.update_task(
            task_id,
            status="completed",
            progress=1.0,
            stage="done",
            completed_at=datetime.now(),
            report=report,
            last_message="研究完成",
        )

    except Exception as e:
        logger.error(f"研究任务失败 {task_id}: {e}", exc_info=True)
        mgr.update_task(
            task_id,
            status="failed",
            stage="error",
            last_message=f"研究失败: {e}",
        )
        task = mgr.get_task(task_id)
        if task:
            task["errors"].append(str(e))
