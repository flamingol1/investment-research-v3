"""Research routes."""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect

from investresearch.api.deps import get_progress_callback, get_task_manager
from investresearch.api.schemas import (
    ApiResponse,
    ReportDetailResponse,
    ReportSummary,
    ResearchRequest,
    ResearchStatusResponse,
)
from investresearch.core.logging import get_logger
from investresearch.core.models import InvestmentConclusion, ResearchReport

logger = get_logger("api.routes.research")

router = APIRouter(prefix="/api", tags=["research"])

REPORTS_DIR = Path("output/reports")
MARKDOWN_NAME_RE = re.compile(r"^(?P<stock>.+)_(?P<date>\d{8})$")
JSON_NAME_RE = re.compile(r"^(?P<stock>.+)_(?P<date>\d{8})_(?P<kind>conclusion|meta|chart_pack|evidence_pack)$")


def _report_paths(stock_code: str, report_date: str) -> dict[str, Path]:
    return {
        "markdown": REPORTS_DIR / f"{stock_code}_{report_date}.md",
        "conclusion": REPORTS_DIR / f"{stock_code}_{report_date}_conclusion.json",
        "meta": REPORTS_DIR / f"{stock_code}_{report_date}_meta.json",
        "chart_pack": REPORTS_DIR / f"{stock_code}_{report_date}_chart_pack.json",
        "evidence_pack": REPORTS_DIR / f"{stock_code}_{report_date}_evidence_pack.json",
        "latest_conclusion": REPORTS_DIR / f"{stock_code}_conclusion.json",
        "latest_meta": REPORTS_DIR / f"{stock_code}_meta.json",
        "latest_chart_pack": REPORTS_DIR / f"{stock_code}_chart_pack.json",
        "latest_evidence_pack": REPORTS_DIR / f"{stock_code}_evidence_pack.json",
    }


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning(f"Failed to parse JSON file {path}: {exc}")
        return None


def _load_meta(stock_code: str, report_date: str) -> dict[str, Any]:
    paths = _report_paths(stock_code, report_date)
    return _load_json(paths["meta"]) or _load_json(paths["latest_meta"]) or {}


def _load_conclusion(stock_code: str, report_date: str) -> InvestmentConclusion | None:
    paths = _report_paths(stock_code, report_date)
    raw = _load_json(paths["conclusion"]) or _load_json(paths["latest_conclusion"])
    if not raw:
        return None
    try:
        payload = raw.get("conclusion", raw)
        return InvestmentConclusion(**payload)
    except Exception as exc:
        logger.warning(f"Failed to build conclusion model for {stock_code}/{report_date}: {exc}")
        return None


def _load_pack(stock_code: str, report_date: str, kind: str) -> list[dict[str, Any]]:
    paths = _report_paths(stock_code, report_date)
    latest_key = f"latest_{kind}"
    raw = _load_json(paths[kind]) or _load_json(paths[latest_key]) or []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return list(raw.get(kind, []))
    return []


def _build_report_summary(
    stock_code: str,
    report_date: str,
    *,
    meta: dict[str, Any] | None = None,
    conclusion: InvestmentConclusion | None = None,
    has_full_report: bool = False,
) -> ReportSummary:
    meta = meta or {}
    return ReportSummary(
        stock_code=stock_code,
        stock_name=meta.get("stock_name", ""),
        report_date=report_date,
        depth=meta.get("depth", "standard"),
        recommendation=conclusion.recommendation if conclusion else "",
        risk_level=conclusion.risk_level if conclusion else "",
        target_price_low=conclusion.target_price_low if conclusion else None,
        target_price_high=conclusion.target_price_high if conclusion else None,
        current_price=conclusion.current_price if conclusion else None,
        upside_pct=conclusion.upside_pct if conclusion else None,
        has_full_report=has_full_report,
        agents_completed=list(meta.get("agents_completed", [])),
    )


def _report_has_material_output(report: ResearchReport) -> bool:
    return bool(report.markdown) or report.conclusion is not None


def _report_failed(report: ResearchReport) -> bool:
    return bool(report.errors) and not _report_has_material_output(report)


def _iter_saved_reports() -> list[tuple[str, str]]:
    if not REPORTS_DIR.exists():
        return []

    keys: set[tuple[str, str]] = set()

    for path in REPORTS_DIR.glob("*.md"):
        match = MARKDOWN_NAME_RE.match(path.stem)
        if match:
            keys.add((match.group("stock"), match.group("date")))

    for path in REPORTS_DIR.glob("*.json"):
        match = JSON_NAME_RE.match(path.stem)
        if match:
            keys.add((match.group("stock"), match.group("date")))

    return sorted(keys, key=lambda item: (item[1], item[0]), reverse=True)


def _run_research_sync(task_id: str, stock_code: str, depth: str) -> ResearchReport:
    from investresearch.decision_layer.coordinator import ResearchCoordinator

    progress_cb = get_progress_callback(task_id)
    coordinator = ResearchCoordinator(progress_callback=progress_cb)
    return asyncio.run(coordinator.run_research(stock_code, depth=depth))


@router.post("/research", response_model=ApiResponse)
async def start_research(req: ResearchRequest) -> ApiResponse:
    """Start an async research task."""
    mgr = get_task_manager()
    task_id = mgr.create_task(req.stock_code, req.depth)
    asyncio.create_task(_run_research_background(task_id, req.stock_code, req.depth))
    return ApiResponse(
        success=True,
        message="研究任务已启动",
        data={"task_id": task_id, "stock_code": req.stock_code, "depth": req.depth},
    )


@router.get("/research/{task_id}", response_model=ResearchStatusResponse)
async def get_research_status(task_id: str) -> ResearchStatusResponse:
    """Get the latest state for a research task."""
    mgr = get_task_manager()
    task = mgr.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")

    report_summary = None
    report = task.get("report")
    if isinstance(report, ResearchReport) and _report_has_material_output(report):
        report_summary = _build_report_summary(
            report.stock_code,
            report.report_date.strftime("%Y%m%d"),
            meta={
                "stock_name": report.stock_name,
                "depth": report.depth,
                "agents_completed": report.agents_completed,
            },
            conclusion=report.conclusion,
            has_full_report=bool(report.markdown),
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
        message=task.get("last_message", ""),
        stage_detail=task.get("stage_detail"),
        data_summary=task.get("data_summary", []),
        recent_events=task.get("recent_events", []),
        completed_agents=task.get("completed_agents", []),
        active_agents=task.get("active_agents", []),
        started_at=started if isinstance(started, datetime) else None,
        completed_at=completed if isinstance(completed, datetime) else None,
        report=report_summary,
        errors=task.get("errors", []),
    )


@router.websocket("/ws/research/{task_id}")
async def research_websocket(websocket: WebSocket, task_id: str) -> None:
    """Stream research progress updates."""
    mgr = get_task_manager()
    task = mgr.get_task(task_id)

    await websocket.accept()

    if not task:
        await websocket.send_json({"error": f"任务 {task_id} 不存在"})
        await websocket.close()
        return

    queue = mgr.subscribe_ws(task_id)
    try:
        await websocket.send_json(mgr.serialize_task(task_id))
        if task.get("status") in ("completed", "failed"):
            return

        while True:
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=30.0)
                await websocket.send_json(msg)
                if msg.get("status") in ("completed", "failed"):
                    break
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    finally:
        mgr.unsubscribe_ws(task_id, queue)


@router.get("/reports", response_model=list[ReportSummary])
async def list_reports() -> list[ReportSummary]:
    """List saved reports."""
    summaries: list[ReportSummary] = []
    for stock_code, report_date in _iter_saved_reports():
        paths = _report_paths(stock_code, report_date)
        meta = _load_meta(stock_code, report_date)
        conclusion = _load_conclusion(stock_code, report_date)
        summaries.append(
            _build_report_summary(
                stock_code,
                report_date,
                meta=meta,
                conclusion=conclusion,
                has_full_report=paths["markdown"].exists(),
            )
        )
    return summaries


@router.get("/reports/{stock_code}/{date}", response_model=ReportDetailResponse)
async def get_report(stock_code: str, date: str) -> ReportDetailResponse:
    """Get a saved report by stock code and date."""
    paths = _report_paths(stock_code, date)
    markdown = paths["markdown"].read_text(encoding="utf-8") if paths["markdown"].exists() else ""
    meta = _load_meta(stock_code, date)
    conclusion = _load_conclusion(stock_code, date)

    if not markdown and conclusion is None and not meta:
        raise HTTPException(status_code=404, detail=f"报告 {stock_code}/{date} 不存在")

    return ReportDetailResponse(
        stock_code=stock_code,
        stock_name=meta.get("stock_name", ""),
        report_date=date,
        depth=meta.get("depth", "standard"),
        markdown=markdown,
        conclusion=conclusion,
        chart_pack=_load_pack(stock_code, date, "chart_pack"),
        evidence_pack=_load_pack(stock_code, date, "evidence_pack"),
        agents_completed=list(meta.get("agents_completed", [])),
        agents_skipped=list(meta.get("agents_skipped", [])),
        errors=list(meta.get("errors", [])),
    )


async def _run_research_background(task_id: str, stock_code: str, depth: str) -> None:
    """Run the full research pipeline in the background."""
    mgr = get_task_manager()

    try:
        mgr.update_task(
            task_id,
            status="running",
            started_at=datetime.now(),
            stage="init",
            progress=0.0,
            current_agent="init",
            last_message="初始化研究流程...",
            stage_detail={
                "headline": "准备启动研究任务",
                "note": f"标的 {stock_code}，研究深度 {depth}",
                "metrics": [],
                "bullets": [],
            },
        )

        report = await asyncio.to_thread(_run_research_sync, task_id, stock_code, depth)

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        report_date = report.report_date.strftime("%Y%m%d")
        paths = _report_paths(stock_code, report_date)

        if report.markdown:
            paths["markdown"].write_text(report.markdown, encoding="utf-8")

        if report.conclusion:
            conclusion_payload = json.dumps(
                report.conclusion.model_dump(mode="json"),
                ensure_ascii=False,
                indent=2,
            )
            paths["conclusion"].write_text(conclusion_payload, encoding="utf-8")
            paths["latest_conclusion"].write_text(conclusion_payload, encoding="utf-8")

        if report.chart_pack:
            chart_payload = json.dumps(
                [item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in report.chart_pack],
                ensure_ascii=False,
                indent=2,
            )
            paths["chart_pack"].write_text(chart_payload, encoding="utf-8")
            paths["latest_chart_pack"].write_text(chart_payload, encoding="utf-8")

        if report.evidence_pack:
            evidence_payload = json.dumps(
                [item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in report.evidence_pack],
                ensure_ascii=False,
                indent=2,
            )
            paths["evidence_pack"].write_text(evidence_payload, encoding="utf-8")
            paths["latest_evidence_pack"].write_text(evidence_payload, encoding="utf-8")

        meta_payload = json.dumps(
            {
                "stock_code": report.stock_code,
                "stock_name": report.stock_name,
                "report_date": report_date,
                "depth": report.depth,
                "chart_pack_count": len(report.chart_pack),
                "evidence_pack_count": len(report.evidence_pack),
                "agents_completed": report.agents_completed,
                "agents_skipped": report.agents_skipped,
                "errors": report.errors,
            },
            ensure_ascii=False,
            indent=2,
        )
        paths["meta"].write_text(meta_payload, encoding="utf-8")
        paths["latest_meta"].write_text(meta_payload, encoding="utf-8")

        if _report_failed(report):
            mgr.update_task(
                task_id,
                status="failed",
                stage="error",
                current_agent="error",
                completed_at=datetime.now(),
                report=report,
                errors=report.errors,
                last_message=f"研究失败: {report.errors[0]}",
                stage_detail={
                    "headline": "研究流程中断",
                    "note": "后端未产出可用报告",
                    "metrics": [],
                    "bullets": report.errors[:5],
                },
            )
            return

        mgr.update_task(
            task_id,
            status="completed",
            progress=1.0,
            stage="done",
            current_agent="done",
            completed_at=datetime.now(),
            report=report,
            errors=report.errors,
            last_message="研究完成",
            completed_agents=report.agents_completed,
            active_agents=[],
        )
    except Exception as exc:
        logger.error(f"研究任务失败 {task_id}: {exc}", exc_info=True)
        task = mgr.get_task(task_id)
        errors = list(task.get("errors", [])) if task else []
        errors.append(str(exc))
        mgr.update_task(
            task_id,
            status="failed",
            stage="error",
            current_agent="error",
            completed_at=datetime.now(),
            errors=errors,
            last_message=f"研究失败: {exc}",
            stage_detail={
                "headline": "后台任务异常退出",
                "note": "请查看错误信息后重试",
                "metrics": [],
                "bullets": [str(exc)],
            },
        )
