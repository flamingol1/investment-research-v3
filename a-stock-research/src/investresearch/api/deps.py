"""Shared API dependencies and task state."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Callable

from investresearch.core.config import Config


_TRACKED_AGENT_STAGES = {
    "data_collector",
    "data_cleaner",
    "screener",
    "financial",
    "business_model",
    "industry",
    "governance",
    "valuation",
    "risk",
    "report",
    "conclusion",
}


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def _normalize_metric(metric: dict[str, Any]) -> dict[str, str]:
    return {
        "key": str(metric.get("key", "")),
        "label": str(metric.get("label", "")),
        "value": _stringify(metric.get("value", "")),
        "tone": str(metric.get("tone", "default")),
    }


def _normalize_metrics(metrics: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for metric in metrics or []:
        if not isinstance(metric, dict):
            continue
        normalized.append(_normalize_metric(metric))
    return normalized


def _normalize_detail(detail: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(detail, dict):
        return None

    normalized = {
        "headline": str(detail.get("headline", "")),
        "note": str(detail.get("note", "")),
        "metrics": _normalize_metrics(detail.get("metrics")),
        "bullets": [str(item) for item in detail.get("bullets", []) if item],
    }

    if not normalized["headline"] and not normalized["note"] and not normalized["metrics"] and not normalized["bullets"]:
        return None
    return normalized


def _normalize_agents(items: list[Any] | None) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for item in items or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


class TaskManager:
    """Manage background research tasks and websocket subscribers."""

    def __init__(self) -> None:
        self._tasks: dict[str, dict[str, Any]] = {}
        self._counter = 0
        self._event_counter = 0

    def create_task(self, stock_code: str, depth: str) -> str:
        self._counter += 1
        task_id = f"research_{self._counter:04d}"
        self._tasks[task_id] = {
            "task_id": task_id,
            "stock_code": stock_code,
            "depth": depth,
            "status": "pending",
            "progress": 0.0,
            "stage": "init",
            "current_agent": "",
            "last_message": "",
            "stage_detail": None,
            "data_summary": [],
            "recent_events": [],
            "completed_agents": [],
            "active_agents": [],
            "started_at": None,
            "completed_at": None,
            "report": None,
            "errors": [],
            "ws_subscribers": [],
        }
        return task_id

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        return self._tasks.get(task_id)

    def list_tasks(self) -> list[dict[str, Any]]:
        return list(self._tasks.values())

    def next_event_id(self) -> int:
        self._event_counter += 1
        return self._event_counter

    def build_event(
        self,
        *,
        stage: str,
        agent: str,
        status: str,
        message: str,
        detail: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "id": self.next_event_id(),
            "stage": stage,
            "agent": agent,
            "status": status,
            "message": message,
            "created_at": datetime.now().isoformat(),
            "detail": detail,
        }

    def update_task(self, task_id: str, *, event: dict[str, Any] | None = None, **kwargs: Any) -> None:
        task = self._tasks.get(task_id)
        if task is None:
            return

        task.update(kwargs)

        if event:
            task["recent_events"] = [*task.get("recent_events", []), event][-20:]

        self._notify_ws(task_id, event=event)

    def subscribe_ws(self, task_id: str) -> asyncio.Queue:
        task = self._tasks.get(task_id)
        if task is None:
            raise KeyError(f"Task {task_id} not found")

        queue: asyncio.Queue = asyncio.Queue()
        loop = asyncio.get_running_loop()
        task["ws_subscribers"].append((queue, loop))
        return queue

    def unsubscribe_ws(self, task_id: str, queue: asyncio.Queue) -> None:
        task = self._tasks.get(task_id)
        if task is None:
            return
        task["ws_subscribers"] = [
            (subscriber_queue, loop)
            for subscriber_queue, loop in task["ws_subscribers"]
            if subscriber_queue is not queue
        ]

    def serialize_task(self, task_id: str, *, event: dict[str, Any] | None = None) -> dict[str, Any]:
        task = self._tasks.get(task_id)
        if task is None:
            return {}

        return {
            "stage": task.get("stage", ""),
            "agent": task.get("current_agent", ""),
            "status": task.get("status", "running"),
            "progress": task.get("progress", 0.0),
            "message": task.get("last_message", ""),
            "stage_detail": task.get("stage_detail"),
            "data_summary": task.get("data_summary", []),
            "recent_events": task.get("recent_events", []),
            "completed_agents": task.get("completed_agents", []),
            "active_agents": task.get("active_agents", []),
            "event": event,
            "timestamp": datetime.now().isoformat(),
        }

    def _notify_ws(self, task_id: str, *, event: dict[str, Any] | None = None) -> None:
        task = self._tasks.get(task_id)
        if task is None:
            return

        msg = self.serialize_task(task_id, event=event)

        alive_subscribers: list[tuple[asyncio.Queue, asyncio.AbstractEventLoop]] = []
        for queue, loop in task["ws_subscribers"]:
            if loop.is_closed():
                continue
            try:
                loop.call_soon_threadsafe(queue.put_nowait, msg)
                alive_subscribers.append((queue, loop))
            except RuntimeError:
                continue

        task["ws_subscribers"] = alive_subscribers


_task_manager: TaskManager | None = None
_config: Config | None = None

_STAGE_PROGRESS: dict[str, float] = {
    "init": 0.02,
    "data_collector": 0.12,
    "data_cleaner": 0.24,
    "screener": 0.36,
    "analysis": 0.45,
    "financial": 0.58,
    "business_model": 0.64,
    "industry": 0.70,
    "governance": 0.76,
    "valuation": 0.82,
    "risk": 0.86,
    "report": 0.92,
    "conclusion": 0.97,
    "knowledge_base": 0.99,
    "done": 1.0,
    "error": 1.0,
}

_STAGE_ALIASES: dict[str, str] = {
    "collector": "data_collector",
    "cleaner": "data_cleaner",
    "screen": "screener",
    "analysis": "analysis",
    "financial": "financial",
    "business": "business_model",
    "industry": "industry",
    "governance": "governance",
    "valuation": "valuation",
    "risk": "risk",
    "report": "report",
    "conclusion": "conclusion",
    "knowledge": "knowledge_base",
    "done": "done",
    "failed": "error",
    "error": "error",
}


def get_task_manager() -> TaskManager:
    global _task_manager
    if _task_manager is None:
        _task_manager = TaskManager()
    return _task_manager


def get_config() -> Config:
    global _config
    if _config is None:
        _config = Config()
    return _config


def _resolve_stage(step: str, message: str) -> str:
    for text in (step, message):
        lowered = text.lower()
        for stage in _STAGE_PROGRESS:
            if stage in lowered:
                return stage

        for alias, stage in _STAGE_ALIASES.items():
            if alias in lowered:
                return stage

    return step


def get_progress_callback(task_id: str) -> Callable[..., None]:
    """Create a progress callback bound to a specific task."""

    mgr = get_task_manager()

    def callback(
        step: str,
        message: str,
        detail: dict[str, Any] | None = None,
        stage_status: str = "running",
    ) -> None:
        normalized_step = _resolve_stage(step, message)
        task = mgr.get_task(task_id) or {}
        previous_progress = float(task.get("progress", 0.0) or 0.0)
        progress = max(previous_progress, _STAGE_PROGRESS.get(normalized_step, previous_progress))

        normalized_detail = _normalize_detail(detail)
        stage_detail = normalized_detail or {
            "headline": message,
            "note": "",
            "metrics": [],
            "bullets": [],
        }

        data_summary = task.get("data_summary", [])
        if isinstance(detail, dict) and "data_summary" in detail:
            data_summary = _normalize_metrics(detail.get("data_summary"))

        completed_agents = _normalize_agents(task.get("completed_agents"))
        if isinstance(detail, dict) and "completed_agents" in detail:
            completed_agents = _normalize_agents(detail.get("completed_agents"))
        elif stage_status == "completed" and normalized_step in _TRACKED_AGENT_STAGES:
            completed_agents = _normalize_agents([*completed_agents, normalized_step])

        active_agents = _normalize_agents(task.get("active_agents"))
        if isinstance(detail, dict) and "active_agents" in detail:
            active_agents = _normalize_agents(detail.get("active_agents"))
        elif stage_status in {"completed", "failed"} and normalized_step in active_agents:
            active_agents = [agent for agent in active_agents if agent != normalized_step]

        current_agent = normalized_step
        if isinstance(detail, dict) and detail.get("current_agent"):
            current_agent = str(detail.get("current_agent"))
        elif active_agents and normalized_step in _TRACKED_AGENT_STAGES:
            current_agent = "analysis"

        event = mgr.build_event(
            stage=normalized_step,
            agent=current_agent,
            status=stage_status,
            message=message,
            detail=normalized_detail,
        )

        mgr.update_task(
            task_id,
            stage=normalized_step,
            last_message=message,
            current_agent=current_agent,
            progress=progress,
            stage_detail=stage_detail,
            data_summary=data_summary,
            completed_agents=completed_agents,
            active_agents=active_agents,
            event=event,
        )

    return callback
