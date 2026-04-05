"""依赖注入 - 单例管理共享资源"""

from __future__ import annotations

import asyncio
from typing import Any, Callable
from collections.abc import AsyncGenerator

from investresearch.core.config import Config
from investresearch.core.logging import get_logger
from investresearch.core.models import ResearchReport

logger = get_logger("api.deps")


class TaskManager:
    """管理后台研究任务"""

    def __init__(self) -> None:
        self._tasks: dict[str, dict[str, Any]] = {}
        self._counter = 0

    def create_task(self, stock_code: str, depth: str) -> str:
        """创建新任务，返回task_id"""
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
            "started_at": None,
            "completed_at": None,
            "report": None,
            "errors": [],
            "ws_queues": [],  # list of asyncio.Queue for WebSocket subscribers
        }
        return task_id

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        return self._tasks.get(task_id)

    def list_tasks(self) -> list[dict[str, Any]]:
        return list(self._tasks.values())

    def update_task(self, task_id: str, **kwargs: Any) -> None:
        task = self._tasks.get(task_id)
        if task:
            task.update(kwargs)
            # 通知所有WebSocket订阅者
            self._notify_ws(task_id)

    def subscribe_ws(self, task_id: str) -> asyncio.Queue:
        """WebSocket订阅，返回消息队列"""
        task = self._tasks.get(task_id)
        if not task:
            raise KeyError(f"Task {task_id} not found")
        queue: asyncio.Queue = asyncio.Queue()
        task["ws_queues"].append(queue)
        return queue

    def unsubscribe_ws(self, task_id: str, queue: asyncio.Queue) -> None:
        task = self._tasks.get(task_id)
        if task and queue in task["ws_queues"]:
            task["ws_queues"].remove(queue)

    def _notify_ws(self, task_id: str) -> None:
        task = self._tasks.get(task_id)
        if not task:
            return
        msg = {
            "stage": task.get("stage", ""),
            "agent": task.get("current_agent", ""),
            "status": task.get("status", "running"),
            "progress": task.get("progress", 0.0),
            "message": task.get("last_message", ""),
        }
        dead_queues = []
        for q in task["ws_queues"]:
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                dead_queues.append(q)
        for q in dead_queues:
            task["ws_queues"].remove(q)


# 全局单例
_task_manager: TaskManager | None = None
_config: Config | None = None


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


def get_progress_callback(task_id: str) -> Callable[[str, str], None]:
    """创建绑定到特定task_id的进度回调"""
    mgr = get_task_manager()

    def callback(step: str, message: str) -> None:
        # 从step推断进度
        stage_progress = {
            "init": 0.0,
            "数据采集": 0.1,
            "数据清洗": 0.2,
            "初筛": 0.3,
            "分析": 0.5,
            "报告生成": 0.85,
            "结论生成": 0.95,
            "完成": 1.0,
        }
        stage = step
        progress = 0.0
        for key, val in stage_progress.items():
            if key in step:
                progress = val
                break
        else:
            progress = 0.5

        mgr.update_task(
            task_id,
            stage=stage,
            last_message=message,
            current_agent=step,
            progress=progress,
        )

    return callback
