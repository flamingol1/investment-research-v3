"""采集进度管理 — 内存级进度事件存储，用于 SSE 流式推送"""

from __future__ import annotations

import asyncio
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from investresearch.core.logging import get_logger

logger = get_logger("intel_hub.progress")

_MAX_ACTIVE_SESSIONS = 100
_STALE_TTL_SECONDS = 300


@dataclass
class CollectSession:
    """一次采集会话的状态"""

    collect_id: str
    stock_code: str
    status: str  # pending | running | completed | failed
    total_steps: int
    current_step: int = 0
    results: list[dict[str, Any]] = field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    event_queues: list[asyncio.Queue] = field(default_factory=list)


class CollectionProgressStore:
    """采集进度存储 (单例)

    管理所有活跃的采集会话，支持 SSE 订阅。
    线程安全: 通过 threading.Lock 保护 _sessions 访问，
    emit_event 从 run_in_executor 线程调用，
    通过 loop.call_soon_threadsafe 跨线程推送事件。
    """

    def __init__(self) -> None:
        self._sessions: dict[str, CollectSession] = {}
        self._lock = threading.Lock()

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """获取当前运行的事件循环（惰性解析）"""
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
            return loop

    def create_session(self, stock_code: str, total_steps: int) -> str:
        """创建采集会话，返回 collect_id"""
        collect_id = uuid.uuid4().hex[:12]
        session = CollectSession(
            collect_id=collect_id,
            stock_code=stock_code,
            status="pending",
            total_steps=total_steps,
            started_at=datetime.now(),
        )

        with self._lock:
            self._cleanup_stale_locked()
            if len(self._sessions) >= _MAX_ACTIVE_SESSIONS:
                raise RuntimeError(
                    f"Too many active sessions (max {_MAX_ACTIVE_SESSIONS})"
                )
            self._sessions[collect_id] = session

        logger.info(f"创建采集会话 | {collect_id} | {stock_code} | {total_steps}步")
        return collect_id

    def get_session(self, collect_id: str) -> CollectSession | None:
        with self._lock:
            return self._sessions.get(collect_id)

    def subscribe(self, collect_id: str) -> asyncio.Queue | None:
        """SSE 端点调用，获取事件队列"""
        with self._lock:
            session = self._sessions.get(collect_id)
            if not session:
                return None
            queue: asyncio.Queue = asyncio.Queue()
            session.event_queues.append(queue)
            return queue

    def unsubscribe(self, collect_id: str, queue: asyncio.Queue) -> None:
        """SSE 连接关闭时清理"""
        with self._lock:
            session = self._sessions.get(collect_id)
            if session and queue in session.event_queues:
                session.event_queues.remove(queue)

    def emit_event(self, collect_id: str, event_type: str, payload: dict[str, Any]) -> None:
        """向所有订阅者推送事件 (线程安全)"""
        with self._lock:
            session = self._sessions.get(collect_id)
            if not session:
                return

            event = {"event": event_type, "collect_id": collect_id, **payload}

            # 更新会话状态
            if event_type == "step_complete":
                session.current_step += 1
                session.results.append(payload)
            elif event_type == "done":
                session.status = "completed"
                session.completed_at = datetime.now()
            elif event_type == "error":
                session.status = "failed"
                session.completed_at = datetime.now()
            elif event_type == "started":
                session.status = "running"

            # 推送到所有订阅者队列
            loop = self._get_loop()
            dead: list[asyncio.Queue] = []
            for q in session.event_queues:
                try:
                    loop.call_soon_threadsafe(q.put_nowait, event)
                except Exception:
                    dead.append(q)
            for q in dead:
                session.event_queues.remove(q)

    def create_progress_callback(self, collect_id: str) -> Callable[[str, dict[str, Any]], None]:
        """创建供 CollectionEngine 调用的进度回调"""

        def on_progress(event_type: str, detail: dict[str, Any]) -> None:
            self.emit_event(collect_id, event_type, detail)

        return on_progress

    def _cleanup_stale_locked(self) -> None:
        """清理超过 5 分钟的已完成会话 (调用方必须持有 self._lock)"""
        now = datetime.now()
        stale_ids = [
            cid
            for cid, session in self._sessions.items()
            if (
                session.status in ("completed", "failed")
                and session.completed_at
                and (now - session.completed_at).total_seconds() > _STALE_TTL_SECONDS
            )
        ]
        for cid in stale_ids:
            del self._sessions[cid]


# ================================================================
# 全局单例 (线程安全)
# ================================================================

_progress_store: CollectionProgressStore | None = None
_store_lock = threading.Lock()


def get_progress_store() -> CollectionProgressStore:
    global _progress_store
    if _progress_store is None:
        with _store_lock:
            if _progress_store is None:
                _progress_store = CollectionProgressStore()
    return _progress_store
