"""采集流式 API — SSE 实时进度推送"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from investresearch.core.logging import get_logger

logger = get_logger("intel_hub.collection_stream_api")

router = APIRouter(tags=["采集流式"])


# ================================================================
# 后台采集执行函数 (在线程池中运行)
# ================================================================


def _run_collect_stock_background(
    collect_id: str,
    stock_code: str,
    data_types: list[str] | None,
    store: Any,
) -> None:
    """线程池中执行一键采集，通过 store 推送进度"""
    from ..service import IntelligenceHub

    hub = IntelligenceHub()
    try:
        hub.initialize()
        callback = store.create_progress_callback(collect_id)
        hub.collect_stock_streaming(stock_code, callback, data_types)
    except Exception as e:
        logger.error(f"采集后台任务异常: {e}")
        store.emit_event(collect_id, "error", {"error": str(e)})
    finally:
        hub.close()


def _run_task_background(
    collect_id: str,
    task_id: int,
    store: Any,
) -> None:
    """线程池中执行任务，通过 store 推送进度"""
    from ..service import IntelligenceHub

    hub = IntelligenceHub()
    try:
        hub.initialize()
        callback = store.create_progress_callback(collect_id)
        hub.run_task_streaming(task_id, callback)
    except Exception as e:
        logger.error(f"任务后台执行异常: {e}")
        store.emit_event(collect_id, "error", {"error": str(e)})
    finally:
        hub.close()


def _on_executor_done(future: asyncio.Future) -> None:
    """观察 run_in_executor 结果，记录未捕获异常"""
    exc = future.exception()
    if exc:
        logger.error(f"后台执行器任务异常: {exc}", exc_info=exc)


# ================================================================
# SSE 事件生成器
# ================================================================


async def _sse_event_generator(collect_id: str, store: Any) -> AsyncGenerator[str, None]:
    """生成 SSE 事件流"""
    session = store.get_session(collect_id)
    if not session:
        yield f"event: error\ndata: {json.dumps({'error': '会话不存在'})}\n\n"
        return

    queue = store.subscribe(collect_id)

    try:
        # 先回放已完成的步骤
        for result in session.results:
            yield f"event: step_complete\ndata: {json.dumps(result)}\n\n"

        # 如果已经结束，直接发送完成/错误事件
        if session.status == "completed":
            yield f"event: done\ndata: {json.dumps({'replay': True})}\n\n"
            return
        if session.status == "failed":
            yield f"event: error\ndata: {json.dumps({'replay': True})}\n\n"
            return

        # 流式推送实时事件
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
                event_type = event.get("event", "message")
                # 序列化时排除 event 字段
                payload = {k: v for k, v in event.items() if k != "event"}
                yield f"event: {event_type}\ndata: {json.dumps(payload)}\n\n"

                if event_type in ("done", "error"):
                    break
            except asyncio.TimeoutError:
                # 心跳保活
                yield f"event: heartbeat\ndata: {json.dumps({'ts': datetime.now(timezone.utc).isoformat()})}\n\n"

    except asyncio.CancelledError:
        return
    finally:
        store.unsubscribe(collect_id, queue)


# ================================================================
# API 端点
# ================================================================


@router.post("/collect/stock/{code}/stream")
async def collect_stock_stream(code: str, data_types: list[str] | None = None):
    """一键采集 — 流式版，立即返回 collect_id"""
    from ..collectors.progress import get_progress_store
    from ..collectors.tasks import ALL_DATA_TYPES

    store = get_progress_store()
    types_ = data_types or ALL_DATA_TYPES
    collect_id = store.create_session(code, len(types_))

    # 后台启动采集，并观察 Future
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(
        None,
        _run_collect_stock_background,
        collect_id,
        code,
        data_types,
        store,
    )
    future.add_done_callback(_on_executor_done)

    return {
        "collect_id": collect_id,
        "stock_code": code,
        "total_steps": len(types_),
    }


@router.post("/tasks/{task_id}/stream")
async def run_task_stream(task_id: int):
    """执行任务 — 流式版，立即返回 collect_id"""
    from ..collectors.progress import get_progress_store
    from ..collectors.tasks import ALL_DATA_TYPES
    from ..repository import CollectionTaskRepository, get_session

    # 检查任务是否存在且未运行
    db_session = get_session()
    try:
        repo = CollectionTaskRepository(db_session)
        task = repo.get_by_id(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
        if task.status == "running":
            raise HTTPException(status_code=409, detail=f"任务正在运行中: {task_id}")

        data_types = [task.task_type] if task.task_type != "all" else list(ALL_DATA_TYPES)
    finally:
        db_session.close()

    store = get_progress_store()
    collect_id = store.create_session(task.target, len(data_types))

    # 后台启动任务，并观察 Future
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(
        None,
        _run_task_background,
        collect_id,
        task_id,
        store,
    )
    future.add_done_callback(_on_executor_done)

    return {
        "collect_id": collect_id,
        "stock_code": task.target,
        "total_steps": len(data_types),
    }


@router.get("/collect/progress/{collect_id}/stream")
async def collection_progress_sse(collect_id: str):
    """SSE 端点 — 推送采集进度事件流"""
    from ..collectors.progress import get_progress_store

    store = get_progress_store()

    return StreamingResponse(
        _sse_event_generator(collect_id, store),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
