"""采集任务与执行 API"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..models.schemas import TaskCreate, TaskUpdate, TaskRead

router = APIRouter(tags=["采集管理"])


class CollectRequest(BaseModel):
    """一键采集请求"""
    stock_codes: list[str] = Field(description="股票代码列表")
    data_types: list[str] | None = Field(default=None, description="数据类型(空=全部)")
    preferred_source: str | None = Field(default=None, description="首选数据源")


class BatchCollectRequest(BaseModel):
    """批量采集请求"""
    stock_codes: list[str] = Field(description="股票代码列表", min_length=1)
    data_types: list[str] | None = Field(default=None)
    preferred_source: str | None = Field(default=None)


def _get_hub():
    from ..service import IntelligenceHub
    return IntelligenceHub()


# ================================================================
# 采集任务 CRUD
# ================================================================


@router.get("/tasks", response_model=list[TaskRead])
async def list_tasks():
    """列出所有采集任务"""
    hub = _get_hub()
    try:
        hub.initialize()
        return hub.list_tasks()
    finally:
        hub.close()


@router.post("/tasks", response_model=TaskRead)
async def create_task(data: TaskCreate):
    """创建采集任务"""
    hub = _get_hub()
    try:
        hub.initialize()
        return hub.create_task(data)
    finally:
        hub.close()


@router.get("/tasks/{task_id}", response_model=TaskRead)
async def get_task(task_id: int):
    """获取任务详情"""
    from ..repository import CollectionTaskRepository, get_session
    session = get_session()
    try:
        repo = CollectionTaskRepository(session)
        task = repo.get_by_id(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
        return TaskRead.model_validate(task)
    finally:
        session.close()


@router.put("/tasks/{task_id}", response_model=TaskRead)
async def update_task(task_id: int, data: TaskUpdate):
    """更新采集任务"""
    from ..repository import CollectionTaskRepository, get_session
    session = get_session()
    try:
        repo = CollectionTaskRepository(session)
        task = repo.update(task_id, data)
        if task is None:
            raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
        session.commit()
        return TaskRead.model_validate(task)
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()


@router.delete("/tasks/{task_id}")
async def delete_task(task_id: int):
    """删除采集任务"""
    from ..repository import CollectionTaskRepository, get_session
    session = get_session()
    try:
        repo = CollectionTaskRepository(session)
        if not repo.delete(task_id):
            raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
        session.commit()
        return {"message": f"任务 {task_id} 已删除"}
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()


@router.post("/tasks/{task_id}/run")
async def run_task(task_id: int):
    """手动触发执行采集任务"""
    hub = _get_hub()
    try:
        hub.initialize()
        results = hub.run_task(task_id)
        return {
            "task_id": task_id,
            "results": [
                {
                    "data_type": r.data_type,
                    "source": r.source_name,
                    "status": r.status,
                    "records_fetched": r.records_fetched,
                    "duration_ms": r.duration_ms,
                    "error": r.error,
                }
                for r in results
            ],
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    finally:
        hub.close()


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(task_id: int, limit: int = 50):
    """获取任务执行日志"""
    from ..repository import CollectionLogRepository, get_session
    session = get_session()
    try:
        repo = CollectionLogRepository(session)
        logs = repo.list_by_task(task_id, limit)
        return [
            {
                "id": log.id,
                "source_name": log.source_name,
                "data_type": log.data_type,
                "status": log.status,
                "records_fetched": log.records_fetched,
                "records_stored": log.records_stored,
                "error_message": log.error_message,
                "duration_ms": log.duration_ms,
                "started_at": str(log.started_at) if log.started_at else None,
                "completed_at": str(log.completed_at) if log.completed_at else None,
            }
            for log in logs
        ]
    finally:
        session.close()


# ================================================================
# 一键采集
# ================================================================


@router.post("/collect/stock/{code}")
async def collect_stock(code: str, data_types: list[str] | None = None):
    """一键采集指定股票全量数据"""
    hub = _get_hub()
    try:
        hub.initialize()
        results = hub.collect_stock(code, data_types)
        return {
            "stock_code": code,
            "results": [
                {
                    "data_type": r.data_type,
                    "source": r.source_name,
                    "status": r.status,
                    "records_fetched": r.records_fetched,
                    "duration_ms": r.duration_ms,
                    "error": r.error,
                }
                for r in results
            ],
            "success_count": sum(1 for r in results if r.status == "success"),
            "failed_count": sum(1 for r in results if r.status == "failed"),
        }
    finally:
        hub.close()


@router.post("/collect/batch")
async def batch_collect(request: BatchCollectRequest):
    """批量采集多只股票"""
    hub = _get_hub()
    try:
        hub.initialize()
        all_results: dict[str, Any] = {}
        total_success = 0
        total_failed = 0

        for code in request.stock_codes:
            results = hub.collect_stock(code, request.data_types)
            success = sum(1 for r in results if r.status == "success")
            failed = sum(1 for r in results if r.status == "failed")
            total_success += success
            total_failed += failed
            all_results[code] = {
                "success": success,
                "failed": failed,
                "types": [
                    {
                        "data_type": r.data_type,
                        "status": r.status,
                        "source": r.source_name,
                    }
                    for r in results
                ],
            }

        return {
            "stocks": all_results,
            "total_success": total_success,
            "total_failed": total_failed,
        }
    finally:
        hub.close()


@router.get("/logs")
async def get_recent_logs(target: str | None = None, limit: int = 50):
    """获取最近采集日志"""
    hub = _get_hub()
    try:
        hub.initialize()
        return hub.get_collection_logs(target, limit)
    finally:
        hub.close()
