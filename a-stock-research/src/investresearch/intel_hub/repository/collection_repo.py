"""采集任务与日志 CRUD"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Sequence

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from ..models.db_models import CollectionTask, CollectionLog
from ..models.schemas import TaskCreate, TaskUpdate


class CollectionTaskRepository:
    """采集任务仓储"""

    def __init__(self, session: Session) -> None:
        self._session = session

    def list_all(self) -> Sequence[CollectionTask]:
        stmt = select(CollectionTask).order_by(CollectionTask.id.desc())
        return self._session.execute(stmt).scalars().all()

    def get_by_id(self, task_id: int) -> CollectionTask | None:
        return self._session.get(CollectionTask, task_id)

    def get_by_target(self, target: str) -> Sequence[CollectionTask]:
        stmt = select(CollectionTask).where(CollectionTask.target == target)
        return self._session.execute(stmt).scalars().all()

    def get_enabled(self) -> Sequence[CollectionTask]:
        stmt = (
            select(CollectionTask)
            .where(CollectionTask.enabled.is_(True))
            .order_by(CollectionTask.id)
        )
        return self._session.execute(stmt).scalars().all()

    def create(self, data: TaskCreate, source_id: int | None = None) -> CollectionTask:
        task = CollectionTask(
            name=data.name,
            task_type=data.task_type,
            target=data.target,
            schedule_type=data.schedule_type,
            schedule_expr=data.schedule_expr,
            enabled=data.enabled,
            source_id=source_id,
        )
        self._session.add(task)
        self._session.flush()
        return task

    def update(self, task_id: int, data: TaskUpdate) -> CollectionTask | None:
        task = self.get_by_id(task_id)
        if task is None:
            return None

        for field in ("name", "task_type", "schedule_type", "schedule_expr", "enabled"):
            val = getattr(data, field, None)
            if val is not None:
                setattr(task, field, val)

        task.updated_at = datetime.now()
        self._session.flush()
        return task

    def delete(self, task_id: int) -> bool:
        task = self.get_by_id(task_id)
        if task is None:
            return False
        self._session.delete(task)
        self._session.flush()
        return True

    def mark_running(self, task_id: int) -> None:
        task = self.get_by_id(task_id)
        if task:
            task.status = "running"
            task.last_run_at = datetime.now()
            self._session.flush()

    def mark_completed(self, task_id: int, success: bool) -> None:
        task = self.get_by_id(task_id)
        if task:
            task.status = "success" if success else "failed"
            if success:
                task.success_count = (task.success_count or 0) + 1
            else:
                task.fail_count = (task.fail_count or 0) + 1
            task.updated_at = datetime.now()
            self._session.flush()


class CollectionLogRepository:
    """采集日志仓储"""

    def __init__(self, session: Session) -> None:
        self._session = session

    def create(
        self,
        task_id: int | None,
        source_id: int | None,
        source_name: str,
        target: str,
        data_type: str,
        status: str = "running",
        records_fetched: int = 0,
        records_stored: int = 0,
        error_message: str = "",
        duration_ms: int = 0,
        raw_data_path: str = "",
    ) -> CollectionLog:
        log = CollectionLog(
            task_id=task_id,
            source_id=source_id,
            source_name=source_name,
            target=target,
            data_type=data_type,
            status=status,
            records_fetched=records_fetched,
            records_stored=records_stored,
            error_message=error_message,
            duration_ms=duration_ms,
            raw_data_path=raw_data_path,
        )
        self._session.add(log)
        self._session.flush()
        return log

    def complete_log(
        self, log_id: int, status: str, records_stored: int = 0, error: str = ""
    ) -> None:
        log = self._session.get(CollectionLog, log_id)
        if log:
            log.status = status
            log.records_stored = records_stored
            log.error_message = error
            log.completed_at = datetime.now()
            if log.started_at and log.completed_at:
                log.duration_ms = int(
                    (log.completed_at - log.started_at).total_seconds() * 1000
                )
            self._session.flush()

    def list_by_task(self, task_id: int, limit: int = 50) -> Sequence[CollectionLog]:
        stmt = (
            select(CollectionLog)
            .where(CollectionLog.task_id == task_id)
            .order_by(CollectionLog.started_at.desc())
            .limit(limit)
        )
        return self._session.execute(stmt).scalars().all()

    def list_by_target(self, target: str, limit: int = 50) -> Sequence[CollectionLog]:
        stmt = (
            select(CollectionLog)
            .where(CollectionLog.target == target)
            .order_by(CollectionLog.started_at.desc())
            .limit(limit)
        )
        return self._session.execute(stmt).scalars().all()

    def get_recent_stats(self, hours: int = 24) -> dict[str, int]:
        """获取最近 N 小时的采集统计"""
        cutoff = datetime.now() - timedelta(hours=hours)
        stmt = (
            select(CollectionLog.status, func.count(CollectionLog.id))
            .where(CollectionLog.started_at >= cutoff)
            .group_by(CollectionLog.status)
        )
        rows = self._session.execute(stmt).all()
        return {status: count for status, count in rows}
