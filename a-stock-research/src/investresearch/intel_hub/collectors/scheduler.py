"""采集调度器 - APScheduler 定时采集 + 健康检测"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from ..models.db_models import CollectionTask
from ..repository.collection_repo import CollectionTaskRepository, CollectionLogRepository
from ..repository.source_repo import SourceRepository
from ..sources.registry import SourceRegistry

from investresearch.core.logging import get_logger

logger = get_logger("intel_hub.scheduler")


class IntelScheduler:
    """情报中心调度器

    功能:
    1. 从数据库加载启用的定时任务
    2. 按计划触发采集
    3. 定期执行数据源健康检查
    4. 记录采集结果和监控数据
    """

    def __init__(
        self,
        session_factory: Any,
        registry: SourceRegistry,
    ) -> None:
        self._session_factory = session_factory
        self._registry = registry
        self._scheduler = None
        self._running = False

    def start(self) -> None:
        """启动调度器"""
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.interval import IntervalTrigger
        except ImportError:
            logger.warning("APScheduler 未安装，调度器不可用。安装: pip install apscheduler")
            return

        self._scheduler = BackgroundScheduler()

        # 加载启用的定时任务
        self._load_scheduled_tasks()

        # 健康检查：每30分钟
        self._scheduler.add_job(
            self._health_check_job,
            trigger=IntervalTrigger(minutes=30),
            id="health_check",
            name="数据源健康检查",
            replace_existing=True,
        )

        self._scheduler.start()
        self._running = True
        logger.info("调度器已启动")

    def stop(self) -> None:
        """停止调度器"""
        if self._scheduler and self._running:
            self._scheduler.shutdown(wait=False)
            self._running = False
            logger.info("调度器已停止")

    def _load_scheduled_tasks(self) -> None:
        """从数据库加载定时任务并注册到调度器"""
        if self._scheduler is None:
            return

        session = self._session_factory()
        try:
            repo = CollectionTaskRepository(session)
            tasks = repo.get_enabled()

            for task in tasks:
                if task.schedule_type == "interval" and task.schedule_expr:
                    try:
                        interval_seconds = int(task.schedule_expr)
                        self._scheduler.add_job(
                            self._run_task_job,
                            trigger=IntervalTrigger(seconds=interval_seconds),
                            id=f"task_{task.id}",
                            name=task.name,
                            args=[task.id],
                            replace_existing=True,
                        )
                        logger.info(f"注册定时任务: {task.name} (间隔 {interval_seconds}s)")
                    except (ValueError, Exception) as e:
                        logger.warning(f"注册任务 {task.name} 失败: {e}")

                elif task.schedule_type == "cron" and task.schedule_expr:
                    try:
                        from apscheduler.triggers.cron import CronTrigger
                        parts = task.schedule_expr.split()
                        if len(parts) == 5:
                            trigger = CronTrigger(
                                minute=parts[0],
                                hour=parts[1],
                                day=parts[2],
                                month=parts[3],
                                day_of_week=parts[4],
                            )
                            self._scheduler.add_job(
                                self._run_task_job,
                                trigger=trigger,
                                id=f"task_{task.id}",
                                name=task.name,
                                args=[task.id],
                                replace_existing=True,
                            )
                            logger.info(f"注册Cron任务: {task.name} ({task.schedule_expr})")
                    except Exception as e:
                        logger.warning(f"注册Cron任务 {task.name} 失败: {e}")
        finally:
            session.close()

    def _run_task_job(self, task_id: int) -> None:
        """执行定时采集任务"""
        session = self._session_factory()
        try:
            from .engine import CollectionEngine
            engine = CollectionEngine(session, self._registry)
            results = engine.run_task(task_id)
            success = sum(1 for r in results if r.status == "success")
            failed = sum(1 for r in results if r.status == "failed")
            logger.info(f"定时任务 {task_id} 完成: {success} 成功, {failed} 失败")
        except Exception as e:
            logger.error(f"定时任务 {task_id} 执行失败: {e}")
        finally:
            session.close()

    def _health_check_job(self) -> None:
        """执行所有数据源健康检查"""
        session = self._session_factory()
        try:
            repo = SourceRepository(session)
            health_results = self._registry.health_check_all()

            for name, health in health_results.items():
                repo.update_health(name, health.status, health.error or "")
                if health.status != "healthy":
                    logger.warning(f"数据源 {name} 状态: {health.status} - {health.error}")

            session.commit()
            healthy = sum(1 for h in health_results.values() if h.status == "healthy")
            logger.info(f"健康检查完成: {healthy}/{len(health_results)} 正常")
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
        finally:
            session.close()

    def get_status(self) -> dict[str, Any]:
        """获取调度器状态"""
        if self._scheduler is None:
            return {"status": "not_installed", "jobs": []}

        jobs = []
        for job in self._scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time) if job.next_run_time else None,
            })

        return {
            "status": "running" if self._running else "stopped",
            "jobs": jobs,
        }

    def reload_tasks(self) -> None:
        """重新加载定时任务"""
        if self._scheduler is None:
            return

        # 移除所有已有任务
        for job in self._scheduler.get_jobs():
            if job.id != "health_check":
                self._scheduler.remove_job(job.id)

        # 重新加载
        self._load_scheduled_tasks()
        logger.info("定时任务已重新加载")
