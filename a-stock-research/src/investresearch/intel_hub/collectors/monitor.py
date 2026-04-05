"""采集监控 - 健康检测、采集成功率统计、异常告警"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from ..models.db_models import CollectionLog, IntelSource
from ..repository.collection_repo import CollectionLogRepository
from ..repository.source_repo import SourceRepository

from investresearch.core.logging import get_logger

logger = get_logger("intel_hub.monitor")


class CollectionMonitor:
    """采集监控器

    提供:
    1. 采集统计 (成功率、耗时、数据量)
    2. 数据源状态汇总
    3. 异常检测与告警
    """

    def __init__(self, session: Session) -> None:
        self._session = session
        self._log_repo = CollectionLogRepository(session)
        self._source_repo = SourceRepository(session)

    def get_dashboard(self) -> dict[str, Any]:
        """获取监控仪表盘数据"""
        stats_24h = self._log_repo.get_recent_stats(hours=24)
        stats_7d = self._log_repo.get_recent_stats(hours=168)

        # 数据源状态
        sources = self._source_repo.list_all()
        source_status = [
            {
                "name": s.name,
                "display_name": s.display_name,
                "enabled": s.enabled,
                "health": s.health_status,
                "last_check": str(s.last_health_check) if s.last_health_check else None,
                "last_error": s.last_error,
            }
            for s in sources
        ]

        return {
            "sources": source_status,
            "stats_24h": stats_24h,
            "stats_7d": stats_7d,
            "total_success_24h": stats_24h.get("success", 0),
            "total_failed_24h": stats_24h.get("failed", 0),
            "total_partial_24h": stats_24h.get("partial", 0),
        }

    def get_source_health(self) -> list[dict[str, Any]]:
        """获取所有数据源健康状态"""
        sources = self._source_repo.list_all()
        return [
            {
                "name": s.name,
                "display_name": s.display_name,
                "health_status": s.health_status,
                "last_health_check": str(s.last_health_check) if s.last_health_check else None,
                "last_error": s.last_error,
                "enabled": s.enabled,
            }
            for s in sources
        ]

    def get_collection_stats(self, hours: int = 24) -> dict[str, Any]:
        """获取采集统计"""
        stats = self._log_repo.get_recent_stats(hours=hours)

        # 计算总采集次数和成功率
        total = sum(stats.values())
        success_rate = stats.get("success", 0) / total if total > 0 else 0.0

        # 平均耗时
        avg_duration = self._session.query(
            func.avg(CollectionLog.duration_ms)
        ).filter(
            CollectionLog.started_at >= datetime.now() - timedelta(hours=hours)
        ).scalar() or 0

        return {
            "period_hours": hours,
            "total_collections": total,
            "success": stats.get("success", 0),
            "failed": stats.get("failed", 0),
            "partial": stats.get("partial", 0),
            "success_rate": round(success_rate, 4),
            "avg_duration_ms": int(avg_duration),
        }

    def detect_anomalies(self) -> list[dict[str, Any]]:
        """检测采集异常"""
        anomalies: list[dict[str, Any]] = []

        # 检查数据源状态
        sources = self._source_repo.list_all()
        for s in sources:
            if s.enabled and s.health_status == "down":
                anomalies.append({
                    "type": "source_down",
                    "severity": "critical",
                    "source": s.name,
                    "message": f"数据源 {s.display_name} 不可用",
                    "detail": s.last_error,
                })
            elif s.enabled and s.health_status == "degraded":
                anomalies.append({
                    "type": "source_degraded",
                    "severity": "warning",
                    "source": s.name,
                    "message": f"数据源 {s.display_name} 性能下降",
                    "detail": s.last_error,
                })

        # 检查近期失败率
        stats_1h = self._log_repo.get_recent_stats(hours=1)
        total_1h = sum(stats_1h.values())
        if total_1h >= 5:
            fail_rate = stats_1h.get("failed", 0) / total_1h
            if fail_rate > 0.5:
                anomalies.append({
                    "type": "high_failure_rate",
                    "severity": "critical",
                    "message": f"过去1小时失败率 {fail_rate:.0%} ({stats_1h.get('failed', 0)}/{total_1h})",
                })

        return anomalies
