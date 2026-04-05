"""动态跟踪Agent - 监控指标校验 + 风险触发预警

核心逻辑:
1. 读取跟踪列表
2. 对每个标的获取当前实时数据
3. 逐项检查监控指标是否触发阈值
4. 生成MonitoringAlert列表
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import (
    AgentInput,
    AgentOutput,
    AgentStatus,
    InvestmentConclusion,
    MonitoringAlert,
    WatchListItem,
)

from .chroma_store import ChromaKnowledgeStore
from .watch_list import WatchListManager

logger = get_logger("agent.dynamic_tracker")


class DynamicTrackerAgent(AgentBase[AgentInput, AgentOutput]):
    """动态跟踪Agent

    纯规则检查（不依赖LLM），对跟踪列表中的标的进行指标校验和预警。
    """

    agent_name: str = "dynamic_tracker"

    def __init__(
        self,
        knowledge_store: ChromaKnowledgeStore | None = None,
        watch_manager: WatchListManager | None = None,
    ) -> None:
        super().__init__()
        self._store = knowledge_store
        self._watch_manager = watch_manager

    @property
    def store(self) -> ChromaKnowledgeStore:
        if self._store is None:
            self._store = ChromaKnowledgeStore()
        return self._store

    @property
    def watch_mgr(self) -> WatchListManager:
        if self._watch_manager is None:
            self._watch_manager = WatchListManager()
        return self._watch_manager

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行动态跟踪检查"""
        self.logger.info("开始动态跟踪检查")

        # 1. 获取跟踪列表
        watch_list = self.watch_mgr.get_all()
        if not watch_list.items:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.SUCCESS,
                data={"alerts": [], "checked_count": 0},
                summary="跟踪列表为空，无标的需要检查",
            )

        # 2. 获取预警阈值配置
        thresholds = self.config.get_alert_thresholds()

        # 3. 逐标的检查
        all_alerts: list[dict[str, Any]] = []
        checked_count = 0
        errors: list[str] = []

        for item in watch_list.items:
            try:
                alerts = self._check_stock(item, thresholds)
                all_alerts.extend(alerts)

                # 更新标的跟踪状态
                max_severity = self._max_severity(alerts)
                self.watch_mgr.update_status(item.stock_code, max_severity)
                self.watch_mgr.update_last_checked(item.stock_code)
                checked_count += 1

                status_text = "正常" if not alerts else f"{len(alerts)}个预警"
                self.logger.info(
                    f"[{item.stock_code}] {item.stock_name or ''} | {status_text}"
                )
            except Exception as e:
                errors.append(f"[{item.stock_code}] 检查失败: {e}")
                self.logger.warning(f"[{item.stock_code}] 跟踪检查失败: {e}")

        # 4. 保存跟踪列表状态
        self.watch_mgr.save()

        summary_parts = [f"检查{checked_count}个标的"]
        critical = sum(1 for a in all_alerts if a.get("severity") == "critical")
        warning = sum(1 for a in all_alerts if a.get("severity") == "warning")
        if critical:
            summary_parts.append(f"严重预警{critical}个")
        if warning:
            summary_parts.append(f"一般预警{warning}个")
        if not all_alerts:
            summary_parts.append("全部正常")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={
                "alerts": all_alerts,
                "checked_count": checked_count,
                "total_items": len(watch_list.items),
            },
            errors=errors,
            summary=" | ".join(summary_parts),
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验输出"""
        if output.status == AgentStatus.FAILED:
            return
        data = output.data
        if "alerts" not in data:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, ["缺少 alerts 字段"])

    # ================================================================
    # 检查逻辑
    # ================================================================

    def _check_stock(
        self, item: WatchListItem, global_thresholds: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """检查单个标的"""
        alerts: list[dict[str, Any]] = []

        # 合并全局阈值和标的自定义阈值
        thresholds = {**global_thresholds, **item.alert_thresholds}

        # 获取实时行情数据
        realtime = self._get_realtime_snapshot(item.stock_code)
        if realtime is None:
            self.logger.warning(f"[{item.stock_code}] 无法获取实时数据，跳过")
            return alerts

        # 获取最新研究结论（只查一次，传给所有检查方法）
        conclusion = self.store.get_conclusion(item.stock_code)

        # 检查各项指标
        self._check_pe(realtime, conclusion, item, thresholds, alerts)
        self._check_price_change(realtime, item, thresholds, alerts)
        self._check_risk_level(conclusion, item, thresholds, alerts)

        return alerts

    def _check_pe(
        self,
        realtime: dict[str, Any],
        conclusion: InvestmentConclusion | None,
        item: WatchListItem,
        thresholds: dict[str, Any],
        alerts: list[dict[str, Any]],
    ) -> None:
        """检查PE是否超出合理区间"""
        pe_ttm = realtime.get("pe_ttm")
        if pe_ttm is None:
            return

        # 自定义PE阈值优先
        pe_max = item.alert_thresholds.get("pe_ttm_max")
        if pe_max and pe_ttm > float(pe_max):
            alerts.append(self._make_alert(
                item, "threshold", "warning",
                "PE_TTM", f"{pe_ttm:.1f}", str(pe_max),
                f"PE_TTM={pe_ttm:.1f} 超过阈值 {pe_max}",
            ))

    def _check_price_change(
        self,
        realtime: dict[str, Any],
        item: WatchListItem,
        thresholds: dict[str, Any],
        alerts: list[dict[str, Any]],
    ) -> None:
        """检查涨跌幅"""
        threshold = thresholds.get("price_change_pct_week", 10)
        change_pct = realtime.get("change_pct")
        if change_pct is None:
            return

        if abs(change_pct) > threshold:
            severity = "warning" if abs(change_pct) < threshold * 1.5 else "critical"
            direction = "上涨" if change_pct > 0 else "下跌"
            alerts.append(self._make_alert(
                item, "threshold", severity,
                "price_change", f"{change_pct:.1f}%", f"{threshold}%",
                f"日{direction} {abs(change_pct):.1f}% 超过阈值 {threshold}%",
            ))

    def _check_risk_level(
        self,
        conclusion: InvestmentConclusion | None,
        item: WatchListItem,
        thresholds: dict[str, Any],
        alerts: list[dict[str, Any]],
    ) -> None:
        """基于研究结论的风险等级检查"""
        if not conclusion:
            return

        # 检查结论中的风险等级是否升级
        if conclusion.risk_level in ("高", "极高"):
            alerts.append(self._make_alert(
                item, "threshold", "critical" if conclusion.risk_level == "极高" else "warning",
                "risk_level", conclusion.risk_level, "中",
                f"风险等级: {conclusion.risk_level}",
            ))

        # 检查建议是否为卖出/观望
        if conclusion.recommendation in ("卖出", "观望"):
            alerts.append(self._make_alert(
                item, "threshold", "warning",
                "recommendation", conclusion.recommendation, "",
                f"投资建议已变为: {conclusion.recommendation}",
            ))

    # ================================================================
    # 工具方法
    # ================================================================

    def _get_realtime_snapshot(self, stock_code: str) -> dict[str, Any] | None:
        """获取实时行情快照（复用collector逻辑）"""
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot_em()
            if df is None or df.empty:
                return None

            row = df[df["代码"] == stock_code]
            if row.empty:
                return None

            row = row.iloc[0]
            from investresearch.data_layer.collector import DataCollectorAgent
            close = DataCollectorAgent._safe_float(row.get("最新价"))
            change_pct = DataCollectorAgent._safe_float(row.get("涨跌幅"))
            pe_ttm = DataCollectorAgent._safe_float(row.get("市盈率-动态"))
            pb_mrq = DataCollectorAgent._safe_float(row.get("市净率"))

            return {
                "price": close,
                "change_pct": change_pct,
                "pe_ttm": pe_ttm,
                "pb_mrq": pb_mrq,
            }
        except Exception as e:
            self.logger.warning(f"实时行情获取失败: {e}")
            return None

    @staticmethod
    def _make_alert(
        item: WatchListItem,
        alert_type: str,
        severity: str,
        metric_name: str,
        current_value: str,
        threshold_value: str | None,
        message: str,
    ) -> dict[str, Any]:
        """创建预警字典"""
        return MonitoringAlert(
            stock_code=item.stock_code,
            stock_name=item.stock_name,
            alert_type=alert_type,
            severity=severity,
            metric_name=metric_name,
            current_value=current_value,
            threshold_value=threshold_value,
            message=message,
        ).model_dump(mode="json")

    @staticmethod
    def _max_severity(alerts: list[dict[str, Any]]) -> str:
        """获取最高严重程度"""
        if not alerts:
            return "normal"
        severities = [a.get("severity", "info") for a in alerts]
        if "critical" in severities:
            return "critical"
        if "warning" in severities:
            return "warning"
        return "normal"
