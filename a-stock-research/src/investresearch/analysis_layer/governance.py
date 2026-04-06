"""Deterministic governance analysis agent."""

from __future__ import annotations

from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import AgentInput, AgentOutput, AgentStatus
from investresearch.core.trust import get_module_profile, merge_evidence_refs

logger = get_logger("agent.governance")

SYSTEM_PROMPT = "Governance analysis is implemented deterministically in this build."


class GovernanceAgent(AgentBase[AgentInput, AgentOutput]):
    """Analyze governance quality, capital allocation, and official compliance evidence."""

    agent_name: str = "governance"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        cleaned = input_data.context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行治理分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始治理分析 | {stock_code} {stock_name}")

        result = self._build_result(input_data.context)
        score = result.get("governance_score")
        integrity = result.get("management_integrity", "未知")
        score_text = "待验证" if score is None else f"{score}/10"
        summary = f"治理评分: {score_text}, 管理层诚信: {integrity}"

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"governance": result},
            data_sources=["governance", "announcements", "shareholders", "financials", "compliance_events"],
            confidence=0.76 if result.get("evidence_status") == "ok" else 0.45,
            summary=summary,
        )

    def validate_output(self, output: AgentOutput) -> None:
        if output.status != AgentStatus.SUCCESS:
            return

        governance = output.data.get("governance", {})
        errors: list[str] = []

        score = governance.get("governance_score")
        if score is not None and not (0 <= score <= 10):
            errors.append(f"governance_score 超出范围: {score}")

        if governance.get("management_integrity") not in {"优", "良", "中", "差"}:
            errors.append(f"management_integrity 无效: {governance.get('management_integrity')}")

        for field_name in ("management_assessment", "capital_allocation", "conclusion"):
            if not governance.get(field_name):
                errors.append(f"缺少 {field_name}")

        if errors:
            from investresearch.core.exceptions import AgentValidationError

            raise AgentValidationError(self.agent_name, errors)

    def _get_model(self) -> str:
        return self.config.get_layer_model("analysis_layer", task="governance")

    def _build_result(self, context: dict[str, Any]) -> dict[str, Any]:
        cleaned = context.get("cleaned_data", {})
        governance = cleaned.get("governance", {})
        shareholders = cleaned.get("shareholders", {})
        financials = [item for item in cleaned.get("financials", []) if isinstance(item, dict)]
        compliance_events = [item for item in cleaned.get("compliance_events", []) if isinstance(item, dict)]

        profile = get_module_profile(cleaned, "governance")
        shareholder_profile = get_module_profile(cleaned, "shareholders")
        compliance_profile = get_module_profile(cleaned, "compliance_events")

        actual_controller = (
            governance.get("actual_controller")
            or cleaned.get("stock_info", {}).get("actual_controller")
            or "待验证"
        )
        pledge_ratio = governance.get("equity_pledge_ratio")
        related_transaction = governance.get("related_transaction") or "数据不足，无法判断"
        management_changes = governance.get("management_changes", []) or []
        dividend_history = governance.get("dividend_history", []) or []
        buyback_history = governance.get("buyback_history", []) or []
        refinancing_history = governance.get("refinancing_history", []) or []
        latest_financial = financials[0] if financials else {}

        evidence_refs = merge_evidence_refs(
            profile.evidence_refs,
            shareholder_profile.evidence_refs,
            compliance_profile.evidence_refs,
            get_module_profile(cleaned, "announcements").evidence_refs,
        )

        evidence_status = "ok" if (
            profile.completeness >= 0.4 or shareholder_profile.completeness >= 0.4
        ) else "insufficient"

        management_integrity = self._infer_integrity(governance, compliance_events, profile.completeness)
        governance_score = None
        if evidence_status == "ok":
            base_score = max(profile.completeness, shareholder_profile.completeness * 0.8)
            if compliance_events and management_integrity == "差":
                base_score = min(base_score, 0.35)
            governance_score = round(base_score * 10, 1)

        conclusion = self._build_conclusion(evidence_status, management_integrity, compliance_events)

        return {
            "governance_score": governance_score,
            "management_assessment": self._infer_management_assessment(
                actual_controller,
                management_changes,
                evidence_status,
                compliance_events,
            ),
            "management_integrity": management_integrity,
            "controller_analysis": self._infer_controller_analysis(actual_controller, pledge_ratio, shareholders),
            "related_transactions": related_transaction,
            "equity_pledge": (
                f"股权质押比例约 {float(pledge_ratio):.2f}%"
                if isinstance(pledge_ratio, (int, float))
                else "缺少稳定股权质押证据"
            ),
            "capital_allocation": self._infer_capital_allocation(
                latest_financial,
                dividend_history,
                buyback_history,
                refinancing_history,
                evidence_status,
            ),
            "dividend_policy": self._infer_dividend_policy(dividend_history, latest_financial, evidence_status),
            "incentive_plan": "当前未稳定抽取股权激励公告，需结合后续公告继续跟踪。",
            "conclusion": conclusion,
            "evidence_status": evidence_status,
            "missing_fields": profile.missing_fields,
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }

    @staticmethod
    def _infer_integrity(
        governance: dict[str, Any],
        compliance_events: list[dict[str, Any]],
        completeness: float,
    ) -> str:
        if compliance_events:
            high_risk = any(str(item.get("severity", "")).lower() == "high" for item in compliance_events)
            if high_risk:
                return "差"
            return "中"
        if governance.get("lawsuit_info"):
            return "差"
        if governance.get("related_transaction") or governance.get("guarantee_info"):
            return "中"
        if completeness >= 0.6:
            return "良"
        return "中"

    @staticmethod
    def _infer_management_assessment(
        actual_controller: str,
        management_changes: list[dict[str, Any]],
        evidence_status: str,
        compliance_events: list[dict[str, Any]],
    ) -> str:
        if compliance_events:
            latest_event = compliance_events[0]
            return (
                f"已检索到官方合规事件 {len(compliance_events)} 条，最近一条为"
                f"[{latest_event.get('title', 'N/A')}]，管理层诚信与信息披露需从严跟踪。"
            )
        if evidence_status != "ok":
            return f"当前仅确认实控人/股东层信息（{actual_controller}），管理层能力仍需后续公告与经营兑现验证。"
        if management_changes:
            return f"管理层存在 {len(management_changes)} 条变动或增减持记录，需继续观察稳定性与执行效果。"
        return f"当前未见频繁管理层变动，可基于实控人 {actual_controller} 做基础判断。"

    @staticmethod
    def _infer_controller_analysis(actual_controller: str, pledge_ratio: Any, shareholders: dict[str, Any]) -> str:
        top10_ratio = shareholders.get("top10_total_ratio")
        if isinstance(pledge_ratio, (int, float)):
            return (
                f"实控人为 {actual_controller}；已识别股权质押比例约 {float(pledge_ratio):.2f}%；"
                f"前十大股东合计持股 {top10_ratio or '待验证'}%。"
            )
        return f"实控人为 {actual_controller}；控制权结构已识别，但质押与穿透控制链仍待补充。"

    @staticmethod
    def _infer_capital_allocation(
        latest_financial: dict[str, Any],
        dividend_history: list[dict[str, Any]],
        buyback_history: list[dict[str, Any]],
        refinancing_history: list[dict[str, Any]],
        evidence_status: str,
    ) -> str:
        if evidence_status != "ok":
            return "资本配置资料不足，当前仅能跟踪自由现金流、分红、回购与再融资公告的后续补齐。"
        parts: list[str] = []
        if latest_financial.get("operating_cashflow") is not None:
            parts.append(f"经营现金流={latest_financial.get('operating_cashflow')}")
        if latest_financial.get("free_cashflow") is not None:
            parts.append(f"自由现金流={latest_financial.get('free_cashflow')}")
        parts.append(f"分红记录={len(dividend_history)}")
        parts.append(f"回购记录={len(buyback_history)}")
        parts.append(f"再融资记录={len(refinancing_history)}")
        return "；".join(parts)

    @staticmethod
    def _infer_dividend_policy(
        dividend_history: list[dict[str, Any]],
        latest_financial: dict[str, Any],
        evidence_status: str,
    ) -> str:
        if evidence_status != "ok":
            return "分红政策待验证"
        if dividend_history:
            return f"已识别 {len(dividend_history)} 条分红记录，可继续跟踪分红率与现金流匹配度。"
        if latest_financial.get("operating_cashflow") is not None:
            return "存在现金流支撑，但分红公告仍需继续补齐。"
        return "分红政策资料不足"

    @staticmethod
    def _build_conclusion(
        evidence_status: str,
        management_integrity: str,
        compliance_events: list[dict[str, Any]],
    ) -> str:
        if compliance_events and management_integrity == "差":
            return "官方合规事件已对治理判断形成负面证据，后续应优先跟踪监管进展与信息披露修复。"
        if evidence_status != "ok":
            return "治理证据仍有限，当前结论只适合做保守判断，需继续等待治理与资本配置资料补齐。"
        return "治理结构已有基础证据支撑，但仍需结合后续公告持续验证管理层执行与资本配置效率。"
