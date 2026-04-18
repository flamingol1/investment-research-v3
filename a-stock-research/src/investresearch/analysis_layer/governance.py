"""Hybrid governance analysis agent."""

from __future__ import annotations

from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import AgentInput, AgentOutput, AgentStatus
from investresearch.core.trust import get_field_quality_trace, get_module_profile, merge_evidence_refs

logger = get_logger("agent.governance")

SYSTEM_PROMPT = """你是一位A股公司治理研究员。

请只基于输入中已经提供的治理、股东、财务、合规与公告资料输出 JSON：
{
  "governance_score": 7.2,
  "management_assessment": "管理层评估",
  "management_integrity": "优/良/中/差",
  "controller_analysis": "实控人与控制权分析",
  "related_transactions": "关联交易判断",
  "equity_pledge": "股权质押判断",
  "capital_allocation": "资本配置判断",
  "dividend_policy": "分红政策判断",
  "incentive_plan": "激励计划判断",
  "conclusion": "治理综合结论"
}

要求：
- 不要编造新的处罚事件、股东结构、资本运作记录或监管结论
- 若存在官方合规事件，不要给出与事实冲突的过度乐观评价
- management_integrity 只能是 优/良/中/差
- 资料不足时明确写“待验证”或“资料不足”
"""


class GovernanceAgent(AgentBase[AgentInput, AgentOutput]):
    """Analyze governance quality, capital allocation, and compliance evidence."""

    agent_name: str = "governance"
    execution_mode: str = "hybrid"

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

        baseline = self._build_result(input_data.context)
        result = dict(baseline)
        allow_live_llm = bool(input_data.context.get("_allow_live_llm"))
        llm_invoked = False
        model_used: str | None = None
        runtime_mode = "deterministic"

        if allow_live_llm:
            model = self._get_model()
            model_used = model
            llm_invoked = True
            try:
                llm_result = await self.llm.call_json(
                    prompt=self._build_prompt(stock_code, stock_name, input_data.context),
                    system_prompt=SYSTEM_PROMPT,
                    model=model,
                )
                result = self._merge_llm_result(baseline, llm_result, input_data.context)
                runtime_mode = "llm"
            except Exception as exc:
                self.logger.warning(f"治理分析LLM不可用，退回规则结果 | {exc}")
                runtime_mode = "hybrid"

        result = self._normalize_result(result, baseline, input_data.context)
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
            execution_mode=runtime_mode,
            llm_invoked=llm_invoked,
            model_used=model_used if runtime_mode != "deterministic" else None,
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

    def _build_prompt(self, stock_code: str, stock_name: str, context: dict[str, Any]) -> str:
        cleaned = context.get("cleaned_data", {})
        info = cleaned.get("stock_info", {})
        governance = cleaned.get("governance", {})
        shareholders = cleaned.get("shareholders", {})
        financials = [item for item in cleaned.get("financials", []) if isinstance(item, dict)]
        compliance_events = [item for item in cleaned.get("compliance_events", []) if isinstance(item, dict)]
        announcements = [item for item in cleaned.get("announcements", []) if isinstance(item, dict)]

        parts = [f"## 标的\n- 股票代码: {stock_code}\n- 股票名称: {stock_name or 'N/A'}"]
        trace_sections = [
            ("股权质押", get_field_quality_trace(cleaned, "governance.equity_pledge_ratio")),
            ("担保信息", get_field_quality_trace(cleaned, "governance.guarantee_info")),
            ("诉讼信息", get_field_quality_trace(cleaned, "governance.lawsuit_info")),
            ("管理层变动", get_field_quality_trace(cleaned, "governance.management_changes")),
            ("董监高持股", get_field_quality_trace(cleaned, "shareholders.management_share_ratio")),
        ]
        trace_lines: list[str] = []
        for label, trace in trace_sections:
            if trace is None:
                continue
            evidence_state = getattr(trace.evidence_state, "value", trace.evidence_state)
            trace_lines.append(
                f"- {label}: 值状态={trace.value_state.value} / 证据状态={evidence_state} / 置信度={trace.confidence_score:.0%}"
            )
        if trace_lines:
            parts.extend(["## 治理字段证据状态", *trace_lines, ""])
        parts.extend(
            [
                "## 治理与控制权基础信息",
                f"- 实控人: {governance.get('actual_controller') or info.get('actual_controller') or '待验证'}",
                f"- 股权质押比例: {governance.get('equity_pledge_ratio', 'N/A')}",
                f"- 关联交易: {governance.get('related_transaction', 'N/A')}",
                f"- 担保信息: {governance.get('guarantee_info', 'N/A')}",
                f"- 诉讼信息: {governance.get('lawsuit_info', 'N/A')}",
                f"- 管理层变动条数: {len(governance.get('management_changes', []) or [])}",
                f"- 前十大股东合计持股: {shareholders.get('top10_total_ratio', 'N/A')}",
                "",
            ]
        )

        if financials:
            latest = financials[0]
            parts.extend(
                [
                    "## 最新资本配置线索",
                    f"- 报告期: {latest.get('report_date', 'N/A')}",
                    f"- 经营现金流: {latest.get('operating_cashflow', 'N/A')}",
                    f"- 自由现金流: {latest.get('free_cashflow', 'N/A')}",
                    f"- ROE: {latest.get('roe', 'N/A')}",
                    f"- 分红记录数: {len(governance.get('dividend_history', []) or [])}",
                    f"- 回购记录数: {len(governance.get('buyback_history', []) or [])}",
                    f"- 再融资记录数: {len(governance.get('refinancing_history', []) or [])}",
                    "",
                ]
            )

        if compliance_events:
            parts.append("## 官方合规事件")
            for item in compliance_events[:5]:
                excerpt = item.get("excerpt") or item.get("summary") or ""
                parts.append(
                    f"- {item.get('publish_date', 'N/A')} [{item.get('severity', 'N/A')}] "
                    f"{item.get('title', 'N/A')}: {str(excerpt)[:180]}"
                )
            parts.append("")

        if announcements:
            parts.append("## 公告摘录")
            for item in announcements[:4]:
                excerpt = item.get("excerpt") or item.get("summary") or ""
                parts.append(
                    f"- {item.get('announcement_date', 'N/A')} {item.get('title', 'N/A')}: {str(excerpt)[:160]}"
                )
            parts.append("")

        parts.append("请基于以上资料判断治理质量、管理层诚信与资本配置，不要补充输入中不存在的新事实。")
        return "\n".join(parts)

    def _build_result(self, context: dict[str, Any]) -> dict[str, Any]:
        cleaned = context.get("cleaned_data", {})
        governance = cleaned.get("governance", {})
        shareholders = cleaned.get("shareholders", {})
        financials = [item for item in cleaned.get("financials", []) if isinstance(item, dict)]
        compliance_events = [item for item in cleaned.get("compliance_events", []) if isinstance(item, dict)]

        profile = get_module_profile(cleaned, "governance")
        shareholder_profile = get_module_profile(cleaned, "shareholders")
        compliance_profile = get_module_profile(cleaned, "compliance_events")
        announcement_profile = get_module_profile(cleaned, "announcements")

        actual_controller = (
            governance.get("actual_controller")
            or cleaned.get("stock_info", {}).get("actual_controller")
            or "待验证"
        )
        pledge_trace = get_field_quality_trace(cleaned, "governance.equity_pledge_ratio")
        guarantee_trace = get_field_quality_trace(cleaned, "governance.guarantee_info")
        lawsuit_trace = get_field_quality_trace(cleaned, "governance.lawsuit_info")
        management_trace = get_field_quality_trace(cleaned, "governance.management_changes")
        management_share_trace = get_field_quality_trace(cleaned, "shareholders.management_share_ratio")
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
            announcement_profile.evidence_refs,
        )

        trace_scores = [
            float(getattr(trace, "confidence_score", 0.0) or 0.0)
            for trace in (pledge_trace, guarantee_trace, lawsuit_trace, management_trace, management_share_trace)
            if trace is not None
        ]
        avg_trace_score = round(sum(trace_scores) / len(trace_scores), 2) if trace_scores else 0.0
        hard_failures = any(self._trace_failed(trace) for trace in (pledge_trace, guarantee_trace, lawsuit_trace, management_trace))
        governance_signal_count = sum(
            1
            for item in (
                actual_controller if actual_controller != "待验证" else None,
                pledge_ratio,
                management_changes,
                dividend_history,
                buyback_history,
                refinancing_history,
                compliance_events,
            )
            if item not in (None, "", [], {})
        )
        evidence_constraints = self._collect_evidence_constraints(
            pledge_trace,
            guarantee_trace,
            lawsuit_trace,
            management_trace,
            management_share_trace,
        )
        critical_trace_ready = all(
            self._trace_is_usable(trace, min_score=0.58) or self._trace_is_verified_absent(trace)
            for trace in (pledge_trace, guarantee_trace, lawsuit_trace)
        )

        if hard_failures:
            evidence_status = "insufficient"
        elif (
            critical_trace_ready
            and (
                avg_trace_score >= 0.7
                or (profile.completeness >= 0.4 and shareholder_profile.completeness >= 0.4)
                or governance_signal_count >= 4
            )
        ):
            evidence_status = "ok"
        elif (
            avg_trace_score >= 0.45
            or profile.completeness >= 0.3
            or shareholder_profile.completeness >= 0.3
            or governance_signal_count >= 2
        ):
            evidence_status = "partial"
        else:
            evidence_status = "insufficient"

        management_integrity = self._infer_integrity(
            governance,
            compliance_events,
            profile.completeness,
            lawsuit_trace=lawsuit_trace,
            guarantee_trace=guarantee_trace,
        )
        governance_score = None
        if evidence_status == "ok" and critical_trace_ready:
            fallback_signal_score = 0.62 if governance_signal_count >= 4 else 0.48 if governance_signal_count >= 2 else 0.0
            base_score = max(profile.completeness, shareholder_profile.completeness * 0.8, avg_trace_score, fallback_signal_score)
            if compliance_events and management_integrity == "差":
                base_score = min(base_score, 0.35)
            governance_score = round(base_score * 10, 1)

        missing_fields = sorted(set(profile.missing_fields))
        missing_fields.extend(f"shareholders.{item}" for item in shareholder_profile.missing_fields[:3])
        missing_fields.extend(f"compliance_events.{item}" for item in compliance_profile.missing_fields[:3])
        missing_fields.extend(f"announcements.{item}" for item in announcement_profile.missing_fields[:2])

        conclusion = self._build_conclusion(evidence_status, management_integrity, compliance_events)

        return {
            "governance_score": governance_score,
            "management_assessment": self._infer_management_assessment(
                actual_controller,
                management_changes,
                evidence_status,
                compliance_events,
                management_share_trace=management_share_trace,
            ),
            "management_integrity": management_integrity,
            "controller_analysis": self._infer_controller_analysis(
                actual_controller,
                pledge_ratio,
                shareholders,
                pledge_trace=pledge_trace,
            ),
            "related_transactions": related_transaction,
            "equity_pledge": self._build_pledge_summary(pledge_ratio, pledge_trace),
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
            "evidence_constraints": evidence_constraints,
            "missing_fields": missing_fields[:10],
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }

    def _merge_llm_result(
        self,
        baseline: dict[str, Any],
        llm_result: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(baseline)
        for key in (
            "management_assessment",
            "controller_analysis",
            "related_transactions",
            "equity_pledge",
            "capital_allocation",
            "dividend_policy",
            "incentive_plan",
            "conclusion",
        ):
            value = self._clean_text(llm_result.get(key))
            if value:
                merged[key] = value

        integrity = self._normalize_integrity(llm_result.get("management_integrity"))
        if integrity:
            merged["management_integrity"] = self._guard_integrity(integrity, context)

        score = self._safe_float(llm_result.get("governance_score"))
        if score is not None and baseline.get("evidence_status") == "ok":
            score = round(max(0.0, min(score, 10.0)), 1)
            if merged.get("management_integrity") == "差":
                score = min(score, 4.5)
            elif merged.get("management_integrity") == "中":
                score = min(score, 7.2)
            merged["governance_score"] = score

        return merged

    def _normalize_result(
        self,
        result: dict[str, Any],
        baseline: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = dict(baseline)
        normalized.update(result)

        integrity = self._normalize_integrity(normalized.get("management_integrity")) or baseline.get(
            "management_integrity", "中"
        )
        normalized["management_integrity"] = self._guard_integrity(integrity, context)

        score = self._safe_float(normalized.get("governance_score"))
        if baseline.get("evidence_status") != "ok":
            normalized["governance_score"] = baseline.get("governance_score")
        elif score is None:
            normalized["governance_score"] = baseline.get("governance_score")
        else:
            score = round(max(0.0, min(score, 10.0)), 1)
            if normalized["management_integrity"] == "差":
                score = min(score, 4.5)
            elif normalized["management_integrity"] == "中":
                score = min(score, 7.2)
            normalized["governance_score"] = score

        for key in (
            "management_assessment",
            "controller_analysis",
            "related_transactions",
            "equity_pledge",
            "capital_allocation",
            "dividend_policy",
            "incentive_plan",
            "conclusion",
        ):
            value = self._clean_text(normalized.get(key))
            normalized[key] = value or baseline.get(key, "")

        if baseline.get("evidence_status") != "ok":
            normalized["conclusion"] = baseline.get("conclusion", normalized["conclusion"])

        normalized["evidence_status"] = baseline.get("evidence_status", "insufficient")
        normalized["evidence_constraints"] = list(baseline.get("evidence_constraints", []) or [])
        normalized["missing_fields"] = baseline.get("missing_fields", [])
        normalized["evidence_refs"] = baseline.get("evidence_refs", [])
        return normalized

    @staticmethod
    def _infer_integrity(
        governance: dict[str, Any],
        compliance_events: list[dict[str, Any]],
        completeness: float,
        *,
        lawsuit_trace: Any = None,
        guarantee_trace: Any = None,
    ) -> str:
        if compliance_events:
            high_risk = any(str(item.get("severity", "")).lower() == "high" for item in compliance_events)
            if high_risk:
                return "差"
            return "中"
        if GovernanceAgent._trace_is_verified_absent(lawsuit_trace):
            return "良" if completeness >= 0.5 else "中"
        if governance.get("lawsuit_info") and not GovernanceAgent._trace_is_verified_absent(lawsuit_trace):
            return "差"
        if governance.get("related_transaction"):
            return "中"
        if governance.get("guarantee_info") and not GovernanceAgent._trace_is_verified_absent(guarantee_trace):
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
        *,
        management_share_trace: Any = None,
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
        if GovernanceAgent._trace_is_usable(management_share_trace, min_score=0.6):
            return f"已补齐董监高持股线索，当前未见频繁管理层变动，可围绕实控人 {actual_controller} 跟踪执行一致性。"
        return f"当前未见频繁管理层变动，可基于实控人 {actual_controller} 做基础判断。"

    @staticmethod
    def _infer_controller_analysis(
        actual_controller: str,
        pledge_ratio: Any,
        shareholders: dict[str, Any],
        *,
        pledge_trace: Any = None,
    ) -> str:
        top10_ratio = shareholders.get("top10_total_ratio")
        if isinstance(pledge_ratio, (int, float)):
            return (
                f"实控人为 {actual_controller}；已识别股权质押比例约 {float(pledge_ratio):.2f}%；"
                f"前十大股东合计持股 {top10_ratio or '待验证'}%。"
            )
        if GovernanceAgent._trace_is_verified_absent(pledge_trace):
            return f"实控人为 {actual_controller}；已核查暂未见稳定股权质押记录，前十大股东合计持股 {top10_ratio or '待验证'}%。"
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

    @staticmethod
    def _build_pledge_summary(pledge_ratio: Any, pledge_trace: Any) -> str:
        if isinstance(pledge_ratio, (int, float)):
            return f"股权质押比例约 {float(pledge_ratio):.2f}%"
        if GovernanceAgent._trace_is_verified_absent(pledge_trace):
            return "已核查暂未见股权质押记录"
        if GovernanceAgent._trace_failed(pledge_trace):
            return "股权质押采集失败，当前不能据此下结论"
        return "缺少稳定股权质押证据"

    def _guard_integrity(self, integrity: str, context: dict[str, Any]) -> str:
        cleaned = context.get("cleaned_data", {})
        governance = cleaned.get("governance", {})
        compliance_events = [item for item in cleaned.get("compliance_events", []) if isinstance(item, dict)]
        lawsuit_trace = get_field_quality_trace(cleaned, "governance.lawsuit_info")
        guarantee_trace = get_field_quality_trace(cleaned, "governance.guarantee_info")

        if any(str(item.get("severity", "")).lower() == "high" for item in compliance_events):
            return "差"
        if governance.get("lawsuit_info") and not self._trace_is_verified_absent(lawsuit_trace):
            return "差"
        if (
            compliance_events
            or governance.get("related_transaction")
            or (governance.get("guarantee_info") and not self._trace_is_verified_absent(guarantee_trace))
        ):
            order = {"差": 0, "中": 1, "良": 2, "优": 3}
            return integrity if order.get(integrity, 1) <= order["中"] else "中"
        return integrity

    @staticmethod
    def _normalize_integrity(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        mapping = {
            "优秀": "优",
            "优": "优",
            "良好": "良",
            "良": "良",
            "中性": "中",
            "一般": "中",
            "中": "中",
            "较差": "差",
            "差": "差",
        }
        return mapping.get(text)

    @staticmethod
    def _clean_text(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        return text[:240]

    @staticmethod
    def _trace_is_usable(trace: Any, *, min_score: float) -> bool:
        if trace is None:
            return False
        value_state = str(getattr(getattr(trace, "value_state", ""), "value", getattr(trace, "value_state", "")))
        if value_state == "verified_absent":
            return True
        if value_state != "present":
            return False
        evidence_state = str(getattr(getattr(trace, "evidence_state", ""), "value", getattr(trace, "evidence_state", "")))
        if evidence_state in {"consistent", "verified_absent"}:
            return True
        return evidence_state == "single_source" and float(getattr(trace, "confidence_score", 0.0) or 0.0) >= min_score

    @staticmethod
    def _trace_is_verified_absent(trace: Any) -> bool:
        if trace is None:
            return False
        value_state = str(getattr(getattr(trace, "value_state", ""), "value", getattr(trace, "value_state", "")))
        evidence_state = str(getattr(getattr(trace, "evidence_state", ""), "value", getattr(trace, "evidence_state", "")))
        return value_state == "verified_absent" or evidence_state == "verified_absent"

    @staticmethod
    def _trace_failed(trace: Any) -> bool:
        if trace is None:
            return False
        value_state = str(getattr(getattr(trace, "value_state", ""), "value", getattr(trace, "value_state", "")))
        evidence_state = str(getattr(getattr(trace, "evidence_state", ""), "value", getattr(trace, "evidence_state", "")))
        return value_state == "collection_failed" or evidence_state == "collection_failed"

    @classmethod
    def _collect_evidence_constraints(cls, *traces: Any) -> list[str]:
        notes: list[str] = []
        for trace in traces:
            if trace is None or cls._trace_is_usable(trace, min_score=0.58):
                continue
            evidence_state = str(getattr(getattr(trace, "evidence_state", ""), "value", getattr(trace, "evidence_state", "")))
            value_state = str(getattr(getattr(trace, "value_state", ""), "value", getattr(trace, "value_state", "")))
            notes.append(
                f"{getattr(trace, 'label', getattr(trace, 'field', 'field'))}证据偏弱: 值状态={value_state}，证据状态={evidence_state}"
            )
        return notes[:5]

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None or value == "" or value == "-":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
