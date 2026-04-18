"""Hybrid financial analysis agent with evidence-aware guardrails."""

from __future__ import annotations

from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import AgentInput, AgentOutput, AgentStatus
from investresearch.core.trust import get_field_quality_trace, get_module_profile, merge_evidence_refs

logger = get_logger("agent.financial")

SYSTEM_PROMPT = """你是一位专业的A股财务分析专家，精通会计核查和财务造假识别。

## 你的任务
对给定股票进行5维度财务深度分析，输出结构化的分析结论。

## 五个分析维度
1. **盈利能力**: ROE、ROIC、毛利率、净利率趋势，杜邦分析拆解
2. **成长性**: 营收同比增速、净利润同比增速、扣非净利润增速，3-5年复合增长率
3. **偿债能力**: 资产负债率、流动比率、速动比率、利息保障倍数
4. **运营效率**: 应收账款周转率、存货周转率、总资产周转率、应收周转天数
5. **现金流质量**: 经营现金流、自由现金流、净现比(经营现金流/净利润)、现金流可持续性

## A股常见财务异常模式（必须检查）
- 营收增长但经营现金流为负（利润含金量不足）
- 应收账款增速远超营收增速（可能虚增收入）
- 毛利率与同行严重偏离（可能虚构成本）
- 频繁大额资产减值（可能前期虚增资产）
- 商誉占净资产比例过高（减值风险）
- 其他应收款/预付款异常（可能关联方占款）

## 分析要求
- 必须分析3-5年趋势，不能只看单一年份
- 每个维度必须给出0-10分评分和趋势判断
- 现金流验证是强制要求：经营现金流vs净利润对比
- 所有评分必须有数据引用

## 重要约束
- 若输入标注某些核心字段为弱证据/单源/分歧/采集失败，必须下调相关维度评分
- 若经营现金流、自由现金流、净现比证据不足，不得写“现金流良好/匹配/改善”这类强判断
- dimensions数组必须包含全部5个维度
- anomaly_flags为空时说明"未发现明显财务异常"
- cashflow_verification必须包含具体净现比或“净现比待验证”
- 所有数值必须引用输入中的实际数据，不得编造

## 输出格式（严格JSON）
```json
{
  "overall_score": 7.5,
  "dimensions": [
    {
      "dimension": "盈利能力",
      "score": 8.0,
      "trend": "改善|稳定|恶化",
      "key_metrics": {"roe": 15.2, "gross_margin": 45.0},
      "analysis": "该维度分析说明...",
      "concerns": ["关注点1"]
    }
  ],
  "trend_summary": "3-5年整体财务趋势总结",
  "cashflow_verification": "现金流与利润匹配度验证结论",
  "anomaly_flags": ["异常标记1", "异常标记2"],
  "peer_comparison": "与同行业公司的对比结论（如有数据）",
  "conclusion": "综合财务健康度结论"
}
```"""

_DIMENSIONS = ["盈利能力", "成长性", "偿债能力", "运营效率", "现金流质量"]


class FinancialAgent(AgentBase[AgentInput, AgentOutput]):
    """Evidence-aware financial analysis agent."""

    agent_name: str = "financial"
    execution_mode: str = "hybrid"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        cleaned = input_data.context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行财务分析"],
            )

        financials = [item for item in cleaned.get("financials", []) if isinstance(item, dict)]
        if not financials:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无财务数据，无法执行财务分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始财务分析 | {stock_code} {stock_name} | {len(financials)}期数据")

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
                    prompt=self._build_prompt(stock_code, stock_name, cleaned),
                    system_prompt=SYSTEM_PROMPT,
                    model=model,
                )
                result = self._merge_llm_result(baseline, llm_result)
                runtime_mode = "llm"
            except Exception as exc:
                self.logger.warning(f"财务分析LLM不可用，退回规则兜底 | {exc}")
                runtime_mode = "hybrid"

        result = self._apply_guardrails(result, baseline, cleaned)
        result = self._normalize_result(result, baseline)

        score = result.get("overall_score")
        score_text = "待验证" if score is None else f"{score}/10"
        summary = f"财务综合评分: {score_text}"
        self.logger.info(f"财务分析完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"financial": result},
            data_sources=["financials", "announcements", "research_reports", "cross_verification"],
            confidence=0.8 if result.get("evidence_status") == "ok" else 0.56 if result.get("evidence_status") == "partial" else 0.4,
            summary=summary,
            execution_mode=runtime_mode,
            llm_invoked=llm_invoked,
            model_used=model_used if runtime_mode != "deterministic" else None,
        )

    def validate_output(self, output: AgentOutput) -> None:
        if output.status != AgentStatus.SUCCESS:
            return

        financial = output.data.get("financial", {})
        errors: list[str] = []

        score = financial.get("overall_score")
        if score is None or not isinstance(score, (int, float)):
            errors.append("缺少有效的overall_score")
        elif not (0 <= score <= 10):
            errors.append(f"overall_score超出范围: {score}")

        dimensions = financial.get("dimensions", [])
        if not isinstance(dimensions, list) or len(dimensions) != 5:
            errors.append(f"dimensions不足: 需要5个维度，实际{len(dimensions) if isinstance(dimensions, list) else '非列表'}")

        if not financial.get("cashflow_verification"):
            errors.append("缺少cashflow_verification")
        if not financial.get("conclusion"):
            errors.append("缺少conclusion")

        if errors:
            from investresearch.core.exceptions import AgentValidationError

            raise AgentValidationError(self.agent_name, errors)

    def _get_model(self) -> str:
        return self.config.get_layer_model("analysis_layer", task="financial")

    def _build_prompt(self, stock_code: str, stock_name: str, cleaned: dict[str, Any]) -> str:
        parts = [f"## 标的: {stock_code} {stock_name}\n"]
        info = cleaned.get("stock_info", {})
        if info:
            parts.append(f"行业: {info.get('industry_sw', 'N/A')}")
            parts.append("")

        trace_fields = [
            "financials.latest.revenue",
            "financials.latest.net_profit",
            "financials.latest.deduct_net_profit",
            "financials.latest.operating_cashflow",
            "financials.latest.free_cashflow",
            "financials.latest.equity",
            "financials.latest.goodwill_ratio",
            "financials.latest.contract_liabilities",
        ]
        trace_lines: list[str] = []
        for field in trace_fields:
            trace = get_field_quality_trace(cleaned, field)
            if trace is None:
                continue
            evidence_state = getattr(trace.evidence_state, "value", trace.evidence_state)
            trace_lines.append(
                f"- {trace.label}: 值状态={trace.value_state.value} / 证据状态={evidence_state} / "
                f"置信度={trace.confidence_score:.0%} / 报告期={trace.report_period or '待验证'}"
            )
        if trace_lines:
            parts.extend(["### 核心字段证据状态", *trace_lines, ""])

        financials = [item for item in cleaned.get("financials", []) if isinstance(item, dict)]
        if financials:
            parts.append("### 多期财务数据")
            parts.append("| 报告期 | 营收 | 净利润 | 扣非净利润 | 营收增速 | 净利增速 | 毛利率 | 净利率 | ROE | 资产负债率 | 经营现金流 | 自由现金流 |")
            parts.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
            for item in financials[:8]:
                parts.append(
                    f"| {item.get('report_date', 'N/A')} "
                    f"| {self._fmt(item.get('revenue'))} "
                    f"| {self._fmt(item.get('net_profit'))} "
                    f"| {self._fmt(item.get('deduct_net_profit'))} "
                    f"| {self._fmt_pct(item.get('revenue_yoy'))} "
                    f"| {self._fmt_pct(item.get('net_profit_yoy'))} "
                    f"| {self._fmt_pct(item.get('gross_margin'))} "
                    f"| {self._fmt_pct(item.get('net_margin'))} "
                    f"| {self._fmt_pct(item.get('roe'))} "
                    f"| {self._fmt_pct(item.get('debt_ratio'))} "
                    f"| {self._fmt(item.get('operating_cashflow'))} "
                    f"| {self._fmt(item.get('free_cashflow'))} |"
                )
            parts.append("")

        cross_verification = cleaned.get("cross_verification", {})
        if cross_verification and cross_verification.get("verified_metrics"):
            parts.append("### 多源交叉验证")
            parts.append(f"- 整体置信度: {cross_verification.get('overall_confidence', 0):.0%}")
            if cross_verification.get("summary"):
                parts.append(f"- 总结: {cross_verification.get('summary')}")
            for metric in cross_verification.get("verified_metrics", [])[:6]:
                if not isinstance(metric, dict):
                    continue
                parts.append(
                    f"- {metric.get('metric_name', 'N/A')}: "
                    f"recommended={metric.get('recommended_value', 'N/A')} | "
                    f"consistency={metric.get('consistency_flag', 'N/A')} | "
                    f"sources={', '.join(metric.get('sources', [])[:4]) or 'N/A'}"
                )
            if cross_verification.get("divergent_metrics"):
                parts.append(f"- 强制谨慎字段: {', '.join(cross_verification.get('divergent_metrics', [])[:5])}")
            parts.append("")

        announcements = [item for item in cleaned.get("announcements", []) if isinstance(item, dict)]
        if announcements:
            periodic = [
                item for item in announcements
                if item.get("announcement_type_normalized") in {"annual_report", "semi_annual", "quarterly_report"}
            ][:4]
            if periodic:
                parts.append("### 定期报告原文摘录")
                for item in periodic:
                    excerpt = item.get("excerpt") or "；".join(item.get("highlights", [])[:2])
                    parts.append(
                        f"- {item.get('announcement_date', 'N/A')} {item.get('title', 'N/A')}: {str(excerpt)[:220]}"
                    )
                parts.append("")

        research_reports = [item for item in cleaned.get("research_reports", []) if isinstance(item, dict)]
        if research_reports:
            parts.append("### 卖方研报原文摘录")
            for item in research_reports[:3]:
                excerpt = item.get("excerpt") or item.get("summary") or ""
                parts.append(
                    f"- {item.get('publish_date', 'N/A')} {item.get('institution', 'N/A')}《{item.get('title', 'N/A')}》: {str(excerpt)[:180]}"
                )
            parts.append("")

        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append("### 当前估值指标")
            parts.append(f"- 最新价: {realtime.get('close', 'N/A')}")
            parts.append(f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}")
            parts.append(f"- PB: {realtime.get('pb_mrq', 'N/A')}")
            parts.append(f"- 总市值: {realtime.get('market_cap', 'N/A')}")
            parts.append("")

        parts.append(
            f"请根据以上{len(financials)}期财务数据，对该标的进行5维度深度财务分析，按指定JSON格式输出。"
        )
        if len(financials) < 3:
            parts.append("注意：财务数据期数较少(少于3期)，请在分析中注明数据局限性。")
        return "\n".join(parts)

    def _build_result(self, context: dict[str, Any]) -> dict[str, Any]:
        cleaned = context.get("cleaned_data", {})
        financials = [item for item in cleaned.get("financials", []) if isinstance(item, dict)]
        latest = financials[0] if financials else {}

        financial_profile = get_module_profile(cleaned, "financials")
        announcement_profile = get_module_profile(cleaned, "announcements")
        report_profile = get_module_profile(cleaned, "research_reports")
        cross_profile = get_module_profile(cleaned, "cross_verification")

        traces = {
            field: get_field_quality_trace(cleaned, field)
            for field in (
                "financials.latest.revenue",
                "financials.latest.net_profit",
                "financials.latest.deduct_net_profit",
                "financials.latest.operating_cashflow",
                "financials.latest.free_cashflow",
                "financials.latest.equity",
                "financials.latest.goodwill_ratio",
                "financials.latest.contract_liabilities",
            )
        }

        revenue_ok = self._trace_is_usable(traces["financials.latest.revenue"], min_score=0.68)
        profit_ok = self._trace_is_usable(traces["financials.latest.net_profit"], min_score=0.68)
        cashflow_ok = self._trace_is_usable(traces["financials.latest.operating_cashflow"], min_score=0.7)
        equity_ok = self._trace_is_usable(traces["financials.latest.equity"], min_score=0.68)
        evidence_constraints = self._collect_evidence_constraints(
            traces["financials.latest.revenue"],
            traces["financials.latest.net_profit"],
            traces["financials.latest.operating_cashflow"],
            traces["financials.latest.equity"],
            traces["financials.latest.free_cashflow"],
            traces["financials.latest.deduct_net_profit"],
        )

        evidence_status = "insufficient"
        if revenue_ok and profit_ok and cashflow_ok and equity_ok:
            evidence_status = "ok"
        elif revenue_ok and profit_ok and (cashflow_ok or equity_ok):
            evidence_status = "partial"
        elif financial_profile.completeness >= 0.4:
            evidence_status = "partial"

        profitability = self._build_profitability_dimension(latest, evidence_status)
        growth = self._build_growth_dimension(latest, evidence_status)
        solvency = self._build_solvency_dimension(latest, evidence_status)
        efficiency = self._build_efficiency_dimension(latest, evidence_status)
        cashflow = self._build_cashflow_dimension(latest, traces, evidence_status)
        dimensions = [profitability, growth, solvency, efficiency, cashflow]

        anomalies = self._detect_anomalies(latest, traces)
        overall_score = round(sum(item["score"] for item in dimensions) / len(dimensions), 1) if dimensions else 0.0
        if evidence_status == "partial":
            overall_score = min(overall_score, 6.4)
        elif evidence_status != "ok":
            overall_score = min(overall_score, 5.2)

        evidence_refs = merge_evidence_refs(
            financial_profile.evidence_refs,
            announcement_profile.evidence_refs,
            report_profile.evidence_refs,
            cross_profile.evidence_refs,
        )
        missing_fields = [
            field
            for field, trace in traces.items()
            if trace is not None and not self._trace_is_usable(trace, min_score=0.6)
        ]

        return {
            "overall_score": round(max(0.0, min(overall_score, 10.0)), 1),
            "dimensions": dimensions,
            "trend_summary": self._build_trend_summary(financials, evidence_status),
            "cashflow_verification": self._build_cashflow_verification(latest, traces, evidence_status),
            "anomaly_flags": anomalies or ["未发现明显财务异常"],
            "peer_comparison": self._build_peer_comparison(cleaned),
            "conclusion": self._build_conclusion(overall_score, evidence_status, anomalies),
            "evidence_status": evidence_status,
            "evidence_constraints": evidence_constraints,
            "missing_fields": missing_fields[:8],
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }

    def _merge_llm_result(self, baseline: dict[str, Any], llm_result: dict[str, Any]) -> dict[str, Any]:
        merged = dict(baseline)
        for key in ("trend_summary", "cashflow_verification", "peer_comparison", "conclusion"):
            value = str(llm_result.get(key) or "").strip()
            if value:
                merged[key] = value

        score = self._safe_float(llm_result.get("overall_score"))
        if score is not None:
            merged["overall_score"] = round(max(0.0, min(score, 10.0)), 1)

        anomaly_flags = self._normalize_text_list(llm_result.get("anomaly_flags"), limit=6)
        if anomaly_flags:
            merged["anomaly_flags"] = anomaly_flags

        llm_dimensions = self._normalize_dimensions(llm_result.get("dimensions"), baseline.get("dimensions", []))
        if llm_dimensions:
            merged["dimensions"] = llm_dimensions
        return merged

    def _apply_guardrails(
        self,
        result: dict[str, Any],
        baseline: dict[str, Any],
        cleaned: dict[str, Any],
    ) -> dict[str, Any]:
        guarded = dict(baseline)
        guarded.update(result)

        traces = {
            field: get_field_quality_trace(cleaned, field)
            for field in (
                "financials.latest.revenue",
                "financials.latest.net_profit",
                "financials.latest.operating_cashflow",
                "financials.latest.free_cashflow",
                "financials.latest.equity",
            )
        }
        core_weak = [
            field for field, trace in traces.items()
            if trace is not None and not self._trace_is_usable(trace, min_score=0.65)
        ]
        cashflow_weak = any(
            field in core_weak
            for field in ("financials.latest.operating_cashflow", "financials.latest.free_cashflow")
        )

        guarded["evidence_status"] = baseline.get("evidence_status", "partial")
        guarded["evidence_constraints"] = list(baseline.get("evidence_constraints", []) or [])
        guarded["missing_fields"] = list(baseline.get("missing_fields", []) or [])
        guarded["evidence_refs"] = list(baseline.get("evidence_refs", []) or [])

        dimensions = self._normalize_dimensions(guarded.get("dimensions"), baseline.get("dimensions", []))
        if cashflow_weak:
            for item in dimensions:
                if item.get("dimension") == "现金流质量":
                    item["score"] = min(float(item.get("score") or 0.0), 5.0)
                    concerns = self._normalize_text_list(item.get("concerns"), limit=5)
                    concerns.insert(0, "经营现金流/自由现金流证据不足，现金流质量不能做强判断")
                    item["concerns"] = self._unique(concerns, limit=5)
                    item["analysis"] = baseline["cashflow_verification"]
                    break
            guarded["cashflow_verification"] = baseline.get("cashflow_verification", guarded.get("cashflow_verification", ""))

        if self._contains_unsupported_cashflow_claim(guarded.get("trend_summary")):
            guarded["trend_summary"] = baseline.get("trend_summary", guarded.get("trend_summary", ""))
        if self._contains_unsupported_cashflow_claim(guarded.get("conclusion")):
            guarded["conclusion"] = baseline.get("conclusion", guarded.get("conclusion", ""))

        anomalies = self._normalize_text_list(guarded.get("anomaly_flags"), limit=6)
        for note in list(baseline.get("evidence_constraints", []) or []):
            if note not in anomalies:
                anomalies.append(note)
        guarded["anomaly_flags"] = self._unique(anomalies, limit=6) or ["未发现明显财务异常"]

        guarded["dimensions"] = dimensions
        computed_score = round(sum(float(item.get("score") or 0.0) for item in dimensions) / len(dimensions), 1) if dimensions else 0.0
        score = self._safe_float(guarded.get("overall_score"))
        guarded["overall_score"] = computed_score if score is None else round(max(0.0, min(score, 10.0)), 1)

        if core_weak:
            guarded["overall_score"] = min(float(guarded["overall_score"]), 6.2)
        if cashflow_weak:
            guarded["overall_score"] = min(float(guarded["overall_score"]), 5.8)
        if baseline.get("evidence_status") == "partial":
            guarded["overall_score"] = min(float(guarded["overall_score"]), 6.4)
        elif baseline.get("evidence_status") != "ok":
            guarded["overall_score"] = min(float(guarded["overall_score"]), 5.2)
        return guarded

    def _normalize_result(self, result: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(baseline)
        normalized.update(result)
        normalized["dimensions"] = self._normalize_dimensions(normalized.get("dimensions"), baseline.get("dimensions", []))
        score = self._safe_float(normalized.get("overall_score"))
        if score is None:
            score = round(sum(float(item.get("score") or 0.0) for item in normalized["dimensions"]) / len(normalized["dimensions"]), 1)
        normalized["overall_score"] = round(max(0.0, min(score, 10.0)), 1)
        normalized["trend_summary"] = str(normalized.get("trend_summary") or baseline.get("trend_summary") or "").strip()
        normalized["cashflow_verification"] = str(
            normalized.get("cashflow_verification") or baseline.get("cashflow_verification") or ""
        ).strip()
        normalized["peer_comparison"] = str(normalized.get("peer_comparison") or baseline.get("peer_comparison") or "").strip()
        normalized["conclusion"] = str(normalized.get("conclusion") or baseline.get("conclusion") or "").strip()
        normalized["anomaly_flags"] = self._normalize_text_list(
            normalized.get("anomaly_flags"),
            limit=6,
        ) or list(baseline.get("anomaly_flags", []) or [])
        normalized["evidence_status"] = baseline.get("evidence_status", "partial")
        normalized["evidence_constraints"] = list(baseline.get("evidence_constraints", []) or [])
        normalized["missing_fields"] = list(baseline.get("missing_fields", []) or [])
        normalized["evidence_refs"] = list(baseline.get("evidence_refs", []) or [])
        return normalized

    @classmethod
    def _build_profitability_dimension(cls, latest: dict[str, Any], evidence_status: str) -> dict[str, Any]:
        roe = cls._safe_float(latest.get("roe"))
        gross_margin = cls._safe_float(latest.get("gross_margin"))
        net_margin = cls._safe_float(latest.get("net_margin"))
        score = 5.2
        concerns: list[str] = []
        if roe is not None:
            score += 1.6 if roe >= 15 else 1.0 if roe >= 10 else 0.4 if roe >= 5 else -0.8
        else:
            concerns.append("ROE待验证")
        if gross_margin is not None:
            score += 0.8 if gross_margin >= 35 else 0.4 if gross_margin >= 20 else -0.6 if gross_margin < 10 else 0.0
        else:
            concerns.append("毛利率待验证")
        if net_margin is not None:
            score += 0.7 if net_margin >= 12 else 0.3 if net_margin >= 5 else -0.6 if net_margin < 3 else 0.0
        else:
            concerns.append("净利率待验证")
        if evidence_status != "ok":
            score -= 0.5
        return cls._build_dimension(
            dimension="盈利能力",
            score=score,
            trend=cls._trend_from_values(latest.get("roe"), latest.get("gross_margin")),
            key_metrics={"roe": roe, "gross_margin": gross_margin, "net_margin": net_margin},
            analysis=f"ROE={cls._fmt_pct(roe)}，毛利率={cls._fmt_pct(gross_margin)}，净利率={cls._fmt_pct(net_margin)}。",
            concerns=concerns,
        )

    @classmethod
    def _build_growth_dimension(cls, latest: dict[str, Any], evidence_status: str) -> dict[str, Any]:
        revenue_yoy = cls._safe_float(latest.get("revenue_yoy"))
        profit_yoy = cls._safe_float(latest.get("net_profit_yoy"))
        deduct_profit = cls._safe_float(latest.get("deduct_net_profit"))
        score = 5.0
        concerns: list[str] = []
        if revenue_yoy is not None:
            score += 1.5 if revenue_yoy >= 20 else 1.0 if revenue_yoy >= 10 else 0.4 if revenue_yoy >= 0 else -1.0
        else:
            concerns.append("营收增速待验证")
        if profit_yoy is not None:
            score += 1.3 if profit_yoy >= 20 else 0.8 if profit_yoy >= 10 else 0.3 if profit_yoy >= 0 else -1.1
        else:
            concerns.append("净利润增速待验证")
        if deduct_profit is None:
            concerns.append("扣非净利润待验证")
        if evidence_status != "ok":
            score -= 0.5
        return cls._build_dimension(
            dimension="成长性",
            score=score,
            trend=cls._trend_from_values(revenue_yoy, profit_yoy),
            key_metrics={"revenue_yoy": revenue_yoy, "net_profit_yoy": profit_yoy, "deduct_net_profit": deduct_profit},
            analysis=f"营收同比={cls._fmt_pct(revenue_yoy)}，净利润同比={cls._fmt_pct(profit_yoy)}，扣非净利润={cls._fmt(deduct_profit)}。",
            concerns=concerns,
        )

    @classmethod
    def _build_solvency_dimension(cls, latest: dict[str, Any], evidence_status: str) -> dict[str, Any]:
        debt_ratio = cls._safe_float(latest.get("debt_ratio"))
        current_ratio = cls._safe_float(latest.get("current_ratio"))
        quick_ratio = cls._safe_float(latest.get("quick_ratio"))
        score = 5.4
        concerns: list[str] = []
        if debt_ratio is not None:
            score += 1.0 if debt_ratio <= 45 else 0.4 if debt_ratio <= 60 else -1.0
        else:
            concerns.append("资产负债率待验证")
        if current_ratio is not None:
            score += 0.6 if current_ratio >= 1.5 else 0.2 if current_ratio >= 1.0 else -0.8
        else:
            concerns.append("流动比率待验证")
        if quick_ratio is not None:
            score += 0.4 if quick_ratio >= 1.0 else -0.4 if quick_ratio < 0.7 else 0.0
        else:
            concerns.append("速动比率待验证")
        if evidence_status == "insufficient":
            score -= 0.5
        return cls._build_dimension(
            dimension="偿债能力",
            score=score,
            trend=cls._trend_from_values(current_ratio, quick_ratio),
            key_metrics={"debt_ratio": debt_ratio, "current_ratio": current_ratio, "quick_ratio": quick_ratio},
            analysis=f"资产负债率={cls._fmt_pct(debt_ratio)}，流动比率={cls._fmt(current_ratio)}，速动比率={cls._fmt(quick_ratio)}。",
            concerns=concerns,
        )

    @classmethod
    def _build_efficiency_dimension(cls, latest: dict[str, Any], evidence_status: str) -> dict[str, Any]:
        receivable_turnover = cls._safe_float(latest.get("receivable_turnover"))
        inventory_turnover = cls._safe_float(latest.get("inventory_turnover"))
        total_assets = cls._safe_float(latest.get("total_assets"))
        equity = cls._safe_float(latest.get("equity"))
        score = 5.0
        concerns: list[str] = []
        if receivable_turnover is not None:
            score += 0.7 if receivable_turnover >= 5 else 0.3 if receivable_turnover >= 3 else -0.5
        else:
            concerns.append("应收周转率待验证")
        if inventory_turnover is not None:
            score += 0.7 if inventory_turnover >= 4 else 0.3 if inventory_turnover >= 2 else -0.5
        else:
            concerns.append("存货周转率待验证")
        if total_assets is None or equity is None:
            concerns.append("总资产/权益口径仍待补齐")
        if evidence_status == "insufficient":
            score -= 0.4
        return cls._build_dimension(
            dimension="运营效率",
            score=score,
            trend=cls._trend_from_values(receivable_turnover, inventory_turnover),
            key_metrics={"receivable_turnover": receivable_turnover, "inventory_turnover": inventory_turnover},
            analysis=f"应收周转率={cls._fmt(receivable_turnover)}，存货周转率={cls._fmt(inventory_turnover)}。",
            concerns=concerns,
        )

    @classmethod
    def _build_cashflow_dimension(
        cls,
        latest: dict[str, Any],
        traces: dict[str, Any],
        evidence_status: str,
    ) -> dict[str, Any]:
        operating_cashflow = cls._safe_float(latest.get("operating_cashflow"))
        free_cashflow = cls._safe_float(latest.get("free_cashflow"))
        net_profit = cls._safe_float(latest.get("net_profit"))
        cash_to_profit = cls._safe_float(latest.get("cash_to_profit"))
        if cash_to_profit is None and operating_cashflow not in (None, 0) and net_profit not in (None, 0):
            cash_to_profit = round(float(operating_cashflow) / float(net_profit), 2)

        score = 5.0
        concerns: list[str] = []
        if operating_cashflow is not None:
            score += 1.1 if operating_cashflow > 0 else -1.2
        else:
            concerns.append("经营现金流待验证")
        if free_cashflow is not None:
            score += 0.8 if free_cashflow > 0 else -0.8
        else:
            concerns.append("自由现金流待验证")
        if cash_to_profit is not None:
            score += 1.0 if cash_to_profit >= 1 else 0.3 if cash_to_profit >= 0.6 else -1.0
        else:
            concerns.append("净现比待验证")

        if not cls._trace_is_usable(traces.get("financials.latest.operating_cashflow"), min_score=0.7):
            score = min(score, 5.0)
            concerns.insert(0, "经营现金流证据偏弱")
        if not cls._trace_is_usable(traces.get("financials.latest.free_cashflow"), min_score=0.65):
            score = min(score, 5.4)
            concerns.append("自由现金流证据偏弱")
        if evidence_status != "ok":
            score = min(score, 5.8 if evidence_status == "partial" else 4.8)

        analysis = f"经营现金流={cls._fmt(operating_cashflow)}，自由现金流={cls._fmt(free_cashflow)}，净现比={cls._fmt(cash_to_profit)}。"
        return cls._build_dimension(
            dimension="现金流质量",
            score=score,
            trend=cls._trend_from_values(operating_cashflow, free_cashflow),
            key_metrics={"operating_cashflow": operating_cashflow, "free_cashflow": free_cashflow, "cash_to_profit": cash_to_profit},
            analysis=analysis,
            concerns=concerns,
        )

    @classmethod
    def _build_cashflow_verification(cls, latest: dict[str, Any], traces: dict[str, Any], evidence_status: str) -> str:
        operating_cashflow = cls._safe_float(latest.get("operating_cashflow"))
        free_cashflow = cls._safe_float(latest.get("free_cashflow"))
        net_profit = cls._safe_float(latest.get("net_profit"))
        cash_to_profit = cls._safe_float(latest.get("cash_to_profit"))
        if cash_to_profit is None and operating_cashflow not in (None, 0) and net_profit not in (None, 0):
            cash_to_profit = round(float(operating_cashflow) / float(net_profit), 2)

        if not cls._trace_is_usable(traces.get("financials.latest.operating_cashflow"), min_score=0.7):
            return "经营现金流核心证据不足，净现比待验证，当前不能据此判断利润含金量。"
        if cash_to_profit is None:
            return "经营现金流与净利润匹配度缺少稳定口径，净现比待验证。"
        if operating_cashflow is not None and net_profit is not None:
            relation = "匹配较好" if cash_to_profit >= 1 else "基本匹配" if cash_to_profit >= 0.6 else "偏弱"
            suffix = ""
            if free_cashflow is not None and free_cashflow < 0:
                suffix = "，但自由现金流仍为负"
            if evidence_status != "ok":
                suffix += "，且相关证据仍需继续补齐"
            return f"经营现金流={cls._fmt(operating_cashflow)}，净利润={cls._fmt(net_profit)}，净现比={cls._fmt(cash_to_profit)}，两者{relation}{suffix}。"
        return "现金流与利润匹配度仍待验证。"

    @classmethod
    def _build_peer_comparison(cls, cleaned: dict[str, Any]) -> str:
        cross_verification = cleaned.get("cross_verification", {})
        confidence = cls._safe_float(cross_verification.get("overall_confidence"))
        divergent = list(cross_verification.get("divergent_metrics", []) or [])
        if confidence is not None:
            if divergent:
                return f"公司多源财务交叉验证置信度约{confidence:.0%}，但在{', '.join(divergent[:4])}上仍存在分歧。"
            return f"公司多源财务交叉验证置信度约{confidence:.0%}，核心财务口径整体较稳定。"
        return "同行与多源财务对比资料仍有限，当前以公司口径核验为主。"

    @classmethod
    def _build_trend_summary(cls, financials: list[dict[str, Any]], evidence_status: str) -> str:
        latest = financials[0] if financials else {}
        revenue_yoy = cls._safe_float(latest.get("revenue_yoy"))
        profit_yoy = cls._safe_float(latest.get("net_profit_yoy"))
        roe = cls._safe_float(latest.get("roe"))
        text = (
            f"最新一期营收同比{cls._fmt_pct(revenue_yoy)}，净利润同比{cls._fmt_pct(profit_yoy)}，"
            f"ROE为{cls._fmt_pct(roe)}。"
        )
        if evidence_status == "partial":
            text += " 核心财务口径已有部分支撑，但现金流或权益字段仍需继续补证。"
        elif evidence_status != "ok":
            text += " 核心财务证据仍偏弱，当前只适合做保守判断。"
        else:
            text += " 财务趋势已具备基础证据支撑。"
        return text

    @classmethod
    def _build_conclusion(cls, overall_score: float, evidence_status: str, anomalies: list[str]) -> str:
        if evidence_status != "ok":
            return "核心财务证据仍不完整，当前结论仅适合做保守财务判断，不能把弱证据包装成高质量结论。"
        if anomalies and anomalies != ["未发现明显财务异常"]:
            return "财务质量具备基础支撑，但已出现需要重点跟踪的异常信号，不能简单给出高分乐观结论。"
        if overall_score >= 7.5:
            return "财务质量整体较稳，盈利、成长与现金流之间暂未见明显背离。"
        return "财务质量中性偏稳，仍需继续跟踪后续财报兑现情况。"

    @classmethod
    def _detect_anomalies(cls, latest: dict[str, Any], traces: dict[str, Any]) -> list[str]:
        flags: list[str] = []
        revenue_yoy = cls._safe_float(latest.get("revenue_yoy"))
        operating_cashflow = cls._safe_float(latest.get("operating_cashflow"))
        net_profit = cls._safe_float(latest.get("net_profit"))
        goodwill_ratio = cls._safe_float(latest.get("goodwill_ratio"))
        debt_ratio = cls._safe_float(latest.get("debt_ratio"))
        cash_to_profit = cls._safe_float(latest.get("cash_to_profit"))
        if cash_to_profit is None and operating_cashflow not in (None, 0) and net_profit not in (None, 0):
            cash_to_profit = round(float(operating_cashflow) / float(net_profit), 2)

        if revenue_yoy is not None and revenue_yoy > 0 and operating_cashflow is not None and operating_cashflow < 0:
            flags.append("营收增长但经营现金流为负，利润含金量需重点复核")
        if cash_to_profit is not None and cash_to_profit < 0.6:
            flags.append(f"净现比仅{cash_to_profit:.2f}，现金流与利润匹配度偏弱")
        if goodwill_ratio is not None and goodwill_ratio >= 30:
            flags.append(f"商誉/净资产约{goodwill_ratio:.1f}%，存在减值风险")
        if debt_ratio is not None and debt_ratio >= 65:
            flags.append(f"资产负债率约{debt_ratio:.1f}%，偿债压力偏高")
        if not cls._trace_is_usable(traces.get("financials.latest.operating_cashflow"), min_score=0.7):
            flags.append("经营现金流字段仅单源或弱证据，现金流质量判断需降级")
        return cls._unique(flags, limit=6)

    @staticmethod
    def _trace_is_usable(trace: Any, *, min_score: float) -> bool:
        if trace is None:
            return False
        value_state = str(getattr(getattr(trace, "value_state", ""), "value", getattr(trace, "value_state", "")))
        if value_state != "present":
            return False
        evidence_state = str(getattr(getattr(trace, "evidence_state", ""), "value", getattr(trace, "evidence_state", "")))
        if evidence_state == "consistent":
            return True
        return evidence_state == "single_source" and float(getattr(trace, "confidence_score", 0.0) or 0.0) >= min_score

    @classmethod
    def _collect_evidence_constraints(cls, *traces: Any) -> list[str]:
        notes: list[str] = []
        for trace in traces:
            if trace is None or cls._trace_is_usable(trace, min_score=0.65):
                continue
            evidence_state = str(getattr(getattr(trace, "evidence_state", ""), "value", getattr(trace, "evidence_state", "")))
            value_state = str(getattr(getattr(trace, "value_state", ""), "value", getattr(trace, "value_state", "")))
            notes.append(
                f"{getattr(trace, 'label', getattr(trace, 'field', 'field'))}证据偏弱: 值状态={value_state}，证据状态={evidence_state}"
            )
        return notes[:6]

    @classmethod
    def _normalize_dimensions(cls, value: Any, baseline_dimensions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        baseline_map = {str(item.get("dimension")): dict(item) for item in baseline_dimensions if isinstance(item, dict)}
        normalized_map = {name: dict(baseline_map.get(name, {"dimension": name, "score": 5.0, "trend": "稳定", "key_metrics": {}, "analysis": "", "concerns": []})) for name in _DIMENSIONS}
        for item in value if isinstance(value, list) else []:
            if not isinstance(item, dict):
                continue
            dimension = str(item.get("dimension") or "").strip()
            if dimension not in normalized_map:
                continue
            merged = dict(normalized_map[dimension])
            merged["score"] = round(max(0.0, min(float(cls._safe_float(item.get("score")) or merged.get("score") or 0.0), 10.0)), 1)
            trend = str(item.get("trend") or merged.get("trend") or "稳定").strip()
            merged["trend"] = trend if trend in {"改善", "稳定", "恶化"} else merged.get("trend", "稳定")
            merged["analysis"] = str(item.get("analysis") or merged.get("analysis") or "").strip()
            key_metrics = item.get("key_metrics")
            merged["key_metrics"] = key_metrics if isinstance(key_metrics, dict) else dict(merged.get("key_metrics", {}))
            merged["concerns"] = cls._normalize_text_list(item.get("concerns"), limit=5) or list(merged.get("concerns", []) or [])
            normalized_map[dimension] = merged
        return [normalized_map[name] for name in _DIMENSIONS]

    @classmethod
    def _build_dimension(
        cls,
        *,
        dimension: str,
        score: float,
        trend: str,
        key_metrics: dict[str, Any],
        analysis: str,
        concerns: list[str],
    ) -> dict[str, Any]:
        return {
            "dimension": dimension,
            "score": round(max(0.0, min(score, 10.0)), 1),
            "trend": trend if trend in {"改善", "稳定", "恶化"} else "稳定",
            "key_metrics": key_metrics,
            "analysis": analysis,
            "concerns": cls._unique(concerns, limit=5),
        }

    @staticmethod
    def _trend_from_values(primary: Any, secondary: Any) -> str:
        values = [FinancialAgent._safe_float(primary), FinancialAgent._safe_float(secondary)]
        values = [value for value in values if value is not None]
        if not values:
            return "稳定"
        positive = sum(1 for value in values if value > 0)
        negative = sum(1 for value in values if value < 0)
        if positive == len(values):
            return "改善"
        if negative == len(values):
            return "恶化"
        return "稳定"

    @staticmethod
    def _normalize_text_list(value: Any, *, limit: int) -> list[str]:
        if isinstance(value, list):
            return FinancialAgent._unique(value, limit=limit)
        if value in (None, "", [], {}):
            return []
        return FinancialAgent._unique([value], limit=limit)

    @staticmethod
    def _contains_unsupported_cashflow_claim(value: Any) -> bool:
        text = str(value or "").strip()
        if not text:
            return False
        cashflow_terms = ("现金流", "净现比", "经营活动产生的现金流量净额")
        positive_terms = ("良好", "匹配", "较好", "改善", "稳健", "充沛", "健康", "优异", "转好", "转正")
        caution_terms = ("待验证", "缺失", "异常", "存疑", "不匹配", "矛盾", "不一致", "风险", "不足", "偏弱")
        return any(term in text for term in cashflow_terms) and any(term in text for term in positive_terms) and not any(
            term in text for term in caution_terms
        )

    @staticmethod
    def _unique(items: list[Any], *, limit: int) -> list[str]:
        deduped: list[str] = []
        for item in items:
            if item in (None, "", [], {}):
                continue
            text = str(item).strip()
            if not text or text in deduped:
                continue
            deduped.append(text)
            if len(deduped) >= limit:
                break
        return deduped

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value in (None, "", "-"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _fmt(v: Any) -> str:
        if v is None or v == "":
            return "N/A"
        try:
            n = float(v)
            if abs(n) >= 1e8:
                return f"{n/1e8:.1f}亿"
            if abs(n) >= 1e4:
                return f"{n/1e4:.1f}万"
            return f"{n:.2f}"
        except (ValueError, TypeError):
            return str(v)

    @staticmethod
    def _fmt_pct(v: Any) -> str:
        if v is None or v == "":
            return "N/A"
        try:
            return f"{float(v):.1f}%"
        except (ValueError, TypeError):
            return str(v)
