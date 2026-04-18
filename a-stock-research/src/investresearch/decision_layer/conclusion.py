"""投资结论Agent - 汇总全部分析结论生成标准化投资结论卡片

输出规格:
- recommendation: 买入(强烈)/买入(谨慎)/持有/观望/卖出
- 目标价区间
- 风险等级
- 买入/卖出理由
- 核心假设
- 跟踪指标
- 仓位建议
- 持有周期
"""

from __future__ import annotations

from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.exceptions import AgentValidationError
from investresearch.core.models import (
    AgentInput,
    AgentOutput,
    AgentStatus,
    MonitoringLayer,
    MonitoringPlanItem,
)
from investresearch.core.trust import build_process_consistency_notes, merge_evidence_refs

from .formatting import fmt_cap

SYSTEM_PROMPT = """你是一位资深的A股投资决策分析师，擅长将多维度的研究结果整合为清晰的投资结论。

## 你的任务
根据提供的全部分析结果，生成标准化的投资结论卡片。

## 输出格式（严格JSON）
```json
{
  "recommendation": "买入(强烈)/买入(谨慎)/持有/观望/卖出",
  "confidence_level": "高/中/低",
  "target_price_low": 22.0,
  "target_price_high": 28.0,
  "current_price": 20.5,
  "upside_pct": 20.0,
  "risk_level": "低/中/高/极高",
  "key_reasons_buy": ["理由1", "理由2"],
  "key_reasons_sell": ["理由1", "理由2"],
  "key_assumptions": ["假设1", "假设2"],
  "monitoring_points": ["指标1", "指标2", "指标3"],
  "position_advice": "仓位建议",
  "holding_period": "建议持有周期",
  "stop_loss_price": 18.0,
  "conclusion_summary": "一段话综合结论"
}
```

## 判定逻辑
### recommendation判定标准
- **买入(强烈)**: 多维度分析均正面，估值明显低于合理区间，风险可控
- **买入(谨慎)**: 整体正面但存在需要跟踪的不确定因素
- **持有**: 已持有的建议继续持有，等待更明确信号
- **观望**: 分析结果不明确，或存在较大不确定性
- **卖出**: 存在重大风险或估值严重偏高

### confidence_level判定标准
- **高**: 数据充分、分析一致、结论明确
- **中**: 数据基本充分但部分维度存在不确定性
- **低**: 数据覆盖不足或分析结果矛盾

### 约束
- recommendation必须是以上5种之一
- risk_level必须是"低"/"中"/"高"/"极高"之一
- key_reasons_buy至少1条（即使recommendation为卖出，也需列出可能的买入理由）
- key_assumptions至少2条
- monitoring_points至少3条
- target_price_low和target_price_high必须基于估值分析和情景测算
- conclusion_summary需在200字以内
- 所有结论必须有分析数据支撑，不得凭空推断
"""


class ConclusionAgent(AgentBase[AgentInput, AgentOutput]):
    """投资结论Agent - 生成标准化投资结论卡片"""

    agent_name: str = "conclusion"
    execution_mode: str = "hybrid"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行投资结论生成"""
        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or input_data.context.get(
            "cleaned_data", {}
        ).get("stock_info", {}).get("name", "")
        self.logger.info(f"开始生成投资结论 | {stock_code} {stock_name}")

        baseline = self._build_conclusion(stock_code, stock_name, input_data.context)
        result = dict(baseline)
        allow_live_llm = bool(input_data.context.get("_allow_live_llm"))
        llm_invoked = False
        model_used: str | None = None
        runtime_mode = "deterministic"

        if allow_live_llm:
            try:
                prompt = self._build_prompt(stock_code, stock_name, input_data.context)
                model = self._get_model()
                llm_result = await self.llm.call_json(
                    prompt=prompt,
                    system_prompt=SYSTEM_PROMPT,
                    model=model,
                )
                llm_invoked = True
                model_used = model
                runtime_mode = "llm"
                result = self._merge_llm_conclusion(baseline, llm_result)
            except Exception as exc:
                self.logger.warning(f"结论层LLM不可用，退回规则兜底 | {exc}")

        result = self._apply_guardrails(result, baseline, input_data.context)
        result = self._normalize_conclusion(result)

        rec = result.get("recommendation", "未知")
        risk = result.get("risk_level", "未知")
        summary = f"结论: {rec} | 风险: {risk}"
        self.logger.info(f"投资结论完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"conclusion": result},
            data_sources=["综合分析结果"],
            confidence=self._confidence_to_score(result.get("confidence_level")),
            summary=summary,
            execution_mode=runtime_mode,
            llm_invoked=llm_invoked,
            model_used=model_used,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验投资结论输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        conclusion = self._normalize_conclusion(output.data.get("conclusion", {}))
        output.data["conclusion"] = conclusion
        errors = []

        # recommendation校验
        recommendation = conclusion.get("recommendation")
        valid_recs = {"买入(强烈)", "买入(谨慎)", "持有", "观望", "卖出"}
        if recommendation not in valid_recs:
            errors.append(
                f"recommendation无效: {recommendation}，必须为: {valid_recs}"
            )

        # risk_level校验
        risk_level = conclusion.get("risk_level")
        valid_risks = {"低", "中", "高", "极高"}
        if risk_level not in valid_risks:
            errors.append(f"risk_level无效: {risk_level}，必须为: {valid_risks}")

        # confidence_level校验
        confidence = conclusion.get("confidence_level")
        valid_confidences = {"高", "中", "低"}
        if confidence not in valid_confidences:
            errors.append(
                f"confidence_level无效: {confidence}，必须为: {valid_confidences}"
            )

        # 必填列表校验
        reasons_buy = conclusion.get("key_reasons_buy", [])
        if not isinstance(reasons_buy, list) or len(reasons_buy) < 1:
            errors.append("key_reasons_buy至少需要1条")

        assumptions = conclusion.get("key_assumptions", [])
        if not isinstance(assumptions, list) or len(assumptions) < 2:
            errors.append("key_assumptions至少需要2条")

        monitors = conclusion.get("monitoring_points", [])
        if not isinstance(monitors, list) or len(monitors) < 3:
            errors.append("monitoring_points至少需要3条")

        # summary校验
        summary = conclusion.get("conclusion_summary", "")
        if not summary:
            errors.append("缺少conclusion_summary")

        if errors:
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取决策层模型"""
        return self.config.get_layer_model("decision_layer", task="conclusion")

    @staticmethod
    def _normalize_conclusion(conclusion: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(conclusion)
        normalized["recommendation"] = ConclusionAgent._normalize_recommendation(
            normalized.get("recommendation")
        )
        normalized["risk_level"] = ConclusionAgent._normalize_simple_label(
            normalized.get("risk_level"),
            {
                "低风险": "低",
                "中风险": "中",
                "中等风险": "中",
                "高风险": "高",
                "极高风险": "极高",
            },
        )
        normalized["confidence_level"] = ConclusionAgent._normalize_simple_label(
            normalized.get("confidence_level"),
            {
                "高置信": "高",
                "高信心": "高",
                "中置信": "中",
                "中等": "中",
                "中等信心": "中",
                "低置信": "低",
                "低信心": "低",
            },
        )
        normalized["consistency_notes"] = ConclusionAgent._unique(
            ConclusionAgent._as_list(normalized.get("consistency_notes", [])),
            limit=5,
        )
        return normalized

    @staticmethod
    def _normalize_recommendation(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip().replace("（", "(").replace("）", ")").replace(" ", "")
        aliases = {
            "强烈买入": "买入(强烈)",
            "买入强烈": "买入(强烈)",
            "谨慎买入": "买入(谨慎)",
            "买入谨慎": "买入(谨慎)",
            "增持": "买入(谨慎)",
            "中性": "观望",
        }
        return aliases.get(text, text)

    @staticmethod
    def _normalize_simple_label(value: Any, aliases: dict[str, str]) -> str:
        if value is None:
            return ""
        text = str(value).strip().replace("（", "(").replace("）", ")").replace(" ", "")
        return aliases.get(text, text)

    def _build_prompt(
        self, stock_code: str, stock_name: str, context: dict[str, Any]
    ) -> str:
        """构建投资结论提示词 - 精炼所有分析结论"""
        parts = [f"## 标的: {stock_code} {stock_name}\n"]
        parts.append("请根据以下分析结论，生成标准化投资结论卡片。\n")

        # 当前价格
        cleaned = context.get("cleaned_data", {})
        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append(f"### 当前股价: {realtime.get('close', 'N/A')}")
            parts.append(f"- 市值: {fmt_cap(realtime.get('market_cap'))}")
            parts.append(f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}")
            parts.append(f"- PB(MRQ): {realtime.get('pb_mrq', 'N/A')}")
            parts.append("")

        # 初筛结论
        screening = context.get("screening", {})
        if screening:
            parts.append(f"### 初筛: {screening.get('verdict', 'N/A')}")
            parts.append(f"- 建议: {screening.get('recommendation', 'N/A')}")
            parts.append("")

        cashflow_caution = self._cashflow_caution_reason(
            cleaned,
            context.get("risk_analysis", {}),
        )
        if cashflow_caution:
            parts.append("### 数据质量约束")
            parts.append(f"- {cashflow_caution}")
            parts.append(
                "- 若提及现金流，只能表述为“待验证/缺失/口径异常/需后续财报确认”，不得将现金流改善、现金流与利润匹配良好、净现比改善作为买入理由。"
            )
            parts.append("")

        quality_gate = cleaned.get("quality_gate", {})
        if quality_gate:
            parts.append("### 证据闸门状态")
            parts.append(f"- blocked: {quality_gate.get('blocked', False)}")
            parts.append(f"- 核心证据分: {quality_gate.get('core_evidence_score', 0):.0%}")
            parts.append(f"- 阻断字段: {self._stringify(quality_gate.get('blocking_fields', []), default='无')}")
            parts.append(f"- 弱证据字段: {self._stringify(quality_gate.get('weak_fields', []), default='无')}")
            for note in list(quality_gate.get("consistency_notes", []) or [])[:4]:
                parts.append(f"- 约束说明: {note}")
            parts.append("")

        # 各分析结论（精炼版）
        analysis_summaries = {
            "financial_analysis": ("财务分析", ["overall_score", "conclusion"]),
            "business_model_analysis": ("商业模式", ["model_score", "moat_overall", "conclusion"]),
            "industry_analysis": ("行业分析", ["lifecycle", "competition_pattern", "conclusion"]),
            "governance_analysis": ("治理分析", ["governance_score", "conclusion"]),
            "valuation_analysis": (
                "估值分析",
                ["valuation_level", "reasonable_range_low", "reasonable_range_high", "conclusion"],
            ),
            "risk_analysis": ("风险分析", ["overall_risk_level", "risk_score", "conclusion"]),
        }

        for key, (label, fields) in analysis_summaries.items():
            data = context.get(key, {})
            if data:
                parts.append(f"### {label}")
                for field in fields:
                    val = data.get(field)
                    if val is not None:
                        parts.append(f"- {field}: {val}")
                parts.append("")

        deep_review = context.get("deep_research_review", {})
        if deep_review:
            parts.append("### 深度复核")
            for field in (
                "counter_thesis",
                "review_summary",
                "confidence_adjustment",
            ):
                value = deep_review.get(field)
                if value:
                    parts.append(f"- {field}: {value}")
            for field in (
                "challenge_points",
                "key_assumptions",
                "sensitivity_checks",
                "what_would_change_my_mind",
            ):
                values = deep_review.get(field)
                if values:
                    parts.append(f"- {field}: {values}")
            parts.append("")

        parts.append(
            "请综合以上所有分析结论，生成标准化投资结论卡片。"
            "目标价区间需基于估值分析和情景测算综合确定。"
        )
        return "\n".join(parts)

    def _build_conclusion(self, stock_code: str, stock_name: str, context: dict[str, Any]) -> dict[str, Any]:
        cleaned = context.get("cleaned_data", {})
        realtime = cleaned.get("realtime", {})
        screening = context.get("screening", {})
        industry = context.get("industry_analysis", {})
        business = context.get("business_model_analysis", {})
        governance = context.get("governance_analysis", {})
        financial = context.get("financial_analysis", {})
        valuation = context.get("valuation_analysis", {})
        risk = context.get("risk_analysis", {})
        deep_review = context.get("deep_research_review", {})
        quality_gate = cleaned.get("quality_gate", {})
        cashflow_caution = self._cashflow_caution_reason(cleaned, risk)

        current_price = self._safe_float(valuation.get("current_price") or realtime.get("close"))
        low = self._safe_float(valuation.get("reasonable_range_low"))
        high = self._safe_float(valuation.get("reasonable_range_high"))
        midpoint = (low + high) / 2 if low is not None and high is not None else None
        upside_pct = (
            round((midpoint / current_price - 1) * 100, 2)
            if midpoint is not None and current_price not in (None, 0)
            else None
        )

        evidence_refs = merge_evidence_refs(
            valuation.get("evidence_refs", []),
            risk.get("evidence_refs", []),
            governance.get("evidence_refs", []),
            business.get("evidence_refs", []),
            industry.get("evidence_refs", []),
        )

        risk_level = str(risk.get("overall_risk_level") or "中")
        confidence_level = self._infer_confidence(cleaned, valuation, governance, industry)
        recommendation = self._infer_recommendation(
            screening=screening,
            valuation=valuation,
            risk_level=risk_level,
            confidence_level=confidence_level,
            upside_pct=upside_pct,
            business=business,
            governance=governance,
        )

        key_reasons_buy = self._unique(
            [
                valuation.get("conclusion"),
                business.get("conclusion"),
                industry.get("conclusion"),
                None
                if cashflow_caution
                and self._contains_unsupported_cashflow_claim(financial.get("conclusion"))
                else financial.get("conclusion"),
            ],
            limit=4,
        ) or ["现阶段仅保留研究跟踪价值，待更多证据支持再强化买入逻辑"]
        key_reasons_sell = self._unique(
            [
                *list(risk.get("fatal_risks", []) or []),
                *list(screening.get("key_risks", []) or []),
                "关键数据缺口仍需补齐",
            ],
            limit=4,
        )

        key_assumptions = self._unique(
            [
                "财务与经营趋势不会出现显著恶化",
                "行业政策和景气方向不发生突变",
                "治理信息缺口能够通过后续公告持续补齐",
                *list(deep_review.get("key_assumptions", []) or []),
            ],
            limit=4,
        )
        monitoring_points = self._unique(
            list(risk.get("monitoring_points", []) or [])
            + list((screening.get("key_risks", []) or []))
            + list(deep_review.get("what_would_change_my_mind", []) or []),
            limit=5,
        ) or ["经营兑现", "行业景气", "治理公告"]
        monitoring_plan = self._build_monitoring_plan(monitoring_points)
        failure_conditions = self._unique(
            list(risk.get("fatal_risks", []) or [])
            + list(deep_review.get("challenge_points", []) or [])
            + ["主营业务兑现低于预期", "治理风险或政策风险显著升级"],
            limit=5,
        )
        catalysts = self._build_catalysts(cleaned)
        conclusion_summary = self._build_summary(stock_code, stock_name, recommendation, confidence_level, valuation, risk_level)
        consistency_notes = build_process_consistency_notes(
            screening=screening,
            valuation=valuation,
            conclusion={
                "recommendation": recommendation,
            },
        ) + list(quality_gate.get("consistency_notes", []) or [])

        return {
            "recommendation": recommendation,
            "confidence_level": confidence_level,
            "target_price_low": low,
            "target_price_high": high,
            "current_price": current_price,
            "upside_pct": upside_pct,
            "risk_level": risk_level,
            "key_reasons_buy": key_reasons_buy,
            "key_reasons_sell": key_reasons_sell,
            "core_thesis": self._unique(
                [
                    business.get("profit_driver"),
                    industry.get("company_position"),
                    None
                    if cashflow_caution
                    and self._contains_unsupported_cashflow_claim(financial.get("trend_summary"))
                    else financial.get("trend_summary"),
                ],
                limit=3,
            ),
            "expectation_gap": self._build_expectation_gap(valuation, confidence_level),
            "catalysts": catalysts,
            "key_assumptions": key_assumptions,
            "valuation_range": f"{low}-{high}" if low is not None and high is not None else "估值区间待验证",
            "return_breakdown": self._build_return_breakdown(valuation, business),
            "major_risks": self._unique(list(risk.get("fatal_risks", []) or []) + list(key_reasons_sell), limit=5),
            "failure_conditions": failure_conditions,
            "monitoring_points": monitoring_points,
            "monitoring_plan": [item.model_dump(mode="json") for item in monitoring_plan],
            "position_advice": self._build_position_advice(recommendation, confidence_level),
            "holding_period": "6-12个月" if recommendation.startswith("买入") else "等待关键信息补齐后再决定",
            "stop_loss_price": round(current_price * 0.88, 2) if current_price and recommendation.startswith("买入") else None,
            "consistency_notes": consistency_notes[:5],
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
            "execution_trace": list(context.get("execution_trace", []) or []),
            "conclusion_summary": conclusion_summary[:180],
        }

    def _merge_llm_conclusion(
        self,
        baseline: dict[str, Any],
        llm_result: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(baseline)
        for key, value in llm_result.items():
            if value in (None, "", [], {}):
                continue
            merged[key] = value

        for key, limit in (
            ("key_reasons_buy", 4),
            ("key_reasons_sell", 4),
            ("key_assumptions", 4),
            ("monitoring_points", 5),
            ("core_thesis", 4),
            ("major_risks", 5),
            ("failure_conditions", 5),
            ("catalysts", 4),
            ("return_breakdown", 4),
        ):
            merged[key] = self._unique(
                self._as_list(llm_result.get(key, [])) + self._as_list(baseline.get(key, [])),
                limit=limit,
            )
        return merged

    def _apply_guardrails(
        self,
        conclusion: dict[str, Any],
        baseline: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        guarded = self._merge_llm_conclusion(baseline, conclusion)
        cleaned = context.get("cleaned_data", {})
        valuation = context.get("valuation_analysis", {})
        governance = context.get("governance_analysis", {})
        industry = context.get("industry_analysis", {})
        risk = context.get("risk_analysis", {})
        deep_review = context.get("deep_research_review", {})
        quality_gate = cleaned.get("quality_gate", {}) if isinstance(cleaned, dict) else {}
        cashflow_caution = self._cashflow_caution_reason(cleaned, risk)

        coverage = float(cleaned.get("coverage_ratio", 0.0) or 0.0)
        valuation_ok = valuation.get("evidence_status") == "ok"
        critical_ok = sum(
            1
            for item in (valuation, governance, industry)
            if item.get("evidence_status") == "ok"
        )
        blocked = bool(quality_gate.get("blocked"))
        core_evidence = float(quality_gate.get("core_evidence_score", 0.0) or 0.0)
        blocking_fields = [str(item) for item in list(quality_gate.get("blocking_fields", []) or []) if str(item).strip()]
        weak_fields = [str(item) for item in list(quality_gate.get("weak_fields", []) or []) if str(item).strip()]

        if blocked or blocking_fields or core_evidence < 0.7:
            guarded["recommendation"] = "观望"
            guarded["target_price_low"] = None
            guarded["target_price_high"] = None
            guarded["upside_pct"] = None
            guarded["valuation_range"] = "核心证据不足，暂不输出估值区间"

        if not valuation_ok:
            guarded["target_price_low"] = baseline.get("target_price_low")
            guarded["target_price_high"] = baseline.get("target_price_high")
            guarded["upside_pct"] = baseline.get("upside_pct")
            if str(guarded.get("recommendation", "")).startswith("买入"):
                guarded["recommendation"] = "观望"

        risk_level = guarded.get("risk_level") or baseline.get("risk_level") or "中"
        if not (blocked or blocking_fields):
            if risk_level == "极高":
                guarded["recommendation"] = "卖出"
            elif risk_level == "高" and guarded.get("recommendation") == "买入(强烈)":
                guarded["recommendation"] = "买入(谨慎)"

        confidence_level = guarded.get("confidence_level") or baseline.get("confidence_level") or "中"
        if blocked or blocking_fields or core_evidence < 0.7:
            confidence_level = "低"
        elif coverage < 0.45 or critical_ok == 0:
            confidence_level = "低"
        elif coverage < 0.7 and confidence_level == "高":
            confidence_level = "中"
        if cashflow_caution and confidence_level == "高":
            confidence_level = "中"

        adjustment = str(deep_review.get("confidence_adjustment") or "").lower()
        if adjustment in {"lower", "decrease"}:
            confidence_level = self._lower_confidence_label(confidence_level)
        elif adjustment in {"raise", "increase"} and coverage >= 0.75 and critical_ok >= 2:
            confidence_level = self._raise_confidence_label(confidence_level)
        guarded["confidence_level"] = confidence_level

        if cashflow_caution:
            cashflow_risk = "关键现金流字段缺失或口径待验证，暂不能用利润含金量支撑买入逻辑"
            guarded["key_reasons_buy"] = self._filter_unsupported_cashflow_claims(
                guarded.get("key_reasons_buy", []),
                fallback=self._as_list(baseline.get("key_reasons_buy", [])),
                minimum=1,
                limit=4,
            )
            guarded["core_thesis"] = self._filter_unsupported_cashflow_claims(
                guarded.get("core_thesis", []),
                fallback=self._as_list(baseline.get("core_thesis", [])),
                minimum=2,
                limit=4,
            )
            guarded["key_reasons_sell"] = self._unique(
                [cashflow_risk] + self._as_list(guarded.get("key_reasons_sell", [])),
                limit=4,
            )
            guarded["major_risks"] = self._unique(
                [cashflow_risk] + self._as_list(guarded.get("major_risks", [])),
                limit=5,
            )
            guarded["failure_conditions"] = self._unique(
                [cashflow_risk] + self._as_list(guarded.get("failure_conditions", [])),
                limit=5,
            )

        guarded["key_reasons_buy"] = self._ensure_minimum_list(
            self._as_list(guarded.get("key_reasons_buy", [])),
            ["当前仅保留跟踪价值，买入逻辑仍需更多证据支持"],
            minimum=1,
            limit=4,
        )
        guarded["key_reasons_sell"] = self._ensure_minimum_list(
            self._as_list(guarded.get("key_reasons_sell", [])),
            ["关键数据与验证条件尚未齐备"],
            minimum=1,
            limit=4,
        )
        guarded["key_assumptions"] = self._ensure_minimum_list(
            self._as_list(guarded.get("key_assumptions", [])),
            self._as_list(baseline.get("key_assumptions", [])),
            minimum=2,
            limit=4,
        )
        guarded["monitoring_points"] = self._ensure_minimum_list(
            self._as_list(guarded.get("monitoring_points", [])),
            self._as_list(baseline.get("monitoring_points", [])) or ["经营兑现", "行业景气", "治理公告"],
            minimum=3,
            limit=5,
        )
        guarded["monitoring_plan"] = [
            item.model_dump(mode="json")
            for item in self._build_monitoring_plan(self._as_list(guarded.get("monitoring_points", [])))
        ]
        guarded["failure_conditions"] = self._ensure_minimum_list(
            self._as_list(guarded.get("failure_conditions", [])),
            self._as_list(baseline.get("failure_conditions", [])),
            minimum=2,
            limit=5,
        )
        guarded["major_risks"] = self._unique(
            self._as_list(guarded.get("major_risks", []))
            + self._as_list(guarded.get("key_reasons_sell", [])),
            limit=5,
        )
        if blocked or blocking_fields:
            gate_reason = (
                f"核心证据闸门未满足，阻断字段包括: {', '.join(blocking_fields[:5])}"
                if blocking_fields
                else "核心证据闸门未满足，当前结论仅允许保守输出"
            )
            guarded["key_reasons_sell"] = self._unique(
                [gate_reason] + self._as_list(guarded.get("key_reasons_sell", [])),
                limit=4,
            )
            guarded["major_risks"] = self._unique(
                [gate_reason] + self._as_list(guarded.get("major_risks", [])),
                limit=5,
            )
        elif weak_fields:
            weak_reason = f"部分核心字段仍为弱证据: {', '.join(weak_fields[:5])}"
            guarded["key_reasons_sell"] = self._unique(
                [weak_reason] + self._as_list(guarded.get("key_reasons_sell", [])),
                limit=4,
            )
        guarded["position_advice"] = self._build_position_advice(
            guarded["recommendation"],
            guarded["confidence_level"],
        )
        guarded["holding_period"] = (
            "6-12个月" if str(guarded.get("recommendation", "")).startswith("买入") else "等待关键信息补齐后再决定"
        )

        current_price = self._safe_float(guarded.get("current_price"))
        if str(guarded.get("recommendation", "")).startswith("买入") and current_price:
            guarded["stop_loss_price"] = round(current_price * 0.88, 2)
        else:
            guarded["stop_loss_price"] = None

        low = self._safe_float(guarded.get("target_price_low"))
        high = self._safe_float(guarded.get("target_price_high"))
        if low is not None and high is not None:
            guarded["valuation_range"] = f"{low}-{high}"
        else:
            guarded["valuation_range"] = baseline.get("valuation_range", "估值区间待验证")

        guarded["expectation_gap"] = guarded.get("expectation_gap") or baseline.get("expectation_gap", "")
        guarded["execution_trace"] = list(context.get("execution_trace", []) or [])
        guarded["consistency_notes"] = self._unique(
            self._as_list(guarded.get("consistency_notes", []))
            + build_process_consistency_notes(
                screening=context.get("screening", {}),
                valuation=valuation,
                conclusion=guarded,
            )
            + self._as_list(quality_gate.get("consistency_notes", [])),
            limit=5,
        )

        summary = str(guarded.get("conclusion_summary") or baseline.get("conclusion_summary") or "").strip()
        if cashflow_caution and self._contains_unsupported_cashflow_claim(summary):
            summary = str(baseline.get("conclusion_summary") or "").strip()
        if blocked or blocking_fields:
            summary = (
                f"{context.get('stock_code', '')} {context.get('stock_name', '')} 当前核心证据不足，"
                f"结论已降级为观望，需先补齐 {', '.join(blocking_fields[:3]) or '关键阻断字段'}。"
            ).strip()
        if not summary:
            summary = self._build_summary(
                context.get("stock_code", ""),
                context.get("stock_name", ""),
                guarded["recommendation"],
                guarded["confidence_level"],
                valuation,
                risk_level,
        )
        guarded["conclusion_summary"] = summary[:180]
        return guarded

    @staticmethod
    def _cashflow_caution_reason(cleaned: dict[str, Any], risk: dict[str, Any]) -> str:
        financials = cleaned.get("financials", []) or []
        suspect = any(
            isinstance(item, dict) and item.get("_cashflow_suspect_fields")
            for item in financials[:4]
        )
        missing_fields = set(cleaned.get("missing_fields", []) or [])
        missing_cashflow = any(
            field in missing_fields
            for field in (
                "financials.operating_cashflow",
                "financials.free_cashflow",
                "financials.cash_to_profit",
            )
        )
        risk_texts = " ".join(
            str(item)
            for item in (
                *(risk.get("fatal_risks", []) or []),
                risk.get("conclusion", ""),
            )
        )
        risk_flag = any(
            token in risk_texts for token in ("现金流", "真实性存疑", "口径", "量纲")
        )
        if suspect or missing_cashflow or risk_flag:
            return "现金流相关指标存在缺失或口径待验证情况，不能将其作为买入论据"
        return ""

    @staticmethod
    def _contains_unsupported_cashflow_claim(value: Any) -> bool:
        text = str(value or "").strip()
        if not text:
            return False
        cashflow_terms = ("现金流", "净现比", "经营活动产生的现金流量净额")
        positive_terms = ("良好", "匹配", "较好", "改善", "稳健", "充沛", "健康", "优异", "转好", "转正")
        caution_terms = ("待验证", "缺失", "异常", "存疑", "不匹配", "矛盾", "不一致", "风险")
        has_cashflow = any(term in text for term in cashflow_terms)
        has_positive = any(term in text for term in positive_terms)
        has_caution = any(term in text for term in caution_terms)
        return has_cashflow and has_positive and not has_caution

    def _filter_unsupported_cashflow_claims(
        self,
        items: Any,
        *,
        fallback: list[str] | None = None,
        minimum: int = 0,
        limit: int = 4,
    ) -> list[str]:
        filtered = [
            item
            for item in self._as_list(items)
            if not self._contains_unsupported_cashflow_claim(item)
        ]
        fallback_items = [
            item
            for item in self._as_list(fallback or [])
            if not self._contains_unsupported_cashflow_claim(item)
        ]
        return self._ensure_minimum_list(
            filtered,
            fallback_items,
            minimum=minimum,
            limit=limit,
        )

    @staticmethod
    def _infer_confidence(cleaned: dict[str, Any], valuation: dict[str, Any], governance: dict[str, Any], industry: dict[str, Any]) -> str:
        quality_gate = cleaned.get("quality_gate", {}) if isinstance(cleaned, dict) else {}
        if quality_gate.get("blocked") or float(quality_gate.get("core_evidence_score", 0.0) or 0.0) < 0.7:
            return "低"
        coverage = float(cleaned.get("coverage_ratio", 0.0) or 0.0)
        critical_ok = sum(
            1
            for item in [valuation, governance, industry]
            if item.get("evidence_status") == "ok"
        )
        if coverage >= 0.75 and critical_ok >= 2:
            return "高"
        if coverage >= 0.5 and critical_ok >= 1:
            return "中"
        return "低"

    @staticmethod
    def _infer_recommendation(
        *,
        screening: dict[str, Any],
        valuation: dict[str, Any],
        risk_level: str,
        confidence_level: str,
        upside_pct: float | None,
        business: dict[str, Any],
        governance: dict[str, Any],
    ) -> str:
        valuation_level = valuation.get("valuation_level")
        moat = business.get("moat_overall")
        governance_score = governance.get("governance_score")
        verdict = screening.get("verdict")

        if risk_level == "极高":
            return "卖出"
        if confidence_level == "低" or valuation.get("evidence_status") != "ok":
            return "观望"
        if valuation_level == "严重高估":
            return "卖出"
        if valuation_level == "高估":
            return "观望"
        if valuation_level == "低估" and risk_level in {"低", "中"} and upside_pct is not None and upside_pct >= 15:
            if moat in {"宽", "窄"} and isinstance(governance_score, (int, float)) and governance_score >= 5.5 and verdict != "重点警示":
                return "买入(强烈)"
            return "买入(谨慎)"
        if valuation_level in {"低估", "合理"} and risk_level in {"低", "中"}:
            return "持有"
        return "观望"

    @staticmethod
    def _build_expectation_gap(valuation: dict[str, Any], confidence_level: str) -> str:
        if confidence_level == "低":
            return "缺少一致预期与分歧数据，暂不输出明确预期差。"
        valuation_level = valuation.get("valuation_level", "待验证")
        if valuation_level == "低估":
            return "当前市场定价较历史估值锚偏谨慎，若经营兑现，有估值修复空间。"
        if valuation_level == "高估":
            return "当前市场预期已较为饱满，需警惕兑现不及预期后的估值回落。"
        return "当前预期差不显著，需等待新增证据。"

    @staticmethod
    def _build_catalysts(cleaned: dict[str, Any]) -> list[str]:
        catalysts: list[str] = []
        announcements = cleaned.get("announcements", [])
        policy_documents = cleaned.get("policy_documents", [])
        if announcements:
            catalysts.append(f"公告催化：{announcements[0].get('title', '最新公告')}")  # type: ignore[index]
        if policy_documents:
            catalysts.append(f"政策催化：{policy_documents[0].get('title', '最新政策')}")  # type: ignore[index]
        catalysts.append("季度财报兑现与经营数据更新")
        return catalysts[:3]

    @staticmethod
    def _build_return_breakdown(valuation: dict[str, Any], business: dict[str, Any]) -> list[str]:
        components = ["盈利兑现"]
        if valuation.get("valuation_level") == "低估":
            components.append("估值修复")
        if business.get("moat_overall") in {"宽", "窄"}:
            components.append("竞争优势巩固")
        return components[:3]

    @staticmethod
    def _build_position_advice(recommendation: str, confidence_level: str) -> str:
        if recommendation == "买入(强烈)":
            return "可考虑分批建仓，但仍需跟踪风险触发指标"
        if recommendation == "买入(谨慎)":
            return "宜轻仓试错，等待更多证据验证后再提高仓位"
        if recommendation == "持有":
            return "以持有观察为主，不宜追高"
        if recommendation == "卖出":
            return "应以风险控制为先，避免逆势加仓"
        return "当前以观察和补证为主，仓位应保持保守"

    @staticmethod
    def _build_monitoring_plan(points: list[str]) -> list[MonitoringPlanItem]:
        layers = [MonitoringLayer.LEADING, MonitoringLayer.VALIDATION, MonitoringLayer.RISK_TRIGGER]
        plan: list[MonitoringPlanItem] = []
        for index, point in enumerate(points[:3]):
            layer = layers[index] if index < len(layers) else MonitoringLayer.VALIDATION
            plan.append(
                MonitoringPlanItem(
                    layer=layer,
                    metric=point,
                    trigger="出现显著偏离或新增事件即重算",
                    update_frequency="weekly" if layer != MonitoringLayer.RISK_TRIGGER else "daily",
                    rationale="用于验证投资逻辑是否持续成立",
                )
            )
        return plan

    @staticmethod
    def _build_summary(
        stock_code: str,
        stock_name: str,
        recommendation: str,
        confidence_level: str,
        valuation: dict[str, Any],
        risk_level: str,
    ) -> str:
        valuation_level = valuation.get("valuation_level", "待验证")
        return (
            f"{stock_code} {stock_name} 当前建议为{recommendation}，置信度{confidence_level}，"
            f"风险等级{risk_level}，估值判断{valuation_level}。"
        )

    @staticmethod
    def _confidence_to_score(label: Any) -> float:
        mapping = {"高": 0.85, "中": 0.68, "低": 0.45}
        return mapping.get(str(label), 0.5)

    @staticmethod
    def _lower_confidence_label(label: str) -> str:
        if label == "高":
            return "中"
        if label == "中":
            return "低"
        return "低"

    @staticmethod
    def _raise_confidence_label(label: str) -> str:
        if label == "低":
            return "中"
        if label == "中":
            return "高"
        return "高"

    @staticmethod
    def _ensure_minimum_list(
        items: list[Any],
        fallbacks: list[Any],
        *,
        minimum: int,
        limit: int,
    ) -> list[str]:
        values = ConclusionAgent._unique(items, limit=limit)
        if len(values) >= minimum:
            return values
        fallback_values = ConclusionAgent._unique(fallbacks, limit=limit)
        for item in fallback_values:
            if item not in values:
                values.append(item)
            if len(values) >= minimum or len(values) >= limit:
                break
        return values[:limit]

    @staticmethod
    def _as_list(value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        if value in (None, "", {}):
            return []
        return [value]

    @staticmethod
    def _unique(items: list[Any], limit: int = 5) -> list[str]:
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
