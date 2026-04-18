"""Deep review agent for counter-thesis and assumption stress testing."""

from __future__ import annotations

from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.exceptions import AgentValidationError
from investresearch.core.models import AgentInput, AgentOutput, AgentStatus

SYSTEM_PROMPT = """你是一位A股研究主管，负责在形成最终投资结论前做深度复核。

你的任务不是重复已有结论，而是专注于：
1. 反方论证
2. 关键假设
3. 假设敏感性
4. 什么新证据会改变当前判断

请严格输出JSON：
{
  "counter_thesis": "一句话反方观点",
  "supporting_signals": ["支持当前主论点的证据1", "证据2"],
  "challenge_points": ["最可能推翻主结论的挑战1", "挑战2"],
  "key_assumptions": ["关键假设1", "关键假设2"],
  "sensitivity_checks": ["最敏感变量1", "最敏感变量2"],
  "what_would_change_my_mind": ["会改变判断的新证据1", "新证据2"],
  "confidence_adjustment": "raise/keep/lower",
  "review_summary": "100字内总结"
}

要求：
- challenge_points 至少2条
- key_assumptions 至少2条
- sensitivity_checks 至少2条
- what_would_change_my_mind 至少2条
- confidence_adjustment 只能是 raise/keep/lower
- review_summary 不要重复原始结论，而要点出最关键的不确定性
"""


class DeepResearchReviewAgent(AgentBase[AgentInput, AgentOutput]):
    """Perform a second-pass deep review for deep mode."""

    agent_name: str = "deep_review"
    execution_mode: str = "llm"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        context = input_data.context
        cleaned = context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行深度复核"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始深度复核 | {stock_code} {stock_name}")

        allow_live_llm = bool(input_data.context.get("_allow_live_llm"))
        model = self._get_model()
        if allow_live_llm:
            result = await self.llm.call_json(
                prompt=self._build_prompt(stock_code, stock_name, context),
                system_prompt=SYSTEM_PROMPT,
                model=model,
            )
            result = self._normalize_result(result)
            summary = result.get("review_summary", "已完成深度复核")
            llm_invoked = True
            model_used = model
            runtime_mode = "llm"
        else:
            result = self._build_deterministic_review(context)
            summary = result.get("review_summary", "已完成深度复核")
            llm_invoked = False
            model_used = None
            runtime_mode = "deterministic"

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"deep_review": result},
            data_sources=["综合分析结果"],
            confidence=0.76,
            summary=summary,
            execution_mode=runtime_mode,
            llm_invoked=llm_invoked,
            model_used=model_used,
        )

    def validate_output(self, output: AgentOutput) -> None:
        if output.status != AgentStatus.SUCCESS:
            return

        payload = self._normalize_result(output.data.get("deep_review", {}))
        output.data["deep_review"] = payload
        errors: list[str] = []

        if not payload.get("counter_thesis"):
            errors.append("缺少 counter_thesis")
        if len(payload.get("challenge_points", [])) < 2:
            errors.append("challenge_points 至少需要2条")
        if len(payload.get("key_assumptions", [])) < 2:
            errors.append("key_assumptions 至少需要2条")
        if len(payload.get("sensitivity_checks", [])) < 2:
            errors.append("sensitivity_checks 至少需要2条")
        if len(payload.get("what_would_change_my_mind", [])) < 2:
            errors.append("what_would_change_my_mind 至少需要2条")
        if payload.get("confidence_adjustment") not in {"raise", "keep", "lower"}:
            errors.append("confidence_adjustment 只能是 raise/keep/lower")
        if not payload.get("review_summary"):
            errors.append("缺少 review_summary")

        if errors:
            raise AgentValidationError(self.agent_name, errors)

    def _get_model(self) -> str:
        return self.config.get_layer_model("decision_layer", task="deep_review")

    def _build_prompt(self, stock_code: str, stock_name: str, context: dict[str, Any]) -> str:
        cleaned = context.get("cleaned_data", {})
        realtime = cleaned.get("realtime", {})
        parts = [f"## 标的\n- 股票代码: {stock_code}\n- 股票名称: {stock_name or 'N/A'}"]

        if realtime:
            parts.extend(
                [
                    "## 当前快照",
                    f"- 最新价: {realtime.get('close', 'N/A')}",
                    f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}",
                    f"- PB(MRQ): {realtime.get('pb_mrq', 'N/A')}",
                    "",
                ]
            )

        module_fields = {
            "screening": ["verdict", "recommendation", "key_risks"],
            "financial_analysis": ["overall_score", "trend_summary", "cashflow_verification", "conclusion"],
            "business_model_analysis": ["model_score", "moat_overall", "profit_driver", "negative_view", "conclusion"],
            "industry_analysis": ["lifecycle", "competition_pattern", "prosperity_direction", "company_position", "conclusion"],
            "governance_analysis": ["governance_score", "management_integrity", "capital_allocation", "conclusion"],
            "valuation_analysis": ["valuation_level", "reasonable_range_low", "reasonable_range_high", "margin_of_safety", "conclusion"],
            "risk_analysis": ["overall_risk_level", "risk_score", "fatal_risks", "monitoring_points", "conclusion"],
        }

        for key, fields in module_fields.items():
            payload = context.get(key, {})
            if not payload:
                continue
            parts.append(f"## {key}")
            for field in fields:
                value = payload.get(field)
                if value in (None, "", [], {}):
                    continue
                parts.append(f"- {field}: {value}")
            parts.append("")

        parts.append("请做反方论证、关键假设与敏感性复核，不要重复总结已有模块。")
        return "\n".join(parts)

    @staticmethod
    def _build_deterministic_review(context: dict[str, Any]) -> dict[str, Any]:
        risk = context.get("risk_analysis", {})
        valuation = context.get("valuation_analysis", {})
        business = context.get("business_model_analysis", {})
        return {
            "counter_thesis": "当前结论仍可能被景气回落或估值验证不足所推翻。",
            "supporting_signals": ["财务与经营信号暂未出现明显恶化"],
            "challenge_points": list(risk.get("fatal_risks", []) or [])[:2] or ["需求兑现不及预期", "估值锚仍需验证"],
            "key_assumptions": [
                "主营业务不会显著恶化",
                "关键估值假设能够继续成立",
            ],
            "sensitivity_checks": [
                "需求增速",
                "利润率变化",
            ],
            "what_would_change_my_mind": [
                "季度经营数据明显低于预期",
                "估值区间重新下修",
            ],
            "confidence_adjustment": "lower" if valuation.get("evidence_status") != "ok" else "keep",
            "review_summary": business.get("negative_view") or "深度复核提示需要继续跟踪关键假设。",
        }

    @staticmethod
    def _normalize_result(result: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(result)
        normalized["supporting_signals"] = DeepResearchReviewAgent._normalize_list(
            normalized.get("supporting_signals", [])
        )
        normalized["challenge_points"] = DeepResearchReviewAgent._normalize_list(
            normalized.get("challenge_points", [])
        )
        normalized["key_assumptions"] = DeepResearchReviewAgent._normalize_list(
            normalized.get("key_assumptions", [])
        )
        normalized["sensitivity_checks"] = DeepResearchReviewAgent._normalize_list(
            normalized.get("sensitivity_checks", [])
        )
        normalized["what_would_change_my_mind"] = DeepResearchReviewAgent._normalize_list(
            normalized.get("what_would_change_my_mind", [])
        )
        normalized["confidence_adjustment"] = str(
            normalized.get("confidence_adjustment") or "keep"
        ).strip().lower()
        return normalized

    @staticmethod
    def _normalize_list(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if value in (None, ""):
            return []
        return [str(value).strip()]
