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
from investresearch.core.trust import merge_evidence_refs

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

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行投资结论生成"""
        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or input_data.context.get(
            "cleaned_data", {}
        ).get("stock_info", {}).get("name", "")
        self.logger.info(f"开始生成投资结论 | {stock_code} {stock_name}")

        result = self._build_conclusion(stock_code, stock_name, input_data.context)
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
            confidence=0.8,
            summary=summary,
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
                financial.get("conclusion"),
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
            ],
            limit=4,
        )
        monitoring_points = self._unique(
            list(risk.get("monitoring_points", []) or []) + list((screening.get("key_risks", []) or [])),
            limit=5,
        ) or ["经营兑现", "行业景气", "治理公告"]
        monitoring_plan = self._build_monitoring_plan(monitoring_points)
        failure_conditions = self._unique(
            list(risk.get("fatal_risks", []) or [])
            + ["主营业务兑现低于预期", "治理风险或政策风险显著升级"],
            limit=5,
        )
        catalysts = self._build_catalysts(cleaned)
        conclusion_summary = self._build_summary(stock_code, stock_name, recommendation, confidence_level, valuation, risk_level)

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
            "core_thesis": self._unique([business.get("profit_driver"), industry.get("company_position"), financial.get("trend_summary")], limit=3),
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
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
            "conclusion_summary": conclusion_summary[:180],
        }

    @staticmethod
    def _infer_confidence(cleaned: dict[str, Any], valuation: dict[str, Any], governance: dict[str, Any], industry: dict[str, Any]) -> str:
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
