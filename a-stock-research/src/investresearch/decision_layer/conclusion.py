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
)

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

        prompt = self._build_prompt(stock_code, stock_name, input_data.context)
        model = self._get_model()

        result = await self.llm.call_json(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            model=model,
        )

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

        conclusion = output.data.get("conclusion", {})
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
