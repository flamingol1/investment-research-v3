"""估值Agent - 多方法估值分析

估值方法（按适用性选择）:
1. PE相对估值: 当前PE vs 历史PE区间
2. PB相对估值: 当前PB vs 历史PB区间（适用于重资产行业）
3. DCF估值: 简化DCF验证当前股价隐含的增长假设
4. PEG估值: PE/G（适用于成长股）
5. PS估值: 市销率（适用于高增长/未盈利公司）
"""

from __future__ import annotations

from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import (
    AgentInput,
    AgentOutput,
    AgentStatus,
)

logger = get_logger("agent.valuation")

SYSTEM_PROMPT = """你是一位专业的A股估值分析专家。

## 你的任务
使用多种估值方法评估股票的合理价值，给出估值结论和合理价格区间。

## 估值方法（至少使用2种，按适用性选择）
1. **PE相对估值**: 当前PE对比历史PE范围(最低/中位/最高)，判断高估/低估
2. **PB相对估值**: 当前PB对比历史PB范围，适用于银行、地产等重资产行业
3. **PEG估值**: PE/G比率，适用于增长确定性较高的成长股，PEG<1偏低估
4. **PS估值**: 市销率对比，适用于高增长但尚未稳定盈利的公司
5. **DCF验证**: 不做精确DCF计算，而是反向验证——当前股价隐含了什么样的增长预期？这个预期是否合理？

## 分析要求
- 必须至少使用2种估值方法
- 必须计算合理价格区间（低-高）
- 必须说明每种方法的核心假设和局限性
- 必须计算历史估值分位数（如有数据）
- DCF仅用于验证隐含假设，不用于精确计算
- 最终给出明确的估值水平判断

## 输出格式（严格JSON）
```json
{
  "methods": [
    {
      "method": "PE",
      "intrinsic_value": 25.5,
      "upside_pct": 15.3,
      "assumptions": ["假设1", "假设2"],
      "limitations": ["局限1"]
    }
  ],
  "pe_percentile": 35.2,
  "pb_percentile": 28.5,
  "reasonable_range_low": 22.0,
  "reasonable_range_high": 30.0,
  "current_price": 22.1,
  "margin_of_safety": 0.0,
  "valuation_level": "低估|合理|高估|严重高估",
  "conclusion": "估值综合结论"
}
```

## 重要约束
- 至少2种方法，如果数据充分建议3种以上
- 合理价格区间必须合理反映不确定性（区间不能太窄）
- 估值水平判断必须有数据支撑
- 所有数值必须基于实际数据，不得编造
- 如果缺乏足够的历史估值数据，请在limitations中说明
"""


class ValuationAgent(AgentBase[AgentInput, AgentOutput]):
    """估值Agent - 多方法估值分析"""

    agent_name: str = "valuation"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行估值分析"""
        cleaned = input_data.context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行估值分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始估值分析 | {stock_code} {stock_name}")

        prompt = self._build_prompt(stock_code, stock_name, cleaned, input_data.context)
        model = self._get_model()

        result = await self.llm.call_json(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            model=model,
        )

        level = result.get("valuation_level", "未知")
        price = result.get("current_price")
        low = result.get("reasonable_range_low")
        high = result.get("reasonable_range_high")

        summary_parts = [f"估值水平: {level}"]
        if price and low and high:
            summary_parts.append(f"合理区间: {low}-{high}, 当前: {price}")
        summary = " | ".join(summary_parts)

        self.logger.info(f"估值分析完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"valuation": result},
            data_sources=["akshare", "baostock"],
            confidence=0.7,
            summary=summary,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验估值输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        valuation = output.data.get("valuation", {})
        errors = []

        methods = valuation.get("methods", [])
        if not isinstance(methods, list) or len(methods) < 2:
            errors.append(f"估值方法不足: 需要>=2种，实际{len(methods) if isinstance(methods, list) else '非列表'}")

        level = valuation.get("valuation_level")
        if level not in ("低估", "合理", "高估", "严重高估"):
            errors.append(f"valuation_level无效: {level}")

        if not valuation.get("conclusion"):
            errors.append("缺少conclusion")

        low = valuation.get("reasonable_range_low")
        high = valuation.get("reasonable_range_high")
        if low is not None and high is not None and low >= high:
            errors.append(f"合理区间异常: low={low} >= high={high}")

        if errors:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取分析层模型"""
        return self.config.get_layer_model("analysis_layer", task="valuation")

    def _build_prompt(self, stock_code: str, stock_name: str, cleaned: dict, context: dict) -> str:
        """构建估值分析提示词"""
        parts = [f"## 标的: {stock_code} {stock_name}\n"]

        # 当前估值指标
        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append("### 当前估值指标")
            parts.append(f"- 最新价: {realtime.get('close', 'N/A')}")
            parts.append(f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}")
            parts.append(f"- PB(MRQ): {realtime.get('pb_mrq', 'N/A')}")
            parts.append(f"- 总市值: {self._fmt_cap(realtime.get('market_cap'))}")
            parts.append("")

        # 历史估值数据摘要
        valuation = cleaned.get("valuation", [])
        if valuation:
            pe_values = [v.get("pe_ttm") for v in valuation if isinstance(v, dict) and v.get("pe_ttm") is not None]
            pb_values = [v.get("pb_mrq") for v in valuation if isinstance(v, dict) and v.get("pb_mrq") is not None]

            parts.append("### 历史估值范围（月度数据）")
            if pe_values:
                pe_float = [float(v) for v in pe_values if self._safe_float(v) is not None]
                if pe_float:
                    parts.append(f"- PE(TTM): 最低={min(pe_float):.1f} | 中位={sorted(pe_float)[len(pe_float)//2]:.1f} | 最高={max(pe_float):.1f} | 数据点={len(pe_float)}")
            if pb_values:
                pb_float = [float(v) for v in pb_values if self._safe_float(v) is not None]
                if pb_float:
                    parts.append(f"- PB(MRQ): 最低={min(pb_float):.2f} | 中位={sorted(pb_float)[len(pb_float)//2]:.2f} | 最高={max(pb_float):.2f} | 数据点={len(pb_float)}")

            # 显示最近几个月的估值数据
            parts.append("\n### 近期月度估值")
            parts.append("| 日期 | PE(TTM) | PB(MRQ) |")
            parts.append("|---|---|---|")
            for v in valuation[-6:]:
                if isinstance(v, dict):
                    parts.append(f"| {v.get('date', 'N/A')} | {v.get('pe_ttm', 'N/A')} | {v.get('pb_mrq', 'N/A')} |")
            parts.append("")

        # 关键财务数据（DCF假设用）
        financials = cleaned.get("financials", [])
        if financials:
            latest = financials[0] if isinstance(financials[0], dict) else {}
            parts.append("### DCF关键参数（最新期）")
            parts.append(f"- 报告期: {latest.get('report_date', 'N/A')}")
            parts.append(f"- 营业收入: {self._fmt_num(latest.get('revenue'))}")
            parts.append(f"- 净利润: {self._fmt_num(latest.get('net_profit'))}")
            parts.append(f"- 经营现金流: {self._fmt_num(latest.get('operating_cashflow'))}")
            parts.append(f"- 总资产: {self._fmt_num(latest.get('total_assets'))}")
            parts.append(f"- 净资产: {self._fmt_num(latest.get('equity'))}")
            parts.append(f"- ROE: {latest.get('roe', 'N/A')}")
            parts.append(f"- 营收增速: {latest.get('revenue_yoy', 'N/A')}")
            parts.append("")

        # 行业信息
        info = cleaned.get("stock_info", {})
        if info:
            parts.append(f"行业: {info.get('industry_sw', 'N/A')}")
            parts.append("")

        # 可选：上游财务分析结论
        financial_analysis = context.get("financial_analysis")
        if financial_analysis:
            parts.append("### 财务分析参考")
            parts.append(f"- 综合评分: {financial_analysis.get('overall_score', 'N/A')}/10")
            conclusion = financial_analysis.get("conclusion", "")
            if conclusion:
                parts.append(f"- 财务结论: {conclusion[:200]}")
            parts.append("")

        parts.append("请根据以上数据对该标的进行多方法估值分析，按指定JSON格式输出。至少使用2种估值方法。")
        return "\n".join(parts)

    @staticmethod
    def _fmt_cap(v: Any) -> str:
        """格式化市值"""
        if v is None:
            return "N/A"
        try:
            n = float(v)
            if n >= 1e12:
                return f"{n/1e12:.1f}万亿"
            elif n >= 1e8:
                return f"{n/1e8:.1f}亿"
            else:
                return f"{n/1e4:.1f}万"
        except (ValueError, TypeError):
            return str(v)

    @staticmethod
    def _fmt_num(v: Any) -> str:
        """格式化金额"""
        if v is None:
            return "N/A"
        try:
            n = float(v)
            if abs(n) >= 1e8:
                return f"{n/1e8:.1f}亿"
            elif abs(n) >= 1e4:
                return f"{n/1e4:.1f}万"
            else:
                return f"{n:.2f}"
        except (ValueError, TypeError):
            return str(v)

    @staticmethod
    def _safe_float(v: Any) -> float | None:
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
