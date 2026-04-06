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
from investresearch.core.trust import get_module_profile, merge_evidence_refs

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
        context = input_data.context
        cleaned = context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行估值分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始估值分析 | {stock_code} {stock_name}")

        result = self._build_result(context)

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
            data_sources=["realtime", "valuation", "valuation_percentile", "financials"],
            confidence=0.8 if result.get("evidence_status") == "ok" else 0.45,
            summary=summary,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验估值输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        valuation = output.data.get("valuation", {})
        errors = []

        methods = valuation.get("methods", [])
        if valuation.get("evidence_status") == "ok" and (not isinstance(methods, list) or len(methods) < 1):
            errors.append(f"估值方法不足: 需要>=2种，实际{len(methods) if isinstance(methods, list) else '非列表'}")

        level = valuation.get("valuation_level")
        if level not in ("低估", "合理", "高估", "严重高估", "待验证"):
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

    def _build_result(self, context: dict[str, Any]) -> dict[str, Any]:
        cleaned = context.get("cleaned_data", {})
        realtime = cleaned.get("realtime", {})
        valuation = cleaned.get("valuation", [])
        valuation_percentile = cleaned.get("valuation_percentile", {})
        financials = cleaned.get("financials", [])

        valuation_profile = get_module_profile(cleaned, "valuation")
        percentile_profile = get_module_profile(cleaned, "valuation_percentile")
        financial_profile = get_module_profile(cleaned, "financials")

        current_price = self._safe_float(realtime.get("close"))
        current_pe = self._safe_float(realtime.get("pe_ttm"))
        current_pb = self._safe_float(realtime.get("pb_mrq"))

        methods: list[dict[str, Any]] = []
        intrinsic_values: list[float] = []

        pe_values = [self._safe_float(item.get("pe_ttm")) for item in valuation if isinstance(item, dict)]
        pe_values = [value for value in pe_values if value is not None and value > 0]
        if current_price is not None and current_pe and pe_values:
            median_pe = self._median(pe_values)
            intrinsic = round(current_price * median_pe / current_pe, 2)
            intrinsic_values.append(intrinsic)
            methods.append(
                {
                    "method": "PE",
                    "intrinsic_value": intrinsic,
                    "upside_pct": round((intrinsic / current_price - 1) * 100, 2),
                    "assumptions": [f"历史PE中位数约 {median_pe:.2f}", "利润质量没有发生结构性恶化"],
                    "limitations": ["仅适用于盈利口径稳定时", "未纳入一致预期分歧"],
                }
            )

        pb_values = [self._safe_float(item.get("pb_mrq")) for item in valuation if isinstance(item, dict)]
        pb_values = [value for value in pb_values if value is not None and value > 0]
        if current_price is not None and current_pb and pb_values:
            median_pb = self._median(pb_values)
            intrinsic = round(current_price * median_pb / current_pb, 2)
            intrinsic_values.append(intrinsic)
            methods.append(
                {
                    "method": "PB",
                    "intrinsic_value": intrinsic,
                    "upside_pct": round((intrinsic / current_price - 1) * 100, 2),
                    "assumptions": [f"历史PB中位数约 {median_pb:.2f}", "净资产质量和盈利能力维持稳定"],
                    "limitations": ["更适合重资产或资产质量稳定公司", "未纳入行业轮动影响"],
                }
            )

        latest_financial = financials[0] if financials and isinstance(financials[0], dict) else {}
        if current_price is not None:
            methods.append(
                {
                    "method": "DCF审计",
                    "intrinsic_value": None,
                    "upside_pct": None,
                    "assumptions": [
                        f"最新营收增速={latest_financial.get('revenue_yoy', 'N/A')}",
                        f"经营现金流={latest_financial.get('operating_cashflow', 'N/A')}",
                    ],
                    "limitations": ["当前仅作为隐含假设审计工具", "缺少一致预期数据，不输出精确DCF数值"],
                }
            )

        low = round(min(intrinsic_values), 2) if intrinsic_values else None
        high = round(max(intrinsic_values), 2) if intrinsic_values else None
        midpoint = round((low + high) / 2, 2) if low is not None and high is not None else None
        margin_of_safety = (
            round((midpoint / current_price - 1) * 100, 2)
            if midpoint is not None and current_price not in (None, 0)
            else None
        )

        pe_percentile = self._safe_float(valuation_percentile.get("pe_ttm_percentile"))
        pb_percentile = self._safe_float(valuation_percentile.get("pb_mrq_percentile"))
        valuation_level = self._infer_valuation_level(pe_percentile, pb_percentile, low, high, current_price)
        evidence_refs = merge_evidence_refs(
            valuation_profile.evidence_refs,
            percentile_profile.evidence_refs,
            financial_profile.evidence_refs,
        )
        evidence_status = "ok" if intrinsic_values or pe_percentile is not None or pb_percentile is not None else "insufficient"
        conclusion = self._build_conclusion(valuation_level, low, high, current_price, evidence_status)

        return {
            "methods": methods,
            "pe_percentile": pe_percentile,
            "pb_percentile": pb_percentile,
            "reasonable_range_low": low,
            "reasonable_range_high": high,
            "current_price": current_price,
            "margin_of_safety": margin_of_safety,
            "valuation_level": valuation_level,
            "conclusion": conclusion,
            "evidence_status": evidence_status,
            "missing_fields": sorted(set(valuation_profile.missing_fields + percentile_profile.missing_fields)),
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }

    @staticmethod
    def _median(values: list[float]) -> float:
        ordered = sorted(values)
        mid = len(ordered) // 2
        if len(ordered) % 2 == 1:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2

    @staticmethod
    def _infer_valuation_level(
        pe_percentile: float | None,
        pb_percentile: float | None,
        low: float | None,
        high: float | None,
        current_price: float | None,
    ) -> str:
        percentiles = [value for value in [pe_percentile, pb_percentile] if value is not None]
        if percentiles:
            avg_pct = sum(percentiles) / len(percentiles)
            if avg_pct <= 25:
                return "低估"
            if avg_pct <= 60:
                return "合理"
            if avg_pct <= 85:
                return "高估"
            return "严重高估"
        if low is not None and high is not None and current_price is not None:
            if current_price < low:
                return "低估"
            if current_price > high:
                return "高估"
            return "合理"
        return "待验证"

    @staticmethod
    def _build_conclusion(
        valuation_level: str,
        low: float | None,
        high: float | None,
        current_price: float | None,
        evidence_status: str,
    ) -> str:
        if evidence_status != "ok":
            return "估值证据不足，当前只保留历史分位和相对估值草图，目标区间待验证。"
        if low is not None and high is not None and current_price is not None:
            return f"基于历史估值中位数法，合理区间约 {low}-{high}，当前价格 {current_price}，估值判断为{valuation_level}。"
        return f"当前估值判断为{valuation_level}，但缺少完整价格区间。"

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
