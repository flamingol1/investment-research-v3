"""财务分析Agent - 五维度深度财务分析

分析维度:
1. 盈利能力: ROE/ROIC/毛利率/净利率
2. 成长性: 营收增速/净利润增速/扣非增速
3. 偿债能力: 资产负债率/流动比率/速动比率
4. 运营效率: 应收周转率/存货周转率/总资产周转率
5. 现金流质量: 经营现金流/自由现金流/净现比/现金流可持续性
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
```

## 重要约束
- dimensions数组必须包含全部5个维度
- anomaly_flags为空时说明"未发现明显财务异常"
- cashflow_verification必须包含具体的净现比数据
- 所有数值必须引用实际数据，不得编造
"""


class FinancialAgent(AgentBase[AgentInput, AgentOutput]):
    """财务分析Agent - 五维度深度分析"""

    agent_name: str = "financial"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行财务分析"""
        cleaned = input_data.context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行财务分析"],
            )

        financials = cleaned.get("financials", [])
        if not financials:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无财务数据，无法执行财务分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始财务分析 | {stock_code} {stock_name} | {len(financials)}期数据")

        prompt = self._build_prompt(stock_code, stock_name, cleaned)
        model = self._get_model()

        result = await self.llm.call_json(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            model=model,
        )

        score = result.get("overall_score", 0)
        summary = f"财务综合评分: {score}/10"
        self.logger.info(f"财务分析完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"financial": result},
            data_sources=["akshare", "baostock"],
            confidence=min(score / 10.0, 1.0),
            summary=summary,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验财务分析输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        financial = output.data.get("financial", {})
        errors = []

        score = financial.get("overall_score")
        if score is None or not isinstance(score, (int, float)):
            errors.append("缺少有效的overall_score")
        elif not (0 <= score <= 10):
            errors.append(f"overall_score超出范围: {score}")

        dimensions = financial.get("dimensions", [])
        if not isinstance(dimensions, list) or len(dimensions) < 3:
            errors.append(f"dimensions不足: 需要5个维度，至少3个，实际{len(dimensions) if isinstance(dimensions, list) else '非列表'}")

        if not financial.get("cashflow_verification"):
            errors.append("缺少cashflow_verification")

        if not financial.get("conclusion"):
            errors.append("缺少conclusion")

        if errors:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取分析层模型"""
        return self.config.get_layer_model("analysis_layer", task="financial")

    def _build_prompt(self, stock_code: str, stock_name: str, cleaned: dict) -> str:
        """构建财务分析提示词"""
        parts = [f"## 标的: {stock_code} {stock_name}\n"]

        # 股票信息
        info = cleaned.get("stock_info", {})
        if info:
            parts.append(f"行业: {info.get('industry_sw', 'N/A')}")
            parts.append("")

        # 多期财务数据
        financials = cleaned.get("financials", [])
        if financials:
            parts.append("### 多期财务数据")
            parts.append("| 报告期 | 营收 | 净利润 | 营收增速 | 净利增速 | 毛利率 | 净利率 | ROE | 资产负债率 | 经营现金流 |")
            parts.append("|---|---|---|---|---|---|---|---|---|---|")

            for f in financials[:8]:  # 最多显示8期
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('revenue'))} "
                    f"| {self._fmt(f.get('net_profit'))} "
                    f"| {self._fmt_pct(f.get('revenue_yoy'))} "
                    f"| {self._fmt_pct(f.get('net_profit_yoy'))} "
                    f"| {self._fmt_pct(f.get('gross_margin'))} "
                    f"| {self._fmt_pct(f.get('net_margin'))} "
                    f"| {self._fmt_pct(f.get('roe'))} "
                    f"| {self._fmt_pct(f.get('debt_ratio'))} "
                    f"| {self._fmt(f.get('operating_cashflow'))} |"
                )
            parts.append("")

            # 偿债能力指标
            parts.append("### 偿债能力指标")
            parts.append("| 报告期 | 流动比率 | 速动比率 | 产权比率 |")
            parts.append("|---|---|---|---|")
            for f in financials[:5]:
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('current_ratio'))} "
                    f"| {self._fmt(f.get('quick_ratio'))} "
                    f"| {self._fmt(f.get('debt_ratio'))} |"
                )
            parts.append("")

            # 运营效率指标
            parts.append("### 运营效率指标")
            parts.append("| 报告期 | 应收周转率 | 存货周转率 | 总资产 | 权益 |")
            parts.append("|---|---|---|---|---|")
            for f in financials[:5]:
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('receivable_turnover'))} "
                    f"| {self._fmt(f.get('inventory_turnover'))} "
                    f"| {self._fmt(f.get('total_assets'))} "
                    f"| {self._fmt(f.get('equity'))} |"
                )
            parts.append("")

        announcements = cleaned.get("announcements", [])
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

        research_reports = cleaned.get("research_reports", [])
        if research_reports:
            parts.append("### 卖方研报原文摘录")
            for item in research_reports[:3]:
                excerpt = item.get("excerpt") or item.get("summary") or ""
                parts.append(
                    f"- {item.get('publish_date', 'N/A')} {item.get('institution', 'N/A')}《{item.get('title', 'N/A')}》: {str(excerpt)[:180]}"
                )
            parts.append("")

        # 实时行情
        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append("### 当前估值指标")
            parts.append(f"- 最新价: {realtime.get('close', 'N/A')}")
            parts.append(f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}")
            parts.append(f"- PB: {realtime.get('pb_mrq', 'N/A')}")
            parts.append(f"- 总市值: {realtime.get('market_cap', 'N/A')}")
            parts.append("")

        n_periods = len(financials)
        parts.append(f"请根据以上{n_periods}期财务数据，对该标的进行5维度深度财务分析，按指定JSON格式输出。")
        if n_periods < 3:
            parts.append("注意：财务数据期数较少(少于3期)，请在分析中注明数据局限性。")

        return "\n".join(parts)

    @staticmethod
    def _fmt(v: Any) -> str:
        """格式化数值"""
        if v is None or v == "":
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
    def _fmt_pct(v: Any) -> str:
        """格式化百分比"""
        if v is None or v == "":
            return "N/A"
        try:
            value = float(v)
            if abs(value) <= 1.2:
                value *= 100
            return f"{value:.1f}%"
        except (ValueError, TypeError):
            return str(v)
