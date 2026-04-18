"""初筛Agent - 快速排雷检查

检查维度:
1. 退市风险 (ST/退市新规)
2. 合规处罚 (监管处罚/立案调查)
3. 财务异常 (营收/利润/现金流异常)
4. 治理风险 (实控人/关联交易)
5. 商誉减值 (商誉占比/减值历史)
6. 经营连续性 (主营是否可持续)
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

logger = get_logger("agent.screener")

SYSTEM_PROMPT = """你是一位专业的A股标的筛选分析师。

## 你的任务
对给定股票进行快速排雷检查，判断该标的是否值得深度研究。

## 检查维度（6项必查）
1. **退市风险**: 是否ST/*ST/退市风险警示、营收是否低于1亿、净利润是否连续亏损、净资产是否为负
2. **合规处罚**: 近3年是否有重大监管处罚、立案调查、行政处罚
3. **财务异常**: 营收利润是否异常波动、现金流与利润是否严重背离、应收账款是否激增、存货是否异常
4. **治理风险**: 实控人是否频繁变更、是否存在关联交易占款、股权质押比例是否过高
5. **商誉减值**: 商誉占净资产比例是否过高(>30%)、是否有大额减值历史
6. **经营连续性**: 主营业务是否清晰、是否存在重大转型不确定性

## 判定标准
- **通过**: 无重大风险，值得深度研究
- **重点警示**: 存在需要关注但非致命的风险，仍可继续研究但需重点关注
- **刚性剔除**: 存在不可逆转的重大风险（如ST、严重造假、即将退市），不值得浪费时间

## 输出格式（严格JSON）
```json
{
  "verdict": "通过|重点警示|刚性剔除",
  "checks": [
    {
      "item": "检查项目名",
      "status": "pass|warning|reject",
      "detail": "检查详情和判断依据",
      "evidence": "具体数据支撑（数字、比率等）"
    }
  ],
  "key_risks": ["风险点1", "风险点2"],
  "recommendation": "是否建议继续深度研究的具体建议",
  "confidence": 0.8
}
```

## 重要约束
- 所有结论必须引用具体数据，不得使用模糊表述
- 只有确认的刚性风险才能标记为reject，不确定的风险标记为warning
- checks数组必须包含全部6个维度的检查结果
"""


class ScreenerAgent(AgentBase[AgentInput, AgentOutput]):
    """初筛Agent - 快速排雷，判断是否值得深度研究"""

    agent_name: str = "screener"
    execution_mode: str = "llm"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行初筛分析"""
        cleaned = input_data.context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行初筛"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始初筛 | {stock_code} {stock_name}")

        prompt = self._build_prompt(stock_code, stock_name, cleaned)
        model = self._get_model()

        result = await self.llm.call_json(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            model=model,
        )

        summary = f"初筛结论: {result.get('verdict', '未知')}"
        self.logger.info(f"初筛完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"screening": result},
            data_sources=["akshare", "baostock"],
            confidence=result.get("confidence", 0.5),
            summary=summary,
            llm_invoked=True,
            model_used=model,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验初筛输出"""
        if output.status != AgentStatus.SUCCESS:
            return  # FAILED状态不需要校验data

        screening = output.data.get("screening", {})
        errors = []

        verdict = screening.get("verdict")
        if verdict not in ("通过", "重点警示", "刚性剔除"):
            errors.append(f"verdict无效: {verdict}")

        checks = screening.get("checks", [])
        if not isinstance(checks, list) or len(checks) < 4:
            errors.append(f"checks数量不足: {len(checks) if isinstance(checks, list) else '非列表'}，需要至少4项")

        if not screening.get("recommendation"):
            errors.append("缺少recommendation")

        if errors:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取分析层模型"""
        return self.config.get_layer_model("analysis_layer", task="screener")

    def _build_prompt(self, stock_code: str, stock_name: str, cleaned: dict) -> str:
        """构建初筛提示词"""
        parts = [f"## 标的: {stock_code} {stock_name}\n"]

        # 股票基本信息
        info = cleaned.get("stock_info", {})
        if info:
            parts.append("### 股票基本信息")
            parts.append(f"- 交易所: {info.get('exchange', 'N/A')}")
            parts.append(f"- 行业: {info.get('industry_sw', 'N/A')}")
            parts.append(f"- 实控人: {info.get('actual_controller', 'N/A')}")
            parts.append(f"- 上市日期: {info.get('listing_date', 'N/A')}")
            parts.append("")

        # 最新财务数据
        financials = cleaned.get("financials", [])
        if financials:
            latest = financials[0] if isinstance(financials[0], dict) else financials[0]
            parts.append("### 最新财务数据")
            parts.append(f"- 报告期: {latest.get('report_date', 'N/A')}")
            parts.append(f"- 营业收入: {latest.get('revenue', 'N/A')}")
            parts.append(f"- 净利润: {latest.get('net_profit', 'N/A')}")
            parts.append(f"- 总资产: {latest.get('total_assets', 'N/A')}")
            parts.append(f"- 资产负债率: {latest.get('debt_ratio', 'N/A')}")
            parts.append(f"- 经营现金流: {latest.get('operating_cashflow', 'N/A')}")
            parts.append(f"- ROE: {latest.get('roe', 'N/A')}")
            parts.append(f"- 毛利率: {latest.get('gross_margin', 'N/A')}")
            parts.append("")

            # 多期对比（最近3期）
            if len(financials) >= 2:
                parts.append("### 近3期核心指标趋势")
                parts.append("| 报告期 | 营收 | 净利润 | ROE | 资产负债率 |")
                parts.append("|---|---|---|---|---|")
                for f in financials[:3]:
                    if isinstance(f, dict):
                        parts.append(
                            f"| {f.get('report_date', 'N/A')} "
                            f"| {f.get('revenue', 'N/A')} "
                            f"| {f.get('net_profit', 'N/A')} "
                            f"| {f.get('roe', 'N/A')} "
                            f"| {f.get('debt_ratio', 'N/A')} |"
                        )
                parts.append("")

        # 实时行情
        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append("### 实时行情")
            parts.append(f"- 最新价: {realtime.get('close', 'N/A')}")
            parts.append(f"- 市盈率(TTM): {realtime.get('pe_ttm', 'N/A')}")
            parts.append(f"- 市净率: {realtime.get('pb_mrq', 'N/A')}")
            parts.append(f"- 总市值: {realtime.get('market_cap', 'N/A')}")
            parts.append("")

        parts.append("请根据以上数据对该标的进行快速排雷检查，按指定JSON格式输出结果。")
        return "\n".join(parts)
