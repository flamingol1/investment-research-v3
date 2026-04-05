"""治理分析Agent - 管理层评估、资本配置分析

分析维度:
1. 管理层评估: 能力/诚信/稳定性
2. 实控人分析: 股权结构/质押情况/控制力
3. 关联交易: 占比/公允性
4. 资本配置: 投资效率/并购历史/分红政策
5. 激励机制: 股权激励/管理层持股
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

logger = get_logger("agent.governance")

SYSTEM_PROMPT = """你是一位专业的A股公司治理分析师，擅长管理层评估和资本配置效率分析。

## 你的任务
对给定股票进行公司治理深度分析，评估管理层质量和资本配置效率。

## 分析框架
### 1. 管理层评估
- **能力评估**: CEO/核心管理层的行业经验、过往业绩、战略眼光
- **诚信评估**: 是否有违规记录、承诺兑现情况、信息披露质量
- **稳定性**: 核心管理层变动频率、任期
- **管理层持股**: 是否与股东利益一致

### 2. 实控人分析
- 股权结构: 持股比例、控制路径
- 质押情况: 股权质押比例（高质押=高风险）
- 控制力: 实控人是否真正掌控公司
- 家族企业vs职业经理人治理

### 3. 关联交易
- 关联交易占比和规模
- 定价公允性
- 是否存在利益输送嫌疑

### 4. 资本配置效率
- **投资效率**: ROI、资本开支vs经营现金流、增量资本回报率
- **并购历史**: 并购频率、商誉占比、整合效果
- **分红政策**: 分红率、分红持续性、分红融资比
- **回购/增持**: 是否有积极回购或管理层增持
- **再融资**: 增发/配股频率，资金使用效率

### 5. 激励机制
- 是否有股权激励计划
- 激励条件是否合理（业绩考核指标）
- 管理层持股比例

## 输出格式（严格JSON）
```json
{
  "governance_score": 7.0,
  "management_assessment": "管理层综合评估",
  "management_integrity": "优/良/中/差",
  "controller_analysis": "实控人分析",
  "related_transactions": "关联交易评估",
  "equity_pledge": "股权质押情况",
  "capital_allocation": "资本配置效率评估",
  "dividend_policy": "分红政策评估",
  "incentive_plan": "股权激励评估",
  "conclusion": "治理综合结论"
}
```

## 重要约束
- governance_score范围0-10
- management_integrity只能取"优"/"良"/"中"/"差"
- 每个维度的评估都必须有具体依据
- 对缺失数据需明确说明"数据不足无法判断"
- 所有负面判断必须有证据支撑
"""


class GovernanceAgent(AgentBase[AgentInput, AgentOutput]):
    """治理分析Agent - 管理层评估、资本配置分析"""

    agent_name: str = "governance"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行治理分析"""
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

        prompt = self._build_prompt(stock_code, stock_name, cleaned)
        model = self._get_model()

        result = await self.llm.call_json(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            model=model,
        )

        score = result.get("governance_score", 0)
        integrity = result.get("management_integrity", "未知")
        summary = f"治理评分: {score}/10, 管理层诚信: {integrity}"
        self.logger.info(f"治理分析完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"governance": result},
            data_sources=["akshare", "baostock"],
            confidence=min(score / 10.0, 1.0),
            summary=summary,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验治理分析输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        gov = output.data.get("governance", {})
        errors = []

        score = gov.get("governance_score")
        if score is None or not isinstance(score, (int, float)):
            errors.append("缺少有效的governance_score")
        elif not (0 <= score <= 10):
            errors.append(f"governance_score超出范围: {score}")

        integrity = gov.get("management_integrity")
        if integrity not in ("优", "良", "中", "差"):
            errors.append(f"management_integrity无效: {integrity}")

        if not gov.get("management_assessment"):
            errors.append("缺少management_assessment")

        if not gov.get("capital_allocation"):
            errors.append("缺少capital_allocation")

        if not gov.get("conclusion"):
            errors.append("缺少conclusion")

        if errors:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取分析层模型"""
        return self.config.get_layer_model("analysis_layer", task="governance")

    def _build_prompt(self, stock_code: str, stock_name: str, cleaned: dict) -> str:
        """构建治理分析提示词"""
        parts = [f"## 标的: {stock_code} {stock_name}\n"]

        # 公司基本信息
        info = cleaned.get("stock_info", {})
        if info:
            parts.append("### 公司基本信息")
            parts.append(f"- 实控人: {info.get('actual_controller', 'N/A')}")
            parts.append(f"- 实控人性质: {info.get('controller_type', 'N/A')}")
            parts.append(f"- 行业: {info.get('industry_sw', 'N/A')}")
            parts.append(f"- 上市日期: {info.get('listing_date', 'N/A')}")
            parts.append("")

        # 财务数据（资本配置效率分析用）
        financials = cleaned.get("financials", [])
        if financials:
            parts.append("### 财务数据（资本配置效率分析）")
            parts.append("| 报告期 | 营收 | 净利润 | 营收增速 | ROE | 经营现金流 | 投资现金流 | 筹资现金流 |")
            parts.append("|---|---|---|---|---|---|---|---|")
            for f in financials[:5]:
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('revenue'))} "
                    f"| {self._fmt(f.get('net_profit'))} "
                    f"| {self._fmt_pct(f.get('revenue_yoy'))} "
                    f"| {self._fmt_pct(f.get('roe'))} "
                    f"| {self._fmt(f.get('operating_cashflow'))} "
                    f"| {self._fmt(f.get('investing_cashflow'))} "
                    f"| {self._fmt(f.get('financing_cashflow'))} |"
                )
            parts.append("")

            # 资产质量指标
            latest = financials[0] if isinstance(financials[0], dict) else {}
            parts.append("### 资产质量指标")
            parts.append(f"- 商誉/净资产: {latest.get('goodwill_ratio', 'N/A')}")
            parts.append(f"- 资产负债率: {latest.get('debt_ratio', 'N/A')}")
            parts.append(f"- ROIC: {latest.get('roic', 'N/A')}")
            parts.append("")

        # 市值
        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append(f"### 总市值: {self._fmt_cap(realtime.get('market_cap'))}")
            parts.append("")

        # 上游分析参考
        screening = cleaned.get("screening", {})
        if screening:
            checks = screening.get("checks", [])
            governance_checks = [c for c in checks if "治理" in c.get("item", "")]
            if governance_checks:
                parts.append("### 初筛中的治理风险")
                for c in governance_checks:
                    parts.append(f"- {c.get('item', '')}: {c.get('status', '')} - {c.get('detail', '')}")
                parts.append("")

        parts.append("请根据以上数据对该标的进行公司治理深度分析，按指定JSON格式输出。")
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
            return f"{float(v):.1f}%"
        except (ValueError, TypeError):
            return str(v)

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
