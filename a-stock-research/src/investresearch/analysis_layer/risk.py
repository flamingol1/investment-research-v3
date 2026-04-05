"""风险分析Agent - 全维度风险清单、三情景测算

分析维度:
1. 行业风险: 技术变革、政策变化、周期波动
2. 经营风险: 客户集中、供应商依赖、产能风险
3. 财务风险: 杠杆率、流动性、商誉减值
4. 治理风险: 实控人、关联交易、信息不对称
5. 市场风险: 估值、流动性、机构持仓
6. 三情景测算: 乐观/中性/悲观
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

logger = get_logger("agent.risk")

SYSTEM_PROMPT = """你是一位专业的A股风险评估分析师，擅长识别投资风险和情景分析。

## 你的任务
对给定股票进行全维度风险分析和三情景测算。

## 风险维度（6大类，每类至少1个风险项）
### 1. 行业风险
- 技术颠覆风险
- 政策/监管变化风险
- 行业周期波动风险
- 替代品威胁

### 2. 经营风险
- 客户集中度风险（大客户依赖）
- 供应商依赖风险
- 产能扩张/收缩风险
- 产品价格波动风险
- 新项目/新业务失败风险

### 3. 财务风险
- 杠杆率过高风险
- 流动性风险
- 商誉减值风险
- 汇率/利率风险
- 应收账款坏账风险

### 4. 治理风险
- 实控人风险（质押、变更、违规）
- 关联交易利益输送风险
- 管理层道德风险
- 信息披露风险

### 5. 市场风险
- 估值过高风险
- 流动性不足风险
- 机构集中抛售风险
- 大小非解禁风险

### 6. 政策风险
- 产业政策变化
- 环保/安全监管趋严
- 国际贸易摩擦
- 数据安全/反垄断

## 三情景测算
### 乐观情景（概率约25%）
- 核心假设
- 目标价
- 上行空间

### 中性情景（概率约50%）
- 核心假设
- 目标价
- 上行空间

### 悲观情景（概率约25%）
- 核心假设
- 目标价
- 下行空间

## 输出格式（严格JSON）
```json
{
  "overall_risk_level": "低/中/高/极高",
  "risk_score": 5.5,
  "risks": [
    {
      "category": "行业/经营/财务/治理/市场/政策",
      "risk_name": "风险名称",
      "severity": "高/中/低",
      "probability": "高/中/低",
      "impact": "影响说明",
      "mitigation": "缓解措施"
    }
  ],
  "scenarios": [
    {
      "scenario": "乐观",
      "target_price": 30.0,
      "upside_pct": 35.0,
      "assumptions": ["假设1", "假设2"],
      "probability": 25.0
    },
    {
      "scenario": "中性",
      "target_price": 22.0,
      "upside_pct": 5.0,
      "assumptions": ["假设1", "假设2"],
      "probability": 50.0
    },
    {
      "scenario": "悲观",
      "target_price": 15.0,
      "upside_pct": -30.0,
      "assumptions": ["假设1", "假设2"],
      "probability": 25.0
    }
  ],
  "fatal_risks": ["致命风险1", "致命风险2"],
  "monitoring_points": ["跟踪指标1", "跟踪指标2"],
  "conclusion": "风险综合结论"
}
```

## 重要约束
- risks至少包含6个风险项（覆盖全部6大类）
- scenarios必须包含乐观/中性/悲观三种情景
- fatal_risks列出可能否定投资逻辑的致命风险
- monitoring_points至少列出3个需持续跟踪的指标
- overall_risk_level只能取"低"/"中"/"高"/"极高"
- risk_score范围0-10（越高越危险）
- 所有风险评估必须有数据支撑
"""


class RiskAgent(AgentBase[AgentInput, AgentOutput]):
    """风险分析Agent - 全维度风险清单、三情景测算"""

    agent_name: str = "risk"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行风险分析"""
        cleaned = input_data.context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行风险分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始风险分析 | {stock_code} {stock_name}")

        prompt = self._build_prompt(stock_code, stock_name, cleaned)
        model = self._get_model()

        result = await self.llm.call_json(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            model=model,
        )

        level = result.get("overall_risk_level", "未知")
        score = result.get("risk_score", 0)
        fatal_count = len(result.get("fatal_risks", []))
        summary = f"风险等级: {level}({score}/10), 致命风险: {fatal_count}个"
        self.logger.info(f"风险分析完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"risk": result},
            data_sources=["akshare", "baostock"],
            confidence=0.7,
            summary=summary,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验风险分析输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        risk = output.data.get("risk", {})
        errors = []

        level = risk.get("overall_risk_level")
        if level not in ("低", "中", "高", "极高"):
            errors.append(f"overall_risk_level无效: {level}")

        score = risk.get("risk_score")
        if score is None or not isinstance(score, (int, float)):
            errors.append("缺少有效的risk_score")
        elif not (0 <= score <= 10):
            errors.append(f"risk_score超出范围: {score}")

        risks = risk.get("risks", [])
        if not isinstance(risks, list) or len(risks) < 4:
            errors.append(f"risks不足: 需要>=6项(覆盖6大类)，至少4项，实际{len(risks) if isinstance(risks, list) else '非列表'}")

        # 检查风险类别覆盖
        if isinstance(risks, list):
            categories = set(r.get("category", "") for r in risks if isinstance(r, dict))
            required = {"行业", "经营", "财务", "治理", "市场", "政策"}
            missing = required - categories
            if len(missing) > 2:
                errors.append(f"风险类别覆盖不足，缺少: {missing}")

        scenarios = risk.get("scenarios", [])
        if not isinstance(scenarios, list) or len(scenarios) < 3:
            errors.append(f"scenarios不足: 需要乐观/中性/悲观3种，实际{len(scenarios) if isinstance(scenarios, list) else '非列表'}")

        monitors = risk.get("monitoring_points", [])
        if not isinstance(monitors, list) or len(monitors) < 2:
            errors.append(f"monitoring_points不足: 需要>=3个，至少2个")

        if not risk.get("conclusion"):
            errors.append("缺少conclusion")

        if errors:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取分析层模型"""
        return self.config.get_layer_model("analysis_layer", task="risk")

    def _build_prompt(self, stock_code: str, stock_name: str, cleaned: dict) -> str:
        """构建风险分析提示词"""
        parts = [f"## 标的: {stock_code} {stock_name}\n"]

        # 公司基本信息
        info = cleaned.get("stock_info", {})
        if info:
            parts.append("### 公司基本信息")
            parts.append(f"- 行业: {info.get('industry_sw', 'N/A')}")
            parts.append(f"- 实控人: {info.get('actual_controller', 'N/A')}")
            parts.append(f"- 上市日期: {info.get('listing_date', 'N/A')}")
            parts.append("")

        # 财务数据（财务风险评估用）
        financials = cleaned.get("financials", [])
        if financials:
            parts.append("### 财务数据（风险识别）")
            parts.append("| 报告期 | 营收 | 净利润 | 营收增速 | 净利增速 | 毛利率 | ROE | 资产负债率 | 商誉/净资产 |")
            parts.append("|---|---|---|---|---|---|---|---|---|")
            for f in financials[:5]:
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('revenue'))} "
                    f"| {self._fmt(f.get('net_profit'))} "
                    f"| {self._fmt_pct(f.get('revenue_yoy'))} "
                    f"| {self._fmt_pct(f.get('net_profit_yoy'))} "
                    f"| {self._fmt_pct(f.get('gross_margin'))} "
                    f"| {self._fmt_pct(f.get('roe'))} "
                    f"| {self._fmt_pct(f.get('debt_ratio'))} "
                    f"| {self._fmt_pct(f.get('goodwill_ratio'))} |"
                )
            parts.append("")

            # 现金流风险
            parts.append("### 现金流分析")
            parts.append("| 报告期 | 经营现金流 | 投资现金流 | 筹资现金流 | 自由现金流 | 净现比 |")
            parts.append("|---|---|---|---|---|---|")
            for f in financials[:5]:
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('operating_cashflow'))} "
                    f"| {self._fmt(f.get('investing_cashflow'))} "
                    f"| {self._fmt(f.get('financing_cashflow'))} "
                    f"| {self._fmt(f.get('free_cashflow'))} "
                    f"| {f.get('cash_to_profit', 'N/A')} |"
                )
            parts.append("")

        # 当前估值（市场风险评估用）
        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append("### 当前估值与市值")
            parts.append(f"- 最新价: {realtime.get('close', 'N/A')}")
            parts.append(f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}")
            parts.append(f"- PB(MRQ): {realtime.get('pb_mrq', 'N/A')}")
            parts.append(f"- 总市值: {self._fmt_cap(realtime.get('market_cap'))}")
            parts.append("")

        # 上游分析结论
        screening = cleaned.get("screening", {})
        if screening:
            parts.append(f"### 初筛结论: {screening.get('verdict', 'N/A')}")
            key_risks = screening.get("key_risks", [])
            if key_risks:
                parts.append(f"- 初筛风险: {', '.join(key_risks[:5])}")
            parts.append("")

        financial_analysis = cleaned.get("financial_analysis", {})
        if financial_analysis:
            anomaly_flags = financial_analysis.get("anomaly_flags", [])
            if anomaly_flags:
                parts.append(f"### 财务异常标记: {', '.join(anomaly_flags[:5])}")
            parts.append("")

        valuation_analysis = cleaned.get("valuation_analysis", {})
        if valuation_analysis:
            parts.append(f"### 估值结论: {valuation_analysis.get('valuation_level', 'N/A')}")
            parts.append(f"- 合理区间: {valuation_analysis.get('reasonable_range_low', 'N/A')}-{valuation_analysis.get('reasonable_range_high', 'N/A')}")
            parts.append("")

        parts.append("请根据以上数据对该标的进行全维度风险分析和三情景测算，按指定JSON格式输出。必须覆盖行业/经营/财务/治理/市场/政策6大类风险。")
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
