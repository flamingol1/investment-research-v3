"""行业分析Agent - 生命周期判断、竞争格局、景气度指标

分析维度:
1. 行业生命周期: 初创期/成长期/成熟期/衰退期
2. 市场空间: 市场规模、增速、天花板
3. 竞争格局: 集中度(CR5)、竞争强度、竞争对手
4. 景气度: 当前景气方向、核心景气指标
5. 政策环境: 监管态度、产业政策
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

logger = get_logger("agent.industry")

SYSTEM_PROMPT = """你是一位专业的A股行业研究分析师，擅长行业赛道判断和竞争格局分析。

## 你的任务
对给定股票所属行业进行β分析，判断行业赛道质量和公司行业地位。

## 分析框架
### 1. 行业生命周期判断
- **初创期**: 市场小但增速极快，技术路线未定型，玩家少
- **成长期**: 市场快速扩大，渗透率快速提升，参与者增多，竞争加剧
- **成熟期**: 增速放缓，渗透率高，格局稳定，龙头份额集中
- **衰退期**: 市场萎缩，产能过剩，企业退出
- 判断依据：市场规模增速、渗透率、参与者数量变化、产能周期

### 2. 市场空间分析
- 当前市场规模（估算）
- 5年复合增速
- 天花板在哪里（渗透率上限/替代品威胁）
- 增量市场 vs 存量博弈

### 3. 竞争格局
- 行业集中度(CR5/CR10)
- 竞争模式: 价格战/差异化/寡头协调
- 主要竞争对手（至少3家）及其优势
- 新进入者威胁
- 替代品威胁

### 4. 景气度判断
- 当前处于景气上行/平稳/下行
- 列出3-5个核心景气度跟踪指标
- 产业链传导信号（上游/中游/下游）

### 5. 政策环境
- 监管态度: 鼓励/中性/限制
- 产业政策影响
- 潜在政策风险

### 6. 公司行业地位
- 市场份额排名
- 相对竞争对手的优势/劣势
- 行业β中的个股α来源

## 输出格式（严格JSON）
```json
{
  "lifecycle": "初创期/成长期/成熟期/衰退期",
  "lifecycle_evidence": "生命周期判断的具体依据",
  "market_size": 5000,
  "market_growth": 15.0,
  "competition_pattern": "寡头垄断/寡头竞争/垄断竞争/完全竞争",
  "cr5": 45.0,
  "top_competitors": [
    {
      "name": "竞争对手名称",
      "market_share": 20.0,
      "advantage": "竞争优势",
      "threat_level": "高/中/低"
    }
  ],
  "prosperity_indicators": ["指标1", "指标2", "指标3"],
  "prosperity_direction": "上行/平稳/下行",
  "policy_stance": "政策态度描述",
  "company_position": "公司在行业中的地位描述",
  "conclusion": "行业综合结论"
}
```

## 重要约束
- lifecycle只能取"初创期"/"成长期"/"成熟期"/"衰退期"
- top_competitors至少列出3个竞争对手
- prosperity_indicators至少3个指标
- prosperity_direction只能取"上行"/"平稳"/"下行"
- competition_pattern只能取4个标准值之一
- 所有判断必须有依据，不得凭空臆断
"""


class IndustryAgent(AgentBase[AgentInput, AgentOutput]):
    """行业分析Agent - 生命周期判断、竞争格局、景气度指标"""

    agent_name: str = "industry"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行行业分析"""
        cleaned = input_data.context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行行业分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始行业分析 | {stock_code} {stock_name}")

        prompt = self._build_prompt(stock_code, stock_name, cleaned)
        model = self._get_model()

        result = await self.llm.call_json(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            model=model,
        )

        lifecycle = result.get("lifecycle", "未知")
        direction = result.get("prosperity_direction", "未知")
        summary = f"行业生命周期: {lifecycle}, 景气方向: {direction}"
        self.logger.info(f"行业分析完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"industry": result},
            data_sources=["akshare", "baostock"],
            confidence=0.6,
            summary=summary,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验行业分析输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        industry = output.data.get("industry", {})
        errors = []

        lifecycle = industry.get("lifecycle")
        if lifecycle not in ("初创期", "成长期", "成熟期", "衰退期"):
            errors.append(f"lifecycle无效: {lifecycle}")

        direction = industry.get("prosperity_direction")
        if direction not in ("上行", "平稳", "下行"):
            errors.append(f"prosperity_direction无效: {direction}")

        competitors = industry.get("top_competitors", [])
        if not isinstance(competitors, list) or len(competitors) < 2:
            errors.append(f"top_competitors不足: 需要>=3个，至少2个，实际{len(competitors) if isinstance(competitors, list) else '非列表'}")

        indicators = industry.get("prosperity_indicators", [])
        if not isinstance(indicators, list) or len(indicators) < 2:
            errors.append(f"prosperity_indicators不足: 需要>=3个，至少2个")

        if not industry.get("lifecycle_evidence"):
            errors.append("缺少lifecycle_evidence")

        if not industry.get("conclusion"):
            errors.append("缺少conclusion")

        if errors:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取分析层模型"""
        return self.config.get_layer_model("analysis_layer", task="industry")

    def _build_prompt(self, stock_code: str, stock_name: str, cleaned: dict) -> str:
        """构建行业分析提示词"""
        parts = [f"## 标的: {stock_code} {stock_name}\n"]

        # 公司所属行业
        info = cleaned.get("stock_info", {})
        if info:
            parts.append("### 公司基本信息")
            parts.append(f"- 行业: {info.get('industry_sw', 'N/A')}")
            mb = info.get('main_business') or 'N/A'
            parts.append(f"- 主营业务: {mb[:300]}")
            parts.append("")

        # 行业已有数据
        industry_data = cleaned.get("industry", {})
        if industry_data:
            parts.append("### 已知行业数据")
            if industry_data.get("market_size"):
                parts.append(f"- 市场规模: {industry_data['market_size']}亿元")
            if industry_data.get("cagr_5y"):
                parts.append(f"- 5年复合增速: {industry_data['cagr_5y']}%")
            if industry_data.get("cr5"):
                parts.append(f"- CR5集中度: {industry_data['cr5']}%")
            parts.append("")

        # 财务数据（行业地位判断用）
        financials = cleaned.get("financials", [])
        if financials:
            parts.append("### 公司核心指标（判断行业地位）")
            parts.append("| 报告期 | 营收 | 净利润 | 营收增速 | 毛利率 | ROE |")
            parts.append("|---|---|---|---|---|---|")
            for f in financials[:5]:
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('revenue'))} "
                    f"| {self._fmt(f.get('net_profit'))} "
                    f"| {self._fmt_pct(f.get('revenue_yoy'))} "
                    f"| {self._fmt_pct(f.get('gross_margin'))} "
                    f"| {self._fmt_pct(f.get('roe'))} |"
                )
            parts.append("")

        # 市值数据
        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append(f"### 当前总市值: {self._fmt_cap(realtime.get('market_cap'))}")
            parts.append("")

        # 上游分析参考
        financial_analysis = cleaned.get("financial_analysis", {})
        if financial_analysis:
            parts.append("### 财务分析参考")
            parts.append(f"- 综合评分: {financial_analysis.get('overall_score', 'N/A')}/10")
            parts.append("")

        parts.append("请根据以上数据对该标的所属行业进行全面分析，按指定JSON格式输出。重点关注行业生命周期、竞争格局和景气度。")
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
