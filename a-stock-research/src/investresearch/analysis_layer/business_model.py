"""商业模式Agent - 盈利结构拆解、护城河验证、反证视角

分析维度:
1. 收入结构拆解: 分产品/分地区/分业务线的营收和利润贡献
2. 盈利模式分析: 资产模式(轻/重)、客户类型、定价权
3. 护城河识别: 品牌/网络效应/转换成本/成本优势/规模效应
4. 反证视角: 为什么这个商业模式可能失败
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

logger = get_logger("agent.business_model")

SYSTEM_PROMPT = """你是一位专业的A股商业模式分析师，擅长拆解企业盈利逻辑和识别竞争壁垒。

## 你的任务
对给定股票进行商业模式深度分析，包含盈利结构拆解、护城河验证和反证审视。

## 分析框架
### 1. 收入结构拆解
- 按产品线/业务板块拆分收入和利润贡献
- 按地区拆分（如有数据）
- 识别最大收入来源和增长最快的业务
- 计算各业务线占比和趋势

### 2. 盈利模式分析
- **资产模式**: 轻/重/混合，判断依据（资产周转率、固定资产占比）
- **客户类型**: ToB/ToC/ToG，集中度如何
- **定价权**: 是否能持续提价或维持高毛利
- **核心盈利驱动力**: 量驱动？价驱动？份额驱动？
- **合同类型**: 订单制/订阅制/项目制（判断收入可预测性）

### 3. 护城河识别（至少分析3种）
- **品牌壁垒**: 品牌溢价能力、复购率、用户粘性
- **网络效应**: 用户越多价值越大、双边网络
- **转换成本**: 客户更换供应商的成本和风险
- **成本优势**: 规模效应、独特资源、工艺壁垒
- **规模效应**: 产量/渠道/研发投入的规模经济
- **专利/牌照**: 技术壁垒、行政许可

### 4. 反证视角（必须包含）
- 列出3个该商业模式最可能失败的路径
- 历史上同行业失败的案例教训
- 技术颠覆/政策变化对该模式的影响

## 输出格式（严格JSON）
```json
{
  "model_score": 7.5,
  "revenue_structure": [
    {
      "segment_name": "产品/业务名称",
      "revenue": 1000000000,
      "ratio": 45.5,
      "growth": 12.3,
      "gross_margin": 35.0
    }
  ],
  "profit_driver": "核心盈利驱动力说明",
  "asset_model": "轻/重/混合",
  "client_concentration": "客户集中度评估",
  "moats": [
    {
      "moat_type": "品牌/网络效应/转换成本/成本优势/规模效应/专利/无",
      "strength": "强/中/弱/无",
      "evidence": "支撑证据",
      "sustainability": "可持续性判断"
    }
  ],
  "moat_overall": "宽/窄/无",
  "negative_view": "为什么这个商业模式可能失败（反证视角）",
  "conclusion": "商业模式综合结论"
}
```

## 重要约束
- revenue_structure至少拆解2个维度
- moats至少分析3种护城河可能性
- negative_view必须包含至少3个失败路径
- 所有评分必须有数据引用
- moat_overall只能取"宽"/"窄"/"无"三个值
"""


class BusinessModelAgent(AgentBase[AgentInput, AgentOutput]):
    """商业模式Agent - 盈利结构拆解、护城河验证、反证视角"""

    agent_name: str = "business_model"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行商业模式分析"""
        cleaned = input_data.context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行商业模式分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始商业模式分析 | {stock_code} {stock_name}")

        prompt = self._build_prompt(stock_code, stock_name, cleaned)
        model = self._get_model()

        result = await self.llm.call_json(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            model=model,
        )

        score = result.get("model_score", 0)
        moat = result.get("moat_overall", "未知")
        summary = f"商业模式评分: {score}/10, 护城河: {moat}"
        self.logger.info(f"商业模式分析完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"business_model": result},
            data_sources=["akshare", "baostock"],
            confidence=min(score / 10.0, 1.0),
            summary=summary,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验商业模式分析输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        bm = output.data.get("business_model", {})
        errors = []

        score = bm.get("model_score")
        if score is None or not isinstance(score, (int, float)):
            errors.append("缺少有效的model_score")
        elif not (0 <= score <= 10):
            errors.append(f"model_score超出范围: {score}")

        moat = bm.get("moat_overall")
        if moat not in ("宽", "窄", "无"):
            errors.append(f"moat_overall无效: {moat}，应为宽/窄/无")

        moats = bm.get("moats", [])
        if not isinstance(moats, list) or len(moats) < 2:
            errors.append(f"moats不足: 需要>=3种护城河分析，至少2种，实际{len(moats) if isinstance(moats, list) else '非列表'}")

        if not bm.get("negative_view"):
            errors.append("缺少negative_view（反证视角）")

        if not bm.get("conclusion"):
            errors.append("缺少conclusion")

        if errors:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取分析层模型"""
        return self.config.get_layer_model("analysis_layer", task="business_model")

    def _build_prompt(self, stock_code: str, stock_name: str, cleaned: dict) -> str:
        """构建商业模式分析提示词"""
        parts = [f"## 标的: {stock_code} {stock_name}\n"]

        # 股票基本信息
        info = cleaned.get("stock_info", {})
        if info:
            parts.append("### 公司基本信息")
            parts.append(f"- 行业: {info.get('industry_sw', 'N/A')}")
            parts.append(f"- 实控人: {info.get('actual_controller', 'N/A')}")
            mb = info.get('main_business') or 'N/A'
            parts.append(f"- 主营业务: {mb[:300]}")
            parts.append("")

        # 财务数据（收入结构分析用）
        financials = cleaned.get("financials", [])
        if financials:
            parts.append("### 多期财务数据")
            parts.append("| 报告期 | 营收 | 净利润 | 毛利率 | 净利率 | ROE | 经营现金流 |")
            parts.append("|---|---|---|---|---|---|---|")
            for f in financials[:5]:
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('revenue'))} "
                    f"| {self._fmt(f.get('net_profit'))} "
                    f"| {self._fmt_pct(f.get('gross_margin'))} "
                    f"| {self._fmt_pct(f.get('net_margin'))} "
                    f"| {self._fmt_pct(f.get('roe'))} "
                    f"| {self._fmt(f.get('operating_cashflow'))} |"
                )
            parts.append("")

            # 资产结构（判断轻/重资产）
            latest = financials[0] if isinstance(financials[0], dict) else {}
            parts.append("### 资产结构（判断轻/重资产模式）")
            parts.append(f"- 总资产: {self._fmt(latest.get('total_assets'))}")
            parts.append(f"- 净资产: {self._fmt(latest.get('equity'))}")
            parts.append(f"- 资产负债率: {latest.get('debt_ratio', 'N/A')}")
            parts.append(f"- ROE: {latest.get('roe', 'N/A')}")
            parts.append(f"- ROIC: {latest.get('roic', 'N/A')}")
            parts.append("")

        # 实时行情
        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append("### 当前估值")
            parts.append(f"- 总市值: {self._fmt_cap(realtime.get('market_cap'))}")
            parts.append(f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}")
            parts.append("")

        # 上游分析结论（如有）
        screening = cleaned.get("screening", {})
        if screening:
            verdict = screening.get("verdict", "")
            if verdict:
                parts.append(f"### 初筛结论参考: {verdict}")

        financial_analysis = cleaned.get("financial_analysis", {})
        if financial_analysis:
            parts.append("### 财务分析参考")
            parts.append(f"- 综合评分: {financial_analysis.get('overall_score', 'N/A')}/10")
            conclusion = financial_analysis.get("conclusion", "")
            if conclusion:
                parts.append(f"- 结论: {conclusion[:200]}")
            parts.append("")

        parts.append("请根据以上数据对该标的进行商业模式深度分析，按指定JSON格式输出。必须包含收入结构拆解、护城河验证和反证视角。")
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
