"""报告生成Agent - 基于全部分析结果生成8步闭环深度研究报告

报告结构（8步闭环）:
1. 企业画像（基本信息概览）
2. 初筛结论
3. 行业赛道β分析
4. 商业模式与α分析
5. 公司治理与资本配置
6. 财务质量深度核查
7. 估值定价与预期差分析
8. 风险识别与情景分析
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

SYSTEM_PROMPT = """你是一位资深的A股证券研究员，擅长撰写深度研究报告。

## 你的任务
根据提供的全部分析数据，撰写一份结构完整、逻辑严谨的深度研究报告。

## 报告结构（严格按8步闭环）
报告必须包含以下8个章节，每节必须包含：结论 → 论据 → 数据来源

### 1. 企业画像
- 公司基本信息（成立日期、上市日期、市值、所属行业）
- 主营业务概述
- 股权结构概要

### 2. 初筛结论
- 初筛判定结果
- 关键风险点
- 是否值得继续研究

### 3. 行业赛道β分析
- 行业生命周期定位
- 市场规模与增速
- 竞争格局（CR5、主要对手）
- 景气度方向
- 政策环境
- 标的在行业中的地位

### 4. 商业模式与α分析
- 收入结构拆解
- 核心盈利驱动力
- 资产模式（轻/重/混合）
- 护城河评估（类型、强度、可持续性）
- 反证视角

### 5. 公司治理与资本配置
- 管理层评估
- 实控人分析
- 关联交易、股权质押
- 资本配置效率
- 分红政策

### 6. 财务质量深度核查
- 盈利能力（毛利率、净利率、ROE趋势）
- 成长性（营收/利润增速）
- 偿债能力（资产负债率、流动比率）
- 运营效率（周转率）
- 现金流质量（净现比、自由现金流）
- 财务异常标记
- 同行对比

### 7. 估值定价与预期差分析
- PE/PB历史分位
- 多方法估值结果
- 合理估值区间
- 安全边际
- 预期差来源

### 8. 风险识别与情景分析
- 全维度风险清单
- 致命风险
- 三情景测算（乐观/中性/悲观目标价）
- 需持续跟踪的指标

## 写作要求
- 使用Markdown格式
- 数据必须引用具体数值，避免模糊表述
- 每节开头用一句话总结结论
- 关键数据用**粗体**标注
- 风险和不确定因素不可回避
- 整体字数3000-5000字
- 客观中立，不夸大不低估
"""


class ReportAgent(AgentBase[AgentInput, AgentOutput]):
    """报告生成Agent - 汇总全部分析结果，生成Markdown深度报告"""

    agent_name: str = "report"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行报告生成"""
        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or input_data.context.get(
            "cleaned_data", {}
        ).get("stock_info", {}).get("name", "")
        self.logger.info(f"开始生成报告 | {stock_code} {stock_name}")

        prompt = self._build_prompt(stock_code, stock_name, input_data.context)
        model = self._get_model()

        # 报告是Markdown文本，用call()而非call_json()
        markdown = await self.llm.call(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            model=model,
            max_tokens=16384,
        )

        self.logger.info(f"报告生成完成 | 字数={len(markdown)}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"markdown": markdown},
            data_sources=["综合分析结果"],
            confidence=0.8,
            summary=f"深度研究报告({len(markdown)}字)",
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验报告输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        markdown = output.data.get("markdown", "")
        errors = []

        if not markdown or len(markdown) < 200:
            errors.append(f"报告内容过短: {len(markdown)}字")

        # 检查必要章节
        required_sections = ["企业画像", "行业", "财务", "估值", "风险"]
        for section in required_sections:
            if section not in markdown:
                errors.append(f"报告缺少章节: {section}")

        if errors:
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取决策层模型"""
        return self.config.get_layer_model("decision_layer", task="report")

    def _build_prompt(
        self, stock_code: str, stock_name: str, context: dict[str, Any]
    ) -> str:
        """构建报告生成提示词 - 汇总所有分析结果"""
        parts = [f"## 标的: {stock_code} {stock_name}\n"]
        parts.append("请根据以下分析数据，撰写一份完整的8步闭环深度研究报告。\n")

        # 基本数据
        cleaned = context.get("cleaned_data", {})
        if cleaned:
            info = cleaned.get("stock_info", {})
            if info:
                parts.append("### 公司基本信息")
                parts.append(f"- 名称: {info.get('name', 'N/A')}")
                parts.append(f"- 交易所: {info.get('exchange', 'N/A')}")
                parts.append(f"- 行业: {info.get('industry_sw', 'N/A')}")
                parts.append(f"- 实控人: {info.get('actual_controller', 'N/A')}")
                parts.append(f"- 上市日期: {info.get('listing_date', 'N/A')}")
                parts.append(f"- 主营业务: {info.get('main_business', 'N/A')}")
                parts.append("")

            realtime = cleaned.get("realtime", {})
            if realtime:
                parts.append("### 实时行情")
                parts.append(f"- 最新价: {realtime.get('close', 'N/A')}")
                parts.append(f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}")
                parts.append(f"- PB(MRQ): {realtime.get('pb_mrq', 'N/A')}")
                parts.append(f"- 总市值: {fmt_cap(realtime.get('market_cap'))}")
                parts.append("")

        # 初筛结果
        screening = context.get("screening", {})
        if screening:
            parts.append(f"### 初筛结论: {screening.get('verdict', 'N/A')}")
            parts.append(f"- 建议: {screening.get('recommendation', 'N/A')}")
            key_risks = screening.get("key_risks", [])
            if key_risks:
                parts.append(f"- 关键风险: {', '.join(key_risks[:5])}")
            parts.append("")

        # 行业分析
        industry = context.get("industry_analysis", {})
        if industry:
            parts.append("### 行业分析结果")
            parts.append(f"- 生命周期: {industry.get('lifecycle', 'N/A')}")
            parts.append(f"- 竞争格局: {industry.get('competition_pattern', 'N/A')}")
            parts.append(f"- 景气方向: {industry.get('prosperity_direction', 'N/A')}")
            parts.append(f"- 结论: {industry.get('conclusion', 'N/A')}")
            parts.append("")

        # 商业模式
        business = context.get("business_model_analysis", {})
        if business:
            parts.append("### 商业模式分析结果")
            parts.append(f"- 评分: {business.get('model_score', 'N/A')}/10")
            parts.append(f"- 资产模式: {business.get('asset_model', 'N/A')}")
            parts.append(f"- 护城河: {business.get('moat_overall', 'N/A')}")
            parts.append(f"- 盈利驱动: {business.get('profit_driver', 'N/A')}")
            parts.append(f"- 结论: {business.get('conclusion', 'N/A')}")
            parts.append("")

        # 治理分析
        governance = context.get("governance_analysis", {})
        if governance:
            parts.append("### 治理分析结果")
            parts.append(f"- 治理评分: {governance.get('governance_score', 'N/A')}/10")
            parts.append(f"- 管理层诚信: {governance.get('management_integrity', 'N/A')}")
            parts.append(f"- 资本配置: {governance.get('capital_allocation', 'N/A')}")
            parts.append(f"- 结论: {governance.get('conclusion', 'N/A')}")
            parts.append("")

        # 财务分析
        financial = context.get("financial_analysis", {})
        if financial:
            parts.append("### 财务分析结果")
            parts.append(f"- 综合评分: {financial.get('overall_score', 'N/A')}/10")
            parts.append(f"- 趋势: {financial.get('trend_summary', 'N/A')}")
            parts.append(f"- 现金流验证: {financial.get('cashflow_verification', 'N/A')}")
            anomaly = financial.get("anomaly_flags", [])
            if anomaly:
                parts.append(f"- 异常标记: {', '.join(anomaly[:5])}")
            parts.append(f"- 结论: {financial.get('conclusion', 'N/A')}")
            parts.append("")

        # 估值分析
        valuation = context.get("valuation_analysis", {})
        if valuation:
            parts.append("### 估值分析结果")
            parts.append(f"- 估值水平: {valuation.get('valuation_level', 'N/A')}")
            low = valuation.get("reasonable_range_low")
            high = valuation.get("reasonable_range_high")
            parts.append(f"- 合理区间: {low}-{high}")
            parts.append(f"- PE分位: {valuation.get('pe_percentile', 'N/A')}")
            parts.append(f"- 安全边际: {valuation.get('margin_of_safety', 'N/A')}")
            parts.append(f"- 结论: {valuation.get('conclusion', 'N/A')}")
            parts.append("")

        # 风险分析
        risk = context.get("risk_analysis", {})
        if risk:
            parts.append("### 风险分析结果")
            parts.append(f"- 整体风险: {risk.get('overall_risk_level', 'N/A')}({risk.get('risk_score', 'N/A')}/10)")
            fatal = risk.get("fatal_risks", [])
            if fatal:
                parts.append(f"- 致命风险: {', '.join(fatal[:3])}")
            scenarios = risk.get("scenarios", [])
            if scenarios:
                parts.append("- 情景测算:")
                for s in scenarios[:3]:
                    if isinstance(s, dict):
                        parts.append(
                            f"  - {s.get('scenario', 'N/A')}: "
                            f"目标价{s.get('target_price', 'N/A')} "
                            f"(上行{s.get('upside_pct', 'N/A')}%)"
                        )
            parts.append(f"- 结论: {risk.get('conclusion', 'N/A')}")
            parts.append("")

        parts.append(
            "请根据以上所有分析数据，撰写完整的8步闭环深度研究报告。"
            "严格按章节结构输出，每节包含结论→论据→数据来源。"
        )
        return "\n".join(parts)
