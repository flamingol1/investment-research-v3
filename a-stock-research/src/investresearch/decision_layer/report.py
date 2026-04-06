"""Report generation agent with model fallback and local markdown fallback."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.exceptions import AgentValidationError, LLMError, LLMRateLimitError
from investresearch.core.llm import MODEL_ALIASES
from investresearch.core.models import AgentInput, AgentOutput, AgentStatus, ChartPackItem, ChartSeries, EvidencePackItem
from investresearch.core.trust import merge_evidence_refs

from .formatting import fmt_cap

REQUIRED_SECTIONS = [
    "企业画像",
    "初筛结论",
    "行业赛道分析",
    "商业模式与护城河",
    "公司治理与资本配置",
    "财务质量深度核查",
    "估值定价与预期差分析",
    "风险识别与情景分析",
]
MIN_REPORT_LENGTH = 1800

SYSTEM_PROMPT = """你是一位资深的A股证券研究员，负责把结构化分析结果整理成可直接阅读的深度研究报告。

请严格使用 Markdown 输出，并按下面 8 个二级标题组织内容，标题文字必须完全一致：
## 企业画像
## 初筛结论
## 行业赛道分析
## 商业模式与护城河
## 公司治理与资本配置
## 财务质量深度核查
## 估值定价与预期差分析
## 风险识别与情景分析

每一节都必须包含：
1. 结论
2. 论据
3. 数据来源

写作要求：
- 优先引用输入中已有的具体数据，不要凭空补数字
- 对缺失数据要明确提示“不足/待验证”
- 保持客观，不夸大，不回避风险
- 整体写成一份完整报告，而不是项目符号堆砌
- 尽量消化输入中的年报/半年报/公告原文、卖方研报摘录和政策原文，不要只复述二级分析结论
- `standard` 深度下尽量写到 4000 字以上，每章至少引用 2 条具体证据或原始资料
"""


class ReportAgent(AgentBase[AgentInput, AgentOutput]):
    """Generate a research report markdown document."""

    agent_name: str = "report"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """Generate a report with model fallback and deterministic local fallback."""
        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or input_data.context.get(
            "cleaned_data", {}
        ).get("stock_info", {}).get("name", "")
        self.logger.info(f"开始生成报告 | {stock_code} {stock_name}")

        warnings = list(input_data.context.get("cleaned_data", {}).get("collection_errors", []))
        cleaned_data = input_data.context.get("cleaned_data", {})
        if cleaned_data.get("missing_fields"):
            warnings.append(f"关键缺失字段: {', '.join(cleaned_data.get('missing_fields', [])[:8])}")

        evidence_pack = [item.model_dump(mode="json") for item in self._build_evidence_pack(input_data.context)]
        chart_pack = [item.model_dump(mode="json") for item in self._build_chart_pack(input_data.context)]
        markdown = self._build_fallback_report(
            stock_code=stock_code,
            stock_name=stock_name,
            context=input_data.context,
            warnings=warnings,
            evidence_pack=evidence_pack,
            chart_pack=chart_pack,
        )

        confidence = min(0.85, max(0.45, float(cleaned_data.get("coverage_ratio", 0.45))))
        summary_prefix = "结构化证据报告"

        self.logger.info(
            f"报告生成完成 | 字数={len(markdown)} | warnings={len(warnings)}"
        )

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={
                "markdown": markdown,
                "chart_pack": chart_pack,
                "evidence_pack": evidence_pack,
            },
            errors=warnings,
            data_sources=["综合分析结果"],
            confidence=confidence,
            summary=f"{summary_prefix}({len(markdown)}字)",
        )

    def validate_output(self, output: AgentOutput) -> None:
        """Validate the final markdown report."""
        if output.status != AgentStatus.SUCCESS:
            return

        markdown = output.data.get("markdown", "")
        errors = self._collect_validation_errors(markdown)
        if errors:
            raise AgentValidationError(self.agent_name, errors)

    def _get_model_candidates(self) -> list[str]:
        """Build an ordered list of models for report generation."""
        configured = [
            self.config.get_layer_model("decision_layer", task="report"),
            self.config.get("llm.layer_models.reporting"),
            self.config.get("llm.layer_models.decision_layer.fallback"),
            self.config.get_layer_model("analysis_layer"),
            "qwen3-plus",
            "qwen3-coder",
            "doubao-lite",
        ]

        models: list[str] = []
        for model in configured:
            if not model or not isinstance(model, str):
                continue
            if model not in MODEL_ALIASES:
                continue
            if model not in models:
                models.append(model)
        return models

    def _build_prompt(
        self, stock_code: str, stock_name: str, context: dict[str, Any]
    ) -> str:
        """Build the LLM prompt from cleaned and analyzed data."""
        parts = [f"# 标的\n- 股票代码: {stock_code}\n- 股票名称: {stock_name or 'N/A'}\n"]
        parts.append("请基于以下结构化信息输出完整深度研究报告。\n")

        cleaned = context.get("cleaned_data", {})
        stock_info = cleaned.get("stock_info", {})
        realtime = cleaned.get("realtime", {})
        if stock_info:
            parts.append("## 基础资料")
            parts.extend(
                [
                    f"- 公司名称: {stock_info.get('name', 'N/A')}",
                    f"- 交易所: {stock_info.get('exchange', 'N/A')}",
                    f"- 所属行业: {stock_info.get('industry_sw', 'N/A')}",
                    f"- 实际控制人: {stock_info.get('actual_controller', 'N/A')}",
                    f"- 上市日期: {stock_info.get('listing_date', 'N/A')}",
                    f"- 主营业务: {stock_info.get('main_business', 'N/A')}",
                    "",
                ]
            )

        if realtime:
            parts.append("## 行情快照")
            parts.extend(
                [
                    f"- 最新价: {realtime.get('close', 'N/A')}",
                    f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}",
                    f"- PB(MRQ): {realtime.get('pb_mrq', 'N/A')}",
                    f"- 总市值: {fmt_cap(realtime.get('market_cap'))}",
                    "",
                ]
            )

        industry_enhanced = cleaned.get("industry_enhanced", {})
        if industry_enhanced:
            parts.append("## 行业高频数据")
            for item in industry_enhanced.get("data_points", [])[:8]:
                parts.append(f"- {item}")
            if industry_enhanced.get("industry_pe") is not None:
                parts.append(f"- 行业PE: {industry_enhanced.get('industry_pe')}")
            if industry_enhanced.get("industry_pb") is not None:
                parts.append(f"- 行业PB: {industry_enhanced.get('industry_pb')}")
            parts.append("")

        announcements = cleaned.get("announcements", [])
        if announcements:
            parts.append("## 年报/公告原文摘录")
            for item in announcements[:6]:
                evidence = item.get("excerpt") or self._stringify(item.get("highlights", [])[:3])
                parts.extend(
                    [
                        f"- 日期: {item.get('announcement_date', 'N/A')}",
                        f"- 标题: {item.get('title', 'N/A')}",
                        f"- 类型: {item.get('announcement_type_normalized', item.get('announcement_type', 'N/A'))}",
                        f"- 原文摘录: {evidence}",
                    ]
                )
            parts.append("")

        research_reports = cleaned.get("research_reports", [])
        if research_reports:
            parts.append("## 卖方研报原文摘录")
            for item in research_reports[:4]:
                evidence = item.get("excerpt") or item.get("summary") or ""
                parts.extend(
                    [
                        f"- 日期: {item.get('publish_date', 'N/A')}",
                        f"- 机构: {item.get('institution', 'N/A')}",
                        f"- 标题: {item.get('title', 'N/A')}",
                        f"- 观点摘录: {evidence}",
                    ]
                )
            parts.append("")

        compliance_events = cleaned.get("compliance_events", [])
        if compliance_events:
            parts.append("## 官方合规事件")
            for item in compliance_events[:3]:
                evidence = item.get("excerpt") or item.get("summary") or ""
                parts.extend(
                    [
                        f"- 日期: {item.get('publish_date', 'N/A')}",
                        f"- 来源: {item.get('source', 'N/A')}",
                        f"- 标题: {item.get('title', 'N/A')}",
                        f"- 摘录: {evidence}",
                    ]
                )
            parts.append("")

        patents = cleaned.get("patents", [])
        if patents:
            parts.append("## 官方专利/技术资料")
            for item in patents[:3]:
                evidence = item.get("excerpt") or item.get("summary") or ""
                parts.extend(
                    [
                        f"- 日期: {item.get('publish_date', 'N/A')}",
                        f"- 类型: {item.get('patent_type', 'N/A')}",
                        f"- 标题: {item.get('title', 'N/A')}",
                        f"- 摘录: {evidence}",
                    ]
                )
            parts.append("")

        policy_documents = cleaned.get("policy_documents", [])
        if policy_documents:
            parts.append("## 政策原文摘录")
            for item in policy_documents[:4]:
                evidence = item.get("excerpt") or item.get("summary") or ""
                parts.extend(
                    [
                        f"- 日期: {item.get('policy_date', 'N/A')}",
                        f"- 发布机构: {item.get('issuing_body', item.get('source', 'gov.cn'))}",
                        f"- 标题: {item.get('title', 'N/A')}",
                        f"- 原文摘录: {evidence}",
                    ]
                )
            parts.append("")

        screening = context.get("screening", {})
        if screening:
            parts.append("## 初筛结果")
            parts.extend(
                [
                    f"- 结论: {screening.get('verdict', 'N/A')}",
                    f"- 建议: {screening.get('recommendation', 'N/A')}",
                    f"- 关键风险: {self._stringify(screening.get('key_risks', []))}",
                    "",
                ]
            )

        analysis_specs = [
            (
                "行业分析",
                "industry_analysis",
                [
                    "lifecycle",
                    "market_size",
                    "market_growth",
                    "competition_pattern",
                    "prosperity_direction",
                    "policy_stance",
                    "company_position",
                    "conclusion",
                ],
            ),
            (
                "商业模式分析",
                "business_model_analysis",
                [
                    "model_score",
                    "asset_model",
                    "moat_overall",
                    "profit_driver",
                    "negative_view",
                    "conclusion",
                ],
            ),
            (
                "治理分析",
                "governance_analysis",
                [
                    "governance_score",
                    "management_assessment",
                    "management_integrity",
                    "capital_allocation",
                    "related_transactions",
                    "dividend_policy",
                    "conclusion",
                ],
            ),
            (
                "财务分析",
                "financial_analysis",
                [
                    "overall_score",
                    "trend_summary",
                    "cashflow_verification",
                    "anomaly_flags",
                    "peer_comparison",
                    "conclusion",
                ],
            ),
            (
                "估值分析",
                "valuation_analysis",
                [
                    "valuation_level",
                    "current_price",
                    "reasonable_range_low",
                    "reasonable_range_high",
                    "pe_percentile",
                    "pb_percentile",
                    "margin_of_safety",
                    "conclusion",
                ],
            ),
            (
                "风险分析",
                "risk_analysis",
                [
                    "overall_risk_level",
                    "risk_score",
                    "fatal_risks",
                    "monitoring_points",
                    "scenarios",
                    "conclusion",
                ],
            ),
        ]

        for title, key, fields in analysis_specs:
            payload = context.get(key, {})
            if not payload:
                continue
            parts.append(f"## {title}")
            for field in fields:
                if field not in payload:
                    continue
                value = payload.get(field)
                if value in ("", None, [], {}):
                    continue
                parts.append(f"- {field}: {self._stringify(value)}")
            parts.append("")

        parts.append("请按指定 8 个标题输出，每节都写出“结论 / 论据 / 数据来源”。务必吸收原始资料摘录，不要只复述分析 JSON。")
        return "\n".join(parts)

    def _build_evidence_pack(self, context: dict[str, Any]) -> list[EvidencePackItem]:
        """Assemble evidence pack for UI and storage."""
        cleaned = context.get("cleaned_data", {})
        raw_items = list(cleaned.get("evidence_refs", []) or [])
        analysis_refs = merge_evidence_refs(
            raw_items,
            context.get("industry_analysis", {}).get("evidence_refs", []),
            context.get("business_model_analysis", {}).get("evidence_refs", []),
            context.get("governance_analysis", {}).get("evidence_refs", []),
            context.get("valuation_analysis", {}).get("evidence_refs", []),
            context.get("risk_analysis", {}).get("evidence_refs", []),
        )

        evidence_pack: list[EvidencePackItem] = []
        for item in analysis_refs[:18]:
            evidence_pack.append(
                EvidencePackItem(
                    category=str(getattr(item, "field", "") or "evidence"),
                    title=str(getattr(item, "title", "") or "evidence"),
                    source=str(getattr(item, "source", "") or ""),
                    url=str(getattr(item, "url", "") or ""),
                    excerpt=str(getattr(item, "excerpt", "") or ""),
                    fields=[str(getattr(item, "field", "") or "")] if getattr(item, "field", "") else [],
                    reference_date=str(getattr(item, "reference_date", "") or ""),
                )
            )

        for item in cleaned.get("compliance_events", [])[:4]:
            if not isinstance(item, dict):
                continue
            evidence_pack.append(
                EvidencePackItem(
                    category="compliance",
                    title=str(item.get("title") or "official_compliance_event"),
                    source=str(item.get("source") or ""),
                    url=str(item.get("url") or ""),
                    excerpt=str(item.get("excerpt") or item.get("summary") or "")[:280],
                    fields=["severity", "event_type"],
                    reference_date=str(item.get("publish_date") or ""),
                )
            )

        for item in cleaned.get("patents", [])[:4]:
            if not isinstance(item, dict):
                continue
            evidence_pack.append(
                EvidencePackItem(
                    category="patent",
                    title=str(item.get("title") or "official_patent"),
                    source=str(item.get("source") or ""),
                    url=str(item.get("url") or ""),
                    excerpt=str(item.get("excerpt") or item.get("summary") or "")[:280],
                    fields=["patent_type", "legal_status"],
                    reference_date=str(item.get("publish_date") or ""),
                )
            )
        return evidence_pack

    def _build_chart_pack(self, context: dict[str, Any]) -> list[ChartPackItem]:
        """Build chart-ready structured data."""
        cleaned = context.get("cleaned_data", {})
        financials = [item for item in cleaned.get("financials", []) if isinstance(item, dict)]
        valuation = [item for item in cleaned.get("valuation", []) if isinstance(item, dict)]
        valuation_analysis = context.get("valuation_analysis", {})
        risk_analysis = context.get("risk_analysis", {})
        industry_enhanced = cleaned.get("industry_enhanced", {})

        chart_pack: list[ChartPackItem] = [
            ChartPackItem(
                chart_id="financial_trend",
                title="财务趋势",
                chart_type="line",
                unit="元",
                summary="展示近几期营收与净利润趋势。",
                series=[
                    ChartSeries(
                        name="营收",
                        points=[{"x": item.get("report_date"), "y": item.get("revenue")} for item in financials[:8]],
                    ),
                    ChartSeries(
                        name="净利润",
                        points=[{"x": item.get("report_date"), "y": item.get("net_profit")} for item in financials[:8]],
                    ),
                ],
            ),
            ChartPackItem(
                chart_id="cashflow_compare",
                title="现金流对比",
                chart_type="bar",
                unit="元",
                summary="对比经营、投资和自由现金流。",
                series=[
                    ChartSeries(
                        name="经营现金流",
                        points=[{"x": item.get("report_date"), "y": item.get("operating_cashflow")} for item in financials[:8]],
                    ),
                    ChartSeries(
                        name="自由现金流",
                        points=[{"x": item.get("report_date"), "y": item.get("free_cashflow")} for item in financials[:8]],
                    ),
                ],
            ),
            ChartPackItem(
                chart_id="valuation_percentile",
                title="估值分位",
                chart_type="line",
                unit="倍",
                summary="展示历史PE/PB样本与当前估值分位。",
                series=[
                    ChartSeries(
                        name="PE(TTM)",
                        points=[{"x": item.get("date"), "y": item.get("pe_ttm")} for item in valuation[-24:]],
                    ),
                    ChartSeries(
                        name="PB(MRQ)",
                        points=[{"x": item.get("date"), "y": item.get("pb_mrq")} for item in valuation[-24:]],
                    ),
                ],
            ),
            ChartPackItem(
                chart_id="peer_placeholder",
                title="同行对比",
                chart_type="table",
                unit="",
                summary="当前版本仅保留同行对比占位，待行业数据库补齐后输出完整 peer 数据。",
                series=[],
            ),
            ChartPackItem(
                chart_id="scenario_analysis",
                title="三情景测算",
                chart_type="bar",
                unit="元",
                summary="展示乐观/中性/悲观情景的价格带。",
                series=[
                    ChartSeries(
                        name="情景目标价",
                        points=[
                            {"x": item.get("scenario"), "y": item.get("target_price")}
                            for item in risk_analysis.get("scenarios", [])
                            if isinstance(item, dict)
                        ],
                    )
                ],
            ),
            ChartPackItem(
                chart_id="industry_prosperity",
                title="行业景气指标",
                chart_type="list",
                unit="",
                summary="展示行业高频指标与景气方向。",
                series=[
                    ChartSeries(
                        name="行业指标",
                        points=[
                            {"x": idx + 1, "y": point}
                            for idx, point in enumerate(list(industry_enhanced.get("data_points", []) or [])[:6])
                        ],
                    )
                ],
            ),
        ]

        if valuation_analysis.get("evidence_refs"):
            valuation_refs = [item for item in valuation_analysis.get("evidence_refs", [])[:3] if isinstance(item, dict)]
            chart_pack[2].evidence_refs = [
                {
                    "source": item.get("source", ""),
                    "source_priority": item.get("source_priority", 0),
                    "title": item.get("title", ""),
                    "field": item.get("field", ""),
                    "excerpt": item.get("excerpt", ""),
                    "url": item.get("url", ""),
                    "reference_date": item.get("reference_date", ""),
                }
                for item in valuation_refs
            ]
        return chart_pack

    def _build_fallback_report(
        self,
        *,
        stock_code: str,
        stock_name: str,
        context: dict[str, Any],
        warnings: list[str],
        evidence_pack: list[dict[str, Any]],
        chart_pack: list[dict[str, Any]],
    ) -> str:
        """Build a deterministic markdown report when all LLM attempts fail."""
        cleaned = context.get("cleaned_data", {})
        stock_info = cleaned.get("stock_info", {})
        realtime = cleaned.get("realtime", {})
        screening = context.get("screening", {})
        industry = context.get("industry_analysis", {})
        business = context.get("business_model_analysis", {})
        governance = context.get("governance_analysis", {})
        financial = context.get("financial_analysis", {})
        valuation = context.get("valuation_analysis", {})
        risk = context.get("risk_analysis", {})
        announcements = cleaned.get("announcements", [])
        research_reports = cleaned.get("research_reports", [])
        industry_enhanced = cleaned.get("industry_enhanced", {})
        policy_documents = cleaned.get("policy_documents", [])
        compliance_events = cleaned.get("compliance_events", [])
        patents = cleaned.get("patents", [])
        missing_fields = list(cleaned.get("missing_fields", []) or [])

        periodic_notice = next(
            (
                item
                for item in announcements
                if item.get("announcement_type_normalized") in {"annual_report", "semi_annual", "quarterly_report"}
            ),
            {},
        )
        governance_notice = next(
            (
                item
                for item in announcements
                if any(keyword in str(item.get("title", "")) for keyword in ("担保", "质押", "增持", "减持", "回购", "问询"))
            ),
            {},
        )
        latest_research = research_reports[0] if research_reports else {}
        latest_policy = policy_documents[0] if policy_documents else {}
        latest_compliance = compliance_events[0] if compliance_events else {}
        latest_patent = patents[0] if patents else {}

        lines = [
            f"# {stock_code} {stock_name or stock_info.get('name', '')} 深度研究报告",
            "",
            f"> 生成时间: {datetime.now():%Y-%m-%d %H:%M:%S}",
        ]
        if warnings:
            lines.append(
                "> 说明: 本次报告生成阶段出现模型限流或响应异常，以下内容基于已完成的结构化分析结果自动整理。"
            )
        lines.append("")

        lines.extend(
            self._build_section(
                title="企业画像",
                conclusion=self._first_non_empty(
                    stock_info.get("main_business"),
                    f"{stock_name or stock_info.get('name', stock_code)} 基础信息已收集，主营业务可继续围绕现有行业框架跟踪。",
                ),
                reasons=[
                    f"公司名称: {stock_info.get('name', stock_name or 'N/A')}",
                    f"所属行业: {stock_info.get('industry_sw', 'N/A')}",
                    f"上市日期: {stock_info.get('listing_date', 'N/A')}",
                    f"实际控制人: {stock_info.get('actual_controller', 'N/A')}",
                    f"最新价: {realtime.get('close', 'N/A')}",
                    f"总市值: {fmt_cap(realtime.get('market_cap'))}",
                    f"最近定期报告: {periodic_notice.get('title', 'N/A')}",
                    f"年报/季报摘录: {periodic_notice.get('excerpt', '暂无原文摘录')}",
                ],
                source="基础资料、实时行情、年报/季报公告原文",
                verification=[f"待验证项: {item}" for item in missing_fields[:2]],
                missing=missing_fields[:2],
            )
        )

        lines.extend(
            self._build_section(
                title="初筛结论",
                conclusion=self._first_non_empty(
                    screening.get("recommendation"),
                    screening.get("verdict"),
                    "缺少完整初筛结论，后续判断需结合更多分析结果。",
                ),
                reasons=[
                    f"初筛判定: {screening.get('verdict', 'N/A')}",
                    f"研究建议: {screening.get('recommendation', 'N/A')}",
                    f"关键风险: {self._stringify(screening.get('key_risks', []), default='暂无明确关键风险')}",
                ],
                source="初筛 Agent 输出",
                verification=["待验证项: 初筛更多依赖当前样本数据，需与后续深度分析交叉验证"],
            )
        )

        lines.extend(
            self._build_section(
                title="行业赛道分析",
                conclusion=self._first_non_empty(
                    industry.get("conclusion"),
                    "行业资料不足，当前仅能做定性判断，建议继续验证景气度与竞争格局。",
                ),
                reasons=[
                    f"行业生命周期: {industry.get('lifecycle', 'N/A')}",
                    f"竞争格局: {industry.get('competition_pattern', 'N/A')}",
                    f"景气方向: {industry.get('prosperity_direction', 'N/A')}",
                    f"政策环境: {industry.get('policy_stance', 'N/A')}",
                    f"公司行业地位: {industry.get('company_position', 'N/A')}",
                    f"行业高频数据: {self._stringify(industry_enhanced.get('data_points', []), default='暂无高频行业数据')}",
                    f"政策原文: {latest_policy.get('title', 'N/A')} / {latest_policy.get('excerpt', '暂无政策摘录')}",
                ],
                source="行业分析 Agent 输出、行业高频数据、官方政策原文",
                verification=[f"待验证项: {item}" for item in industry.get("missing_fields", [])[:3]],
                missing=industry.get("missing_fields", [])[:3],
            )
        )

        lines.extend(
            self._build_section(
                title="商业模式与护城河",
                conclusion=self._first_non_empty(
                    business.get("conclusion"),
                    "商业模式分析信息有限，护城河判断需要结合产品与渠道数据继续确认。",
                ),
                reasons=[
                    f"商业模式评分: {business.get('model_score', 'N/A')}",
                    f"资产模式: {business.get('asset_model', 'N/A')}",
                    f"护城河判断: {business.get('moat_overall', 'N/A')}",
                    f"盈利驱动: {business.get('profit_driver', 'N/A')}",
                    f"反证视角: {business.get('negative_view', 'N/A')}",
                    f"卖方研报: {latest_research.get('title', 'N/A')}",
                    f"研报摘录: {latest_research.get('excerpt', latest_research.get('summary', '暂无研报原文摘录'))}",
                ],
                source="商业模式分析 Agent 输出、卖方研报原文、年报摘录",
                verification=[f"待验证项: {item}" for item in business.get("missing_fields", [])[:3]],
                missing=business.get("missing_fields", [])[:3],
            )
        )

        lines.extend(
            self._build_section(
                title="公司治理与资本配置",
                conclusion=self._first_non_empty(
                    governance.get("conclusion"),
                    "治理层数据不完整，当前治理判断应保持谨慎。",
                ),
                reasons=[
                    f"治理评分: {governance.get('governance_score', 'N/A')}",
                    f"管理层评价: {governance.get('management_assessment', 'N/A')}",
                    f"管理层诚信: {governance.get('management_integrity', 'N/A')}",
                    f"资本配置: {governance.get('capital_allocation', 'N/A')}",
                    f"分红政策: {governance.get('dividend_policy', 'N/A')}",
                    f"治理类公告: {governance_notice.get('title', 'N/A')}",
                    f"公告摘录: {governance_notice.get('excerpt', '暂无治理公告原文摘录')}",
                ],
                source="治理分析 Agent 输出、股东数据、治理公告原文",
                verification=[f"待验证项: {item}" for item in governance.get("missing_fields", [])[:4]],
                missing=governance.get("missing_fields", [])[:4],
            )
        )

        lines.extend(
            self._build_section(
                title="财务质量深度核查",
                conclusion=self._first_non_empty(
                    financial.get("conclusion"),
                    "财务数据覆盖有限，结论主要基于现有趋势与现金流线索。",
                ),
                reasons=[
                    f"综合评分: {financial.get('overall_score', 'N/A')}",
                    f"趋势总结: {financial.get('trend_summary', 'N/A')}",
                    f"现金流验证: {financial.get('cashflow_verification', 'N/A')}",
                    f"异常标记: {self._stringify(financial.get('anomaly_flags', []), default='暂无明显异常标记')}",
                    f"同行对比: {financial.get('peer_comparison', 'N/A')}",
                    f"年报摘录: {periodic_notice.get('excerpt', '暂无财报原文摘录')}",
                    f"卖方验证: {latest_research.get('summary', '暂无卖方研报支持')}",
                ],
                source="财务分析 Agent 输出、定期报告原文、卖方研报原文",
                verification=[f"待验证项: {item}" for item in financial.get("missing_fields", [])[:4]],
                missing=financial.get("missing_fields", [])[:4],
            )
        )

        lines.extend(
            self._build_section(
                title="估值定价与预期差分析",
                conclusion=self._first_non_empty(
                    valuation.get("conclusion"),
                    "估值资料不足，当前更适合用区间思维而非精确定价。",
                ),
                reasons=[
                    f"估值水平: {valuation.get('valuation_level', 'N/A')}",
                    f"当前价格: {valuation.get('current_price', realtime.get('close', 'N/A'))}",
                    f"合理区间: {self._format_range(valuation.get('reasonable_range_low'), valuation.get('reasonable_range_high'))}",
                    f"PE分位: {valuation.get('pe_percentile', 'N/A')}",
                    f"PB分位: {valuation.get('pb_percentile', 'N/A')}",
                    f"安全边际: {valuation.get('margin_of_safety', 'N/A')}",
                ],
                source="估值分析 Agent 输出",
                verification=[f"待验证项: {item}" for item in valuation.get("missing_fields", [])[:4]],
                missing=valuation.get("missing_fields", [])[:4],
            )
        )

        lines.extend(
            self._build_section(
                title="风险识别与情景分析",
                conclusion=self._first_non_empty(
                    risk.get("conclusion"),
                    "风险信息不完整，当前建议以保守仓位和持续跟踪替代一次性重仓判断。",
                ),
                reasons=[
                    f"整体风险: {risk.get('overall_risk_level', 'N/A')} / 评分 {risk.get('risk_score', 'N/A')}",
                    f"致命风险: {self._stringify(risk.get('fatal_risks', []), default='暂无已识别致命风险')}",
                    f"监控指标: {self._stringify(risk.get('monitoring_points', []), default='需继续补充监控指标')}",
                    f"情景分析: {self._format_scenarios(risk.get('scenarios', []))}",
                    f"政策风险原文: {latest_policy.get('excerpt', '暂无政策原文摘录')}",
                    f"公告风险原文: {governance_notice.get('excerpt', periodic_notice.get('excerpt', '暂无公告原文摘录'))}",
                ],
                source="风险分析 Agent 输出、政策原文、公告原文",
                verification=[f"待验证项: {item}" for item in risk.get("missing_fields", [])[:4]],
                missing=risk.get("missing_fields", [])[:4],
            )
        )

        lines.extend(
            [
                "## 证据包摘要",
                "",
                "以下资料可直接作为前端证据视图和人工复核入口：",
            ]
        )
        for item in evidence_pack[:8]:
            lines.append(
                f"- [{item.get('category', 'evidence')}] {item.get('title', 'N/A')} | "
                f"来源: {item.get('source', 'N/A')} | 摘录: {str(item.get('excerpt', ''))[:90]}"
            )
        lines.append("")

        lines.extend(
            [
                "## 图表包摘要",
                "",
                "本次报告同步输出以下结构化图表数据：",
            ]
        )
        for item in chart_pack:
            lines.append(f"- {item.get('title', 'N/A')}: {item.get('summary', '暂无解读')}")
        lines.append("")

        if warnings:
            lines.extend(
                [
                    "## 生成说明",
                    "",
                    "本次自动兜底过程中记录到以下提示：",
                ]
            )
            for warning in warnings:
                lines.append(f"- {warning}")
            lines.append("")

        return "\n".join(lines).strip()

    @staticmethod
    def _build_section(
        title: str,
        conclusion: str,
        reasons: list[str],
        source: str,
        verification: list[str] | None = None,
        missing: list[str] | None = None,
    ) -> list[str]:
        lines = [f"## {title}", "", f"结论：{conclusion}", "", "论据："]
        for reason in reasons:
            lines.append(f"- {reason}")
        if verification:
            lines.extend(["", "待验证项："])
            for item in verification:
                lines.append(f"- {item}")
        if missing:
            lines.extend(["", "缺失字段："])
            for item in missing:
                lines.append(f"- {item}")
        lines.extend(["", f"数据来源：{source}", ""])
        return lines

    @staticmethod
    def _collect_validation_errors(markdown: str) -> list[str]:
        errors: list[str] = []
        if not markdown or len(markdown.strip()) < MIN_REPORT_LENGTH:
            errors.append(f"报告内容过短: {len(markdown.strip()) if markdown else 0}字")
        for section in REQUIRED_SECTIONS:
            if section not in markdown:
                errors.append(f"报告缺少章节: {section}")
        return errors

    @staticmethod
    def _normalize_markdown(stock_code: str, stock_name: str, markdown: str) -> str:
        content = markdown.strip()
        if not content.startswith("#"):
            title = f"# {stock_code} {stock_name} 深度研究报告".strip()
            content = f"{title}\n\n{content}"
        return content

    @staticmethod
    def _first_non_empty(*values: Any) -> str:
        for value in values:
            if value not in (None, "", [], {}):
                return str(value)
        return "N/A"

    @staticmethod
    def _stringify(value: Any, default: str = "N/A") -> str:
        if value in (None, "", [], {}):
            return default
        if isinstance(value, list):
            rendered = [str(item) for item in value if item not in (None, "", [], {})]
            return "；".join(rendered) if rendered else default
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    @staticmethod
    def _format_range(low: Any, high: Any) -> str:
        if low in (None, "") and high in (None, ""):
            return "N/A"
        return f"{low} - {high}"

    @staticmethod
    def _format_scenarios(scenarios: Any) -> str:
        if not isinstance(scenarios, list) or not scenarios:
            return "暂无完整情景测算"

        rendered: list[str] = []
        for scenario in scenarios[:3]:
            if isinstance(scenario, dict):
                rendered.append(
                    f"{scenario.get('scenario', 'N/A')}: 目标价 {scenario.get('target_price', 'N/A')}, "
                    f"上涨空间 {scenario.get('upside_pct', 'N/A')}"
                )
            else:
                rendered.append(str(scenario))
        return "；".join(rendered) if rendered else "暂无完整情景测算"
