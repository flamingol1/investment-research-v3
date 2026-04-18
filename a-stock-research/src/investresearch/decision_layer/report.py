"""Report generation agent with model fallback and local markdown fallback."""

from __future__ import annotations

import json
import math
import re
from datetime import datetime
from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.exceptions import AgentValidationError, LLMError, LLMRateLimitError
from investresearch.core.llm import MODEL_ALIASES
from investresearch.core.models import AgentInput, AgentOutput, AgentStatus, ChartPackItem, ChartSeries, EvidencePackItem, EvidenceRef
from investresearch.core.trust import merge_evidence_refs, normalize_field_quality_map

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

INLINE_CHART_SECTION_HINTS = {
    "industry_prosperity": "行业赛道分析",
    "financial_trend": "财务质量深度核查",
    "cashflow_compare": "财务质量深度核查",
    "valuation_percentile": "估值定价与预期差分析",
    "scenario_analysis": "风险识别与情景分析",
}

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
    execution_mode: str = "hybrid"

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
        prompt = self._build_prompt(stock_code, stock_name, input_data.context, chart_pack)
        allow_live_llm = bool(input_data.context.get("_allow_live_llm"))
        llm_warnings: list[str] = []
        markdown = ""
        llm_invoked = False
        model_used: str | None = None
        runtime_mode = "deterministic"

        if allow_live_llm:
            for model in self._get_model_candidates():
                try:
                    llm_invoked = True
                    candidate = await self.llm.call(
                        prompt=prompt,
                        system_prompt=SYSTEM_PROMPT,
                        model=model,
                    )
                    validation_errors = self._collect_validation_errors(candidate)
                    if validation_errors:
                        llm_warnings.append(f"报告模型 {model} 输出未通过校验: {'; '.join(validation_errors[:2])}")
                        continue
                    markdown = candidate.strip()
                    model_used = model
                    runtime_mode = "llm"
                    break
                except (LLMError, LLMRateLimitError) as exc:
                    llm_warnings.append(f"报告模型 {model} 调用失败: {exc}")

        if not markdown:
            runtime_mode = "hybrid" if llm_invoked else "deterministic"
            markdown = self._build_fallback_report(
                stock_code=stock_code,
                stock_name=stock_name,
                context=input_data.context,
                warnings=warnings + llm_warnings,
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
            errors=warnings + llm_warnings,
            data_sources=["综合分析结果"],
            confidence=confidence,
            summary=f"{summary_prefix}({len(markdown)}字)",
            execution_mode=runtime_mode,
            llm_invoked=llm_invoked,
            model_used=model_used,
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
        self,
        stock_code: str,
        stock_name: str,
        context: dict[str, Any],
        chart_pack: list[dict[str, Any]],
    ) -> str:
        """Build the LLM prompt from cleaned and analyzed data."""
        parts = [f"# 标的\n- 股票代码: {stock_code}\n- 股票名称: {stock_name or 'N/A'}\n"]
        parts.append("请基于以下结构化信息输出完整深度研究报告。\n")
        pipeline_status = context.get("pipeline_status", {})
        skipped_agents = {
            str(item).strip()
            for item in list(pipeline_status.get("agents_skipped", []) or [])
            if str(item).strip()
        }

        if pipeline_status:
            parts.append("## 模块执行状态")
            parts.append(f"- 已完成模块: {self._stringify(pipeline_status.get('agents_completed', []), default='N/A')}")
            parts.append(f"- 跳过/失败模块: {self._stringify(pipeline_status.get('agents_skipped', []), default='无')}")
            for error in list(pipeline_status.get("errors", []) or [])[:6]:
                parts.append(f"- 执行提示: {error}")
            parts.append("- 写作约束: 若某模块被标记为跳过/失败，对应章节必须明确写出“模块未完成，仅基于原始资料做初步整理”，不能写成已完成的结构化分析结论。")
            parts.append("")

        cleaned = context.get("cleaned_data", {})
        stock_info = cleaned.get("stock_info", {})
        realtime = cleaned.get("realtime", {})
        quality_gate = cleaned.get("quality_gate", {})
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

        if quality_gate:
            parts.append("## 证据闸门与字段约束")
            parts.extend(
                [
                    f"- 闸门状态: {'阻断' if quality_gate.get('blocked') else '放行'}",
                    f"- 核心证据分: {quality_gate.get('core_evidence_score', 0):.0%}",
                    f"- 阻断字段: {self._stringify(quality_gate.get('blocking_fields', []), default='无')}",
                    f"- 弱证据字段: {self._stringify(quality_gate.get('weak_fields', []), default='无')}",
                ]
            )
            for note in list(quality_gate.get("consistency_notes", []) or [])[:4]:
                parts.append(f"- 约束说明: {note}")
            parts.append("- 写作约束: 阻断字段和弱证据字段必须在对应章节显式说明，不能写成确定性正面判断。")
            parts.append("")

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

        cross_verification = cleaned.get("cross_verification", {})
        if cross_verification and cross_verification.get("verified_metrics"):
            parts.append("## 多来源交叉验证")
            parts.append(f"- 整体置信度: {cross_verification.get('overall_confidence', 0):.0%}")
            if cross_verification.get("summary"):
                parts.append(f"- 总结: {cross_verification.get('summary')}")
            for metric in cross_verification.get("verified_metrics", [])[:6]:
                if not isinstance(metric, dict):
                    continue
                parts.append(
                    f"- {metric.get('metric_name', 'N/A')}: "
                    f"consistency={metric.get('consistency_flag', 'N/A')} | "
                    f"recommended={metric.get('recommended_value', 'N/A')} | "
                    f"sources={', '.join(metric.get('sources', [])[:4]) or 'N/A'}"
                )
            if cross_verification.get("divergent_metrics"):
                parts.append(f"- 需重点复核: {', '.join(cross_verification.get('divergent_metrics', [])[:5])}")
            parts.append("")

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
                "industry",
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
                "business_model",
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
                "governance",
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
                "financial",
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
                "valuation",
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
                "risk",
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

        for title, agent_name, key, fields in analysis_specs:
            payload = context.get(key, {})
            if not payload and agent_name not in skipped_agents:
                continue
            parts.append(f"## {title}")
            if agent_name in skipped_agents:
                parts.append("- module_status: skipped")
                parts.append("- module_warning: 对应结构化分析模块本次执行失败/跳过，本节只能基于原始资料做初步整理，并明确提示待验证。")
            for field in fields:
                if field not in payload:
                    continue
                value = payload.get(field)
                if value in ("", None, [], {}):
                    continue
                parts.append(f"- {field}: {self._stringify(value)}")
            parts.append("")

        deep_review = context.get("deep_research_review", {})
        if deep_review:
            parts.append("## 深度复核")
            for field in (
                "counter_thesis",
                "review_summary",
                "confidence_adjustment",
                "challenge_points",
                "key_assumptions",
                "sensitivity_checks",
                "what_would_change_my_mind",
            ):
                value = deep_review.get(field)
                if value in ("", None, [], {}):
                    continue
                parts.append(f"- {field}: {self._stringify(value)}")
            parts.append("")

        parts.append("请按指定 8 个标题输出，每节都写出“结论 / 论据 / 数据来源”。务必吸收原始资料摘录，不要只复述分析 JSON。")
        if chart_pack:
            parts.append("## 图表资源")
            parts.append(
                "- 正文如需插入图表，请单独成行使用占位符 `:::charts chart_id1,chart_id2:::`，不要放进代码块，也不要改写 chart_id。"
            )
            parts.append("- 如果某个章节引用了图表，请在该章节的“结论”后、“论据”前插入对应占位符。")
            for item in chart_pack:
                chart_id = str(item.get("chart_id", "") or "").strip()
                if not chart_id:
                    continue
                parts.append(
                    f"- {chart_id}: {item.get('title', 'N/A')} | 建议章节: "
                    f"{INLINE_CHART_SECTION_HINTS.get(chart_id, '按内容相关性放置')} | "
                    f"摘要: {item.get('summary', 'N/A')}"
                )
            parts.append("")

        parts.append("写作检查：严格使用 8 个固定章节标题；如引用图表，请把图表占位符放在对应章节的结论后、论据前。")
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

        field_quality = normalize_field_quality_map(cleaned.get("field_quality"))
        for trace in sorted(
            field_quality.values(),
            key=lambda item: (
                getattr(getattr(item, "blocking_level", ""), "value", getattr(item, "blocking_level", "")) not in {"critical", "core"},
                float(getattr(item, "confidence_score", 0.0) or 0.0),
            ),
        )[:10]:
            evidence_state = getattr(trace.evidence_state, "value", trace.evidence_state)
            value_state = getattr(trace.value_state, "value", trace.value_state)
            source_names = ", ".join(item.source_name for item in list(trace.source_values or [])[:3]) or "待补来源"
            evidence_pack.append(
                EvidencePackItem(
                    category="field_quality",
                    title=str(trace.label or trace.field),
                    source=source_names,
                    url="",
                    excerpt=(
                        f"字段={trace.field} | 值状态={value_state} | 证据状态={evidence_state} | "
                        f"报告期={trace.report_period or '待验证'} | 单位={trace.unit or 'N/A'}"
                    ),
                    fields=[str(trace.field)],
                    reference_date=str(trace.report_period or ""),
                )
            )
        return evidence_pack

    def _build_chart_pack(self, context: dict[str, Any]) -> list[ChartPackItem]:
        """Build chart-ready structured data."""
        cleaned = context.get("cleaned_data", {})
        financials = self._prepare_financial_chart_rows(cleaned.get("financials", []))
        valuation = self._prepare_observation_rows(cleaned.get("valuation", []), label_field="date", keep_last=24)
        valuation_analysis = context.get("valuation_analysis", {})
        risk_analysis = context.get("risk_analysis", {})
        industry_enhanced = cleaned.get("industry_enhanced", {})
        peer_cross = cleaned.get("cross_verification", {}) or context.get("cross_verification", {}) or {}

        chart_pack: list[ChartPackItem] = [
            ChartPackItem(
                chart_id="financial_trend",
                title="财务趋势",
                chart_type="line",
                unit="元",
                summary="展示近几期已披露财报中的营收与净利润趋势。",
                series=[
                    ChartSeries(
                        name="营收",
                        points=self._build_numeric_points(financials, label_field="report_date", value_field="revenue"),
                    ),
                    ChartSeries(
                        name="净利润",
                        points=self._build_numeric_points(financials, label_field="report_date", value_field="net_profit"),
                    ),
                ],
            ),
            ChartPackItem(
                chart_id="cashflow_compare",
                title="现金流对比",
                chart_type="bar",
                unit="元",
                summary="对比近几期已披露财报中的经营与自由现金流。",
                series=[
                    ChartSeries(
                        name="经营现金流",
                        points=self._build_numeric_points(financials, label_field="report_date", value_field="operating_cashflow"),
                    ),
                    ChartSeries(
                        name="自由现金流",
                        points=self._build_numeric_points(financials, label_field="report_date", value_field="free_cashflow"),
                    ),
                ],
            ),
            ChartPackItem(
                chart_id="valuation_percentile",
                title="估值分位",
                chart_type="line",
                unit="倍",
                summary="展示历史可用 PE/PB 样本与当前估值轨迹。",
                series=[
                    ChartSeries(
                        name="PE(TTM)",
                        points=self._build_numeric_points(valuation, label_field="date", value_field="pe_ttm"),
                    ),
                    ChartSeries(
                        name="PB(MRQ)",
                        points=self._build_numeric_points(valuation, label_field="date", value_field="pb_mrq"),
                    ),
                ],
            ),
            ChartPackItem(
                chart_id="peer_comparison",
                title="同行对比",
                chart_type="table",
                unit="",
                summary="基于申万同行识别与交叉验证结果生成的横向对比表。",
                series=self._build_peer_chart_series(peer_cross),
            ),
            ChartPackItem(
                chart_id="scenario_analysis",
                title="三情景测算",
                chart_type="bar",
                unit="元",
                summary="展示乐观/中性/悲观情景的价格带。",
                series=self._build_scenario_chart_series(risk_analysis),
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
                EvidenceRef(
                    source=str(item.get("source", "")),
                    source_priority=int(item.get("source_priority", 0) or 0),
                    title=str(item.get("title", "")),
                    field=str(item.get("field", "")),
                    excerpt=str(item.get("excerpt", "")),
                    url=str(item.get("url", "")),
                    reference_date=str(item.get("reference_date", "")),
                )
                for item in valuation_refs
            ]
        peer_refs = self._build_peer_chart_evidence_refs(peer_cross)
        if peer_refs:
            chart_pack[3].evidence_refs = peer_refs
        risk_refs = [item for item in risk_analysis.get("evidence_refs", [])[:3] if isinstance(item, dict)]
        if risk_refs:
            chart_pack[4].evidence_refs = [
                EvidenceRef(
                    source=str(item.get("source", "")),
                    source_priority=int(item.get("source_priority", 0) or 0),
                    title=str(item.get("title", "")),
                    field=str(item.get("field", "")),
                    excerpt=str(item.get("excerpt", "")),
                    url=str(item.get("url", "")),
                    reference_date=str(item.get("reference_date", "")),
                )
                for item in risk_refs
            ]
        return chart_pack

    @staticmethod
    def _safe_chart_number(value: Any) -> float | None:
        if value in (None, "", "-"):
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric):
            return None
        return numeric

    @staticmethod
    def _normalize_axis_label(value: Any) -> str:
        if value in (None, ""):
            return ""
        return str(value).strip()

    @classmethod
    def _prepare_financial_chart_rows(cls, values: Any) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in values or []:
            if not isinstance(item, dict):
                continue
            report_date = cls._normalize_axis_label(item.get("report_date"))
            if not report_date or not re.match(r"^\d{4}-(03-31|06-30|09-30|12-31)$", report_date):
                continue
            normalized = dict(item)
            normalized["report_date"] = report_date
            rows.append(normalized)
        rows.sort(key=lambda item: str(item.get("report_date") or ""))
        return rows[-8:]

    @classmethod
    def _prepare_observation_rows(
        cls,
        values: Any,
        *,
        label_field: str,
        keep_last: int,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in values or []:
            if not isinstance(item, dict):
                continue
            label = cls._normalize_axis_label(item.get(label_field))
            if not label:
                continue
            normalized = dict(item)
            normalized[label_field] = label
            rows.append(normalized)
        rows.sort(key=lambda item: str(item.get(label_field) or ""))
        return rows[-keep_last:]

    @classmethod
    def _build_numeric_points(
        cls,
        values: list[dict[str, Any]],
        *,
        label_field: str,
        value_field: str,
    ) -> list[dict[str, Any]]:
        points: list[dict[str, Any]] = []
        for item in values:
            label = cls._normalize_axis_label(item.get(label_field))
            numeric = cls._safe_chart_number(item.get(value_field))
            if not label or numeric is None:
                continue
            points.append({"x": label, "y": numeric})
        return points

    def _build_peer_chart_series(self, peer_cross: dict[str, Any]) -> list[ChartSeries]:
        """Build table-ready peer comparison data from cross verification output."""
        peers = [item for item in list(peer_cross.get("peers", []) or []) if isinstance(item, dict)]
        if not peers:
            return []

        series: list[ChartSeries] = []
        for item in peers[:6]:
            stock_code = str(item.get("stock_code") or "").strip() or "N/A"
            industry_level = str(item.get("industry_level") or "").strip() or "N/A"
            rank = item.get("rank_in_industry")
            market_cap = item.get("market_cap")
            points = [
                {"x": "股票代码", "y": stock_code},
                {"x": "行业级别", "y": industry_level},
                {"x": "行业排名", "y": rank if rank not in (None, "") else "N/A"},
                {
                    "x": "总市值(亿元)",
                    "y": round(float(market_cap) / 100000000, 2) if market_cap not in (None, "") else "N/A",
                },
            ]
            series.append(
                ChartSeries(
                    name=str(item.get("stock_name") or stock_code).strip() or stock_code,
                    points=points,
                )
            )
        return series

    def _build_scenario_chart_series(self, risk_analysis: dict[str, Any]) -> list[ChartSeries]:
        """Build scenario chart series in the format expected by the frontend."""
        scenarios = [item for item in list(risk_analysis.get("scenarios", []) or []) if isinstance(item, dict)]
        series: list[ChartSeries] = []
        for item in scenarios[:3]:
            label = str(item.get("scenario") or item.get("name") or "").strip()
            target_price = item.get("target_price")
            if not label or target_price in (None, ""):
                continue
            series.append(
                ChartSeries(
                    name=label,
                    points=[{"x": label, "y": target_price}],
                )
            )
        return series

    def _build_peer_chart_evidence_refs(self, peer_cross: dict[str, Any]) -> list[EvidenceRef]:
        """Collect concise evidence refs for peer comparison charts."""
        refs: list[EvidenceRef] = []
        seen: set[tuple[str, str, str]] = set()
        for metric in list(peer_cross.get("verified_metrics", []) or []):
            if not isinstance(metric, dict):
                continue
            for item in list(metric.get("evidence_refs", []) or []):
                if not isinstance(item, dict):
                    continue
                key = (
                    str(item.get("source", "")),
                    str(item.get("title", "")),
                    str(item.get("reference_date", "")),
                )
                if key in seen:
                    continue
                seen.add(key)
                refs.append(
                    EvidenceRef(
                        source=str(item.get("source", "")),
                        source_priority=int(item.get("source_priority", 0) or 0),
                        title=str(item.get("title", "")),
                        field=str(item.get("field", "")),
                        excerpt=str(item.get("excerpt", "")),
                        url=str(item.get("url", "")),
                        reference_date=str(item.get("reference_date", "")),
                    )
                )
                if len(refs) >= 4:
                    return refs
        return refs

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
        cross_verification = cleaned.get("cross_verification", {})
        screening = context.get("screening", {})
        industry = context.get("industry_analysis", {})
        business = context.get("business_model_analysis", {})
        governance = context.get("governance_analysis", {})
        financial = context.get("financial_analysis", {})
        valuation = context.get("valuation_analysis", {})
        risk = context.get("risk_analysis", {})
        deep_review = context.get("deep_research_review", {})
        pipeline_status = context.get("pipeline_status", {})
        quality_gate = cleaned.get("quality_gate", {})
        skipped_agents = {
            str(item).strip()
            for item in list(pipeline_status.get("agents_skipped", []) or [])
            if str(item).strip()
        }
        announcements = cleaned.get("announcements", [])
        research_reports = cleaned.get("research_reports", [])
        industry_enhanced = cleaned.get("industry_enhanced", {})
        policy_documents = cleaned.get("policy_documents", [])
        compliance_events = cleaned.get("compliance_events", [])
        patents = cleaned.get("patents", [])
        financials = [item for item in cleaned.get("financials", []) if isinstance(item, dict)]
        missing_fields = list(cleaned.get("missing_fields", []) or [])
        latest_financial = financials[0] if financials else {}
        blocked = bool(quality_gate.get("blocked"))
        coverage_ratio = float(quality_gate.get("coverage_ratio") or cleaned.get("coverage_ratio") or 0.0)
        business_summary = self._sanitize_business_summary(stock_info.get("main_business"))
        boundary_items = list(
            dict.fromkeys(
                list(quality_gate.get("blocking_fields", []) or [])
                + list(quality_gate.get("weak_fields", []) or [])
                + missing_fields
            )
        )

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
            f"> 报告形态: {'证据受限摘要' if (blocked or skipped_agents or warnings) else '结构化自动研报'}",
        ]
        if blocked or skipped_agents or warnings:
            notice_parts: list[str] = []
            if blocked:
                notice_parts.append("证据闸门未通过")
            if skipped_agents:
                notice_parts.append(f"存在未完成模块: {self._stringify(sorted(skipped_agents), default='无')}")
            if warnings:
                notice_parts.append("正文由结构化结果自动整理")
            lines.append(f"> 使用提示: {'；'.join(notice_parts)}。建议先把本页当作研究摘要阅读，再结合证据链做复核。")
        lines.extend(
            [
                "",
                "## 阅读提示",
                "",
                f"- 数据覆盖率: {coverage_ratio:.0%}",
                (
                    f"- 核心证据分: {float(quality_gate.get('core_evidence_score') or 0.0):.0%}"
                    if quality_gate
                    else "- 核心证据分: N/A"
                ),
                f"- 当前更适合作为{'阶段性研究摘要' if (blocked or skipped_agents or warnings) else '完整阅读版报告'}",
                "",
            ]
        )

        lines.extend(
            self._build_section(
                title="企业画像",
                conclusion=self._first_non_empty(
                    (
                        f"{stock_name or stock_info.get('name', stock_code)} 当前可先按“{business_summary}”的业务方向跟踪，"
                        "但更细的产品与客户结构仍需结合后续公告继续补证。"
                    )
                    if business_summary
                    else "",
                    f"{stock_name or stock_info.get('name', stock_code)} 基础画像已经建立，但主营业务和经营边界仍需继续补充公告与研报证据。",
                ),
                reasons=[
                    f"主营业务与定位: {business_summary or '暂未抽取到稳定主营业务表述，当前以行业归属与公告原文做替代判断。'}",
                    f"基础信息: 所属行业 {stock_info.get('industry_sw', 'N/A')}，上市日期 {stock_info.get('listing_date', 'N/A')}，实际控制人 {stock_info.get('actual_controller', 'N/A')}。",
                    f"交易状态: 最新价 {realtime.get('close', 'N/A')}，总市值 {fmt_cap(realtime.get('market_cap'))}。",
                    f"最近可核验公告: {periodic_notice.get('title', 'N/A')}。",
                    f"公告摘录: {periodic_notice.get('excerpt', '暂无原文摘录')}",
                ],
                source="基础资料、实时行情、年报/季报公告原文",
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
                    f"初筛判定: {screening.get('verdict', 'N/A')}，研究建议 {screening.get('recommendation', 'N/A')}。",
                    f"关键风险: {self._stringify(screening.get('key_risks', []), default='暂无明确关键风险')}",
                    f"闸门状态: {'阻断' if blocked else '放行'}；若闸门阻断，则后续章节应优先理解为风险提示而非终局结论。",
                ],
                source="初筛 Agent 输出",
            )
        )

        lines.extend(
            self._build_section(
                title="行业赛道分析",
                conclusion=self._first_non_empty(
                    "行业结构化分析模块本次执行失败，当前内容仅基于政策原文与行业高频资料做线索整理，竞争格局和公司行业地位仍待验证。"
                    if "industry" in skipped_agents
                    else "",
                    industry.get("conclusion"),
                    "行业资料不足，当前仅能做定性判断，建议继续验证景气度与竞争格局。",
                ),
                reasons=[
                    "模块状态: industry 模块本次未通过校验，以下内容应理解为线索整理而非完整行业结论。"
                    if "industry" in skipped_agents
                    else "模块状态: industry 模块执行成功。",
                    f"行业判断: 生命周期 {industry.get('lifecycle', 'N/A')}，竞争格局 {industry.get('competition_pattern', 'N/A')}，景气方向 {industry.get('prosperity_direction', 'N/A')}。",
                    f"公司所处位置: {industry.get('company_position', 'N/A')}",
                    f"行业高频线索: {self._stringify(industry_enhanced.get('data_points', []), default='暂无高频行业数据')}",
                    f"政策与外部证据: {latest_policy.get('title', 'N/A')} / {latest_policy.get('excerpt', '暂无政策摘录')}",
                ],
                source="行业分析 Agent 输出、行业高频数据、官方政策原文",
                chart_ids=["industry_prosperity", "peer_comparison"],
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
                    f"商业模式评分与资产模式: {business.get('model_score', 'N/A')} / {business.get('asset_model', 'N/A')}",
                    f"护城河与盈利驱动: {business.get('moat_overall', 'N/A')} / {business.get('profit_driver', 'N/A')}",
                    f"反证视角: {business.get('negative_view', 'N/A')}",
                    f"卖方研报线索: {latest_research.get('title', 'N/A')}",
                    f"研报摘录: {latest_research.get('excerpt', latest_research.get('summary', '暂无研报原文摘录'))}",
                ],
                source="商业模式分析 Agent 输出、卖方研报原文、年报摘录",
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
                    f"治理评分与诚信判断: {governance.get('governance_score', '待验证')} / {governance.get('management_integrity', 'N/A')}",
                    f"管理层评价: {governance.get('management_assessment', 'N/A')}",
                    f"资本配置与分红政策: {governance.get('capital_allocation', 'N/A')} / {governance.get('dividend_policy', 'N/A')}",
                    f"治理类公告: {governance_notice.get('title', 'N/A')}",
                    f"公告摘录: {governance_notice.get('excerpt', '暂无治理公告原文摘录')}",
                    f"合规线索: {latest_compliance.get('title', '暂无官方合规事件')} / {latest_compliance.get('excerpt', latest_compliance.get('summary', '暂无合规摘录'))}",
                ],
                source="治理分析 Agent 输出、股东数据、治理公告原文",
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
                    f"最近财报期: {latest_financial.get('report_date', 'N/A')}，营收 {latest_financial.get('revenue', 'N/A')}，净利润 {latest_financial.get('net_profit', 'N/A')}。",
                    f"综合评分与趋势: {financial.get('overall_score', 'N/A')} / {financial.get('trend_summary', 'N/A')}",
                    f"现金流验证: {financial.get('cashflow_verification', 'N/A')}",
                    f"异常标记: {self._stringify(financial.get('anomaly_flags', []), default='暂无明显异常标记')}",
                    f"多源校验: {cross_verification.get('summary', '暂无多源交叉验证结果')}",
                    f"原文辅助验证: {latest_research.get('summary', '暂无卖方研报支持')} / {periodic_notice.get('excerpt', '暂无财报原文摘录')}",
                ],
                source="财务分析 Agent 输出、定期报告原文、卖方研报原文",
                chart_ids=["financial_trend", "cashflow_compare"],
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
                    f"估值水平: {valuation.get('valuation_level', 'N/A')}，当前价格 {valuation.get('current_price', realtime.get('close', 'N/A'))}。",
                    f"合理区间: {self._format_range(valuation.get('reasonable_range_low'), valuation.get('reasonable_range_high'))}",
                    f"PE/PB 分位: {valuation.get('pe_percentile', 'N/A')} / {valuation.get('pb_percentile', 'N/A')}",
                    f"安全边际: {valuation.get('margin_of_safety', 'N/A')}",
                ],
                source="估值分析 Agent 输出",
                chart_ids=["valuation_percentile"],
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
                    f"政策与公告原文: {latest_policy.get('excerpt', '暂无政策原文摘录')} / {governance_notice.get('excerpt', periodic_notice.get('excerpt', '暂无公告原文摘录'))}",
                ],
                source="风险分析 Agent 输出、政策原文、公告原文",
                chart_ids=["scenario_analysis"],
            )
        )

        if deep_review:
            lines.extend(
                [
                    "## 深度复核附录",
                    "",
                    f"结论：{self._first_non_empty(deep_review.get('review_summary'), deep_review.get('counter_thesis'), '深度复核已执行，但尚未形成稳定附加结论。')}",
                    "",
                    "论据：",
                ]
            )
            for item in (
                list(deep_review.get("challenge_points", []) or [])[:3]
                + list(deep_review.get("sensitivity_checks", []) or [])[:2]
            ):
                lines.append(f"- {item}")
            if deep_review.get("what_would_change_my_mind"):
                lines.extend(["", "改变判断的触发器："])
                for item in list(deep_review.get("what_would_change_my_mind", []) or [])[:3]:
                    lines.append(f"- {item}")
            lines.extend(["", "数据来源：深度复核 Agent", ""])

        lines.extend(
            [
                "## 研究边界与待补证据",
                "",
                "以下事项决定了本次正文更适合作为研究摘要，而不是终局版研报：",
                f"- 证据闸门: {'阻断' if blocked else '放行'}",
            ]
        )
        if skipped_agents:
            lines.append(f"- 待验证项: 以下模块未形成稳定结论 - {self._stringify(sorted(skipped_agents), default='无')}")
        for item in boundary_items[:8]:
            lines.append(f"- 待验证项: {item}")
        if cross_verification.get("divergent_metrics"):
            lines.append(
                f"- 待验证项: 多源口径存在分歧 - {self._stringify(cross_verification.get('divergent_metrics', [])[:4], default='无')}"
            )
        if quality_gate:
            for note in list(quality_gate.get("consistency_notes", []) or [])[:3]:
                lines.append(f"- 约束说明: {note}")
        lines.append("")

        lines.extend(
            [
                "## 证据来源与复核入口",
                "",
                "以下资料可直接作为前端证据视图和人工复核入口：",
            ]
        )
        for item in evidence_pack[:6]:
            lines.append(
                f"- [{item.get('category', 'evidence')}] {item.get('title', 'N/A')} | "
                f"来源: {item.get('source', 'N/A')} | 摘录: {str(item.get('excerpt', ''))[:90]}"
            )
        if chart_pack:
            lines.extend(["", "本次同步输出的图表："])
            for item in chart_pack:
                lines.append(f"- {item.get('title', 'N/A')}: {item.get('summary', '暂无解读')}")
        if latest_patent:
            lines.append(
                f"- 技术/专利线索: {latest_patent.get('title', 'N/A')} | 摘录: {latest_patent.get('excerpt', latest_patent.get('summary', '暂无摘要'))}"
            )
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
        chart_ids: list[str] | None = None,
    ) -> list[str]:
        lines = [f"## {title}", "", f"结论：{conclusion}", "", "论据："]
        for reason in reasons:
            lines.append(f"- {reason}")
        if chart_ids:
            lines.extend(["", f":::charts {','.join(chart_ids)}:::"])
        lines.extend(["", f"数据来源：{source}", ""])
        return lines

    @staticmethod
    def _sanitize_business_summary(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = re.sub(r"^(?:主营业务|主要业务|核心业务|公司主要从事|主要从事的业务)[:：]?\s*", "", text)
        text = text.strip("：:；;，,。 ")
        if len(text) < 10:
            return ""
        if text.startswith(("并", "及", "以及", "形成", "打造", "实现", "推进", "拓展")):
            return ""
        if text.endswith(("创造了", "形成了", "实现了", "打造了", "提升了", "推进了", "拓展了", "包括", "涵盖")):
            return ""
        return text[:120]

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
