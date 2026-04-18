from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from investresearch import cli as cli_module
from investresearch.api.deps import TaskManager
from investresearch.api.routes import research as research_route
from investresearch.core.models import (
    AgentInput,
    AgentOutput,
    AgentStatus,
    QualityGateDecision,
    RegressionBaselineSnapshot,
    ResearchReport,
)
from investresearch.decision_layer.conclusion import ConclusionAgent
from investresearch.decision_layer import coordinator as coordinator_module
from investresearch.decision_layer.coordinator import ResearchCoordinator
from investresearch.decision_layer.report import REQUIRED_SECTIONS, ReportAgent


def _report_context() -> dict:
    return {
        "cleaned_data": {
            "stock_info": {
                "name": "贵州茅台",
                "industry_sw": "白酒",
                "actual_controller": "贵州省国资委",
                "main_business": "高端白酒生产销售",
            },
            "realtime": {"close": 1500.0, "market_cap": 1_800_000_000_000, "pe_ttm": 22.0, "pb_mrq": 8.0},
            "financials": [
                {
                    "report_date": "2025-12-31",
                    "revenue": 180_000_000_000,
                    "net_profit": 90_000_000_000,
                    "operating_cashflow": 100_000_000_000,
                    "free_cashflow": 80_000_000_000,
                }
            ],
            "valuation": [{"date": "2025-12-31", "pe_ttm": 22.0, "pb_mrq": 8.0}],
            "announcements": [],
            "research_reports": [],
            "industry_enhanced": {"data_points": ["白酒行业景气平稳"]},
            "policy_documents": [],
            "coverage_ratio": 0.82,
            "missing_fields": [],
        },
        "screening": {"verdict": "通过", "recommendation": "建议继续深度研究", "key_risks": ["需求波动"]},
        "industry_analysis": {"conclusion": "行业景气平稳", "lifecycle": "成熟期", "competition_pattern": "寡头竞争"},
        "business_model_analysis": {"conclusion": "品牌与渠道构成护城河", "model_score": 7.8, "moat_overall": "宽"},
        "governance_analysis": {"conclusion": "治理结构稳定", "governance_score": 7.2},
        "financial_analysis": {"conclusion": "现金流质量较好", "overall_score": 8.4, "trend_summary": "收入利润持续增长"},
        "valuation_analysis": {
            "conclusion": "估值处于合理偏低区间",
            "valuation_level": "低估",
            "reasonable_range_low": 1600.0,
            "reasonable_range_high": 1800.0,
        },
        "risk_analysis": {
            "conclusion": "主要风险可跟踪",
            "overall_risk_level": "中",
            "risk_score": 4.8,
            "fatal_risks": [],
            "monitoring_points": ["季度营收", "渠道库存", "政策变动"],
            "scenarios": [],
        },
    }


def _valid_markdown() -> str:
    body = []
    for section in REQUIRED_SECTIONS:
        body.extend(
            [
                f"## {section}",
                "",
                "结论：当前章节给出清晰判断，并补充必要约束。",
                "",
                "论据：",
                "- 证据A：来自结构化分析结果。",
                "- 证据B：来自原始资料摘录。",
                "- 证据C：来自经营与估值交叉验证。",
                "",
                "数据来源：公告原文、财报、行业资料、结构化分析模块。",
                "",
                ("补充说明。" * 40),
                "",
            ]
        )
    return "\n".join(body)


def _sample_quality_gate() -> QualityGateDecision:
    return QualityGateDecision(
        blocked=False,
        core_evidence_score=0.86,
        weak_fields=["financials.latest.free_cashflow"],
        reasons=["自由现金流为旁证校验，暂不作为阻断项。"],
        consistency_notes=["初筛通过与最终观望之间已显式说明估值约束。"],
        coverage_ratio=0.84,
        company_cross_confidence=0.91,
        peer_verified=3,
    )


def _sample_baseline_snapshot() -> RegressionBaselineSnapshot:
    return RegressionBaselineSnapshot(
        stock_code="600519",
        stock_name="贵州茅台",
        depth="deep",
        generated_at=datetime(2026, 4, 18, 10, 0, 0),
        coverage_ratio=0.84,
        completeness=0.81,
        core_evidence_score=0.86,
        missing_fields=["industry.market_size"],
        warning_count=1,
        initial_verdict="通过",
        final_recommendation="观望",
        quality_gate_blocked=False,
        quality_gate_reasons=["自由现金流字段待补强"],
        consistency_notes=["估值偏贵导致结论转保守。"],
    )


def _sample_report() -> ResearchReport:
    return ResearchReport(
        stock_code="600519",
        stock_name="贵州茅台",
        report_date=datetime(2026, 4, 18, 9, 30, 0),
        depth="deep",
        markdown=_valid_markdown(),
        quality_gate=_sample_quality_gate(),
        baseline_snapshot=_sample_baseline_snapshot(),
        agents_completed=["data_collector", "data_cleaner", "report", "conclusion"],
        agents_skipped=[],
        errors=[],
    )


@pytest.mark.asyncio
async def test_conclusion_agent_uses_llm_and_applies_guardrails(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = ConclusionAgent()

    async def fake_call_json(*, prompt: str, system_prompt: str | None = None, model: str = "qwen3-plus") -> dict:
        del prompt, system_prompt, model
        return {
            "recommendation": "买入(强烈)",
            "confidence_level": "高",
            "target_price_low": 30.0,
            "target_price_high": 36.0,
            "current_price": 20.0,
            "upside_pct": 60.0,
            "risk_level": "中",
            "key_reasons_buy": ["基本面改善"],
            "key_reasons_sell": ["验证不足"],
            "key_assumptions": ["利润持续增长", "需求恢复"],
            "monitoring_points": ["季度营收", "渠道库存", "政策变化"],
            "position_advice": "积极配置",
            "holding_period": "6-12个月",
            "stop_loss_price": 17.0,
            "conclusion_summary": "模型给出积极结论。",
        }

    monkeypatch.setattr(agent.llm, "call_json", fake_call_json)

    context = {
        "_allow_live_llm": True,
        "cleaned_data": {"coverage_ratio": 0.4, "realtime": {"close": 20.0}},
        "screening": {"verdict": "通过", "key_risks": ["治理信息不足"]},
        "business_model_analysis": {"conclusion": "商业模式待进一步验证", "moat_overall": "窄"},
        "industry_analysis": {"conclusion": "行业核心数据不足", "evidence_status": "partial"},
        "governance_analysis": {"conclusion": "治理资料不足", "evidence_status": "insufficient"},
        "valuation_analysis": {"valuation_level": "待验证", "evidence_status": "insufficient"},
        "risk_analysis": {
            "overall_risk_level": "中",
            "fatal_risks": ["关键资料不足"],
            "monitoring_points": ["季度营收", "渠道库存", "治理公告"],
        },
    }

    output = await agent.safe_run(AgentInput(stock_code="603659", context=context))
    conclusion = output.data["conclusion"]

    assert output.execution_mode == "llm"
    assert output.llm_invoked is True
    assert output.model_used == "qwen3-plus"
    assert conclusion["recommendation"] == "观望"
    assert conclusion["confidence_level"] == "低"
    assert conclusion["target_price_low"] is None
    assert len(conclusion["monitoring_plan"]) == 3


@pytest.mark.asyncio
async def test_conclusion_agent_strips_unsupported_cashflow_bullish_claims(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = ConclusionAgent()

    async def fake_call_json(
        *,
        prompt: str,
        system_prompt: str | None = None,
        model: str = "qwen3-plus",
    ) -> dict:
        del system_prompt, model
        assert "数据质量约束" in prompt
        assert "不得将现金流改善、现金流与利润匹配良好、净现比改善作为买入理由" in prompt
        return {
            "recommendation": "买入(谨慎)",
            "confidence_level": "高",
            "target_price_low": 40.0,
            "target_price_high": 45.0,
            "current_price": 35.91,
            "upside_pct": 18.0,
            "risk_level": "高",
            "key_reasons_buy": ["经营现金流与利润匹配良好，利润含金量提升", "行业龙头份额稳固"],
            "key_reasons_sell": ["行业竞争激烈"],
            "key_assumptions": ["需求恢复", "毛利率稳定"],
            "monitoring_points": ["季度营收", "毛利率", "现金流"],
            "core_thesis": ["经营现金流与利润匹配良好，V型反转成立", "龙头份额稳固"],
            "major_risks": ["行业竞争激烈"],
            "failure_conditions": ["需求恢复不及预期"],
            "position_advice": "谨慎配置",
            "holding_period": "6-12个月",
            "conclusion_summary": "经营现金流与利润匹配良好，财务趋势明显改善，可继续关注。",
        }

    monkeypatch.setattr(agent.llm, "call_json", fake_call_json)

    context = {
        "_allow_live_llm": True,
        "cleaned_data": {
            "coverage_ratio": 0.72,
            "realtime": {"close": 35.91},
            "financials": [
                {
                    "report_date": "2025-12-31",
                    "_cashflow_suspect_fields": ["operating_cashflow", "free_cashflow"],
                }
            ],
            "missing_fields": ["financials.operating_cashflow", "financials.free_cashflow"],
        },
        "screening": {"verdict": "通过", "key_risks": ["现金流真实性待验证"]},
        "business_model_analysis": {"conclusion": "平台化布局形成一定护城河", "moat_overall": "宽", "profit_driver": "平台化布局带来的规模效应"},
        "industry_analysis": {
            "conclusion": "行业龙头受益于集中度提升",
            "evidence_status": "ok",
            "company_position": "隔膜涂覆龙头地位稳固",
        },
        "governance_analysis": {"conclusion": "治理结构稳定", "evidence_status": "ok"},
        "financial_analysis": {
            "conclusion": "盈利改善但现金流质量待验证",
            "overall_score": 7.2,
            "trend_summary": "2025 年迎来修复，但经营现金流与利润匹配良好。",
        },
        "valuation_analysis": {
            "valuation_level": "高估",
            "reasonable_range_low": 25.03,
            "reasonable_range_high": 27.2,
            "current_price": 35.91,
            "conclusion": "当前估值高于合理区间",
            "evidence_status": "ok",
        },
        "risk_analysis": {
            "overall_risk_level": "高",
            "conclusion": "财务数据真实性存疑（现金流与营收规模严重不匹配）",
            "fatal_risks": ["财务数据真实性存疑（现金流与营收规模严重不匹配）"],
            "monitoring_points": ["经营活动产生的现金流量净额", "毛利率", "订单"],
        },
    }

    output = await agent.safe_run(AgentInput(stock_code="603659", context=context))
    conclusion = output.data["conclusion"]

    assert output.status == AgentStatus.SUCCESS
    assert all("匹配良好" not in item for item in conclusion["key_reasons_buy"])
    assert all("匹配良好" not in item for item in conclusion["core_thesis"])
    assert "匹配良好" not in conclusion["conclusion_summary"]
    assert any("关键现金流字段缺失或口径待验证" in item for item in conclusion["key_reasons_sell"])
    assert any("关键现金流字段缺失或口径待验证" in item for item in conclusion["major_risks"])


@pytest.mark.asyncio
async def test_report_agent_prefers_valid_llm_markdown(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = ReportAgent()

    async def fake_call(*, prompt: str, system_prompt: str | None = None, model: str = "qwen3-plus", **kwargs: object) -> str:
        del prompt, system_prompt, model, kwargs
        return _valid_markdown()

    monkeypatch.setattr(agent.llm, "call", fake_call)

    context = _report_context()
    context["_allow_live_llm"] = True

    output = await agent.safe_run(AgentInput(stock_code="600519", context=context))

    assert output.execution_mode == "llm"
    assert output.llm_invoked is True
    assert output.model_used == "qwen3-plus"
    assert all(section in output.data["markdown"] for section in REQUIRED_SECTIONS)


@pytest.mark.asyncio
async def test_report_agent_includes_skipped_module_status_in_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = ReportAgent()
    context = _report_context()
    context["_allow_live_llm"] = True
    context["pipeline_status"] = {
        "agents_completed": ["financial", "valuation"],
        "agents_skipped": ["industry"],
        "errors": ["industry失败: top_competitors不足"],
    }

    async def fake_call(*, prompt: str, system_prompt: str | None = None, model: str = "qwen3-plus", **kwargs: object) -> str:
        del system_prompt, model, kwargs
        assert "跳过/失败模块: industry" in prompt
        assert "module_status: skipped" in prompt
        assert "module_warning: 对应结构化分析模块本次执行失败/跳过" in prompt
        return _valid_markdown()

    monkeypatch.setattr(agent.llm, "call", fake_call)

    output = await agent.safe_run(AgentInput(stock_code="600519", context=context))

    assert output.status == AgentStatus.SUCCESS


@pytest.mark.asyncio
async def test_coordinator_deep_mode_records_execution_trace() -> None:
    coordinator = ResearchCoordinator()
    coordinator._run_industry_peer_collection = lambda **kwargs: {}  # type: ignore[method-assign]
    coordinator._save_to_knowledge_base = lambda report, context: None  # type: ignore[method-assign]

    async def fake_safe_run_agent(agent, input_data: AgentInput) -> AgentOutput:
        name = getattr(agent, "agent_name", "")
        if name in {"financial", "business_model", "industry", "governance", "valuation", "risk", "deep_review", "report", "conclusion"}:
            assert input_data.context.get("_allow_live_llm") is True
        if name == "data_collector":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={
                    "stock_info": {"name": "示例公司"},
                    "coverage_ratio": 0.85,
                    "collection_status": {},
                    "errors": [],
                },
                summary="collector ok",
            )
        if name == "data_cleaner":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={
                        "cleaned": {
                            "coverage_ratio": 0.82,
                            "stock_info": {"name": "示例公司", "industry_sw": "设备"},
                            "realtime": {"close": 20.0},
                            "financials": [{"report_date": "2025-12-31", "revenue": 100.0}],
                            "announcements": [],
                            "policy_documents": [],
                            "research_reports": [],
                            "missing_fields": [],
                            "field_quality": {
                                "stock_info.main_business": {
                                    "field": "stock_info.main_business",
                                    "label": "主营业务",
                                    "value": "核心设备",
                                    "allowed_sources": ["annual_report_structured"],
                                    "unit": "text",
                                    "period_type": "annual",
                                    "blocking_level": "critical",
                                    "report_period": "2025-12-31",
                                    "value_state": "present",
                                    "evidence_state": "consistent",
                                    "source_count": 2,
                                    "confidence_score": 1.0,
                                    "source_values": [],
                                    "notes": [],
                                },
                                "financials.latest.revenue": {
                                    "field": "financials.latest.revenue",
                                    "label": "最新营收",
                                    "value": 100.0,
                                    "allowed_sources": ["eastmoney_profit", "akshare_financial_abstract"],
                                    "unit": "cny",
                                    "period_type": "cumulative",
                                    "blocking_level": "critical",
                                    "report_period": "2025-12-31",
                                    "value_state": "present",
                                    "evidence_state": "consistent",
                                    "source_count": 2,
                                    "confidence_score": 1.0,
                                    "source_values": [],
                                    "notes": [],
                                },
                                "financials.latest.net_profit": {
                                    "field": "financials.latest.net_profit",
                                    "label": "最新归母净利润",
                                    "value": 20.0,
                                    "allowed_sources": ["eastmoney_profit", "akshare_financial_abstract"],
                                    "unit": "cny",
                                    "period_type": "cumulative",
                                    "blocking_level": "critical",
                                    "report_period": "2025-12-31",
                                    "value_state": "present",
                                    "evidence_state": "consistent",
                                    "source_count": 2,
                                    "confidence_score": 1.0,
                                    "source_values": [],
                                    "notes": [],
                                },
                                "financials.latest.operating_cashflow": {
                                    "field": "financials.latest.operating_cashflow",
                                    "label": "最新经营现金流",
                                    "value": 18.0,
                                    "allowed_sources": ["eastmoney_cashflow", "akshare_financial_abstract"],
                                    "unit": "cny",
                                    "period_type": "cumulative",
                                    "blocking_level": "critical",
                                    "report_period": "2025-12-31",
                                    "value_state": "present",
                                    "evidence_state": "consistent",
                                    "source_count": 2,
                                    "confidence_score": 1.0,
                                    "source_values": [],
                                    "notes": [],
                                },
                                "financials.latest.equity": {
                                    "field": "financials.latest.equity",
                                    "label": "最新归母权益",
                                    "value": 60.0,
                                    "allowed_sources": ["eastmoney_balance", "akshare_financial_abstract"],
                                    "unit": "cny",
                                    "period_type": "quarter",
                                    "blocking_level": "critical",
                                    "report_period": "2025-12-31",
                                    "value_state": "present",
                                    "evidence_state": "consistent",
                                    "source_count": 2,
                                    "confidence_score": 1.0,
                                    "source_values": [],
                                    "notes": [],
                                },
                                "industry_enhanced.industry_leaders": {
                                    "field": "industry_enhanced.industry_leaders",
                                    "label": "行业龙头",
                                    "value": ["龙头A", "龙头B"],
                                    "allowed_sources": ["peer_cross_verification"],
                                    "unit": "list",
                                    "period_type": "latest",
                                    "blocking_level": "core",
                                    "report_period": "2025-12-31",
                                    "value_state": "present",
                                    "evidence_state": "consistent",
                                    "source_count": 1,
                                    "confidence_score": 0.88,
                                    "source_values": [],
                                    "notes": [],
                                },
                            },
                        },
                        "warnings": [],
                    },
                    summary="cleaner ok",
                )
        if name == "screener":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={"screening": {"verdict": "通过", "recommendation": "建议继续研究", "key_risks": []}},
                summary="screen ok",
                llm_invoked=True,
                model_used="qwen3-plus",
            )
        if name == "financial":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={"financial": {"overall_score": 8.0, "trend_summary": "增长稳定", "conclusion": "财务稳健"}},
                summary="financial ok",
                llm_invoked=True,
                model_used="qwen3-max",
            )
        if name == "business_model":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={"business_model": {"model_score": 7.5, "moat_overall": "宽", "profit_driver": "品牌", "negative_view": "需求波动", "conclusion": "商业模式健康"}},
                summary="business ok",
            )
        if name == "industry":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={"industry": {"lifecycle": "成熟期", "competition_pattern": "寡头竞争", "prosperity_direction": "平稳", "lifecycle_evidence": "行业成熟", "top_competitors": [{"name": "A"}, {"name": "B"}], "prosperity_indicators": ["销量", "库存"], "conclusion": "行业稳定", "evidence_status": "ok"}},
                summary="industry ok",
            )
        if name == "governance":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={"governance": {"governance_score": 7.0, "management_integrity": "良好", "capital_allocation": "稳健", "conclusion": "治理稳定", "evidence_status": "ok"}},
                summary="governance ok",
            )
        if name == "valuation":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={"valuation": {"valuation_level": "低估", "reasonable_range_low": 24.0, "reasonable_range_high": 28.0, "current_price": 20.0, "conclusion": "估值偏低", "evidence_status": "ok"}},
                summary="valuation ok",
            )
        if name == "risk":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={"risk": {"overall_risk_level": "中", "risk_score": 4.5, "fatal_risks": [], "monitoring_points": ["订单", "毛利率", "政策"], "conclusion": "风险可控", "evidence_status": "ok", "risks": [{"category": "行业"}, {"category": "经营"}, {"category": "财务"}, {"category": "治理"}], "scenarios": [{}, {}, {}]}},
                summary="risk ok",
            )
        if name == "deep_review":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={
                    "deep_review": {
                        "counter_thesis": "需求兑现可能弱于预期",
                        "supporting_signals": ["现金流稳定"],
                        "challenge_points": ["行业竞争加剧", "订单兑现放缓"],
                        "key_assumptions": ["需求恢复", "利润率稳定"],
                        "sensitivity_checks": ["订单增速", "毛利率"],
                        "what_would_change_my_mind": ["季度订单不达预期", "现金流转弱"],
                        "confidence_adjustment": "lower",
                        "review_summary": "深度复核提示需下调置信度。",
                    }
                },
                summary="deep review ok",
                llm_invoked=True,
                model_used="qwen3-plus",
            )
        if name == "report":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={"markdown": _valid_markdown(), "chart_pack": [], "evidence_pack": []},
                summary="report ok",
                llm_invoked=True,
                model_used="qwen3-plus",
            )
        if name == "conclusion":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={
                    "conclusion": {
                        "recommendation": "观望",
                        "confidence_level": "中",
                        "risk_level": "中",
                        "key_reasons_buy": ["估值偏低"],
                        "key_reasons_sell": ["需求仍待验证"],
                        "key_assumptions": ["订单恢复", "利润率稳定"],
                        "monitoring_points": ["订单", "毛利率", "现金流"],
                        "monitoring_plan": [],
                        "conclusion_summary": "综合结论保持谨慎。",
                    }
                },
                summary="conclusion ok",
                llm_invoked=True,
                model_used="qwen3-plus",
            )
        raise AssertionError(f"Unexpected agent: {name}")

    coordinator._safe_run_agent = fake_safe_run_agent  # type: ignore[method-assign]

    report = await coordinator.run_research("600000", depth="deep")
    trace_names = [item.agent_name for item in report.execution_trace]

    assert "deep_review" in report.agents_completed
    assert "deep_review" in trace_names
    assert "report" in trace_names
    assert "conclusion" in trace_names
    assert report.quality_gate is not None
    assert report.quality_gate.blocked is False
    assert report.baseline_snapshot is not None
    assert report.baseline_snapshot.quality_gate_blocked is False


@pytest.mark.asyncio
async def test_coordinator_blocks_downstream_agents_when_quality_gate_triggers() -> None:
    coordinator = ResearchCoordinator()
    coordinator._save_to_knowledge_base = lambda report, context: None  # type: ignore[method-assign]
    coordinator._run_industry_peer_collection = lambda **kwargs: {  # type: ignore[method-assign]
        "status": "ok",
        "peer_count": 4,
        "data_points": [{"metric_name": "market_size"}],
        "verified_metrics": [{"metric_name": "market_size"}],
    }

    called_agents: list[str] = []

    async def fake_safe_run_agent(agent, input_data: AgentInput) -> AgentOutput:
        name = getattr(agent, "agent_name", "")
        called_agents.append(name)
        if name == "data_collector":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={
                    "stock_info": {"name": "示例公司"},
                    "coverage_ratio": 0.78,
                    "collection_status": {},
                    "errors": [],
                },
            )
        if name == "data_cleaner":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={
                    "cleaned": {
                        "coverage_ratio": 0.65,
                        "stock_info": {"name": "示例公司", "industry_sw": "设备"},
                        "cross_verification": {
                            "overall_confidence": 0.47,
                            "divergent_metrics": ["operating_cashflow", "free_cashflow"],
                        },
                        "missing_fields": ["governance.related_transaction"],
                    },
                    "warnings": [],
                },
            )
        raise AssertionError(f"Quality gate should stop before agent {name}")

    coordinator._safe_run_agent = fake_safe_run_agent  # type: ignore[method-assign]

    report = await coordinator.run_research("600000", depth="deep")

    assert called_agents == ["data_collector", "data_cleaner"]
    assert report.conclusion is None
    assert "研究报告（待补证据）" in report.markdown
    assert any("研究闸门触发" in item for item in report.errors)
    assert report.chart_pack
    assert report.evidence_pack
    assert "screener" in report.agents_skipped
    assert "conclusion" in report.agents_skipped
    assert report.quality_gate is not None
    assert report.quality_gate.blocked is True
    assert report.baseline_snapshot is not None
    assert report.baseline_snapshot.quality_gate_blocked is True


@pytest.mark.asyncio
async def test_coordinator_blocks_when_peer_validation_is_insufficient() -> None:
    coordinator = ResearchCoordinator()
    coordinator._save_to_knowledge_base = lambda report, context: None  # type: ignore[method-assign]
    coordinator._run_industry_peer_collection = lambda **kwargs: {  # type: ignore[method-assign]
        "status": "insufficient",
        "peer_count": 8,
        "data_points": [],
        "verified_metrics": [],
    }

    called_agents: list[str] = []

    async def fake_safe_run_agent(agent, input_data: AgentInput) -> AgentOutput:
        name = getattr(agent, "agent_name", "")
        called_agents.append(name)
        if name == "data_collector":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={
                    "stock_info": {"name": "示例公司"},
                    "coverage_ratio": 0.9,
                    "collection_status": {},
                    "errors": [],
                },
            )
        if name == "data_cleaner":
            return AgentOutput(
                agent_name=name,
                status=AgentStatus.SUCCESS,
                data={
                    "cleaned": {
                        "coverage_ratio": 0.82,
                        "stock_info": {"name": "示例公司", "industry_sw": "设备"},
                        "cross_verification": {
                            "overall_confidence": 0.82,
                            "divergent_metrics": [],
                        },
                        "missing_fields": [],
                    },
                    "warnings": [],
                },
            )
        raise AssertionError(f"Peer gate should stop before agent {name}")

    coordinator._safe_run_agent = fake_safe_run_agent  # type: ignore[method-assign]

    report = await coordinator.run_research("600001", depth="deep")

    assert called_agents == ["data_collector", "data_cleaner"]
    assert report.conclusion is None
    assert "同业交叉验证未形成有效结果" in report.markdown
    assert "分析层" in report.agents_skipped


@pytest.mark.asyncio
async def test_cli_run_research_persists_quality_gate_and_baseline_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _sample_report()

    class FakeCoordinator:
        def __init__(self, progress_callback=None) -> None:
            self.progress_callback = progress_callback

        async def run_research(self, stock: str, depth: str = "standard") -> ResearchReport:
            assert stock == "600519"
            assert depth == "deep"
            return report

    monkeypatch.setattr(coordinator_module, "ResearchCoordinator", FakeCoordinator)
    monkeypatch.setattr(cli_module, "_print_conclusion", lambda _: None)
    monkeypatch.setattr(cli_module, "_print_execution_summary", lambda _: None)

    await cli_module._run_research("600519", "deep", str(tmp_path))

    payload = json.loads((tmp_path / "600519_20260418_meta.json").read_text(encoding="utf-8"))

    assert payload["quality_gate"]["core_evidence_score"] == pytest.approx(0.86, rel=1e-6)
    assert payload["quality_gate"]["blocked"] is False
    assert payload["baseline_snapshot"]["stock_code"] == "600519"
    assert payload["baseline_snapshot"]["quality_gate_blocked"] is False
    assert payload["baseline_snapshot"]["quality_gate_reasons"] == ["自由现金流字段待补强"]


@pytest.mark.asyncio
async def test_cli_run_research_clears_stale_latest_conclusion_for_gate_only_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = ResearchReport(
        stock_code="600519",
        stock_name="贵州茅台",
        report_date=datetime(2026, 4, 18, 11, 0, 0),
        depth="quick",
        markdown="# 600519 贵州茅台 研究报告（待补证据）",
        quality_gate=_sample_quality_gate().model_copy(update={"blocked": True}),
        baseline_snapshot=_sample_baseline_snapshot().model_copy(update={"quality_gate_blocked": True}),
        agents_completed=["data_collector", "data_cleaner"],
        agents_skipped=["screener", "分析层", "deep_review", "report", "conclusion"],
        errors=["研究闸门触发: 核心证据不足"],
    )

    class FakeCoordinator:
        def __init__(self, progress_callback=None) -> None:
            self.progress_callback = progress_callback

        async def run_research(self, stock: str, depth: str = "standard") -> ResearchReport:
            assert stock == "600519"
            assert depth == "quick"
            return report

    monkeypatch.setattr(coordinator_module, "ResearchCoordinator", FakeCoordinator)
    monkeypatch.setattr(cli_module, "_print_conclusion", lambda _: None)
    monkeypatch.setattr(cli_module, "_print_execution_summary", lambda _: None)

    stale = tmp_path / "600519_conclusion.json"
    stale.write_text(json.dumps({"recommendation": "买入(谨慎)"}, ensure_ascii=False), encoding="utf-8")

    await cli_module._run_research("600519", "quick", str(tmp_path))

    assert not stale.exists()
    assert (tmp_path / "600519_meta.json").exists()


@pytest.mark.asyncio
async def test_api_background_research_persists_latest_meta_with_quality_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reports_dir = tmp_path / "reports"
    report = _sample_report()
    task_manager = TaskManager()
    task_id = task_manager.create_task("600519", "deep")

    monkeypatch.setattr(research_route, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(research_route, "get_task_manager", lambda: task_manager)
    monkeypatch.setattr(research_route, "_run_research_sync", lambda *args, **kwargs: report)

    await research_route._run_research_background(task_id, "600519", "deep")

    dated_meta = json.loads((reports_dir / "600519_20260418_meta.json").read_text(encoding="utf-8"))
    latest_meta = json.loads((reports_dir / "600519_meta.json").read_text(encoding="utf-8"))

    assert dated_meta["quality_gate"]["core_evidence_score"] == pytest.approx(0.86, rel=1e-6)
    assert latest_meta["quality_gate"]["consistency_notes"] == ["初筛通过与最终观望之间已显式说明估值约束。"]
    assert latest_meta["baseline_snapshot"]["final_recommendation"] == "观望"
    assert task_manager.get_task(task_id)["status"] == "completed"


def test_route_loader_does_not_fallback_to_stale_latest_conclusion_when_dated_report_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(research_route, "REPORTS_DIR", reports_dir)

    (reports_dir / "600519_20260418.md").write_text("# dated report", encoding="utf-8")
    (reports_dir / "600519_20260418_meta.json").write_text(
        json.dumps({"stock_code": "600519", "report_date": "20260418"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "600519_conclusion.json").write_text(
        json.dumps({"recommendation": "买入(谨慎)", "conclusion_summary": "stale"}, ensure_ascii=False),
        encoding="utf-8",
    )

    assert research_route._load_conclusion("600519", "20260418") is None
