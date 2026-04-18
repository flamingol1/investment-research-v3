from __future__ import annotations

import pytest

from investresearch.analysis_layer.business_model import BusinessModelAgent
from investresearch.analysis_layer.financial import FinancialAgent
from investresearch.analysis_layer.governance import GovernanceAgent
from investresearch.analysis_layer.industry import IndustryAgent
from investresearch.analysis_layer.risk import RiskAgent
from investresearch.analysis_layer.valuation import ValuationAgent
from investresearch.core.models import AgentInput, AgentStatus


@pytest.mark.asyncio
async def test_business_model_agent_uses_llm_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = BusinessModelAgent()
    context = {
        "_allow_live_llm": True,
        "cleaned_data": {
            "stock_info": {
                "name": "示例科技",
                "main_business": "高端设备与材料",
                "business_model": "设备销售+服务",
                "asset_model": "轻",
                "client_type": "企业客户",
            },
            "financials": [
                {
                    "report_date": "2025-12-31",
                    "revenue": 1_000_000_000.0,
                    "revenue_yoy": 18.0,
                    "gross_margin": 36.0,
                    "operating_cashflow": 120_000_000.0,
                }
            ],
            "announcements": [{"announcement_date": "2026-03-30", "title": "年度报告", "excerpt": "公司高端设备订单增长明显。"}],
            "research_reports": [{"publish_date": "2026-03-20", "institution": "某券商", "summary": "客户粘性强。"}],
            "patents": [{"publish_date": "2026-03-18", "title": "核心工艺专利", "summary": "官方专利资料。"}],
        },
    }

    async def fake_call_json(*, prompt: str, system_prompt: str | None = None, model: str = "qwen3-plus") -> dict:
        del prompt, system_prompt, model
        return {
            "model_score": 8.3,
            "profit_driver": "高附加值设备与服务复购共同驱动盈利。",
            "asset_model": "轻资产",
            "client_concentration": "核心客户复购率较高",
            "moats": [
                {"moat_type": "品牌", "strength": "medium", "evidence": "品牌与渠道被反复提及", "sustainability": "可持续"}
            ],
            "moat_overall": "宽",
            "negative_view": "若下游资本开支放缓，订单兑现可能承压。",
            "conclusion": "商业模式具备一定护城河，但仍需跟踪订单兑现。",
        }

    monkeypatch.setattr(agent.llm, "call_json", fake_call_json)

    output = await agent.safe_run(AgentInput(stock_code="688001", context=context))
    payload = output.data["business_model"]

    assert output.execution_mode == "llm"
    assert output.llm_invoked is True
    assert output.model_used == "qwen3-plus"
    assert payload["moat_overall"] == "宽"
    assert payload["profit_driver"].startswith("高附加值设备")
    assert payload["evidence_status"] in {"ok", "partial"}


@pytest.mark.asyncio
async def test_financial_agent_caps_score_when_cashflow_evidence_is_weak(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = FinancialAgent()
    context = {
        "_allow_live_llm": True,
        "cleaned_data": {
            "stock_info": {"name": "示例财务", "industry_sw": "设备"},
            "financials": [
                {
                    "report_date": "2025-12-31",
                    "revenue": 1_000_000_000.0,
                    "net_profit": 180_000_000.0,
                    "revenue_yoy": 18.0,
                    "net_profit_yoy": 22.0,
                    "gross_margin": 35.0,
                    "net_margin": 18.0,
                    "roe": 15.0,
                    "operating_cashflow": 210_000_000.0,
                    "free_cashflow": 150_000_000.0,
                }
            ],
            "field_quality": {
                "financials.latest.revenue": {
                    "field": "financials.latest.revenue",
                    "label": "最新营收",
                    "value": 1_000_000_000.0,
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
                    "value": 180_000_000.0,
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
                    "value": 210_000_000.0,
                    "allowed_sources": ["eastmoney_cashflow"],
                    "unit": "cny",
                    "period_type": "cumulative",
                    "blocking_level": "critical",
                    "report_period": "2025-12-31",
                    "value_state": "present",
                    "evidence_state": "single_source",
                    "source_count": 1,
                    "confidence_score": 0.58,
                    "source_values": [],
                    "notes": [],
                },
                "financials.latest.free_cashflow": {
                    "field": "financials.latest.free_cashflow",
                    "label": "最新自由现金流",
                    "value": 150_000_000.0,
                    "allowed_sources": ["eastmoney_cashflow"],
                    "unit": "cny",
                    "period_type": "cumulative",
                    "blocking_level": "core",
                    "report_period": "2025-12-31",
                    "value_state": "present",
                    "evidence_state": "single_source",
                    "source_count": 1,
                    "confidence_score": 0.55,
                    "source_values": [],
                    "notes": [],
                },
                "financials.latest.equity": {
                    "field": "financials.latest.equity",
                    "label": "最新归母权益",
                    "value": 900_000_000.0,
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
            },
            "cross_verification": {"overall_confidence": 0.61, "divergent_metrics": ["operating_cashflow"]},
        },
    }

    async def fake_call_json(*, prompt: str, system_prompt: str | None = None, model: str = "qwen3-plus") -> dict:
        del prompt, system_prompt, model
        return {
            "overall_score": 8.9,
            "dimensions": [
                {"dimension": "盈利能力", "score": 8.6, "trend": "改善", "key_metrics": {}, "analysis": "盈利改善", "concerns": []},
                {"dimension": "成长性", "score": 8.3, "trend": "改善", "key_metrics": {}, "analysis": "增长良好", "concerns": []},
                {"dimension": "偿债能力", "score": 7.2, "trend": "稳定", "key_metrics": {}, "analysis": "偿债平稳", "concerns": []},
                {"dimension": "运营效率", "score": 7.0, "trend": "稳定", "key_metrics": {}, "analysis": "效率尚可", "concerns": []},
                {"dimension": "现金流质量", "score": 9.1, "trend": "改善", "key_metrics": {}, "analysis": "现金流质量优秀", "concerns": []},
            ],
            "trend_summary": "收入利润增长且现金流改善明显。",
            "cashflow_verification": "现金流与利润匹配良好，净现比改善。",
            "anomaly_flags": [],
            "peer_comparison": "同行对比占优。",
            "conclusion": "财务质量优秀，现金流匹配度良好。",
        }

    monkeypatch.setattr(agent.llm, "call_json", fake_call_json)

    output = await agent.safe_run(AgentInput(stock_code="600111", context=context))
    payload = output.data["financial"]

    assert output.execution_mode == "llm"
    assert payload["evidence_status"] == "partial"
    assert payload["overall_score"] <= 5.8
    cashflow_dimension = next(item for item in payload["dimensions"] if item["dimension"] == "现金流质量")
    assert cashflow_dimension["score"] <= 5.0
    assert "待验证" in payload["cashflow_verification"] or "证据不足" in payload["cashflow_verification"]


@pytest.mark.asyncio
async def test_industry_agent_uses_llm_but_keeps_structured_numeric_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = IndustryAgent()
    context = {
        "_allow_live_llm": True,
        "raw_data": {
            "industry": {"market_size": 3200.0, "cagr_5y": 12.0, "cr5": 48.0, "lifecycle": "成熟期"}
        },
        "cross_verification": {
            "peers": [{"stock_name": "龙头公司A"}, {"stock_name": "龙头公司B"}],
            "verified_metrics": [],
        },
        "cleaned_data": {
            "stock_info": {"name": "示例制造", "industry_sw": "高端制造", "main_business": "核心部件"},
            "industry_enhanced": {
                "data_points": ["出货量同比增长", "库存维持健康"],
                "industry_leaders": ["龙头公司A", "龙头公司B", "龙头公司C"],
                "industry_change_pct": 1.5,
            },
            "policy_documents": [{"title": "产业升级政策", "excerpt": "支持高端制造。"}],
            "research_reports": [{"title": "行业景气延续", "summary": "龙头份额提升。"}],
            "financials": [{"report_date": "2025-12-31", "revenue": 100.0}],
            "realtime": {"market_cap": 1000000000.0},
        },
    }

    async def fake_call_json(*, prompt: str, system_prompt: str | None = None, model: str = "qwen3-plus") -> dict:
        del prompt, system_prompt, model
        return {
            "lifecycle": "成熟期",
            "lifecycle_evidence": "行业增速趋稳且格局集中。",
            "market_size": 9999.0,
            "market_growth": 99.0,
            "competition_pattern": "寡头竞争",
            "top_competitors": [
                {"name": "龙头公司A", "advantage": "规模优势", "threat_level": "高"},
                {"name": "龙头公司B", "advantage": "渠道优势", "threat_level": "中"},
                {"name": "未提供公司", "advantage": "幻觉", "threat_level": "高"},
            ],
            "prosperity_indicators": ["出货量", "库存", "订单"],
            "prosperity_direction": "上行",
            "policy_stance": "政策鼓励行业升级。",
            "company_position": "公司位于行业第一梯队。",
            "conclusion": "行业景气改善，龙头优势增强。",
        }

    monkeypatch.setattr(agent.llm, "call_json", fake_call_json)

    output = await agent.safe_run(AgentInput(stock_code="600000", context=context))
    payload = output.data["industry"]

    assert output.execution_mode == "llm"
    assert payload["market_size"] == 3200.0
    assert payload["market_growth"] == 12.0
    assert payload["cr5"] == 48.0
    assert len(payload["top_competitors"]) == 2
    assert all(item["name"] != "未提供公司" for item in payload["top_competitors"])


@pytest.mark.asyncio
async def test_industry_agent_uses_peer_fallback_when_leaders_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = IndustryAgent()
    context = {
        "_allow_live_llm": True,
        "raw_data": {"industry": {"market_size": 1200.0, "cagr_5y": 8.0, "cr5": 35.0}},
        "cross_verification": {
            "peers": [{"stock_name": "同业A"}, {"stock_name": "同业B"}, {"stock_name": "同业C"}],
            "verified_metrics": [],
        },
        "cleaned_data": {
            "stock_info": {"name": "示例材料", "industry_sw": "新材料"},
            "industry_enhanced": {"data_points": ["库存改善", "价格企稳"], "industry_leaders": []},
            "policy_documents": [],
            "research_reports": [],
        },
    }

    async def fake_call_json(*, prompt: str, system_prompt: str | None = None, model: str = "qwen3-plus") -> dict:
        del prompt, system_prompt, model
        return {
            "lifecycle": "成长期",
            "lifecycle_evidence": "需求稳步增长。",
            "competition_pattern": "垄断竞争",
            "top_competitors": [{"name": "未知同业", "advantage": "幻觉", "threat_level": "高"}],
            "prosperity_indicators": ["库存", "价格"],
            "prosperity_direction": "平稳",
            "company_position": "行业地位待验证",
            "conclusion": "行业仍在观察期。",
        }

    monkeypatch.setattr(agent.llm, "call_json", fake_call_json)

    output = await agent.safe_run(AgentInput(stock_code="688009", context=context))
    payload = output.data["industry"]

    assert output.status == AgentStatus.SUCCESS
    assert [item["name"] for item in payload["top_competitors"]] == ["同业A", "同业B", "同业C"]


@pytest.mark.asyncio
async def test_risk_agent_uses_llm_and_normalizes_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = RiskAgent()
    context = {
        "_allow_live_llm": True,
        "cleaned_data": {
            "stock_info": {"name": "示例公司", "industry_sw": "设备", "main_business": "高端装备"},
            "financials": [
                {
                    "report_date": "2025-12-31",
                    "revenue_yoy": 10.0,
                    "debt_ratio": 40.0,
                    "operating_cashflow": 50_000_000.0,
                }
            ],
            "realtime": {"close": 20.0},
            "announcements": [{"title": "年度报告", "excerpt": "经营稳定。"}],
            "policy_documents": [{"title": "行业政策", "excerpt": "支持升级。"}],
            "industry_enhanced": {"industry_change_pct": 1.2},
            "valuation_percentile": {"pe_ttm_percentile": 65.0, "pb_mrq_percentile": 60.0},
            "governance": {},
        },
        "screening": {"key_risks": ["订单波动"]},
    }

    async def fake_call_json(*, prompt: str, system_prompt: str | None = None, model: str = "qwen3-plus") -> dict:
        del prompt, system_prompt, model
        return {
            "overall_risk_level": "中风险",
            "risk_score": 5.8,
            "risks": [
                {"category": "行业风险", "risk_name": "行业需求波动", "severity": "中", "probability": "中", "impact": "景气可能波动", "mitigation": "跟踪景气"},
                {"category": "经营风险", "risk_name": "订单兑现风险", "severity": "中", "probability": "中", "impact": "订单可能不及预期", "mitigation": "跟踪订单"},
                {"category": "财务风险", "risk_name": "现金流波动", "severity": "低", "probability": "中", "impact": "现金流弹性", "mitigation": "跟踪净现比"},
                {"category": "治理风险", "risk_name": "治理披露不足", "severity": "中", "probability": "中", "impact": "治理证据不完整", "mitigation": "补齐公告"},
                {"category": "市场风险", "risk_name": "估值波动", "severity": "中", "probability": "中", "impact": "估值有波动", "mitigation": "控制仓位"},
                {"category": "政策风险", "risk_name": "政策节奏变化", "severity": "中", "probability": "中", "impact": "政策扰动", "mitigation": "跟踪政策"},
            ],
            "scenarios": [
                {"scenario": "乐观情景", "target_price": 23.0, "upside_pct": 15.0, "assumptions": ["订单改善"], "probability": 25.0},
                {"scenario": "中性情景", "target_price": 20.0, "upside_pct": 0.0, "assumptions": ["经营稳定"], "probability": 50.0},
                {"scenario": "悲观情景", "target_price": 16.0, "upside_pct": -20.0, "assumptions": ["需求走弱"], "probability": 25.0},
            ],
            "fatal_risks": ["订单连续两个季度低于预期"],
            "monitoring_points": ["订单", "毛利率", "政策"],
            "conclusion": "风险整体可控，但订单波动需要重点跟踪。",
        }

    monkeypatch.setattr(agent.llm, "call_json", fake_call_json)

    output = await agent.safe_run(AgentInput(stock_code="600000", context=context))
    payload = output.data["risk"]

    assert output.execution_mode == "llm"
    assert output.llm_invoked is True
    assert payload["overall_risk_level"] == "中"
    assert len(payload["risks"]) == 6
    assert [item["scenario"] for item in payload["scenarios"]] == ["乐观", "中性", "悲观"]
    assert payload["monitoring_points"][:3] == ["订单", "毛利率", "政策"]


@pytest.mark.asyncio
async def test_governance_agent_uses_llm_but_keeps_compliance_guardrails(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = GovernanceAgent()
    context = {
        "_allow_live_llm": True,
        "cleaned_data": {
            "stock_info": {"name": "示例治理", "actual_controller": "示例控股"},
            "governance": {
                "actual_controller": "示例控股",
                "equity_pledge_ratio": 8.5,
                "related_transaction": "存在少量关联交易",
                "dividend_history": [{"year": 2025}],
                "buyback_history": [],
                "refinancing_history": [{"year": 2024}],
            },
            "shareholders": {"top10_total_ratio": 58.0},
            "financials": [
                {
                    "report_date": "2025-12-31",
                    "operating_cashflow": 100_000_000.0,
                    "free_cashflow": 82_000_000.0,
                    "roe": 12.5,
                }
            ],
            "announcements": [{"announcement_date": "2026-03-30", "title": "年度报告", "excerpt": "公司治理结构保持稳定。"}],
            "compliance_events": [{"title": "监管关注函", "severity": "medium", "publish_date": "2026-03-18", "excerpt": "要求补充说明关联交易。"}],
        },
    }

    async def fake_call_json(*, prompt: str, system_prompt: str | None = None, model: str = "qwen3-plus") -> dict:
        del prompt, system_prompt, model
        return {
            "governance_score": 8.8,
            "management_assessment": "管理层执行力尚可，但关联交易披露仍需持续跟踪。",
            "management_integrity": "良",
            "controller_analysis": "实控人与股东结构稳定，但需继续关注关联交易边界。",
            "capital_allocation": "经营现金流较稳，分红与再融资节奏基本匹配。",
            "dividend_policy": "公司具备一定分红基础，但仍需跟踪自由现金流质量。",
            "conclusion": "治理总体可跟踪，但合规披露仍需保持谨慎。",
        }

    monkeypatch.setattr(agent.llm, "call_json", fake_call_json)

    output = await agent.safe_run(AgentInput(stock_code="600100", context=context))
    payload = output.data["governance"]

    assert output.execution_mode == "llm"
    assert output.llm_invoked is True
    assert output.model_used == "qwen3-plus"
    assert payload["management_integrity"] == "中"
    assert payload["evidence_status"] == "partial"
    assert payload["governance_score"] is None
    assert payload["capital_allocation"].startswith("经营现金流较稳")


@pytest.mark.asyncio
async def test_valuation_agent_uses_llm_but_preserves_market_data(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = ValuationAgent()
    context = {
        "_allow_live_llm": True,
        "cleaned_data": {
            "stock_info": {"name": "示例估值", "industry_sw": "消费"},
            "realtime": {"close": 10.0, "pe_ttm": 5.0, "pb_mrq": 1.0, "market_cap": 1_000_000_000},
            "valuation": [
                {"date": "2026-01-31", "pe_ttm": 8.0, "pb_mrq": 1.2},
                {"date": "2026-02-28", "pe_ttm": 10.0, "pb_mrq": 1.5},
                {"date": "2026-03-31", "pe_ttm": 12.0, "pb_mrq": 1.8},
            ],
            "valuation_percentile": {"pe_ttm_percentile": 20.0, "pb_mrq_percentile": 25.0},
            "financials": [{"report_date": "2025-12-31", "revenue_yoy": 15.0, "operating_cashflow": 100_000_000.0}],
        },
        "financial_analysis": {"overall_score": 7.8, "conclusion": "盈利质量稳定。"},
    }

    async def fake_call_json(*, prompt: str, system_prompt: str | None = None, model: str = "qwen3-max") -> dict:
        del prompt, system_prompt, model
        return {
            "methods": [
                {
                    "method": "PE",
                    "intrinsic_value": 18.0,
                    "assumptions": ["盈利延续稳定增长", "历史估值均值回归"],
                    "limitations": ["未纳入情绪波动"],
                },
                {
                    "method": "PB",
                    "intrinsic_value": 14.0,
                    "assumptions": ["净资产回报维持稳定"],
                    "limitations": ["资产重估存在偏差"],
                },
                {
                    "method": "DCF",
                    "assumptions": ["隐含增长率可被现金流支撑"],
                    "limitations": ["仅作审计用途"],
                },
            ],
            "reasonable_range_low": 13.0,
            "reasonable_range_high": 18.0,
            "conclusion": "相对估值显示股价仍低于合理区间下沿。",
            "current_price": 999.0,
            "pe_percentile": 99.0,
        }

    monkeypatch.setattr(agent.llm, "call_json", fake_call_json)

    output = await agent.safe_run(AgentInput(stock_code="600519", context=context))
    payload = output.data["valuation"]

    assert output.execution_mode == "llm"
    assert output.llm_invoked is True
    assert output.model_used == "qwen3-max"
    assert payload["current_price"] == 10.0
    assert payload["pe_percentile"] == 20.0
    assert payload["pb_percentile"] == 25.0
    assert payload["reasonable_range_low"] == 14.0
    assert payload["reasonable_range_high"] == 18.0
    assert payload["methods"][0]["assumptions"][0] == "盈利延续稳定增长"
    assert payload["valuation_level"] == "低估"
