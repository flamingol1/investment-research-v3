from __future__ import annotations

from datetime import date, datetime

import pytest

from investresearch.analysis_layer.business_model import BusinessModelAgent
from investresearch.analysis_layer.governance import GovernanceAgent
from investresearch.analysis_layer.valuation import ValuationAgent
from investresearch.core.models import (
    AgentInput,
    Announcement,
    CollectorOutput,
    ComplianceEvent,
    DataSource,
    GovernanceData,
    IndustryEnhancedData,
    InvestmentConclusion,
    MonitoringLayer,
    MonitoringPlanItem,
    PatentRecord,
    StockBasicInfo,
    StockPrice,
)
from investresearch.core.trust import build_module_profile
from investresearch.data_layer.collector import DataCollectorAgent
from investresearch.data_layer.official_sources import OfficialSourceRegistry
from investresearch.decision_layer.conclusion import ConclusionAgent
from investresearch.decision_layer.report import ReportAgent
from investresearch.knowledge_base.chroma_store import ChromaKnowledgeStore
from investresearch.knowledge_base.tracker import DynamicTrackerAgent
from investresearch.knowledge_base.updater import IncrementalUpdaterAgent
from investresearch.knowledge_base.watch_list import WatchListManager


def test_governance_profile_marks_controller_only_as_partial() -> None:
    profile = build_module_profile(
        "governance",
        GovernanceData(actual_controller="贵州省国资委").model_dump(mode="json"),
    )

    assert profile.status.value == "partial"
    assert profile.completeness < 0.4
    assert "equity_pledge_ratio" in profile.missing_fields


def test_collector_coverage_ignores_empty_shell_modules() -> None:
    output = CollectorOutput(
        stock_info=StockBasicInfo(code="300688", name="创业黑马", industry_sw="教育"),
        prices=[StockPrice(code="300688", date="2026-04-01", close=30.0)],  # type: ignore[arg-type]
        realtime=StockPrice(code="300688", date="2026-04-05", close=30.0),  # type: ignore[arg-type]
        financials=[],
        industry_enhanced=IndustryEnhancedData(industry_name=""),
        governance=GovernanceData(actual_controller="牛文文"),
    )

    coverage = DataCollectorAgent._calc_coverage(output)
    assert coverage < 0.4


def test_structured_announcement_extraction_parses_primary_fields() -> None:
    text = (
        "公司主要从事高端白酒的生产、销售与品牌运营，采用直营与经销结合的渠道模式。"
        "经营活动产生的现金流量净额20.50亿元，投资活动产生的现金流量净额-30.25亿元，"
        "筹资活动产生的现金流量净额12.00亿元。合同负债5.20亿元，非经常性损益1.30亿元。"
        "利润分配方案：每10股派发现金红利76.24元（含税）。"
    )

    structured = DataCollectorAgent._extract_structured_announcement_fields(
        text,
        title="2025年年度报告",
        announcement_date=date(2026, 3, 30),
    )

    assert structured["stock_info"]["main_business"].startswith("高端白酒")
    assert structured["stock_info"]["business_model"] != ""
    assert structured["financial_snapshot"]["report_date"] == "2025-12-31"
    assert structured["financial_snapshot"]["operating_cashflow"] is not None
    assert structured["financial_snapshot"]["free_cashflow"] is not None
    assert structured["dividend_plan"] != ""


def test_fill_missing_fields_backfills_structured_announcement_data() -> None:
    agent = DataCollectorAgent()
    output = CollectorOutput(
        stock_info=StockBasicInfo(code="600519", name="贵州茅台"),
        governance=GovernanceData(actual_controller="贵州省国资委"),
        announcements=[
            Announcement(
                title="2025年年度报告",
                announcement_type="年报",
                announcement_date=date(2026, 3, 30),
                source="cninfo",
                structured_fields={
                    "stock_info": {
                        "main_business": "高端白酒生产销售",
                        "business_model": "直营+渠道分销",
                        "asset_model": "重",
                        "client_type": "ToC",
                    },
                    "financial_snapshot": {
                        "report_date": "2025-12-31",
                        "operating_cashflow": 12000000000.0,
                        "investing_cashflow": -3000000000.0,
                        "free_cashflow": 9000000000.0,
                        "contract_liabilities": 4500000000.0,
                    },
                    "dividend_plan": "每10股派发现金红利76.24元（含税）",
                },
            )
        ],
    )

    agent._fill_missing_fields("600519", output)

    assert output.stock_info is not None
    assert output.stock_info.main_business == "高端白酒生产销售"
    assert output.stock_info.business_model == "直营+渠道分销"
    assert output.financials
    assert output.financials[0].source == DataSource.CNINFO
    assert output.financials[0].operating_cashflow == 12000000000.0
    assert output.financials[0].contract_liabilities == 4500000000.0
    assert output.governance is not None
    assert output.governance.dividend_history
    assert output.governance.dividend_history[0]["plan"] == "每10股派发现金红利76.24元（含税）"


def test_official_source_registry_uses_gov_policy_and_csrc_html_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyResponse:
        def __init__(self, payload: dict | None = None, text: str = "") -> None:
            self._payload = payload
            self.text = text

        def json(self) -> dict:
            return self._payload or {}

    def fake_request(method: str, url: str, **kwargs: object) -> DummyResponse:
        del method, kwargs
        if "sousuo.www.gov.cn" in url:
            return DummyResponse(
                {
                    "searchVO": {
                        "catMap": {
                            "gongwen": {
                                "listVO": [
                                    {
                                        "title": "扩大内需政策",
                                        "pubtimeStr": "2026-03-01",
                                        "puborg": "国务院",
                                        "url": "https://www.gov.cn/policy/1",
                                        "summary": "支持消费升级。",
                                    }
                                ]
                            }
                        }
                    }
                }
            )
        return DummyResponse(
            text="""
            <html><body>
              <ul class="result-list">
                <li>
                  <a href="/csrc/c101928/c7537645/content.shtml">中国证券监督管理委员会行政处罚决定书</a>
                  <span>2026-04-01</span>
                  <p>贵州茅台因信息披露违规被行政处罚。</p>
                </li>
              </ul>
            </body></html>
            """
        )

    monkeypatch.setenv("INVESTRESEARCH_CSRC_BASE_URL", "https://www.csrc.gov.cn/searchList/mock")
    registry = OfficialSourceRegistry(fake_request)

    policy_docs = registry.search_policy_documents("白酒", limit=2)
    company_events = registry.search_company_compliance_events(
        stock_code="600519",
        company_name="贵州茅台",
        limit=2,
    )

    assert policy_docs[0]["source"] == "gov.cn"
    assert policy_docs[0]["policy_date"].isoformat() == "2026-03-01"
    assert company_events[0]["source"] == "csrc"
    assert "行政处罚决定书" in company_events[0]["title"]
    assert company_events[0]["event_type"] == "administrative_penalty"


@pytest.mark.asyncio
async def test_governance_agent_uses_official_compliance_events() -> None:
    agent = GovernanceAgent()
    context = {
        "cleaned_data": {
            "stock_info": {"name": "示例公司", "actual_controller": "示例控股"},
            "governance": {"actual_controller": "示例控股"},
            "shareholders": {"top10_total_ratio": 55.0},
            "financials": [{"report_date": "2025-12-31", "operating_cashflow": 100000000.0, "free_cashflow": 80000000.0}],
            "compliance_events": [
                {
                    "title": "行政处罚决定书",
                    "source": "csrc",
                    "publish_date": "2026-04-01",
                    "severity": "high",
                    "summary": "因信息披露违规被处罚",
                    "excerpt": "因信息披露违规被处罚",
                }
            ],
        }
    }

    output = await agent.run(AgentInput(stock_code="600000", context=context))
    governance = output.data["governance"]

    assert governance["management_integrity"] == "差"
    assert "官方合规事件" in governance["management_assessment"]


@pytest.mark.asyncio
async def test_business_model_agent_uses_patents_as_moat_evidence() -> None:
    agent = BusinessModelAgent()
    context = {
        "cleaned_data": {
            "stock_info": {"name": "示例科技", "main_business": "高端设备与材料", "asset_model": "轻"},
            "financials": [{"report_date": "2025-12-31", "revenue": 1000000000.0, "gross_margin": 35.0}],
            "announcements": [],
            "research_reports": [],
            "patents": [
                {
                    "title": "一种高性能涂层设备",
                    "source": "cnipa",
                    "publish_date": "2026-03-15",
                    "patent_type": "发明专利",
                    "summary": "核心工艺相关专利。",
                    "excerpt": "核心工艺相关专利。",
                }
            ],
        }
    }

    output = await agent.run(AgentInput(stock_code="688001", context=context))
    business = output.data["business_model"]

    assert any(item["moat_type"] == "专利" for item in business["moats"])


@pytest.mark.asyncio
async def test_valuation_agent_builds_range_from_history() -> None:
    agent = ValuationAgent()
    cleaned = {
        "realtime": {"close": 10.0, "pe_ttm": 5.0, "pb_mrq": 1.0, "market_cap": 1_000_000_000},
        "valuation": [
            {"date": "2026-01-31", "pe_ttm": 8.0, "pb_mrq": 1.2},
            {"date": "2026-02-28", "pe_ttm": 10.0, "pb_mrq": 1.5},
            {"date": "2026-03-31", "pe_ttm": 12.0, "pb_mrq": 1.8},
        ],
        "valuation_percentile": {"pe_ttm_percentile": 20.0, "pb_mrq_percentile": 25.0},
        "financials": [{"report_date": "2025-12-31", "revenue_yoy": 15.0, "operating_cashflow": 100_000_000}],
    }

    output = await agent.run(AgentInput(stock_code="600519", context={"cleaned_data": cleaned}))
    valuation = output.data["valuation"]

    assert valuation["valuation_level"] == "低估"
    assert valuation["reasonable_range_low"] is not None
    assert valuation["reasonable_range_high"] is not None
    assert len(valuation["methods"]) >= 2


@pytest.mark.asyncio
async def test_conclusion_agent_degrades_to_watch_when_evidence_is_insufficient() -> None:
    agent = ConclusionAgent()
    context = {
        "cleaned_data": {
            "coverage_ratio": 0.42,
            "realtime": {"close": 18.0},
            "announcements": [],
            "policy_documents": [],
        },
        "screening": {"verdict": "通过", "key_risks": ["治理信息不足"]},
        "business_model_analysis": {"conclusion": "商业模式证据仍偏弱", "moat_overall": "无"},
        "industry_analysis": {"conclusion": "行业核心数据不足", "evidence_status": "partial"},
        "governance_analysis": {"conclusion": "治理资料不足", "evidence_status": "insufficient"},
        "valuation_analysis": {"valuation_level": "待验证", "evidence_status": "insufficient"},
        "risk_analysis": {
            "overall_risk_level": "中",
            "fatal_risks": ["关键资料缺失导致投资逻辑暂无法充分验证"],
            "monitoring_points": [
                "行业景气度与政策关键词变化",
                "季度营收/净利润/经营现金流是否兑现",
                "治理事件、质押、担保、诉讼公告",
            ],
        },
    }

    output = await agent.run(AgentInput(stock_code="603659", context=context))
    conclusion = output.data["conclusion"]

    assert conclusion["recommendation"] == "观望"
    assert conclusion["confidence_level"] == "低"
    assert "暂不输出明确预期差" in conclusion["expectation_gap"]
    assert len(conclusion["monitoring_plan"]) == 3


@pytest.mark.asyncio
async def test_report_agent_outputs_chart_and_evidence_pack() -> None:
    agent = ReportAgent()
    context = {
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
                },
                {
                    "report_date": "2024-12-31",
                    "revenue": 160_000_000_000,
                    "net_profit": 80_000_000_000,
                    "operating_cashflow": 90_000_000_000,
                    "free_cashflow": 70_000_000_000,
                },
            ],
            "valuation": [
                {"date": "2025-01-31", "pe_ttm": 30.0, "pb_mrq": 10.0},
                {"date": "2025-12-31", "pe_ttm": 22.0, "pb_mrq": 8.0},
            ],
            "announcements": [
                {
                    "title": "2025年年度报告",
                    "announcement_date": "2026-03-30",
                    "excerpt": "公司主要从事高端白酒的生产和销售。",
                    "announcement_type_normalized": "annual_report",
                }
            ],
            "research_reports": [
                {
                    "title": "高端白酒龙头长期价值",
                    "institution": "某券商",
                    "publish_date": "2026-03-20",
                    "summary": "品牌和渠道优势显著。",
                }
            ],
            "industry_enhanced": {"data_points": ["白酒行业景气平稳", "高端白酒渠道库存健康"]},
            "policy_documents": [
                {
                    "title": "扩大内需战略相关政策",
                    "policy_date": "2026-03-01",
                    "issuing_body": "国务院",
                    "excerpt": "支持消费升级。",
                }
            ],
            "coverage_ratio": 0.82,
            "missing_fields": ["industry.market_size"],
            "evidence_refs": [
                {
                    "source": "cninfo",
                    "title": "2025年年度报告",
                    "field": "main_business",
                    "excerpt": "公司主要从事高端白酒的生产和销售。",
                }
            ],
        },
        "screening": {"verdict": "通过", "recommendation": "建议继续深度研究", "key_risks": ["行业需求波动"]},
        "industry_analysis": {"conclusion": "行业景气平稳", "missing_fields": ["market_size"]},
        "business_model_analysis": {"conclusion": "品牌和渠道构成主要护城河", "missing_fields": []},
        "governance_analysis": {"conclusion": "治理结构稳定", "missing_fields": ["related_transaction"]},
        "financial_analysis": {"conclusion": "现金流质量较好", "trend_summary": "收入利润持续增长", "missing_fields": []},
        "valuation_analysis": {"conclusion": "估值处于合理偏低区域", "missing_fields": []},
        "risk_analysis": {"conclusion": "主要风险可跟踪", "scenarios": [], "missing_fields": []},
    }

    output = await agent.run(AgentInput(stock_code="600519", context=context))

    assert "markdown" in output.data
    assert len(output.data["chart_pack"]) == 6
    assert len(output.data["evidence_pack"]) >= 1
    assert "待验证项" in output.data["markdown"]


def test_chroma_store_loads_latest_evidence_pack(tmp_path) -> None:
    store = ChromaKnowledgeStore(persist_dir=str(tmp_path))
    store._save_heavy_data(
        "600519",
        "2026-04-05T09:00:00",
        "evidence_pack",
        [{"category": "policy", "title": "older"}],
    )
    store._save_heavy_data(
        "600519",
        "2026-04-06T10:00:00",
        "evidence_pack",
        [{"category": "compliance", "title": "latest"}],
    )

    evidence_pack = store.get_latest_evidence_pack("600519")

    assert evidence_pack
    assert evidence_pack[0]["title"] == "latest"


def test_incremental_updater_counts_official_modules() -> None:
    class FakeCollector:
        def _get_stock_info(self, stock_code: str, result: CollectorOutput) -> None:
            result.stock_info = StockBasicInfo(code=stock_code, name="Example Co", main_business="high-end equipment")

        def _get_compliance_events(self, stock_code: str, result: CollectorOutput) -> None:
            del stock_code
            result.compliance_events = [
                ComplianceEvent(
                    title="Administrative penalty decision",
                    source="csrc",
                    publish_date=date(2026, 4, 5),
                    severity="high",
                )
            ]

        def _get_patents(self, stock_code: str, result: CollectorOutput) -> None:
            del stock_code
            result.patents = [
                PatentRecord(
                    title="High performance equipment",
                    source="cnipa",
                    publish_date=date(2026, 4, 4),
                    patent_type="invention",
                )
            ]

        def _get_governance_data(self, stock_code: str, result: CollectorOutput) -> None:
            del stock_code
            result.governance = GovernanceData(lawsuit_info="1 official compliance event")

    agent = IncrementalUpdaterAgent()
    agent._make_collector = lambda: FakeCollector()  # type: ignore[method-assign]

    assert agent._fetch_incremental_compliance_events("600519", date(2026, 4, 1)) == 1
    assert agent._fetch_incremental_patents("600519", date(2026, 4, 1)) == 1
    assert agent._fetch_incremental_governance("600519") == 1


@pytest.mark.asyncio
async def test_dynamic_tracker_raises_alert_for_recent_official_compliance(tmp_path) -> None:
    store = ChromaKnowledgeStore(persist_dir=str(tmp_path / "chroma"))
    store._save_heavy_data(
        "600519",
        "2026-04-06T10:00:00",
        "conclusion",
        InvestmentConclusion(
            recommendation="观望",
            confidence_level="低",
            risk_level="高",
            monitoring_points=["官方合规事件"],
            monitoring_plan=[
                MonitoringPlanItem(
                    layer=MonitoringLayer.RISK_TRIGGER,
                    metric="official_compliance",
                    trigger="出现官方处罚/立案",
                )
            ],
            conclusion_summary="继续跟踪合规风险",
        ).model_dump(mode="json"),
    )
    store._save_heavy_data(
        "600519",
        "2026-04-06T10:00:00",
        "evidence_pack",
        [
            {
                "category": "compliance",
                "title": "行政处罚决定书",
                "source": "csrc",
                "reference_date": datetime.now().strftime("%Y-%m-%d"),
                "excerpt": "因信息披露违规被处罚",
            }
        ],
    )

    watch_mgr = WatchListManager(file_path=str(tmp_path / "watch_list.json"))
    watch_mgr.add("600519", "贵州茅台")

    agent = DynamicTrackerAgent(knowledge_store=store, watch_manager=watch_mgr)
    agent._get_realtime_snapshot = lambda stock_code: {  # type: ignore[method-assign]
        "price": 1500.0,
        "change_pct": 0.0,
        "pe_ttm": 22.0,
        "pb_mrq": 8.0,
    }

    output = await agent.run(AgentInput(stock_code="600519"))
    alerts = output.data["alerts"]

    assert any(alert["metric_name"] == "official_compliance" for alert in alerts)
    assert any(
        alert["severity"] == "critical"
        for alert in alerts
        if alert["metric_name"] == "official_compliance"
    )
