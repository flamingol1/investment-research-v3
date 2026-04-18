from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from investresearch.analysis_layer.business_model import BusinessModelAgent
from investresearch.analysis_layer.financial import FinancialAgent
from investresearch.analysis_layer.governance import GovernanceAgent
from investresearch.analysis_layer.industry import IndustryAgent
from investresearch.analysis_layer.risk import RiskAgent
from investresearch.analysis_layer.valuation import ValuationAgent
from investresearch.core.models import (
    AgentInput,
    Announcement,
    CollectorOutput,
    ComplianceEvent,
    DataSource,
    FieldCollectionStatus,
    FieldEvidenceState,
    FieldValueState,
    FinancialStatement,
    GovernanceData,
    IndustryEnhancedData,
    InvestmentConclusion,
    MonitoringLayer,
    MonitoringPlanItem,
    PatentRecord,
    ShareholderData,
    StockBasicInfo,
    StockPrice,
)
from investresearch.core.trust import build_field_quality_map, build_module_profile
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


def test_structured_announcement_extraction_skips_main_business_noise_and_uses_later_candidate() -> None:
    text = (
        "公司上市以来主营业务的变化情况（如有） 2016年4月，公司通过重大资产重组，主营业务由内河涉。"
        "公司主要从事网络游戏的研发、运营与发行。"
    )

    structured = DataCollectorAgent._extract_structured_announcement_fields(
        text,
        title="2024年年度报告",
        announcement_date=date(2025, 3, 30),
    )

    assert structured["stock_info"]["main_business"] == "网络游戏的研发、运营与发行"


def test_structured_announcement_extraction_skips_main_business_with_revenue_tail() -> None:
    text = (
        "公司主要业务是人工智能企业服务，报告期内实现营业收入298.22万元。"
        "公司主要业务是企业加速服务、企业服务及人工智能服务等。"
    )

    structured = DataCollectorAgent._extract_structured_announcement_fields(
        text,
        title="2025年年度报告",
        announcement_date=date(2026, 4, 8),
    )

    assert structured["stock_info"]["main_business"] == "企业加速服务、企业服务及人工智能服务等"


def test_structured_announcement_extraction_parses_governance_absence_flags() -> None:
    text = (
        "公司实际控制人为贵州省国资委。"
        "报告期内不存在股份质押情况。"
        "是否存在违反规定决策程序对外提供担保的情况 否。"
        "重大诉讼、仲裁事项 □适用 √不适用。"
        "董事、监事和高级管理人员持股变动情况 □适用 √不适用。"
        "董事、监事和高级管理人员未持有公司股份。"
    )

    structured = DataCollectorAgent._extract_structured_announcement_fields(
        text,
        title="2025年年度报告",
        announcement_date=date(2026, 3, 30),
    )

    assert structured["stock_info"]["actual_controller"] == "贵州省国资委"
    assert structured["governance"]["actual_controller"] == "贵州省国资委"
    assert structured["governance"]["equity_pledge_absent"] is True
    assert structured["governance"]["guarantee_absent"] is True
    assert structured["governance"]["lawsuit_absent"] is True
    assert structured["governance"]["management_changes_absent"] is True
    assert structured["shareholders"]["management_share_ratio_absent"] is True


def test_structured_announcement_extraction_parses_total_management_share_ratio() -> None:
    text = (
        "现任及报告期内离任董事、监事和高级管理人员持有本公司股份总数为1,234,567股，"
        "占总股本比例为0.12%。"
    )

    structured = DataCollectorAgent._extract_structured_announcement_fields(
        text,
        title="2025年年度报告",
        announcement_date=date(2026, 3, 30),
    )

    assert structured["shareholders"]["management_share_ratio"] == pytest.approx(0.12, rel=1e-6)


def test_preferred_periodic_announcements_skips_summary_and_legal_opinion() -> None:
    items = [
        {"title": "2024年年度报告摘要"},
        {"title": "关于2024年业绩快报的公告"},
        {"title": "国浩律师（上海）事务所关于公司股东会的法律意见书"},
        {"title": "2024年年度报告"},
    ]

    preferred = DataCollectorAgent._preferred_periodic_announcements(items, "年报")

    assert preferred[0]["title"] == "2024年年度报告"


def test_build_field_quality_prefers_annual_main_business_over_later_half_year_noise() -> None:
    field_quality = build_field_quality_map(
        {
            "stock_info": {"main_business": "是互联网游戏的研发和运营"},
            "announcements": [
                {
                    "announcement_type": "半年报",
                    "announcement_date": "2025-08-28",
                    "title": "2025年半年度报告",
                    "structured_fields": {"stock_info": {"main_business": "的主要业务”相关内容"}},
                },
                {
                    "announcement_type": "年报",
                    "announcement_date": "2025-03-21",
                    "title": "2024年年度报告",
                    "structured_fields": {"stock_info": {"main_business": "是互联网游戏的研发和运营"}},
                },
            ],
            "field_statuses": {},
            "collection_status": {},
        }
    )["stock_info.main_business"]

    assert field_quality.evidence_state == FieldEvidenceState.CONSISTENT
    assert len(field_quality.source_values) == 2
    assert field_quality.source_values[1].reference_date == "2025-03-21"
    assert field_quality.source_values[1].value == "是互联网游戏的研发和运营"


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
    assert output.financials[0].operating_cashflow is None
    assert output.financials[0].contract_liabilities == 4500000000.0
    assert output.financials[0].raw_data is not None
    assert output.financials[0].raw_data["announcement_snapshot_extra"]["operating_cashflow"] == 12000000000.0
    assert output.financials[0].raw_data.get("source_values", {}).get("cninfo_announcement_extract", {}).get("metrics") == {
        "contract_liabilities": 4500000000.0
    }
    assert output.governance is not None
    assert output.governance.dividend_history
    assert output.governance.dividend_history[0]["plan"] == "每10股派发现金红利76.24元（含税）"


def test_fill_missing_fields_upgrades_failed_governance_statuses_from_annual_report() -> None:
    agent = DataCollectorAgent()
    output = CollectorOutput(
        stock_info=StockBasicInfo(code="600519", name="贵州茅台"),
        governance=GovernanceData(),
        shareholders=ShareholderData(),
        announcements=[
            Announcement(
                title="2025年年度报告",
                announcement_type="年报",
                announcement_date=date(2026, 3, 30),
                source="cninfo",
                structured_fields={
                    "stock_info": {"actual_controller": "贵州省国资委"},
                    "governance": {
                        "actual_controller": "贵州省国资委",
                        "equity_pledge_absent": True,
                        "pledge_details": "年报披露未见稳定股权质押记录",
                        "guarantee_absent": True,
                        "guarantee_info": "年报披露未见违规担保或异常对外担保",
                        "lawsuit_absent": True,
                        "lawsuit_info": "年报披露未见重大诉讼或仲裁事项",
                        "management_changes_absent": True,
                    },
                    "shareholders": {"management_share_ratio_absent": True},
                },
            )
        ],
        field_statuses={
            "governance.actual_controller": FieldCollectionStatus(
                field="governance.actual_controller",
                value_state=FieldValueState.COLLECTION_FAILED,
                sources_checked=["stock_hold_control_cninfo"],
            ),
            "governance.equity_pledge_ratio": FieldCollectionStatus(
                field="governance.equity_pledge_ratio",
                value_state=FieldValueState.COLLECTION_FAILED,
                sources_checked=["stock_cg_equity_mortgage_cninfo"],
            ),
            "governance.guarantee_info": FieldCollectionStatus(
                field="governance.guarantee_info",
                value_state=FieldValueState.COLLECTION_FAILED,
                sources_checked=["stock_cg_guarantee_cninfo"],
            ),
            "governance.lawsuit_info": FieldCollectionStatus(
                field="governance.lawsuit_info",
                value_state=FieldValueState.COLLECTION_FAILED,
                sources_checked=["stock_cg_lawsuit_cninfo"],
            ),
            "governance.management_changes": FieldCollectionStatus(
                field="governance.management_changes",
                value_state=FieldValueState.COLLECTION_FAILED,
                sources_checked=["stock_hold_change_cninfo"],
            ),
            "shareholders.management_share_ratio": FieldCollectionStatus(
                field="shareholders.management_share_ratio",
                value_state=FieldValueState.COLLECTION_FAILED,
                sources_checked=["stock_hold_change_cninfo"],
            ),
        },
    )

    agent._fill_missing_fields("600519", output)

    assert output.stock_info is not None
    assert output.stock_info.actual_controller == "贵州省国资委"
    assert output.governance is not None
    assert output.governance.actual_controller == "贵州省国资委"
    assert output.governance.pledge_details == "年报披露未见稳定股权质押记录"
    assert output.field_statuses["governance.actual_controller"].value_state == FieldValueState.PRESENT
    assert output.field_statuses["governance.equity_pledge_ratio"].value_state == FieldValueState.VERIFIED_ABSENT
    assert output.field_statuses["governance.guarantee_info"].value_state == FieldValueState.VERIFIED_ABSENT
    assert output.field_statuses["governance.lawsuit_info"].value_state == FieldValueState.VERIFIED_ABSENT
    assert output.field_statuses["governance.management_changes"].value_state == FieldValueState.VERIFIED_ABSENT
    assert output.field_statuses["shareholders.management_share_ratio"].value_state == FieldValueState.VERIFIED_ABSENT


def test_get_governance_data_keeps_stock_info_controller_when_cninfo_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = DataCollectorAgent()
    output = CollectorOutput(
        stock_info=StockBasicInfo(code="600519", name="贵州茅台", actual_controller="中国贵州茅台酒厂（集团）有限责任公司"),
    )

    def failing_call(*args: object, **kwargs: object) -> pd.DataFrame:
        raise RuntimeError("boom")

    monkeypatch.setattr(agent, "_akshare_call", failing_call)

    agent._get_governance_data("600519", output)

    assert output.governance is not None
    assert output.governance.actual_controller == "中国贵州茅台酒厂（集团）有限责任公司"
    assert output.field_statuses["governance.actual_controller"].value_state == FieldValueState.PRESENT


def test_get_industry_enhanced_cached_payload_restores_field_statuses() -> None:
    class _Cache:
        def __init__(self) -> None:
            self.payload = {
                "industry_enhanced_600519_v2": {
                    "industry_name": "白酒Ⅱ",
                    "industry_code": "340501",
                    "industry_leaders": [],
                    "data_points": [],
                }
            }

        def get(self, key: str) -> dict[str, object] | None:
            return self.payload.get(key)

        def set(self, key: str, value: object, ttl: int | None = None) -> None:
            self.payload[key] = value

    agent = DataCollectorAgent(cache=_Cache())
    output = CollectorOutput()

    agent._get_industry_enhanced("600519", output)

    assert output.industry_enhanced is not None
    assert output.field_statuses["industry_enhanced.data_points"].value_state == FieldValueState.VERIFIED_ABSENT
    assert output.field_statuses["industry_enhanced.industry_leaders"].value_state == FieldValueState.VERIFIED_ABSENT


def test_merge_structured_financials_skips_report_date_only_snapshots() -> None:
    financials = [
        FinancialStatement(
            code="603659",
            report_date=date(2025, 12, 31),
            report_type="annual",
            source=DataSource.AKSHARE,
            revenue=10_000_000_000,
            net_profit=1_000_000_000,
        )
    ]
    announcements = [
        Announcement(
            title="关于召开2026年第一季度业绩说明会的公告",
            announcement_type="日常经营",
            announcement_date=date(2026, 4, 17),
            source="cninfo",
            structured_fields={"financial_snapshot": {"report_date": "2026-03-31"}},
        )
    ]

    merged = DataCollectorAgent._merge_structured_financials("603659", financials, announcements)

    assert len(merged) == 1
    assert merged[0].report_date == date(2025, 12, 31)


def test_merge_structured_financials_keeps_announcement_cashflow_as_sidecar_evidence() -> None:
    financials = [
        FinancialStatement(
            code="002558",
            report_date=date(2025, 9, 30),
            report_type="quarterly",
            source=DataSource.AKSHARE,
            revenue=3_368_000_000.0,
            net_profit=1_417_000_000.0,
            operating_cashflow=1_851_250_272.84,
            contract_liabilities=1_333_275_632.48,
            raw_data={
                "source_values": {
                    "eastmoney_cashflow": {
                        "source_type": "official_statement",
                        "reference_date": "2025-09-30",
                        "metrics": {
                            "operating_cashflow": 1_851_250_272.84,
                            "investing_cashflow": -19_003_128.36,
                            "financing_cashflow": 924_520_682.65,
                            "free_cashflow": 1_832_247_144.48,
                        },
                    }
                }
            },
        )
    ]
    announcements = [
        Announcement(
            title="2025-q3-report",
            announcement_type="quarterly",
            announcement_date=date(2025, 10, 31),
            source="cninfo",
            structured_fields={
                "financial_snapshot": {
                    "report_date": "2025-09-30",
                    "operating_cashflow": 8_882.14,
                    "investing_cashflow": 5_309.60,
                    "financing_cashflow": 3_650.46,
                    "free_cashflow": 14_191.74,
                    "non_recurring_profit": 120_000_000.0,
                    "contract_liabilities": 3_789.64,
                }
            },
        )
    ]

    merged = DataCollectorAgent._merge_structured_financials("002558", financials, announcements)

    latest = merged[0]
    assert latest.operating_cashflow == 1_851_250_272.84
    assert latest.non_recurring_profit == 120_000_000.0
    assert latest.raw_data is not None
    assert latest.raw_data["source_values"]["eastmoney_cashflow"]["metrics"]["operating_cashflow"] == 1_851_250_272.84
    assert latest.raw_data["source_values"]["cninfo_announcement_extract"]["metrics"] == {
        "non_recurring_profit": 120_000_000.0
    }
    assert latest.raw_data["announcement_extracts"][-1]["snapshot"]["operating_cashflow"] == 8_882.14


def test_merge_official_profit_rows_adds_second_official_source_for_revenue() -> None:
    financials = [
        FinancialStatement(
            code="002558",
            report_date=date(2025, 9, 30),
            report_type="quarterly",
            source=DataSource.AKSHARE,
            revenue=3_368_000_000.0,
            net_profit=1_417_000_000.0,
            raw_data={
                "source_values": {
                    "akshare_financial_abstract": {
                        "source_type": "official_statement",
                        "reference_date": "2025-09-30",
                        "metrics": {
                            "revenue": 3_368_000_000.0,
                            "net_profit": 1_417_000_000.0,
                        },
                    }
                }
            },
        )
    ]
    df = pd.DataFrame(
        [
            {
                "REPORT_DATE": "2025-09-30 00:00:00",
                "NOTICE_DATE": "2025-10-29 00:00:00",
                "TOTAL_OPERATE_INCOME": 3_368_421_677.17,
                "PARENT_NETPROFIT": 1_416_820_995.71,
                "DEDUCT_PARENT_NETPROFIT": 1_484_383_097.79,
            }
        ]
    )

    merged = DataCollectorAgent._merge_official_profit_rows("002558", financials, df)

    latest = merged[0]
    assert latest.revenue == 3_368_000_000.0
    assert latest.deduct_net_profit == pytest.approx(1_484_383_097.79, rel=1e-6)
    assert latest.raw_data is not None
    assert latest.raw_data["source_values"]["eastmoney_profit"]["metrics"]["revenue"] == pytest.approx(3_368_421_677.17, rel=1e-6)
    assert latest.raw_data["source_values"]["eastmoney_profit"]["metrics"]["net_profit"] == pytest.approx(1_416_820_995.71, rel=1e-6)


def test_merge_structured_stock_info_prefers_annual_report_main_business_over_business_scope() -> None:
    stock_info = StockBasicInfo(
        code="300688",
        name="创业黑马",
        main_business="技术开发、技术咨询、技术交流、技术转让、技术推广；企业管理咨询；信息系统集成服务",
    )
    announcements = [
        Announcement(
            title="2025年年度报告",
            announcement_type="年报",
            announcement_date=date(2026, 4, 10),
            source="cninfo",
            structured_fields={"stock_info": {"main_business": "企业加速服务与城市产业服务"}},
        )
    ]

    merged = DataCollectorAgent._merge_structured_stock_info(stock_info, announcements)

    assert merged is not None
    assert merged.main_business == "企业加速服务与城市产业服务"


def test_prune_conflicting_financial_source_values_prefers_stronger_official_sources() -> None:
    financials = [
        FinancialStatement(
            code="300688",
            report_date=date(2025, 12, 31),
            report_type="annual",
            source=DataSource.AKSHARE,
            net_profit=-49_090_400.0,
            contract_liabilities=67_863_671.88,
            raw_data={
                "source_values": {
                    "akshare_financial_abstract": {
                        "source_type": "official_statement",
                        "reference_date": "2025-12-31",
                        "metrics": {"net_profit": -49_090_400.0},
                    },
                    "eastmoney_balance": {
                        "source_type": "official_statement",
                        "reference_date": "2025-12-31",
                        "metrics": {"contract_liabilities": 67_863_671.88},
                    },
                    "baostock_financials": {
                        "source_type": "official_statement",
                        "reference_date": "2025-12-31",
                        "metrics": {"net_profit": -77_995_453.47, "gross_margin": 52.22},
                    },
                    "cninfo_announcement_extract": {
                        "source_type": "self_reported",
                        "reference_date": "2025-12-31",
                        "metrics": {"contract_liabilities": 4_603_814.67, "non_recurring_profit": 9_741.7},
                    },
                }
            },
        )
    ]

    cleaned = DataCollectorAgent._prune_conflicting_financial_source_values(financials)

    latest = cleaned[0]
    assert latest.raw_data is not None
    assert "net_profit" not in latest.raw_data["source_values"]["baostock_financials"]["metrics"]
    assert latest.raw_data["source_values"]["baostock_financials"]["metrics"]["gross_margin"] == pytest.approx(52.22, rel=1e-6)
    assert "contract_liabilities" not in latest.raw_data["source_values"]["cninfo_announcement_extract"]["metrics"]
    assert latest.raw_data["source_values"]["cninfo_announcement_extract"]["metrics"]["non_recurring_profit"] == pytest.approx(9_741.7, rel=1e-6)


def test_merge_official_balance_rows_adds_equity_source_without_overwriting_existing_value() -> None:
    financials = [
        FinancialStatement(
            code="002558",
            report_date=date(2025, 9, 30),
            report_type="quarterly",
            source=DataSource.AKSHARE,
            equity=15_170_853_617.0,
            raw_data={
                "source_values": {
                    "derived_per_share": {
                        "source_type": "derived",
                        "reference_date": "2025-09-30",
                        "metrics": {"equity": 15_170_853_617.0},
                    }
                }
            },
        )
    ]
    df = pd.DataFrame(
        [
            {
                "REPORT_DATE": "2025-09-30 00:00:00",
                "NOTICE_DATE": "2025-10-29 00:00:00",
                "TOTAL_ASSETS": 18_189_523_783.62,
                "TOTAL_LIABILITIES": 2_983_853_921.91,
                "TOTAL_EQUITY": 15_205_669_861.71,
                "PARENT_EQUITY_BALANCE": 0.0,
                "CONTRACT_LIAB": 1_333_275_632.48,
                "GOODWILL": 131_619_236.60,
            }
        ]
    )

    merged = DataCollectorAgent._merge_official_balance_rows("002558", financials, df)

    latest = merged[0]
    assert latest.equity == pytest.approx(15_170_853_617.0, rel=1e-6)
    assert latest.total_assets == pytest.approx(18_189_523_783.62, rel=1e-6)
    assert latest.total_liabilities == pytest.approx(2_983_853_921.91, rel=1e-6)
    assert latest.contract_liabilities == pytest.approx(1_333_275_632.48, rel=1e-6)
    assert latest.goodwill_ratio == pytest.approx(round(131_619_236.60 / 15_205_669_861.71 * 100, 4), rel=1e-6)
    assert latest.raw_data is not None
    assert latest.raw_data["source_values"]["eastmoney_balance"]["metrics"]["equity"] == pytest.approx(15_205_669_861.71, rel=1e-6)


def test_shareholder_count_falls_back_to_third_recent_quarter(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = DataCollectorAgent()
    output = CollectorOutput()
    requested_dates: list[str] = []

    def fake_akshare_call(name: str, *args: object, **kwargs: object) -> pd.DataFrame | None:
        assert name == "stock_hold_num_cninfo"
        date_value = str(kwargs.get("date") or "")
        requested_dates.append(date_value)
        if date_value in {"20260331", "20251231"}:
            return pd.DataFrame(
                [{"证券代码": "000001", "本期股东人数": 1000, "股东人数增幅": 1.2}]
            )
        if date_value == "20250930":
            return pd.DataFrame(
                [{"证券代码": "002558", "本期股东人数": 60989, "股东人数增幅": 21.47}]
            )
        return pd.DataFrame()

    monkeypatch.setattr(agent, "_akshare_call", fake_akshare_call)

    agent._get_shareholder_data("002558", output)

    assert requested_dates[:3] == ["20260331", "20251231", "20250930"]
    assert output.shareholders is not None
    assert output.shareholders.shareholder_count == 60989
    assert output.shareholders.shareholder_count_change == pytest.approx(21.47, rel=1e-6)


def test_fill_missing_fields_backfills_financial_totals_and_realtime_from_per_share_metrics() -> None:
    agent = DataCollectorAgent()
    output = CollectorOutput(
        realtime=StockPrice(code="603659", date="2026-04-17", close=35.91),  # type: ignore[arg-type]
        financials=[
            FinancialStatement(
                code="603659",
                report_date=date(2025, 12, 31),
                report_type="annual",
                source=DataSource.AKSHARE,
                revenue=15_711_291_195.73,
                net_profit=2_604_558_407.0,
                gross_margin=31.72,
                raw_data={
                    "eps_ttm": 1.10422,
                    "book_value_per_share": 9.56,
                    "operating_cashflow_per_share": 1.26,
                    "total_share": 2_136_399_076.0,
                    "asset_to_equity": 2.124286,
                },
            )
        ],
    )

    agent._fill_missing_fields("603659", output)

    latest = output.financials[0]
    assert latest.equity is not None
    assert latest.equity == pytest.approx(9.56 * 2_136_399_076.0, rel=1e-6)
    assert latest.operating_cashflow == pytest.approx(1.26 * 2_136_399_076.0, rel=1e-6)
    assert latest.total_assets is not None
    assert output.realtime is not None
    assert output.realtime.market_cap == pytest.approx(35.91 * 2_136_399_076.0, rel=1e-6)
    assert output.realtime.pe_ttm == pytest.approx(35.91 / 1.10422, rel=1e-6)
    assert output.realtime.pb_mrq == pytest.approx(35.91 / 9.56, rel=1e-6)


def test_realtime_quote_keeps_eastmoney_primary_and_sina_observation(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = DataCollectorAgent()
    output = CollectorOutput()
    df = pd.DataFrame(
        [
            {
                "代码": "600000",
                "今开": 10.1,
                "最新价": 10.2,
                "最高": 10.4,
                "最低": 9.9,
                "成交量": 123456.0,
                "成交额": 789000000.0,
                "换手率": 1.23,
                "市盈率-动态": 15.6,
                "市净率": 1.8,
                "总市值": 50_000_000_000.0,
            }
        ]
    )

    monkeypatch.setattr(agent, "_akshare_call", lambda name, *args, **kwargs: df if name == "stock_zh_a_spot_em" else None)
    monkeypatch.setattr(agent, "_rate_limit", lambda: None)

    sina_fields = [
        "示例股份",
        "10.0",
        "9.8",
        "9.9",
        "10.1",
        "9.7",
        "0",
        "0",
        "88888",
        "666000000",
    ] + ["0"] * 22
    sina_line = f'var hq_str_sh600000="{",".join(sina_fields)}";'

    class DummyResponse:
        def __init__(self, text: str) -> None:
            self.text = text
            self.encoding = "gbk"

    class DummySession:
        def __init__(self) -> None:
            self.trust_env = False

        def get(self, *args: object, **kwargs: object) -> DummyResponse:
            del args, kwargs
            return DummyResponse(sina_line)

    monkeypatch.setattr("requests.Session", DummySession)

    agent._get_realtime_quote("600000", output)

    assert output.realtime is not None
    assert output.realtime.close == pytest.approx(10.2, rel=1e-6)
    assert output.realtime.market_cap == pytest.approx(50_000_000_000.0, rel=1e-6)
    assert output.realtime.raw_data is not None
    source_values = output.realtime.raw_data["source_values"]
    assert source_values["eastmoney_realtime"]["metrics"]["close"] == pytest.approx(10.2, rel=1e-6)
    assert source_values["sina_realtime"]["metrics"]["close"] == pytest.approx(9.9, rel=1e-6)
    assert source_values["eastmoney_realtime"]["metrics"]["market_cap"] == pytest.approx(50_000_000_000.0, rel=1e-6)


def test_merge_official_cashflow_rows_overrides_announcement_scale_noise() -> None:
    financials = [
        FinancialStatement(
            code="603659",
            report_date=date(2025, 12, 31),
            report_type="annual",
            source=DataSource.AKSHARE,
            revenue=15_711_000_000.0,
            net_profit=2_359_000_000.0,
            operating_cashflow=2_691_862_835.76,
            investing_cashflow=8_752_649.99,
            free_cashflow=8_753_577.11,
            raw_data={"announcement_extracts": [{"title": "2025年年度报告"}]},
        )
    ]
    df = pd.DataFrame(
        [
            {
                "REPORT_DATE": "2025-12-31 00:00:00",
                "NOTICE_DATE": "2026-03-06 00:00:00",
                "NETCASH_OPERATE": 2_697_297_581.91,
                "NETCASH_INVEST": -2_974_026_025.77,
                "NETCASH_FINANCE": -670_314_612.19,
                "CONSTRUCT_LONG_ASSET": 1_519_166_483.61,
                "TOTAL_INVEST_OUTFLOW": 5_444_664_227.72,
                "TOTAL_INVEST_INFLOW": 2_470_638_201.95,
                "CCE_ADD": -946_816_110.55,
                "END_CCE": 5_189_697_909.67,
            }
        ]
    )

    merged = DataCollectorAgent._merge_official_cashflow_rows("603659", financials, df)

    latest = merged[0]
    assert latest.operating_cashflow == pytest.approx(2_697_297_581.91, rel=1e-6)
    assert latest.investing_cashflow == pytest.approx(-2_974_026_025.77, rel=1e-6)
    assert latest.financing_cashflow == pytest.approx(-670_314_612.19, rel=1e-6)
    assert latest.free_cashflow == pytest.approx(2_697_297_581.91 - 1_519_166_483.61, rel=1e-6)
    assert latest.cash_to_profit == pytest.approx(round(2_697_297_581.91 / 2_359_000_000.0, 2), rel=1e-6)
    assert latest.raw_data is not None
    assert latest.raw_data["capital_expenditure"] == pytest.approx(1_519_166_483.61, rel=1e-6)


def test_merge_goodwill_rows_backfills_goodwill_ratio() -> None:
    financials = [
        FinancialStatement(
            code="603659",
            report_date=date(2025, 12, 31),
            report_type="annual",
            source=DataSource.AKSHARE,
            equity=20_423_975_166.56,
            raw_data={"total_share": 2_136_399_076.0, "book_value_per_share": 9.56},
        )
    ]
    df = pd.DataFrame(
        [
            {
                "report_date": "2025-12-31",
                "metric_name": "goodwill",
                "value": 103_503_907.71,
            }
        ]
    )

    merged = DataCollectorAgent._merge_goodwill_rows(financials, df)

    latest = merged[0]
    assert latest.goodwill_ratio == pytest.approx(round(103_503_907.71 / 20_423_975_166.56 * 100, 4), rel=1e-6)
    assert latest.raw_data is not None
    assert latest.raw_data["goodwill_amount"] == pytest.approx(103_503_907.71, rel=1e-6)


def test_analysis_fmt_pct_does_not_reinflate_cleaned_percentages() -> None:
    assert FinancialAgent._fmt_pct(0.5068) == "0.5%"
    assert RiskAgent._fmt_pct(0.5068) == "0.5%"
    assert IndustryAgent._fmt_pct(0.5068) == "0.5%"


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


def test_industry_pe_source_failure_marks_collection_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = DataCollectorAgent()
    output = CollectorOutput(
        stock_info=StockBasicInfo(code="600519", name="贵州茅台", industry_sw="白酒"),
    )

    def fake_akshare_call(name: str, *args: object, **kwargs: object) -> pd.DataFrame | None:
        del args, kwargs
        if name == "stock_industry_pe_ratio_cninfo":
            raise RuntimeError("source down")
        if name == "stock_board_industry_name_ths":
            return pd.DataFrame([{"name": "白酒", "code": "BK1034"}])
        return pd.DataFrame()

    monkeypatch.setattr(agent, "_get_from_cache", lambda key: None)
    monkeypatch.setattr(agent, "_save_to_cache", lambda *args, **kwargs: None)
    monkeypatch.setattr(agent, "_akshare_call", fake_akshare_call)

    agent._get_industry_enhanced("600519", output)

    assert output.field_statuses["industry_enhanced.industry_pe"].value_state.value == "collection_failed"
    assert output.field_statuses["industry_enhanced.industry_pb"].value_state.value == "collection_failed"


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


@pytest.mark.asyncio
async def test_report_agent_builds_peer_and_scenario_charts() -> None:
    agent = ReportAgent()
    context = {
        "cleaned_data": {
            "stock_info": {"name": "贵州茅台", "industry_sw": "白酒"},
            "financials": [
                {"report_date": "2025-12-31", "revenue": 100.0, "net_profit": 50.0, "operating_cashflow": 60.0, "free_cashflow": 55.0}
            ],
            "valuation": [{"date": "2025-12-31", "pe_ttm": 20.0, "pb_mrq": 8.0}],
            "industry_enhanced": {"data_points": ["白酒行业景气平稳"]},
            "coverage_ratio": 0.8,
            "missing_fields": [],
        },
        "cross_verification": {
            "peers": [
                {"stock_code": "000858", "stock_name": "五粮液", "industry_level": "二级行业", "rank_in_industry": 2, "market_cap": 600000000000},
                {"stock_code": "000596", "stock_name": "古井贡酒", "industry_level": "二级行业", "rank_in_industry": 4, "market_cap": 120000000000},
            ],
            "verified_metrics": [
                {
                    "metric_name": "market_size",
                    "evidence_refs": [
                        {
                            "source": "peer_annual_report",
                            "source_priority": 0,
                            "title": "2025年行业概览",
                            "field": "industry.market_size",
                            "excerpt": "白酒行业保持稳健增长。",
                            "url": "https://example.com/peer.pdf",
                            "reference_date": "2025-12-31",
                        }
                    ],
                }
            ],
        },
        "risk_analysis": {
            "scenarios": [
                {"scenario": "乐观", "target_price": 1800.0},
                {"scenario": "中性", "target_price": 1600.0},
                {"scenario": "悲观", "target_price": 1400.0},
            ],
            "evidence_refs": [
                {
                    "source": "risk_model",
                    "source_priority": 0,
                    "title": "情景测算",
                    "field": "risk.scenarios",
                    "excerpt": "三种情景对应不同价格区间。",
                    "url": "",
                    "reference_date": "2025-12-31",
                }
            ],
        },
    }

    output = await agent.run(AgentInput(stock_code="600519", context=context))
    charts = {item["chart_id"]: item for item in output.data["chart_pack"]}

    assert "peer_placeholder" not in charts
    assert charts["peer_comparison"]["series"]
    assert charts["peer_comparison"]["series"][0]["points"][0]["x"] == "股票代码"
    assert charts["peer_comparison"]["evidence_refs"]
    assert [item["name"] for item in charts["scenario_analysis"]["series"]] == ["乐观", "中性", "悲观"]
    assert charts["scenario_analysis"]["evidence_refs"]


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
