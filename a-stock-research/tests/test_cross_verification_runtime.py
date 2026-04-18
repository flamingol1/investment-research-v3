from __future__ import annotations

import pytest

from investresearch.analysis_layer.risk import RiskAgent
from investresearch.core.models import AgentInput, FinancialStatement, StockPrice
from investresearch.data_layer.cross_verify import CrossVerificationEngine


def test_company_cross_verification_marks_divergent_metrics() -> None:
    financial = FinancialStatement(
        code="600000",
        report_date="2025-12-31",
        report_type="annual",
        revenue=1_000_000_000.0,
        net_profit=120_000_000.0,
        operating_cashflow=90_000_000.0,
        equity=800_000_000.0,
        raw_data={
            "source_values": {
                "akshare_financial_abstract": {
                    "source_type": "official_statement",
                    "reference_date": "2025-12-31",
                    "metrics": {
                        "revenue": 1_000_000_000.0,
                        "net_profit": 120_000_000.0,
                    },
                },
                "cninfo_announcement_extract": {
                    "source_type": "official_statement",
                    "reference_date": "2025-12-31",
                    "metrics": {
                        "revenue": 600_000_000.0,
                    },
                },
                "baostock_financials": {
                    "source_type": "official_statement",
                    "reference_date": "2025-12-31",
                    "metrics": {
                        "net_profit": 121_000_000.0,
                    },
                },
            }
        },
    )
    realtime = StockPrice(
        code="600000",
        date="2026-04-18",
        close=20.0,
        pe_ttm=18.0,
        pb_mrq=2.2,
        market_cap=5_000_000_000.0,
        raw_data={
            "source_values": {
                "eastmoney_realtime": {
                    "source_type": "market_quote",
                    "reference_date": "2026-04-18",
                    "metrics": {"market_cap": 5_000_000_000.0},
                },
                "derived_realtime_from_financials": {
                    "source_type": "derived",
                    "reference_date": "2026-04-18",
                    "metrics": {"market_cap": 4_950_000_000.0},
                },
            }
        },
    )

    result = CrossVerificationEngine().build_data_cross_verification(
        stock_code="600000",
        financials=[financial],
        realtime=realtime,
        valuation_percentile={"pe_ttm_current": 18.2, "pb_mrq_current": 2.18},
    )

    assert "revenue" in result.divergent_metrics
    assert "net_profit" in result.consistent_metrics
    assert result.overall_confidence > 0
    assert "分歧" in result.summary


@pytest.mark.asyncio
async def test_risk_agent_downgrades_evidence_when_cross_verification_diverges() -> None:
    agent = RiskAgent()
    output = await agent.safe_run(
        AgentInput(
            stock_code="600000",
            context={
                "cleaned_data": {
                    "stock_info": {"name": "示例公司", "industry_sw": "设备", "main_business": "高端装备"},
                    "financials": [
                        {
                            "report_date": "2025-12-31",
                            "revenue": 1_000_000_000.0,
                            "net_profit": 120_000_000.0,
                            "revenue_yoy": 8.0,
                            "debt_ratio": 35.0,
                            "operating_cashflow": 60_000_000.0,
                            "equity": 800_000_000.0,
                            "goodwill_ratio": 1.2,
                        },
                        {
                            "report_date": "2025-09-30",
                            "revenue": 760_000_000.0,
                            "net_profit": 92_000_000.0,
                            "revenue_yoy": 7.5,
                            "debt_ratio": 34.0,
                            "operating_cashflow": 41_000_000.0,
                            "equity": 760_000_000.0,
                            "goodwill_ratio": 1.1,
                        },
                        {
                            "report_date": "2025-06-30",
                            "revenue": 500_000_000.0,
                            "net_profit": 63_000_000.0,
                            "revenue_yoy": 7.0,
                            "debt_ratio": 33.0,
                            "operating_cashflow": 30_000_000.0,
                            "equity": 720_000_000.0,
                            "goodwill_ratio": 1.1,
                        },
                        {
                            "report_date": "2025-03-31",
                            "revenue": 240_000_000.0,
                            "net_profit": 30_000_000.0,
                            "revenue_yoy": 6.5,
                            "debt_ratio": 33.0,
                            "operating_cashflow": 15_000_000.0,
                            "equity": 700_000_000.0,
                            "goodwill_ratio": 1.0,
                        }
                    ],
                    "realtime": {"close": 20.0, "market_cap": 5_000_000_000.0},
                    "announcements": [{"title": "年度报告", "excerpt": "经营稳定"}],
                    "policy_documents": [{"title": "产业政策", "excerpt": "支持升级"}],
                    "industry_enhanced": {"industry_change_pct": 1.2},
                    "valuation_percentile": {"pe_ttm_percentile": 65.0, "pb_mrq_percentile": 60.0},
                    "governance": {},
                    "cross_verification": {
                        "divergent_metrics": ["revenue", "market_cap"],
                        "verified_metrics": [
                            {
                                "metric_name": "revenue",
                                "consistency_flag": "divergent",
                                "recommended_value": 100.0,
                                "sources": ["source_a", "source_b"],
                            }
                        ],
                        "overall_confidence": 0.52,
                        "summary": "多来源交叉验证发现 revenue 存在分歧",
                    },
                },
                "screening": {"key_risks": ["订单波动"]},
            },
        )
    )

    payload = output.data["risk"]
    assert payload["evidence_status"] == "partial"
    assert "多源分歧指标" in payload["conclusion"]
    assert any(item["risk_name"] == "关键指标多源分歧" for item in payload["risks"])
