from __future__ import annotations

from investresearch.core.models import StockPrice
from investresearch.data_layer.cross_verify import CrossVerificationEngine


def test_company_cross_verification_uses_realtime_source_values_for_close() -> None:
    realtime = StockPrice(
        code="600000",
        date="2026-04-18",
        close=20.0,
        pe_ttm=18.0,
        pb_mrq=2.2,
        market_cap=5_000_000_000.0,
        raw_data={
            "source_values": {
                "sina_realtime": {
                    "source_type": "market_quote",
                    "reference_date": "2026-04-18",
                    "metrics": {"close": 20.0},
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
        financials=[],
        realtime=realtime,
        valuation_percentile={"pe_ttm_current": 18.1, "pb_mrq_current": 2.19},
    )

    close_metric = next(item for item in result.verified_metrics if item.metric_name == "close")
    assert close_metric.source_count == 2
    assert close_metric.consistency_flag == "consistent"
