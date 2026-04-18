from __future__ import annotations

from investresearch.data_layer.cleaner import DataCleanerAgent


def test_clean_financials_nulls_suspicious_cashflow_fields() -> None:
    agent = DataCleanerAgent()
    cleaned = agent._clean_financials(
        [
            {
                "report_date": "2025-12-31",
                "revenue": 15_711_000_000.0,
                "net_profit": 2_359_000_000.0,
                "operating_cashflow": 927.12,
                "free_cashflow": 8_753_577.11,
                "cash_to_profit": 0.0,
            }
        ]
    )

    item = cleaned[0]
    assert item["operating_cashflow"] is None
    assert item["free_cashflow"] is None
    assert item["cash_to_profit"] is None
    assert item["_cashflow_suspect_fields"] == ["operating_cashflow", "free_cashflow"]


def test_clean_financials_keeps_cash_to_profit_as_ratio() -> None:
    agent = DataCleanerAgent()
    cleaned = agent._clean_financials(
        [
            {
                "report_date": "2025-12-31",
                "revenue": 1_000_000_000.0,
                "net_profit": 100_000_000.0,
                "operating_cashflow": 114_000_000.0,
                "free_cashflow": 80_000_000.0,
                "cash_to_profit": 1.14,
            }
        ]
    )

    item = cleaned[0]
    assert item["cash_to_profit"] == 1.14
    assert item.get("_cashflow_suspect_fields") is None


def test_clean_financials_keeps_goodwill_ratio_in_percent_units() -> None:
    agent = DataCleanerAgent()
    cleaned = agent._clean_financials(
        [
            {
                "report_date": "2025-12-31",
                "revenue": 1_000_000_000.0,
                "net_profit": 100_000_000.0,
                "operating_cashflow": 114_000_000.0,
                "free_cashflow": 80_000_000.0,
                "goodwill_ratio": 0.5068,
            }
        ]
    )

    item = cleaned[0]
    assert item["goodwill_ratio"] == 0.5068
