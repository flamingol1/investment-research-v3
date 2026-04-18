from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from investresearch import cli as cli_module
from investresearch.core.models import QualityGateDecision, RegressionBaselineSnapshot, ResearchReport
from investresearch.core.regression import (
    compare_baseline_snapshots,
    get_default_regression_sample_basket,
)
from investresearch.decision_layer import coordinator as coordinator_module


def _make_report(stock_code: str, stock_name: str, *, coverage_ratio: float = 0.82) -> ResearchReport:
    return ResearchReport(
        stock_code=stock_code,
        stock_name=stock_name,
        report_date=datetime(2026, 4, 18, 9, 30, 0),
        depth="deep",
        markdown="# regression",
        quality_gate=QualityGateDecision(
            blocked=False,
            core_evidence_score=max(0.75, coverage_ratio + 0.04),
            weak_fields=["industry.market_size"],
            reasons=[],
            consistency_notes=["估值结论已受证据约束。"],
            coverage_ratio=coverage_ratio,
            company_cross_confidence=0.88,
            peer_verified=3,
        ),
        baseline_snapshot=RegressionBaselineSnapshot(
            stock_code=stock_code,
            stock_name=stock_name,
            depth="deep",
            generated_at=datetime(2026, 4, 18, 10, 0, 0),
            coverage_ratio=coverage_ratio,
            completeness=coverage_ratio - 0.03,
            core_evidence_score=max(0.75, coverage_ratio + 0.04),
            missing_fields=["industry.market_size"],
            warning_count=1,
            initial_verdict="通过",
            final_recommendation="观望",
            quality_gate_blocked=False,
            quality_gate_reasons=[],
            consistency_notes=["估值偏贵导致结论保持谨慎。"],
        ),
        agents_completed=["data_collector", "data_cleaner", "report", "conclusion"],
    )


def test_default_regression_basket_covers_required_sectors() -> None:
    basket = get_default_regression_sample_basket()
    sectors = {item["sector"] for item in basket}
    stock_codes = {item["stock_code"] for item in basket}

    assert {"game", "consumer", "manufacturing", "cyclical", "pharma"} <= sectors
    assert "002558" in stock_codes


def test_compare_baseline_snapshots_flags_regressions() -> None:
    baseline = RegressionBaselineSnapshot(
        stock_code="002558",
        stock_name="巨人网络",
        depth="deep",
        generated_at=datetime(2026, 4, 17, 10, 0, 0),
        coverage_ratio=0.82,
        completeness=0.8,
        core_evidence_score=0.84,
        missing_fields=["industry.market_size"],
        warning_count=1,
        initial_verdict="通过",
        final_recommendation="观望",
        quality_gate_blocked=False,
        quality_gate_reasons=[],
    )
    current = RegressionBaselineSnapshot(
        stock_code="002558",
        stock_name="巨人网络",
        depth="deep",
        generated_at=datetime(2026, 4, 18, 10, 0, 0),
        coverage_ratio=0.74,
        completeness=0.71,
        core_evidence_score=0.75,
        missing_fields=["industry.market_size", "governance.related_transaction"],
        blocking_fields=["financials.latest.operating_cashflow"],
        divergent_fields=["operating_cashflow"],
        warning_count=5,
        initial_verdict="通过",
        final_recommendation="观望",
        quality_gate_blocked=True,
        quality_gate_reasons=["经营现金流证据不足"],
    )

    comparison = compare_baseline_snapshots(current, baseline)

    assert comparison["status"] == "regressed"
    assert any("coverage_ratio" in alert for alert in comparison["alerts"])
    assert any("质量闸门由放行退化为阻断" in alert for alert in comparison["alerts"])
    assert comparison["new_blocking_fields"] == ["financials.latest.operating_cashflow"]


@pytest.mark.asyncio
async def test_cli_regression_writes_structured_run_and_comparison(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    basket = [
        {"stock_code": "002558", "stock_name": "巨人网络", "sector": "game", "depth": "deep"},
        {"stock_code": "600519", "stock_name": "贵州茅台", "sector": "consumer", "depth": "deep"},
    ]
    baseline_payload = {
        "samples": [
            {"stock_code": "002558", "baseline_snapshot": _make_report("002558", "巨人网络", coverage_ratio=0.8).baseline_snapshot.model_dump(mode="json")},
            {"stock_code": "600519", "baseline_snapshot": _make_report("600519", "贵州茅台", coverage_ratio=0.8).baseline_snapshot.model_dump(mode="json")},
        ]
    }
    baseline_file = tmp_path / "baseline.json"
    baseline_file.write_text(json.dumps(baseline_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    class FakeCoordinator:
        def __init__(self, progress_callback=None) -> None:
            self.progress_callback = progress_callback

        async def run_research(self, stock_code: str, depth: str = "deep") -> ResearchReport:
            assert depth == "deep"
            if stock_code == "002558":
                return _make_report(stock_code, "巨人网络", coverage_ratio=0.83)
            return _make_report(stock_code, "贵州茅台", coverage_ratio=0.82)

    monkeypatch.setattr(coordinator_module, "ResearchCoordinator", FakeCoordinator)
    monkeypatch.setattr(
        "investresearch.core.regression.get_default_regression_sample_basket",
        lambda: basket,
    )

    args = type(
        "Args",
        (),
        {
            "output_dir": str(tmp_path),
            "baseline_file": str(baseline_file),
            "depth": None,
            "strict": False,
        },
    )()

    await cli_module._run_regression(args)

    latest = json.loads((tmp_path / "latest_regression.json").read_text(encoding="utf-8"))

    assert latest["sample_count"] == 2
    assert latest["comparison_failures"] == 0
    assert latest["missing_snapshot_failures"] == 0
    assert latest["samples"][0]["baseline_snapshot"]["stock_code"] == "002558"
    assert latest["samples"][0]["comparison"]["status"] == "ok"


@pytest.mark.asyncio
async def test_cli_regression_strict_mode_fails_on_missing_baseline_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    basket = [{"stock_code": "300760", "stock_name": "迈瑞医疗", "sector": "pharma", "depth": "deep"}]

    class FakeCoordinator:
        def __init__(self, progress_callback=None) -> None:
            self.progress_callback = progress_callback

        async def run_research(self, stock_code: str, depth: str = "deep") -> ResearchReport:
            assert stock_code == "300760"
            assert depth == "deep"
            report = _make_report(stock_code, "迈瑞医疗", coverage_ratio=0.86)
            report.baseline_snapshot = None
            return report

    monkeypatch.setattr(coordinator_module, "ResearchCoordinator", FakeCoordinator)
    monkeypatch.setattr(
        "investresearch.core.regression.get_default_regression_sample_basket",
        lambda: basket,
    )

    with pytest.raises(RuntimeError, match="baseline_snapshot"):
        args = type(
            "Args",
            (),
            {
                "output_dir": str(tmp_path),
                "baseline_file": None,
                "depth": None,
                "strict": True,
            },
        )()
        await cli_module._run_regression(args)
