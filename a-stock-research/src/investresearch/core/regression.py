"""Regression sample basket and baseline comparison helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import RegressionBaselineSnapshot

DEFAULT_REGRESSION_SAMPLE_BASKET: list[dict[str, str]] = [
    {
        "stock_code": "002558",
        "stock_name": "巨人网络",
        "sector": "game",
        "depth": "deep",
        "label": "游戏",
    },
    {
        "stock_code": "600519",
        "stock_name": "贵州茅台",
        "sector": "consumer",
        "depth": "deep",
        "label": "消费",
    },
    {
        "stock_code": "300750",
        "stock_name": "宁德时代",
        "sector": "manufacturing",
        "depth": "deep",
        "label": "制造",
    },
    {
        "stock_code": "601899",
        "stock_name": "紫金矿业",
        "sector": "cyclical",
        "depth": "deep",
        "label": "周期",
    },
    {
        "stock_code": "300760",
        "stock_name": "迈瑞医疗",
        "sector": "pharma",
        "depth": "deep",
        "label": "医药",
    },
]

DEFAULT_DROP_TOLERANCES: dict[str, float] = {
    "coverage_ratio": 0.03,
    "completeness": 0.03,
    "core_evidence_score": 0.04,
}
DEFAULT_WARNING_TOLERANCE = 2


def get_default_regression_sample_basket() -> list[dict[str, str]]:
    """Return a copy of the built-in regression basket."""
    return [dict(item) for item in DEFAULT_REGRESSION_SAMPLE_BASKET]


def normalize_baseline_snapshot(
    snapshot: RegressionBaselineSnapshot | dict[str, Any] | None,
) -> dict[str, Any]:
    """Convert a baseline snapshot into a plain dict."""
    if snapshot is None:
        return {}
    if isinstance(snapshot, RegressionBaselineSnapshot):
        return snapshot.model_dump(mode="json")
    return dict(snapshot)


def index_regression_baseline_payload(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Index a saved regression payload by stock code."""
    indexed: dict[str, dict[str, Any]] = {}
    for item in list((payload or {}).get("samples", []) or []):
        if not isinstance(item, dict):
            continue
        stock_code = str(item.get("stock_code") or "").strip()
        snapshot = item.get("baseline_snapshot")
        if not stock_code or not isinstance(snapshot, dict):
            continue
        indexed[stock_code] = snapshot
    return indexed


def load_regression_baseline_file(path: str | Path) -> dict[str, dict[str, Any]]:
    """Load a saved regression run file into an indexed baseline map."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    return index_regression_baseline_payload(payload)


def compare_baseline_snapshots(
    current: RegressionBaselineSnapshot | dict[str, Any] | None,
    baseline: RegressionBaselineSnapshot | dict[str, Any] | None,
    *,
    tolerances: dict[str, float] | None = None,
    warning_tolerance: int = DEFAULT_WARNING_TOLERANCE,
) -> dict[str, Any]:
    """Compare two structured baseline snapshots and flag regressions."""
    current_data = normalize_baseline_snapshot(current)
    baseline_data = normalize_baseline_snapshot(baseline)

    if not current_data:
        return {
            "status": "missing_current",
            "alerts": ["当前运行未产出 baseline_snapshot。"],
            "metric_deltas": {},
        }
    if not baseline_data:
        return {
            "status": "missing_baseline",
            "alerts": ["未找到可对比的基线快照。"],
            "metric_deltas": {},
        }

    merged_tolerances = dict(DEFAULT_DROP_TOLERANCES)
    merged_tolerances.update(tolerances or {})

    alerts: list[str] = []
    metric_deltas: dict[str, float] = {}
    for metric_name, allowed_drop in merged_tolerances.items():
        current_value = float(current_data.get(metric_name, 0.0) or 0.0)
        baseline_value = float(baseline_data.get(metric_name, 0.0) or 0.0)
        delta = round(current_value - baseline_value, 4)
        metric_deltas[metric_name] = delta
        if delta < -abs(allowed_drop):
            alerts.append(
                f"{metric_name} 从 {baseline_value:.0%} 下降到 {current_value:.0%}，超过允许回撤 {allowed_drop:.0%}。"
            )

    current_warning = int(current_data.get("warning_count") or 0)
    baseline_warning = int(baseline_data.get("warning_count") or 0)
    warning_delta = current_warning - baseline_warning
    metric_deltas["warning_count"] = float(warning_delta)
    if warning_delta > warning_tolerance:
        alerts.append(
            f"warning_count 从 {baseline_warning} 增加到 {current_warning}，超过允许增量 {warning_tolerance}。"
        )

    if not bool(baseline_data.get("quality_gate_blocked")) and bool(current_data.get("quality_gate_blocked")):
        alerts.append("质量闸门由放行退化为阻断。")

    new_missing = sorted(set(current_data.get("missing_fields", []) or []) - set(baseline_data.get("missing_fields", []) or []))
    new_blocking = sorted(set(current_data.get("blocking_fields", []) or []) - set(baseline_data.get("blocking_fields", []) or []))
    new_divergent = sorted(set(current_data.get("divergent_fields", []) or []) - set(baseline_data.get("divergent_fields", []) or []))
    if new_missing:
        alerts.append(f"新增缺失字段: {', '.join(new_missing[:6])}")
    if new_blocking:
        alerts.append(f"新增阻断字段: {', '.join(new_blocking[:6])}")
    if new_divergent:
        alerts.append(f"新增交叉验证分歧字段: {', '.join(new_divergent[:6])}")

    recommendation_changed = (
        str(current_data.get("final_recommendation") or "").strip()
        != str(baseline_data.get("final_recommendation") or "").strip()
    )

    status = "regressed" if alerts else "ok"
    return {
        "status": status,
        "alerts": alerts,
        "metric_deltas": metric_deltas,
        "recommendation_changed": recommendation_changed,
        "current_recommendation": str(current_data.get("final_recommendation") or ""),
        "baseline_recommendation": str(baseline_data.get("final_recommendation") or ""),
        "new_missing_fields": new_missing,
        "new_blocking_fields": new_blocking,
        "new_divergent_fields": new_divergent,
    }
