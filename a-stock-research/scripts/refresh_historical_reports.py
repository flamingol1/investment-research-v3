from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from investresearch.cli import _run_research  # noqa: E402

OUTPUT_ROOT = ROOT / "output"
CANONICAL_REPORTS_DIR = OUTPUT_ROOT / "reports"
AUDIT_DIR = OUTPUT_ROOT / "report_audit"
DEPTH_ORDER = {"quick": 0, "standard": 1, "deep": 2}
SKIP_DIR_NAMES = {"regression_smoke", "report_audit"}
DATED_META_PATTERN = re.compile(r".+_\d{8}_meta\.json$")
CHART_MARKER_RE = re.compile(r"^:::\s*charts?\s+[a-zA-Z0-9_,\s-]+\s*:::$", re.MULTILINE)
LEGACY_FALLBACK_MARKER = "本次报告生成阶段出现模型限流或响应异常"
SECTION_CHART_FALLBACKS = [
    ("## 行业赛道分析", ["industry_prosperity", "peer_comparison"]),
    ("## 财务质量深度核查", ["financial_trend", "cashflow_compare"]),
    ("## 估值定价与预期差分析", ["valuation_percentile"]),
    ("## 风险识别与情景分析", ["scenario_analysis"]),
]


@dataclass
class HistoricalRecord:
    stock_code: str
    stock_name: str
    report_date: str
    depth: str
    directory: str
    markdown_path: Path
    conclusion_path: Path | None
    chart_path: Path | None
    evidence_path: Path | None
    meta_path: Path
    evidence_count: int
    chart_count: int
    peer_placeholder: bool
    has_markdown: bool
    has_conclusion: bool
    gate_blocked: bool
    legacy_fallback_markdown: bool
    legacy_scenario_schema: bool
    recommendation: str
    confidence_level: str
    errors: list[str]


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _iter_meta_files(output_root: Path) -> list[Path]:
    files: list[Path] = []
    for path in output_root.rglob("*_meta.json"):
        if any(part in SKIP_DIR_NAMES for part in path.parts):
            continue
        if not DATED_META_PATTERN.fullmatch(path.name):
            continue
        files.append(path)
    return sorted(files)


def _resolve_companion(parent: Path, dated_stem: str, latest_stem: str, suffix: str) -> Path | None:
    dated = parent / f"{dated_stem}_{suffix}"
    if dated.exists():
        return dated
    latest = parent / f"{latest_stem}_{suffix}"
    if latest.exists():
        return latest
    return None


def _scan_records(output_root: Path) -> list[HistoricalRecord]:
    records: list[HistoricalRecord] = []
    for meta_path in _iter_meta_files(output_root):
        payload = _load_json(meta_path)
        stock_code = str(payload.get("stock_code") or "").strip()
        report_date = str(payload.get("report_date") or "").strip()
        if not stock_code or not report_date:
            continue

        stock_name = str(payload.get("stock_name") or "").strip()
        depth = str(payload.get("depth") or "standard").strip() or "standard"
        parent = meta_path.parent
        stem = f"{stock_code}_{report_date}"

        markdown_path = parent / f"{stem}.md"
        conclusion_path = _resolve_companion(parent, stem, stock_code, "conclusion.json")
        chart_path = _resolve_companion(parent, stem, stock_code, "chart_pack.json")
        evidence_path = _resolve_companion(parent, stem, stock_code, "evidence_pack.json")

        conclusion_payload = _load_json(conclusion_path) if conclusion_path else {}
        chart_payload = _load_json(chart_path) if chart_path else []
        evidence_payload = _load_json(evidence_path) if evidence_path else []
        quality_gate = payload.get("quality_gate") or {}
        markdown_payload = _load_text(markdown_path)

        records.append(
            HistoricalRecord(
                stock_code=stock_code,
                stock_name=stock_name,
                report_date=report_date,
                depth=depth,
                directory=parent.name,
                markdown_path=markdown_path,
                conclusion_path=conclusion_path,
                chart_path=chart_path,
                evidence_path=evidence_path,
                meta_path=meta_path,
                evidence_count=len(evidence_payload) if isinstance(evidence_payload, list) else 0,
                chart_count=len(chart_payload) if isinstance(chart_payload, list) else 0,
                peer_placeholder=any(
                    isinstance(item, dict) and item.get("chart_id") == "peer_placeholder"
                    for item in chart_payload
                )
                if isinstance(chart_payload, list)
                else False,
                has_markdown=markdown_path.exists(),
                has_conclusion=conclusion_path is not None and conclusion_path.exists(),
                gate_blocked=bool(quality_gate.get("blocked", False)),
                legacy_fallback_markdown=LEGACY_FALLBACK_MARKER in markdown_payload,
                legacy_scenario_schema=_has_legacy_scenario_schema(chart_payload),
                recommendation=str((conclusion_payload or {}).get("recommendation") or ""),
                confidence_level=str((conclusion_payload or {}).get("confidence_level") or ""),
                errors=[str(item) for item in list(payload.get("errors", []) or [])],
            )
        )
    return records


def _record_findings(record: HistoricalRecord) -> list[str]:
    findings: list[str] = []
    if not record.has_markdown:
        findings.append("missing_markdown")
    if not record.has_conclusion:
        findings.append("missing_conclusion")
    if record.evidence_count == 0:
        findings.append("zero_evidence_pack")
    if record.chart_count == 0:
        findings.append("zero_chart_pack")
    if record.peer_placeholder:
        findings.append("peer_placeholder")
    if record.legacy_fallback_markdown:
        findings.append("legacy_fallback_markdown")
    if record.legacy_scenario_schema:
        findings.append("legacy_scenario_schema")
    if record.gate_blocked:
        findings.append("quality_gate_blocked")
    if not record.recommendation:
        findings.append("missing_recommendation")
    return findings


def _pick_targets(records: list[HistoricalRecord], stocks: set[str] | None = None) -> list[dict[str, str]]:
    grouped: dict[str, list[HistoricalRecord]] = defaultdict(list)
    for record in records:
        if stocks and record.stock_code not in stocks:
            continue
        grouped[record.stock_code].append(record)

    targets: list[dict[str, str]] = []
    for stock_code, items in sorted(grouped.items()):
        preferred = max(
            items,
            key=lambda item: (
                DEPTH_ORDER.get(item.depth, DEPTH_ORDER["standard"]),
                item.report_date,
                item.directory == "reports",
            ),
        )
        targets.append(
            {
                "stock_code": stock_code,
                "stock_name": preferred.stock_name,
                "depth": preferred.depth,
                "source_directory": preferred.directory,
                "source_report_date": preferred.report_date,
            }
        )
    return targets


def _build_audit_payload(records: list[HistoricalRecord], targets: list[dict[str, str]]) -> dict[str, Any]:
    grouped: dict[str, list[HistoricalRecord]] = defaultdict(list)
    for record in records:
        grouped[record.stock_code].append(record)

    stock_summaries: list[dict[str, Any]] = []
    for stock_code, items in sorted(grouped.items()):
        stock_summaries.append(
            {
                "stock_code": stock_code,
                "stock_name": items[0].stock_name,
                "versions": len(items),
                "directories": sorted({item.directory for item in items}),
                "findings": sorted({finding for item in items for finding in _record_findings(item)}),
                "records": [
                    {
                        "report_date": item.report_date,
                        "depth": item.depth,
                        "directory": item.directory,
                        "evidence_count": item.evidence_count,
                        "chart_count": item.chart_count,
                        "peer_placeholder": item.peer_placeholder,
                        "gate_blocked": item.gate_blocked,
                        "recommendation": item.recommendation,
                        "confidence_level": item.confidence_level,
                        "findings": _record_findings(item),
                        "meta_path": str(item.meta_path),
                    }
                    for item in sorted(items, key=lambda value: (value.report_date, value.directory))
                ],
            }
        )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "canonical_reports_dir": str(CANONICAL_REPORTS_DIR),
        "target_count": len(targets),
        "targets": targets,
        "stocks": stock_summaries,
    }


def _write_audit(payload: dict[str, Any], audit_dir: Path) -> Path:
    audit_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = audit_dir / f"history_audit_{timestamp}.json"
    latest = audit_dir / "latest_history_audit.json"
    content = json.dumps(payload, ensure_ascii=False, indent=2)
    target.write_text(content, encoding="utf-8")
    latest.write_text(content, encoding="utf-8")
    return target


def _has_legacy_scenario_schema(chart_payload: Any) -> bool:
    if not isinstance(chart_payload, list):
        return False
    for item in chart_payload:
        if not isinstance(item, dict) or item.get("chart_id") != "scenario_analysis":
            continue
        series = item.get("series") or []
        if len(series) == 1 and isinstance(series[0], dict) and len(series[0].get("points") or []) > 1:
            return True
    return False


def _artifact_paths(record: HistoricalRecord, suffix: str) -> list[Path]:
    parent = record.meta_path.parent
    dated_stem = f"{record.stock_code}_{record.report_date}"
    candidates = [parent / f"{dated_stem}_{suffix}", parent / f"{record.stock_code}_{suffix}"]
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in candidates:
        if path.exists() and path not in seen:
            unique.append(path)
            seen.add(path)
    return unique


def _safe_number(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric == numeric and numeric not in (float("inf"), float("-inf")) else None


def _normalize_chart_payload(chart_payload: Any) -> tuple[Any, bool]:
    if not isinstance(chart_payload, list):
        return chart_payload, False

    changed = False
    normalized_payload: list[dict[str, Any]] = []
    for item in chart_payload:
        if not isinstance(item, dict):
            normalized_payload.append(item)
            continue

        if item.get("chart_id") == "peer_placeholder":
            changed = True
            continue

        normalized = dict(item)
        series = list(normalized.get("series") or [])

        if normalized.get("chart_id") == "scenario_analysis":
            if len(series) == 1 and isinstance(series[0], dict) and len(series[0].get("points") or []) > 1:
                rebuilt_series = []
                for point in series[0].get("points") or []:
                    if not isinstance(point, dict):
                        continue
                    label = str(point.get("x") or "").replace("情景", "").strip()
                    value = _safe_number(point.get("y"))
                    if not label or value is None:
                        continue
                    rebuilt_series.append({"name": label, "points": [{"x": label, "y": value}]})
                normalized["series"] = rebuilt_series
                changed = True
            else:
                rebuilt_series = []
                for entry in series:
                    if not isinstance(entry, dict):
                        continue
                    name = str(entry.get("name") or "").replace("情景", "").strip()
                    points = []
                    for point in entry.get("points") or []:
                        if not isinstance(point, dict):
                            continue
                        label = str(point.get("x") or name).replace("情景", "").strip()
                        value = _safe_number(point.get("y"))
                        if not label or value is None:
                            continue
                        points.append({"x": label, "y": value})
                    if not points:
                        continue
                    rebuilt_series.append({"name": name or str(points[0]["x"]), "points": [points[0]]})
                if rebuilt_series != series:
                    normalized["series"] = rebuilt_series
                    changed = True
        elif normalized.get("chart_type") in {"line", "bar", "list"}:
            rebuilt_series = []
            for entry in series:
                if not isinstance(entry, dict):
                    continue
                points = []
                for point in entry.get("points") or []:
                    if not isinstance(point, dict):
                        continue
                    label = str(point.get("x") or "").strip()
                    value = point.get("y")
                    if normalized.get("chart_type") in {"line", "bar"}:
                        numeric = _safe_number(value)
                        if not label or numeric is None:
                            continue
                        points.append({"x": label, "y": numeric})
                    else:
                        if not label or value in (None, ""):
                            continue
                        points.append({"x": label, "y": value})
                if not points:
                    changed = True
                    continue
                rebuilt_entry = dict(entry)
                rebuilt_entry["points"] = points
                rebuilt_series.append(rebuilt_entry)
            if rebuilt_series != series:
                normalized["series"] = rebuilt_series
                changed = True

        normalized_payload.append(normalized)

    return normalized_payload, changed


def _inject_chart_markers(markdown: str, chart_payload: Any) -> str:
    if not markdown.strip() or CHART_MARKER_RE.search(markdown):
        return markdown

    if not isinstance(chart_payload, list):
        return markdown

    available_chart_ids = {
        str(item.get("chart_id"))
        for item in chart_payload
        if isinstance(item, dict)
        and any(isinstance(series, dict) and len(series.get("points") or []) > 0 for series in item.get("series") or [])
    }

    next_markdown = markdown
    for heading, chart_ids in SECTION_CHART_FALLBACKS:
        selected = [chart_id for chart_id in chart_ids if chart_id in available_chart_ids]
        if not selected or heading not in next_markdown:
            continue
        next_markdown = next_markdown.replace(heading, f"{heading}\n\n:::charts {','.join(selected)}:::")
    return next_markdown


def _normalize_markdown_payload(markdown: str, chart_payload: Any) -> tuple[str, bool]:
    if not markdown.strip():
        return markdown, False

    normalized = markdown
    if LEGACY_FALLBACK_MARKER in normalized:
        normalized = re.sub(r"\n待验证项：\n(?:- .*\n)+", "\n", normalized)
        normalized = re.sub(r"\n缺失字段：\n(?:- .*\n)+", "\n", normalized)
        normalized = re.sub(r"^## 证据闸门与字段约束[\s\S]*?(?=^## )", "", normalized, flags=re.MULTILINE)
        normalized = normalized.replace("## 证据包摘要", "## 研究边界与待补证据")
        normalized = normalized.replace("## 图表包摘要", "## 证据来源与复核入口")
    normalized = _inject_chart_markers(normalized, chart_payload)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip() + "\n"
    return normalized, normalized != markdown


def _migrate_record(record: HistoricalRecord) -> dict[str, str]:
    markdown_changed = False
    chart_changed = False

    chart_payload = _load_json(record.chart_path) if record.chart_path and record.chart_path.exists() else []
    normalized_chart_payload, chart_changed = _normalize_chart_payload(chart_payload)
    if chart_changed:
        chart_content = json.dumps(normalized_chart_payload, ensure_ascii=False, indent=2)
        for path in _artifact_paths(record, "chart_pack.json"):
            path.write_text(chart_content, encoding="utf-8")

    if record.markdown_path.exists():
        markdown = _load_text(record.markdown_path)
        normalized_markdown, markdown_changed = _normalize_markdown_payload(markdown, normalized_chart_payload)
        if markdown_changed:
            record.markdown_path.write_text(normalized_markdown, encoding="utf-8")

    meta_payload = _load_json(record.meta_path)
    if isinstance(meta_payload, dict):
        meta_changed = False
        if chart_changed and isinstance(normalized_chart_payload, list):
            chart_count = len(normalized_chart_payload)
            if meta_payload.get("chart_pack_count") != chart_count:
                meta_payload["chart_pack_count"] = chart_count
                meta_changed = True
        if markdown_changed or chart_changed:
            meta_payload["artifact_normalized_at"] = datetime.now().isoformat(timespec="seconds")
            meta_changed = True
        if meta_changed:
            content = json.dumps(meta_payload, ensure_ascii=False, indent=2)
            for path in _artifact_paths(record, "meta.json"):
                path.write_text(content, encoding="utf-8")

    status = "unchanged"
    if markdown_changed and chart_changed:
        status = "markdown+chart"
    elif markdown_changed:
        status = "markdown"
    elif chart_changed:
        status = "chart"

    return {"stock_code": record.stock_code, "report_date": record.report_date, "status": status}


def _migrate_records(records: list[HistoricalRecord]) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for record in records:
        results.append(_migrate_record(record))
    return results


async def _refresh_targets(targets: list[dict[str, str]], output_dir: Path) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for target in targets:
        stock_code = target["stock_code"]
        depth = target["depth"]
        try:
            await _run_research(stock_code, depth, str(output_dir))
            results.append({"stock_code": stock_code, "depth": depth, "status": "success"})
        except Exception as exc:  # pragma: no cover - runtime safeguard
            results.append({"stock_code": stock_code, "depth": depth, "status": f"failed: {exc}"})
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and optionally refresh historical research reports.")
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="Normalize existing historical markdown/chart artifacts in place without rerunning research.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Rerun the chosen historical stock reports into the canonical output/reports directory.",
    )
    parser.add_argument(
        "--stocks",
        nargs="*",
        help="Optional stock codes to limit the audit or refresh scope.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(CANONICAL_REPORTS_DIR),
        help="Canonical report output directory.",
    )
    parser.add_argument(
        "--audit-dir",
        default=str(AUDIT_DIR),
        help="Directory for generated audit JSON files.",
    )
    return parser.parse_args()


async def _main() -> int:
    args = _parse_args()
    stock_filter = {str(item).strip() for item in list(args.stocks or []) if str(item).strip()} or None

    records = _scan_records(OUTPUT_ROOT)
    filtered_records = [record for record in records if not stock_filter or record.stock_code in stock_filter]
    targets = _pick_targets(records, stocks=stock_filter)
    audit_payload = _build_audit_payload(filtered_records, targets)
    audit_path = _write_audit(audit_payload, Path(args.audit_dir))

    print(f"[audit] wrote {audit_path}")
    for stock in audit_payload["stocks"]:
        findings = ",".join(stock["findings"]) if stock["findings"] else "clean"
        print(f"[audit] {stock['stock_code']} {stock['stock_name']} | versions={stock['versions']} | findings={findings}")

    if args.migrate:
        print(f"[migrate] normalizing {len(filtered_records)} historical record(s)")
        migration_results = _migrate_records(filtered_records)
        for item in migration_results:
            print(f"[migrate] {item['stock_code']} {item['report_date']} -> {item['status']}")

    if not args.apply:
        return 0

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[refresh] refreshing {len(targets)} stock(s) into {output_dir}")
    refresh_results = await _refresh_targets(targets, output_dir)
    for item in refresh_results:
        print(f"[refresh] {item['stock_code']} depth={item['depth']} -> {item['status']}")

    return 0 if all(item["status"] == "success" for item in refresh_results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
