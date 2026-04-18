from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_refresh_module():
    root = Path(__file__).resolve().parents[1]
    script_path = root / "scripts" / "refresh_historical_reports.py"
    spec = importlib.util.spec_from_file_location("refresh_historical_reports", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_normalize_chart_payload_repairs_legacy_scenario_schema() -> None:
    module = _load_refresh_module()

    payload = [
        {"chart_id": "peer_placeholder", "chart_type": "bar", "series": []},
        {
            "chart_id": "scenario_analysis",
            "chart_type": "bar",
            "series": [
                {
                    "name": "情景目标价",
                    "points": [
                        {"x": "乐观情景", "y": 28.0},
                        {"x": "中性情景", "y": 25.4},
                        {"x": "悲观情景", "y": 19.2},
                    ],
                }
            ],
        },
        {
            "chart_id": "financial_trend",
            "chart_type": "line",
            "series": [
                {
                    "name": "营收",
                    "points": [
                        {"x": "2025-06-30", "y": 100.0},
                        {"x": "2025-09-30", "y": None},
                    ],
                }
            ],
        },
    ]

    normalized, changed = module._normalize_chart_payload(payload)

    assert changed is True
    assert len(normalized) == 2
    scenario_chart = next(item for item in normalized if item["chart_id"] == "scenario_analysis")
    assert [item["name"] for item in scenario_chart["series"]] == ["乐观", "中性", "悲观"]
    financial_chart = next(item for item in normalized if item["chart_id"] == "financial_trend")
    assert financial_chart["series"][0]["points"] == [{"x": "2025-06-30", "y": 100.0}]


def test_normalize_markdown_payload_strips_debug_sections_and_injects_chart_markers() -> None:
    module = _load_refresh_module()

    markdown = """# 示例报告

> 说明: 本次报告生成阶段出现模型限流或响应异常，以下内容基于已完成的结构化分析结果自动整理。

## 证据闸门与字段约束
- 闸门状态: 阻断
- 核心证据分: 69%

## 行业赛道分析

结论：行业仍待验证。

论据：
- 行业景气一般

待验证项：
- top_competitors

缺失字段：
- cr5

数据来源：行业分析 Agent 输出

## 财务质量深度核查

结论：财务趋势仍需复核。

论据：
- 营收存在波动

数据来源：财务分析 Agent 输出

## 证据包摘要

以下资料可直接作为前端证据视图和人工复核入口：
"""

    chart_payload = [
        {"chart_id": "industry_prosperity", "series": [{"name": "行业指标", "points": [{"x": "1", "y": "景气平稳"}]}]},
        {"chart_id": "financial_trend", "series": [{"name": "营收", "points": [{"x": "2025-06-30", "y": 100.0}]}]},
    ]

    normalized, changed = module._normalize_markdown_payload(markdown, chart_payload)

    assert changed is True
    assert "## 证据闸门与字段约束" not in normalized
    assert "待验证项：" not in normalized
    assert "缺失字段：" not in normalized
    assert "## 研究边界与待补证据" in normalized
    assert ":::charts industry_prosperity:::" in normalized
    assert ":::charts financial_trend:::" in normalized
