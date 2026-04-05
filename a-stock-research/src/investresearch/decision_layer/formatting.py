"""决策层共享格式化工具"""

from __future__ import annotations

from typing import Any


def fmt_cap(v: Any) -> str:
    """格式化市值"""
    if v is None:
        return "N/A"
    try:
        n = float(v)
        if n >= 1e12:
            return f"{n/1e12:.1f}万亿"
        elif n >= 1e8:
            return f"{n/1e8:.1f}亿"
        else:
            return f"{n/1e4:.1f}万"
    except (ValueError, TypeError):
        return str(v)
