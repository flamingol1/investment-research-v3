"""情报中心配置"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class IntelHubConfig:
    """情报中心配置"""
    db_path: str = "data/intel_hub.db"
    chroma_dir: str = "data/chroma"
    raw_data_dir: str = "data/raw"

    # 默认采集配置
    default_start_date_days: int = 365 * 3  # 默认采集3年数据
    rate_limit_interval: float = 0.5  # 请求间隔(秒)
    max_retries: int = 3

    # 归档配置
    archive_per_item: bool = True  # 是否按条目拆分归档

    @classmethod
    def from_yaml(cls, path: str | Path) -> "IntelHubConfig":
        """从 YAML 文件加载配置"""
        p = Path(path)
        if not p.exists():
            return cls()

        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        hub_config = data.get("intel_hub", data)
        return cls(
            db_path=hub_config.get("db_path", cls.db_path),
            chroma_dir=hub_config.get("chroma_dir", cls.chroma_dir),
            raw_data_dir=hub_config.get("raw_data_dir", cls.raw_data_dir),
            default_start_date_days=hub_config.get("default_start_date_days", cls.default_start_date_days),
            rate_limit_interval=hub_config.get("rate_limit_interval", cls.rate_limit_interval),
            max_retries=hub_config.get("max_retries", cls.max_retries),
        )
