"""数据源适配器抽象基类"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel


class SourceHealth(BaseModel):
    """数据源健康状态"""
    name: str
    status: str = "unknown"  # healthy/degraded/down/unknown
    latency_ms: int | None = None
    error: str | None = None
    checked_at: datetime = field(default_factory=datetime.now)

    class Config:
        # Allow field default_factory
        pass


class CollectionResult(BaseModel):
    """单次采集结果"""
    target: str
    data_type: str
    source_name: str
    status: str = "success"  # success/partial/failed
    records_fetched: int = 0
    data: dict[str, Any] = {}
    error: str | None = None
    duration_ms: int = 0

    class Config:
        pass


# 支持的采集数据类型
SUPPORTED_DATA_TYPES = [
    "stock_info",       # 股票基础信息
    "daily_prices",     # 历史日线行情
    "realtime_quote",   # 实时行情
    "financials",       # 财务报表
    "valuation",        # 估值数据(PE/PB)
    "announcements",    # 公告披露
    "governance",       # 公司治理
    "research_reports", # 研报摘要
    "shareholders",     # 股东数据
    "industry",         # 行业数据
    "valuation_pct",    # 估值分位
    "news",             # 新闻
    "sentiment",        # 舆情
]


class DataSourceAdapter(ABC):
    """数据源适配器抽象基类

    所有数据源必须实现此接口，统一采集行为。
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """数据源唯一标识"""
        ...

    @property
    @abstractmethod
    def display_name(self) -> str:
        """数据源显示名称"""
        ...

    @property
    def priority(self) -> int:
        """优先级，数字越小越高"""
        return 10

    @abstractmethod
    def get_supported_types(self) -> list[str]:
        """返回此数据源支持的采集类型列表"""
        ...

    @abstractmethod
    def health_check(self) -> SourceHealth:
        """健康检查"""
        ...

    @abstractmethod
    def collect(self, data_type: str, target: str, **kwargs: Any) -> CollectionResult:
        """执行采集

        Args:
            data_type: 数据类型 (stock_info/daily_prices/...)
            target: 采集目标 (股票代码)
            **kwargs: 额外参数 (start_date, end_date, ...)

        Returns:
            CollectionResult 采集结果
        """
        ...

    def supports(self, data_type: str) -> bool:
        """是否支持指定数据类型"""
        return data_type in self.get_supported_types()
