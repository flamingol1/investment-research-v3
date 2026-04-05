from .base import DataSourceAdapter, SourceHealth, CollectionResult
from .akshare_adapter import AKShareAdapter
from .baostock_adapter import BaoStockAdapter
from .registry import SourceRegistry

__all__ = [
    "DataSourceAdapter",
    "SourceHealth",
    "CollectionResult",
    "AKShareAdapter",
    "BaoStockAdapter",
    "SourceRegistry",
]
