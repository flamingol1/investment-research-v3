"""数据源注册中心 - 按优先级选择最优数据源"""

from __future__ import annotations

from typing import Sequence

from .base import DataSourceAdapter, SourceHealth, CollectionResult


class SourceRegistry:
    """数据源注册中心

    管理所有已注册的数据源适配器，按优先级排序，
    支持按数据类型自动选择最优数据源。
    """

    def __init__(self) -> None:
        self._adapters: dict[str, DataSourceAdapter] = {}

    def register(self, adapter: DataSourceAdapter) -> None:
        """注册数据源适配器"""
        self._adapters[adapter.name] = adapter

    def unregister(self, name: str) -> None:
        """注销数据源"""
        self._adapters.pop(name, None)

    def get(self, name: str) -> DataSourceAdapter | None:
        """按名称获取适配器"""
        return self._adapters.get(name)

    def list_all(self) -> list[DataSourceAdapter]:
        """列出所有适配器，按优先级排序"""
        return sorted(self._adapters.values(), key=lambda a: a.priority)

    def list_supporting(self, data_type: str) -> list[DataSourceAdapter]:
        """获取支持指定数据类型的适配器，按优先级排序"""
        return [
            a for a in self.list_all()
            if a.supports(data_type)
        ]

    def select_best(self, data_type: str) -> DataSourceAdapter | None:
        """选择支持指定数据类型的最优数据源"""
        adapters = self.list_supporting(data_type)
        return adapters[0] if adapters else None

    def collect_with_fallback(
        self,
        data_type: str,
        target: str,
        preferred_source: str | None = None,
        **kwargs,
    ) -> CollectionResult:
        """带备源回退的采集

        优先使用指定数据源，失败后按优先级尝试备源。
        """
        # 如果指定了数据源，优先尝试
        if preferred_source:
            adapter = self.get(preferred_source)
            if adapter and adapter.supports(data_type):
                result = adapter.collect(data_type, target, **kwargs)
                if result.status in ("success", "partial"):
                    return result

        # 按优先级尝试所有支持的数据源
        for adapter in self.list_supporting(data_type):
            if preferred_source and adapter.name == preferred_source:
                continue  # 已经试过了
            result = adapter.collect(data_type, target, **kwargs)
            if result.status in ("success", "partial"):
                return result

        return CollectionResult(
            target=target,
            data_type=data_type,
            source_name=preferred_source or "none",
            status="failed",
            error=f"所有数据源均无法采集 {data_type} 数据",
        )

    def health_check_all(self) -> dict[str, SourceHealth]:
        """对所有数据源执行健康检查"""
        results: dict[str, SourceHealth] = {}
        for name, adapter in self._adapters.items():
            results[name] = adapter.health_check()
        return results

    def create_default_registry() -> "SourceRegistry":
        """创建包含默认数据源的注册中心"""
        registry = SourceRegistry()

        # 注册 AKShare (主源)
        from .akshare_adapter import AKShareAdapter
        registry.register(AKShareAdapter())

        # 注册 BaoStock (备源)
        from .baostock_adapter import BaoStockAdapter
        registry.register(BaoStockAdapter())

        return registry
