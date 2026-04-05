"""跟踪列表管理 - JSON文件持久化

管理用户关注的标的列表，支持增删查改。
数据格式: JSON文件，路径由配置决定。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from investresearch.core.config import Config
from investresearch.core.logging import get_logger
from investresearch.core.models import (
    UpdateFrequency,
    WatchList,
    WatchListItem,
)

logger = get_logger("knowledge_base.watch_list")


class WatchListManager:
    """跟踪列表管理器

    用法:
        mgr = WatchListManager()
        mgr.add("300358", "湖南裕能", recommendation="买入(谨慎)")
        items = mgr.get_all()
        mgr.save()
    """

    def __init__(self, file_path: str | None = None) -> None:
        self._config = Config()
        self._file_path = Path(file_path or self._config.get_watch_list_path())
        self._watch_list = self._load()

    def _load(self) -> WatchList:
        """从JSON文件加载跟踪列表"""
        if not self._file_path.exists():
            return WatchList()

        try:
            data = json.loads(self._file_path.read_text(encoding="utf-8"))
            return WatchList(**data)
        except Exception as e:
            logger.warning(f"跟踪列表文件损坏，将创建新列表: {e}")
            # 备份损坏文件
            backup = self._file_path.with_suffix(".json.bak")
            if self._file_path.exists():
                self._file_path.rename(backup)
                logger.info(f"已备份损坏文件到: {backup}")
            return WatchList()

    def save(self) -> None:
        """持久化到JSON文件"""
        self._watch_list.updated_at = datetime.now()
        self._file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            data = self._watch_list.model_dump(mode="json")
            self._file_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            logger.info(f"跟踪列表已保存 | {len(self._watch_list.items)}个标的")
        except Exception as e:
            logger.error(f"跟踪列表保存失败: {e}")

    # ================================================================
    # CRUD 操作
    # ================================================================

    def add(
        self,
        stock_code: str,
        stock_name: str = "",
        recommendation: str = "",
        monitoring_points: list[str] | None = None,
        thresholds: dict[str, Any] | None = None,
        update_frequency: UpdateFrequency = UpdateFrequency.WEEKLY,
    ) -> bool:
        """添加标的到跟踪列表

        Returns:
            True: 添加成功  False: 已存在
        """
        # 检查是否已存在
        if self.get_item(stock_code) is not None:
            logger.info(f"标的 {stock_code} 已在跟踪列表中")
            return False

        item = WatchListItem(
            stock_code=stock_code,
            stock_name=stock_name,
            recommendation=recommendation,
            added_at=datetime.now(),
            update_frequency=update_frequency,
            monitoring_points=monitoring_points or [],
            alert_thresholds=thresholds or {},
        )

        self._watch_list.items.append(item)
        self.save()
        logger.info(f"已添加 {stock_code} {stock_name} 到跟踪列表")
        return True

    def remove(self, stock_code: str) -> bool:
        """从跟踪列表移除标的"""
        original_len = len(self._watch_list.items)
        self._watch_list.items = [
            item for item in self._watch_list.items
            if item.stock_code != stock_code
        ]

        if len(self._watch_list.items) < original_len:
            self.save()
            logger.info(f"已从跟踪列表移除 {stock_code}")
            return True

        logger.info(f"标的 {stock_code} 不在跟踪列表中")
        return False

    def get_all(self) -> WatchList:
        """获取完整跟踪列表（返回副本，修改需通过 add/remove/save）"""
        return self._watch_list.model_copy(deep=True)

    def get_item(self, stock_code: str) -> WatchListItem | None:
        """获取单个标的"""
        for item in self._watch_list.items:
            if item.stock_code == stock_code:
                return item
        return None

    def update_status(self, stock_code: str, status: str) -> None:
        """更新标的跟踪状态"""
        item = self.get_item(stock_code)
        if item:
            item.status = status

    def update_last_checked(self, stock_code: str) -> None:
        """更新最后检查时间"""
        item = self.get_item(stock_code)
        if item:
            item.last_updated_at = datetime.now()

    def update_recommendation(self, stock_code: str, recommendation: str) -> None:
        """更新最近投资建议"""
        item = self.get_item(stock_code)
        if item:
            item.recommendation = recommendation

    def update_report_date(self, stock_code: str, report_date: datetime) -> None:
        """更新最后报告日期"""
        item = self.get_item(stock_code)
        if item:
            item.last_report_date = report_date
