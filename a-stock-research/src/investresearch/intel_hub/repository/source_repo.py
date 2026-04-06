"""数据源 CRUD"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models.db_models import IntelSource
from ..models.schemas import SourceCreate, SourceUpdate


class SourceRepository:
    """数据源仓储"""

    def __init__(self, session: Session) -> None:
        self._session = session

    def list_all(self) -> Sequence[IntelSource]:
        """列出所有数据源，按优先级排序"""
        stmt = select(IntelSource).order_by(IntelSource.priority)
        return self._session.execute(stmt).scalars().all()

    def get_by_name(self, name: str) -> IntelSource | None:
        """按名称获取数据源"""
        stmt = select(IntelSource).where(IntelSource.name == name)
        return self._session.execute(stmt).scalar_one_or_none()

    def get_by_id(self, source_id: int) -> IntelSource | None:
        """按 ID 获取数据源"""
        return self._session.get(IntelSource, source_id)

    def get_enabled(self) -> Sequence[IntelSource]:
        """获取所有启用的数据源，按优先级排序"""
        stmt = (
            select(IntelSource)
            .where(IntelSource.enabled.is_(True))
            .order_by(IntelSource.priority)
        )
        return self._session.execute(stmt).scalars().all()

    def create(self, data: SourceCreate) -> IntelSource:
        """创建数据源"""
        source = IntelSource(
            name=data.name,
            display_name=data.display_name,
            description=data.description,
            enabled=data.enabled,
            priority=data.priority,
            config_json=json.dumps(data.config_json, ensure_ascii=False),
        )
        self._session.add(source)
        self._session.flush()
        return source

    def update(self, name: str, data: SourceUpdate) -> IntelSource | None:
        """更新数据源"""
        source = self.get_by_name(name)
        if source is None:
            return None

        if data.display_name is not None:
            source.display_name = data.display_name
        if data.description is not None:
            source.description = data.description
        if data.enabled is not None:
            source.enabled = data.enabled
        if data.priority is not None:
            source.priority = data.priority
        if data.config_json is not None:
            source.config_json = json.dumps(data.config_json, ensure_ascii=False)

        source.updated_at = datetime.now()
        self._session.flush()
        return source

    def delete(self, name: str) -> bool:
        """删除数据源"""
        source = self.get_by_name(name)
        if source is None:
            return False
        self._session.delete(source)
        self._session.flush()
        return True

    def update_health(self, name: str, status: str, error: str = "") -> None:
        """更新数据源健康状态"""
        source = self.get_by_name(name)
        if source is None:
            return
        source.health_status = status
        source.last_health_check = datetime.now()
        source.last_error = error
        self._session.flush()
