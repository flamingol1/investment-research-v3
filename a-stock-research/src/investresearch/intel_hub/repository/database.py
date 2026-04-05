"""数据库连接与会话管理"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from ..models.db_models import Base

# 默认数据库路径
DEFAULT_DB_PATH = "data/intel_hub.db"


class Database:
    """数据库管理器"""

    def __init__(self, db_url: str | None = None) -> None:
        if db_url is None:
            db_path = Path(DEFAULT_DB_PATH)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_url = f"sqlite:///{db_path}"

        self._engine = create_engine(
            db_url,
            echo=False,
            connect_args={"check_same_thread": False},
        )

        # 启用 SQLite WAL 模式提升并发性能
        @event.listens_for(self._engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, _):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        self._session_factory = sessionmaker(bind=self._engine)

    def create_tables(self) -> None:
        """创建所有表"""
        Base.metadata.create_all(self._engine)

    def get_session(self) -> Session:
        """获取新会话"""
        return self._session_factory()

    def dispose(self) -> None:
        """释放连接池"""
        self._engine.dispose()


# 全局单例
_db: Database | None = None


def init_db(db_url: str | None = None) -> Database:
    """初始化全局数据库实例"""
    global _db
    _db = Database(db_url)
    _db.create_tables()
    return _db


def get_session() -> Session:
    """获取全局数据库会话"""
    if _db is None:
        init_db()
    assert _db is not None
    return _db.get_session()
