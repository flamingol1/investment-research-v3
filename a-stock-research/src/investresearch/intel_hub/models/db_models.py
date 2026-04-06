"""SQLAlchemy ORM 模型 - 情报中心数据库表"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    Index,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    """ORM 基类"""
    pass


class IntelSource(Base):
    """数据源配置表"""
    __tablename__ = "intel_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), unique=True, nullable=False, comment="数据源标识")
    display_name = Column(String(128), nullable=False, comment="显示名称")
    description = Column(Text, default="", comment="描述")
    enabled = Column(Boolean, default=True, comment="是否启用")
    priority = Column(Integer, default=1, comment="优先级(1=最高)")
    config_json = Column(Text, default="{}", comment="JSON格式的数据源特有配置")
    health_status = Column(String(32), default="unknown", comment="healthy/degraded/down/unknown")
    last_health_check = Column(DateTime, nullable=True, comment="上次健康检查时间")
    last_error = Column(Text, default="", comment="最近一次错误信息")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 关联
    collection_logs = relationship("CollectionLog", back_populates="source", lazy="dynamic")
    archives = relationship("IntelArchive", back_populates="source", lazy="dynamic")

    def __repr__(self) -> str:
        return f"<IntelSource {self.name} priority={self.priority} enabled={self.enabled}>"


class CollectionTask(Base):
    """采集任务表"""
    __tablename__ = "intel_collection_tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False, comment="任务名称")
    task_type = Column(String(64), nullable=False, comment="采集类型: stock_info/price/financial/...")
    target = Column(String(64), nullable=False, comment="采集目标(股票代码/行业代码)")
    schedule_type = Column(String(32), default="manual", comment="cron/interval/manual/once")
    schedule_expr = Column(String(256), default="", comment="cron表达式或间隔秒数")
    enabled = Column(Boolean, default=True, comment="是否启用")
    source_id = Column(Integer, ForeignKey("intel_sources.id"), nullable=True, comment="指定数据源(null=自动)")
    status = Column(String(32), default="idle", comment="idle/running/success/failed")
    last_run_at = Column(DateTime, nullable=True)
    next_run_at = Column(DateTime, nullable=True)
    success_count = Column(Integer, default=0)
    fail_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 关联
    source = relationship("IntelSource")
    logs = relationship("CollectionLog", back_populates="task", lazy="dynamic")

    def __repr__(self) -> str:
        return f"<CollectionTask {self.name} type={self.task_type} target={self.target}>"


class CollectionLog(Base):
    """采集执行日志表"""
    __tablename__ = "intel_collection_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, ForeignKey("intel_collection_tasks.id"), nullable=True)
    source_id = Column(Integer, ForeignKey("intel_sources.id"), nullable=True)
    source_name = Column(String(64), comment="实际使用的数据源")
    target = Column(String(64), comment="采集目标")
    data_type = Column(String(64), comment="数据类型")
    status = Column(String(32), nullable=False, comment="success/partial/failed")
    records_fetched = Column(Integer, default=0, comment="获取记录数")
    records_stored = Column(Integer, default=0, comment="存储记录数")
    error_message = Column(Text, default="", comment="错误信息")
    duration_ms = Column(Integer, default=0, comment="耗时(ms)")
    raw_data_path = Column(String(512), default="", comment="原始数据文件路径")
    started_at = Column(DateTime, default=datetime.now)
    completed_at = Column(DateTime, nullable=True)

    # 关联
    task = relationship("CollectionTask", back_populates="logs")
    source = relationship("IntelSource", back_populates="collection_logs")

    __table_args__ = (
        Index("ix_logs_task_id", "task_id"),
        Index("ix_logs_started_at", "started_at"),
        Index("ix_logs_target", "target"),
    )


class IntelArchive(Base):
    """归档资料表"""
    __tablename__ = "intel_archives"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False, index=True, comment="股票代码")
    stock_name = Column(String(64), default="", comment="股票名称")
    category = Column(String(64), nullable=False, comment="资料类别")
    source_name = Column(String(64), default="", comment="数据来源")
    data_date = Column(DateTime, nullable=True, comment="数据日期")
    title = Column(String(256), default="", comment="资料标题")
    summary = Column(Text, default="", comment="摘要(用于向量检索)")
    content_json = Column(Text, default="", comment="结构化内容(JSON)")
    file_path = Column(String(512), default="", comment="关联文件路径")
    tags = Column(String(512), default="", comment="标签(逗号分隔)")
    collection_log_id = Column(Integer, ForeignKey("intel_collection_logs.id"), nullable=True)
    source_id = Column(Integer, ForeignKey("intel_sources.id"), nullable=True)
    indexed = Column(Boolean, default=False, comment="是否已入向量库")
    created_at = Column(DateTime, default=datetime.now)

    # 关联
    source = relationship("IntelSource", back_populates="archives")

    __table_args__ = (
        Index("ix_archive_stock_category", "stock_code", "category"),
        Index("ix_archive_data_date", "data_date"),
        Index("ix_archive_category", "category"),
    )
