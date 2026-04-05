"""Pydantic 请求/响应模型"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


# ============================================================
# 数据源
# ============================================================


class SourceCreate(BaseModel):
    """创建数据源"""
    name: str = Field(description="数据源标识", min_length=1, max_length=64)
    display_name: str = Field(description="显示名称", min_length=1)
    description: str = Field(default="", description="描述")
    enabled: bool = Field(default=True)
    priority: int = Field(default=1, ge=1, description="优先级(1=最高)")
    config_json: dict[str, Any] = Field(default_factory=dict, description="数据源特有配置")


class SourceUpdate(BaseModel):
    """更新数据源"""
    display_name: str | None = None
    description: str | None = None
    enabled: bool | None = None
    priority: int | None = None
    config_json: dict[str, Any] | None = None


class SourceRead(BaseModel):
    """数据源详情"""
    id: int
    name: str
    display_name: str
    description: str
    enabled: bool
    priority: int
    config_json: str
    health_status: str
    last_health_check: datetime | None
    last_error: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class SourceHealth(BaseModel):
    """数据源健康状态"""
    name: str
    status: str = "unknown"  # healthy/degraded/down/unknown
    latency_ms: int | None = None
    error: str | None = None
    checked_at: datetime = Field(default_factory=datetime.now)


# ============================================================
# 采集任务
# ============================================================


class TaskCreate(BaseModel):
    """创建采集任务"""
    name: str = Field(description="任务名称", min_length=1)
    task_type: str = Field(description="采集类型: stock_info/price/financial/valuation/news/all")
    target: str = Field(description="采集目标(股票代码)")
    schedule_type: str = Field(default="manual", description="cron/interval/manual/once")
    schedule_expr: str = Field(default="", description="cron表达式或间隔秒数")
    enabled: bool = Field(default=True)
    source_name: str | None = Field(default=None, description="指定数据源(null=自动选择)")


class TaskUpdate(BaseModel):
    """更新采集任务"""
    name: str | None = None
    task_type: str | None = None
    schedule_type: str | None = None
    schedule_expr: str | None = None
    enabled: bool | None = None
    source_name: str | None = None


class TaskRead(BaseModel):
    """采集任务详情"""
    id: int
    name: str
    task_type: str
    target: str
    schedule_type: str
    schedule_expr: str
    enabled: bool
    source_id: int | None
    status: str
    last_run_at: datetime | None
    next_run_at: datetime | None
    success_count: int
    fail_count: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ============================================================
# 采集结果
# ============================================================


class CollectionResult(BaseModel):
    """单次采集结果"""
    target: str = Field(description="采集目标")
    data_type: str = Field(description="数据类型")
    source_name: str = Field(description="实际使用的数据源")
    status: str = Field(description="success/partial/failed")
    records_fetched: int = 0
    records_stored: int = 0
    duration_ms: int = 0
    error: str | None = None
    data: dict[str, Any] = Field(default_factory=dict, description="采集到的原始数据摘要")


# ============================================================
# 归档资料
# ============================================================


class ArchiveRead(BaseModel):
    """归档资料详情"""
    id: int
    stock_code: str
    stock_name: str
    category: str
    source_name: str
    data_date: datetime | None
    title: str
    summary: str
    tags: str
    indexed: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class ArchiveSearchQuery(BaseModel):
    """归档搜索查询"""
    keyword: str | None = Field(default=None, description="关键词搜索")
    stock_code: str | None = Field(default=None, description="按股票代码筛选")
    category: str | None = Field(default=None, description="按类别筛选")
    source_name: str | None = Field(default=None, description="按数据源筛选")
    date_from: date | None = Field(default=None)
    date_to: date | None = Field(default=None)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class KnowledgeSearchResult(BaseModel):
    """知识库搜索结果"""
    archive_id: int | None = None
    stock_code: str | None = None
    category: str | None = None
    title: str = ""
    summary: str = ""
    content_snippet: str = ""
    relevance_score: float = 0.0
    source_name: str = ""
    data_date: datetime | None = None
