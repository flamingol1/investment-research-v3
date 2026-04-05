"""API请求/响应模型 - 复用core/models.py，补充API层特有字段"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from investresearch.core.models import (
    AgentStatus,
    InvestmentConclusion,
    RiskLevel,
)


# ============================================================
# 请求模型
# ============================================================


class ResearchRequest(BaseModel):
    """发起研究请求"""
    stock_code: str = Field(description="股票代码，如 300358")
    depth: Literal["quick", "standard", "deep"] = Field(default="standard", description="研究深度")


class SearchRequest(BaseModel):
    """语义搜索请求"""
    query: str = Field(description="搜索关键词")
    category: str | None = Field(default=None, description="限制分类")
    num_results: int = Field(default=5, ge=1, le=50, description="返回结果数")


class WatchAddRequest(BaseModel):
    """添加监控请求"""
    stock_code: str = Field(description="股票代码")
    stock_name: str = Field(default="", description="股票名称")


# ============================================================
# 响应模型
# ============================================================


class ResearchProgressMessage(BaseModel):
    """WebSocket研究进度消息"""
    stage: str = Field(description="当前阶段: init/collect/clean/screen/analysis/report/conclusion/done/error")
    agent: str = Field(default="", description="当前Agent名称")
    status: AgentStatus = Field(default=AgentStatus.RUNNING)
    progress: float = Field(default=0.0, ge=0, le=1, description="总体进度 0-1")
    message: str = Field(default="", description="人类可读消息")
    timestamp: datetime = Field(default_factory=datetime.now)


class ResearchStatusResponse(BaseModel):
    """研究任务状态"""
    task_id: str
    stock_code: str
    status: Literal["pending", "running", "completed", "failed"]
    progress: float = Field(default=0.0, ge=0, le=1)
    stage: str = Field(default="init")
    current_agent: str = Field(default="")
    started_at: datetime | None = None
    completed_at: datetime | None = None
    report: ReportSummary | None = None
    errors: list[str] = Field(default_factory=list)


class ReportSummary(BaseModel):
    """报告摘要（用于列表展示）"""
    stock_code: str
    stock_name: str = Field(default="")
    report_date: str = Field(description="报告日期 YYYYMMDD")
    depth: str = Field(default="standard")
    recommendation: str = Field(default="")
    risk_level: str = Field(default="")
    target_price_low: float | None = None
    target_price_high: float | None = None
    current_price: float | None = None
    upside_pct: float | None = None
    has_full_report: bool = Field(default=False)


class ReportDetailResponse(BaseModel):
    """报告完整详情"""
    stock_code: str
    stock_name: str = Field(default="")
    report_date: str
    depth: str = Field(default="standard")
    markdown: str = Field(default="", description="完整Markdown报告")
    conclusion: InvestmentConclusion | None = None
    agents_completed: list[str] = Field(default_factory=list)
    agents_skipped: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class WatchListItemResponse(BaseModel):
    """监控列表项"""
    stock_code: str
    stock_name: str = Field(default="")
    recommendation: str = Field(default="")
    added_at: datetime | None = None
    last_updated_at: datetime | None = None
    last_report_date: datetime | None = None
    status: str = Field(default="normal")
    notes: str = Field(default="")


class WatchListResponse(BaseModel):
    """监控列表"""
    items: list[WatchListItemResponse]
    total: int
    updated_at: datetime | None = None


class SearchItemResponse(BaseModel):
    """搜索结果项"""
    document: str = Field(default="", description="文档内容")
    stock_code: str = Field(default="")
    stock_name: str = Field(default="")
    category: str = Field(default="")
    date: str = Field(default="")
    similarity: float = Field(default=0.0, description="相似度 0-1")


class SearchResponse(BaseModel):
    """搜索结果"""
    query: str
    results: list[SearchItemResponse]
    total: int


class HistoryEntryResponse(BaseModel):
    """历史记录项"""
    stock_code: str
    stock_name: str = Field(default="")
    research_date: str
    depth: str = Field(default="standard")
    recommendation: str | None = None
    risk_level: str | None = None
    target_price_low: float | None = None
    target_price_high: float | None = None
    current_price: float | None = None
    agents_completed: list[str] = Field(default_factory=list)


class HistoryResponse(BaseModel):
    """研究历史"""
    stock_code: str
    stock_name: str = Field(default="")
    entries: list[HistoryEntryResponse]


class ApiResponse(BaseModel):
    """通用API响应包装"""
    success: bool = True
    message: str = ""
    data: Any = None


class UpdateResponse(BaseModel):
    """增量更新响应"""
    stock_code: str
    status: Literal["success", "failed"]
    changes: dict[str, int] = Field(default_factory=dict, description="各数据类型新增条数")
    duration_seconds: float = 0.0
    errors: list[str] = Field(default_factory=list)
