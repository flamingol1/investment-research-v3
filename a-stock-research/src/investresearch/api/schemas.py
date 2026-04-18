"""API request and response models."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from investresearch.core.models import InvestmentConclusion, QualityGateDecision


class ResearchRequest(BaseModel):
    """Start research for a single stock."""

    stock_code: str = Field(description="Stock code, e.g. 300358")
    depth: Literal["quick", "standard", "deep"] = Field(
        default="standard",
        description="Research depth",
    )


class SearchRequest(BaseModel):
    """Semantic search request."""

    query: str = Field(description="Search query")
    category: str | None = Field(default=None, description="Optional category filter")
    num_results: int = Field(default=5, ge=1, le=50, description="Result count")


class WatchAddRequest(BaseModel):
    """Add a stock into the watch list."""

    stock_code: str = Field(description="Stock code")
    stock_name: str = Field(default="", description="Stock name")


class ProgressMetric(BaseModel):
    """Small metric item for live progress display."""

    key: str
    label: str
    value: str
    tone: Literal["default", "info", "success", "warning", "danger"] = "default"


class ProgressDetail(BaseModel):
    """Structured detail for the current stage."""

    headline: str = ""
    note: str = ""
    metrics: list[ProgressMetric] = Field(default_factory=list)
    bullets: list[str] = Field(default_factory=list)


class ResearchEvent(BaseModel):
    """Single research progress event."""

    id: int
    stage: str
    agent: str = ""
    status: Literal["running", "completed", "failed"] = "running"
    message: str = ""
    created_at: datetime = Field(default_factory=datetime.now)
    detail: ProgressDetail | None = None


class ResearchProgressMessage(BaseModel):
    """WebSocket progress payload."""

    stage: str = Field(description="Current stage")
    agent: str = Field(default="", description="Current agent")
    status: Literal["pending", "running", "completed", "failed"] = "running"
    progress: float = Field(default=0.0, ge=0, le=1, description="Overall progress 0-1")
    message: str = Field(default="", description="Human readable progress message")
    stage_detail: ProgressDetail | None = None
    data_summary: list[ProgressMetric] = Field(default_factory=list)
    recent_events: list[ResearchEvent] = Field(default_factory=list)
    completed_agents: list[str] = Field(default_factory=list)
    active_agents: list[str] = Field(default_factory=list)
    event: ResearchEvent | None = None
    timestamp: datetime = Field(default_factory=datetime.now)


class ReportSummary(BaseModel):
    """Saved report summary for listing and status cards."""

    stock_code: str
    stock_name: str = Field(default="")
    report_date: str = Field(description="Report date YYYYMMDD")
    depth: str = Field(default="standard")
    recommendation: str = Field(default="")
    risk_level: str = Field(default="")
    target_price_low: float | None = None
    target_price_high: float | None = None
    current_price: float | None = None
    upside_pct: float | None = None
    has_full_report: bool = Field(default=False)
    agents_completed: list[str] = Field(default_factory=list)


class ResearchStatusResponse(BaseModel):
    """Research task status."""

    task_id: str
    stock_code: str
    status: Literal["pending", "running", "completed", "failed"]
    progress: float = Field(default=0.0, ge=0, le=1)
    stage: str = Field(default="init")
    current_agent: str = Field(default="")
    message: str = Field(default="")
    stage_detail: ProgressDetail | None = None
    data_summary: list[ProgressMetric] = Field(default_factory=list)
    recent_events: list[ResearchEvent] = Field(default_factory=list)
    completed_agents: list[str] = Field(default_factory=list)
    active_agents: list[str] = Field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    report: ReportSummary | None = None
    errors: list[str] = Field(default_factory=list)


class ReportDetailResponse(BaseModel):
    """Full report detail."""

    stock_code: str
    stock_name: str = Field(default="")
    report_date: str
    depth: str = Field(default="standard")
    markdown: str = Field(default="", description="Full Markdown report")
    conclusion: InvestmentConclusion | None = None
    chart_pack: list[dict[str, Any]] = Field(default_factory=list)
    evidence_pack: list[dict[str, Any]] = Field(default_factory=list)
    agents_completed: list[str] = Field(default_factory=list)
    agents_skipped: list[str] = Field(default_factory=list)
    quality_gate: QualityGateDecision | None = None
    errors: list[str] = Field(default_factory=list)


class WatchListItemResponse(BaseModel):
    """Watch list row."""

    stock_code: str
    stock_name: str = Field(default="")
    recommendation: str = Field(default="")
    added_at: datetime | None = None
    last_updated_at: datetime | None = None
    last_report_date: datetime | None = None
    status: str = Field(default="normal")
    notes: str = Field(default="")


class WatchListResponse(BaseModel):
    """Watch list response."""

    items: list[WatchListItemResponse]
    total: int
    updated_at: datetime | None = None


class SearchItemResponse(BaseModel):
    """Search result row."""

    document: str = Field(default="", description="Document content")
    stock_code: str = Field(default="")
    stock_name: str = Field(default="")
    category: str = Field(default="")
    date: str = Field(default="")
    similarity: float = Field(default=0.0, description="Similarity 0-1")


class SearchResponse(BaseModel):
    """Search response."""

    query: str
    results: list[SearchItemResponse]
    total: int
    warning: str = ""


class SecurityLookupItemResponse(BaseModel):
    """Searchable stock universe item."""

    stock_code: str
    stock_name: str = Field(default="")
    exchange: str = Field(default="")
    has_report: bool = Field(default=False)
    in_watchlist: bool = Field(default=False)


class SecurityLookupResponse(BaseModel):
    """Stock lookup response."""

    query: str
    items: list[SecurityLookupItemResponse]
    total: int
    source: str = Field(default="cache")
    fallback: bool = Field(default=False)


class HistoryEntryResponse(BaseModel):
    """Research history row."""

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
    """Research history response."""

    stock_code: str
    stock_name: str = Field(default="")
    entries: list[HistoryEntryResponse]


class ApiResponse(BaseModel):
    """Generic API envelope."""

    success: bool = True
    message: str = ""
    data: Any = None


class UpdateResponse(BaseModel):
    """Incremental update response."""

    stock_code: str
    status: Literal["success", "failed"]
    message: str = ""
    changes: dict[str, int] = Field(default_factory=dict, description="Change counters by data type")
    duration_seconds: float = 0.0
    errors: list[str] = Field(default_factory=list)
