"""核心Pydantic数据模型 - 系统全流程共享的数据结构"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, ConfigDict


# ============================================================
# 枚举类型
# ============================================================


class DataSource(str, Enum):
    """数据来源"""
    AKSHARE = "akshare"
    BAOSTOCK = "baostock"
    TUSHARE = "tushare"
    CNINFO = "cninfo"
    EASTMONEY = "eastmoney"
    MANUAL = "manual"


class IndustryLifecycle(str, Enum):
    """行业生命周期"""
    EMBRYONIC = "初创期"
    GROWTH = "成长期"
    MATURE = "成熟期"
    DECLINE = "衰退期"


class MarketType(str, Enum):
    """市场类型"""
    INCREMENTAL = "增量市场"
    STOCK_COMPETITION = "存量博弈"
    SHRINKING = "收缩市场"


class StockVerdict(str, Enum):
    """标的筛选结论"""
    PASS = "通过"
    WARNING = "重点警示"
    REJECT = "刚性剔除"


class RiskLevel(str, Enum):
    """风险等级"""
    LOW = "低"
    MEDIUM = "中"
    HIGH = "高"
    CRITICAL = "极高"


class ScenarioType(str, Enum):
    """情景类型"""
    OPTIMISTIC = "乐观"
    BASE = "中性"
    PESSIMISTIC = "悲观"


class AgentStatus(str, Enum):
    """Agent执行状态"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class DataQualityStatus(str, Enum):
    """数据质量状态"""
    OK = "ok"
    PARTIAL = "partial"
    FAILED = "failed"


class MonitoringLayer(str, Enum):
    """跟踪层级"""
    LEADING = "leading"
    VALIDATION = "validation"
    RISK_TRIGGER = "risk_trigger"


class FieldValueState(str, Enum):
    """字段值状态，明确区分空值背后的原因。"""

    PRESENT = "present"
    VERIFIED_ABSENT = "verified_absent"
    COLLECTION_FAILED = "collection_failed"
    NOT_APPLICABLE = "not_applicable"
    MISSING = "missing"


class FieldEvidenceState(str, Enum):
    """字段证据状态。"""

    CONSISTENT = "consistent"
    SINGLE_SOURCE = "single_source"
    INSUFFICIENT = "insufficient"
    DIVERGENT = "divergent"
    VERIFIED_ABSENT = "verified_absent"
    COLLECTION_FAILED = "collection_failed"
    NOT_APPLICABLE = "not_applicable"
    UNKNOWN = "unknown"


class FieldPeriodType(str, Enum):
    """字段口径/观察周期。"""

    INSTANT = "instant"
    LATEST = "latest"
    QUARTER = "quarter"
    CUMULATIVE = "cumulative"
    TTM = "ttm"
    ANNUAL = "annual"
    EVENT = "event"
    ROLLING = "rolling"


class BlockingLevel(str, Enum):
    """字段在深度研究中的阻断级别。"""

    NONE = "none"
    WARNING = "warning"
    CORE = "core"
    CRITICAL = "critical"


# ============================================================
# 通用证据/数据质量模型
# ============================================================


class EvidenceRef(BaseModel):
    """可追溯证据引用"""
    source: str = Field(default="", description="来源站点或数据源")
    source_priority: int = Field(default=99, description="来源优先级，数值越小越靠前")
    title: str = Field(default="", description="资料标题")
    field: str = Field(default="", description="支撑字段")
    excerpt: str = Field(default="", description="摘录内容")
    url: str = Field(default="", description="资料链接")
    reference_date: str = Field(default="", description="资料日期")


class ModuleQualityProfile(BaseModel):
    """单个模块的数据质量画像"""
    status: DataQualityStatus = Field(default=DataQualityStatus.FAILED, description="ok/partial/failed")
    completeness: float = Field(default=0.0, ge=0, le=1, description="完整度 0-1")
    missing_fields: list[str] = Field(default_factory=list, description="缺失关键字段")
    source_priority: list[str] = Field(default_factory=list, description="已使用的来源优先级顺序")
    evidence_refs: list[EvidenceRef] = Field(default_factory=list, description="关键证据引用")
    notes: list[str] = Field(default_factory=list, description="模块说明")


class FieldContract(BaseModel):
    """字段级质量契约。"""

    field: str = Field(description="字段路径")
    label: str = Field(default="", description="展示名称")
    allowed_sources: list[str] = Field(default_factory=list, description="允许来源")
    unit: str = Field(default="", description="标准单位")
    period_type: FieldPeriodType = Field(default=FieldPeriodType.LATEST, description="标准口径")
    blocking_level: BlockingLevel = Field(default=BlockingLevel.NONE, description="阻断级别")
    notes: str = Field(default="", description="补充说明")


class FieldSourceValue(BaseModel):
    """字段来自单一来源的一次观测。"""

    source_name: str = Field(default="", description="来源名称")
    source_type: str = Field(default="", description="来源类型")
    reference_date: str = Field(default="", description="来源日期")
    value: Any = Field(default=None, description="原始观测值")
    unit: str = Field(default="", description="单位")
    period_type: str = Field(default="", description="口径")
    excerpt: str = Field(default="", description="证据摘录")


class FieldCollectionStatus(BaseModel):
    """采集阶段记录的字段状态。"""

    field: str = Field(default="", description="字段路径")
    value_state: FieldValueState = Field(default=FieldValueState.MISSING, description="字段值状态")
    sources_checked: list[str] = Field(default_factory=list, description="已检查来源")
    reference_date: str = Field(default="", description="核查日期")
    note: str = Field(default="", description="补充说明")


class FieldQualityTrace(BaseModel):
    """字段级追踪结果。"""

    field: str = Field(description="字段路径")
    label: str = Field(default="", description="展示名称")
    value: Any = Field(default=None, description="规范化后的字段值")
    allowed_sources: list[str] = Field(default_factory=list, description="允许来源")
    unit: str = Field(default="", description="标准单位")
    period_type: FieldPeriodType = Field(default=FieldPeriodType.LATEST, description="标准口径")
    blocking_level: BlockingLevel = Field(default=BlockingLevel.NONE, description="阻断级别")
    report_period: str = Field(default="", description="报告期/事件日期")
    value_state: FieldValueState = Field(default=FieldValueState.MISSING, description="字段值状态")
    evidence_state: FieldEvidenceState = Field(default=FieldEvidenceState.UNKNOWN, description="字段证据状态")
    source_count: int = Field(default=0, description="有效来源数")
    confidence_score: float = Field(default=0.0, ge=0, le=1, description="字段置信度")
    source_values: list[FieldSourceValue] = Field(default_factory=list, description="来源观测")
    notes: list[str] = Field(default_factory=list, description="附加说明")


class QualityGateDecision(BaseModel):
    """核心证据双闸门结果。"""

    blocked: bool = Field(default=False, description="是否阻断")
    gate_type: str = Field(default="dual_gate", description="闸门类型")
    core_evidence_score: float = Field(default=0.0, ge=0, le=1, description="核心证据分")
    blocking_fields: list[str] = Field(default_factory=list, description="阻断字段")
    weak_fields: list[str] = Field(default_factory=list, description="弱证据字段")
    reasons: list[str] = Field(default_factory=list, description="阻断/提示原因")
    consistency_notes: list[str] = Field(default_factory=list, description="流程一致性说明")
    coverage_ratio: float = Field(default=0.0, ge=0, le=1, description="覆盖率")
    company_cross_confidence: float = Field(default=0.0, ge=0, le=1, description="公司交叉验证置信度")
    peer_verified: int = Field(default=0, description="同业验证指标数")


class RegressionBaselineSnapshot(BaseModel):
    """每次运行输出的结构化基线。"""

    stock_code: str = Field(default="", description="股票代码")
    stock_name: str = Field(default="", description="股票名称")
    depth: str = Field(default="standard", description="研究深度")
    generated_at: datetime = Field(default_factory=datetime.now, description="生成时间")
    coverage_ratio: float = Field(default=0.0, ge=0, le=1, description="覆盖率")
    completeness: float = Field(default=0.0, ge=0, le=1, description="完整度")
    core_evidence_score: float = Field(default=0.0, ge=0, le=1, description="核心证据分")
    missing_fields: list[str] = Field(default_factory=list, description="缺失字段")
    blocking_fields: list[str] = Field(default_factory=list, description="阻断字段")
    divergent_fields: list[str] = Field(default_factory=list, description="分歧字段")
    warning_count: int = Field(default=0, description="告警数")
    initial_verdict: str = Field(default="", description="初筛结论")
    final_recommendation: str = Field(default="", description="最终结论")
    quality_gate_blocked: bool = Field(default=False, description="是否触发闸门")
    quality_gate_reasons: list[str] = Field(default_factory=list, description="闸门原因")
    consistency_notes: list[str] = Field(default_factory=list, description="流程一致性说明")


class MonitoringPlanItem(BaseModel):
    """结论卡跟踪计划项"""
    layer: MonitoringLayer = Field(description="跟踪层级")
    metric: str = Field(description="跟踪指标")
    trigger: str = Field(default="", description="触发条件")
    update_frequency: str = Field(default="", description="更新频率")
    rationale: str = Field(default="", description="跟踪原因")


class ChartSeries(BaseModel):
    """图表序列"""
    name: str = Field(description="序列名称")
    points: list[dict[str, Any]] = Field(default_factory=list, description="图表点位")


class ChartPackItem(BaseModel):
    """图表包条目"""
    chart_id: str = Field(description="图表标识")
    title: str = Field(description="图表标题")
    chart_type: str = Field(default="line", description="图表类型")
    unit: str = Field(default="", description="数值单位")
    summary: str = Field(default="", description="图表解读")
    series: list[ChartSeries] = Field(default_factory=list, description="图表序列")
    evidence_refs: list[EvidenceRef] = Field(default_factory=list, description="图表证据")


class EvidencePackItem(BaseModel):
    """证据包条目"""
    category: str = Field(description="证据分类")
    title: str = Field(description="标题")
    source: str = Field(default="", description="来源")
    url: str = Field(default="", description="链接")
    excerpt: str = Field(default="", description="摘录")
    fields: list[str] = Field(default_factory=list, description="支撑字段")
    reference_date: str = Field(default="", description="日期")


# ============================================================
# 数据层模型
# ============================================================


class StockBasicInfo(BaseModel):
    """股票基础信息"""
    model_config = ConfigDict(populate_by_name=True)

    code: str = Field(description="股票代码，如 600519")
    name: str = Field(description="股票名称，如 贵州茅台")
    exchange: str | None = Field(default=None, description="上市板块")
    listing_date: date | None = Field(default=None, description="上市日期")
    industry_sw: str | None = Field(default=None, description="申万行业分类")
    industry_sw_code: str | None = Field(default=None, description="申万行业代码")
    actual_controller: str | None = Field(default=None, description="实际控制人")
    controller_type: str | None = Field(default=None, description="实控人性质")
    main_business: str | None = Field(default=None, description="主营业务")
    business_model: str | None = Field(default=None, description="盈利模式")
    asset_model: str | None = Field(default=None, description="资产模式")
    client_type: str | None = Field(default=None, description="客户类型")


class StockPrice(BaseModel):
    """股票行情数据"""
    code: str
    date: date
    open: float | None = None
    close: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float | None = None
    amount: float | None = None
    turnover_rate: float | None = None
    pe_ttm: float | None = None
    pb_mrq: float | None = None
    ps_ttm: float | None = None
    market_cap: float | None = None
    raw_data: dict[str, Any] | None = Field(default=None, description="鍘熷鏁版嵁JSON")


class FinancialStatement(BaseModel):
    """财务报表数据（三大报表关键字段合并）"""
    code: str = Field(description="股票代码")
    report_date: date = Field(description="报告期")
    report_type: str = Field(description="报表类型")
    source: DataSource = Field(default=DataSource.AKSHARE)

    # 利润表
    revenue: float | None = Field(default=None, description="营业收入(元)")
    revenue_yoy: float | None = Field(default=None, description="营收同比增速(%)")
    net_profit: float | None = Field(default=None, description="净利润(元)")
    net_profit_yoy: float | None = Field(default=None, description="净利润同比增速(%)")
    deduct_net_profit: float | None = Field(default=None, description="扣非净利润(元)")
    gross_margin: float | None = Field(default=None, description="毛利率(%)")
    net_margin: float | None = Field(default=None, description="净利率(%)")

    # 资产负债表
    total_assets: float | None = Field(default=None, description="总资产(元)")
    total_liabilities: float | None = Field(default=None, description="总负债(元)")
    equity: float | None = Field(default=None, description="股东权益(元)")
    debt_ratio: float | None = Field(default=None, description="资产负债率(%)")
    current_ratio: float | None = Field(default=None, description="流动比率")
    quick_ratio: float | None = Field(default=None, description="速动比率")
    goodwill_ratio: float | None = Field(default=None, description="商誉/净资产(%)")

    # 现金流量表
    operating_cashflow: float | None = Field(default=None, description="经营现金流(元)")
    investing_cashflow: float | None = Field(default=None, description="投资现金流(元)")
    financing_cashflow: float | None = Field(default=None, description="筹资现金流(元)")
    free_cashflow: float | None = Field(default=None, description="自由现金流(元)")
    cash_to_profit: float | None = Field(default=None, description="净现比")
    capex_maintenance: float | None = Field(default=None, description="维护性资本开支(元)")
    capex_expansion: float | None = Field(default=None, description="扩张性资本开支(元)")

    # 质量指标
    roe: float | None = Field(default=None, description="ROE(%)")
    roic: float | None = Field(default=None, description="ROIC(%)")
    receivable_turnover: float | None = Field(default=None, description="应收周转率")
    inventory_turnover: float | None = Field(default=None, description="存货周转率")
    non_recurring_profit: float | None = Field(default=None, description="非经常性损益(元)")
    contract_liabilities: float | None = Field(default=None, description="合同负债(元)")

    # 原始数据
    raw_data: dict[str, Any] | None = Field(default=None, description="原始数据JSON")


class IndustryData(BaseModel):
    """行业数据"""
    industry_name: str = Field(description="行业名称")
    industry_code: str | None = None
    lifecycle: IndustryLifecycle | None = None
    market_size: float | None = Field(default=None, description="市场规模(亿元)")
    cagr_5y: float | None = Field(default=None, description="5年复合增速(%)")
    cr5: float | None = Field(default=None, description="CR5集中度(%)")
    market_type: MarketType | None = None
    policy_stance: str | None = Field(default=None, description="政策态度")


# ============================================================
# 数据采集输出模型
# ============================================================


class CollectorOutput(BaseModel):
    """数据采集Agent的完整输出"""
    stock_info: StockBasicInfo | None = None
    prices: list[StockPrice] = Field(default_factory=list, description="历史行情")
    realtime: StockPrice | None = Field(default=None, description="实时行情")
    financials: list[FinancialStatement] = Field(default_factory=list, description="财务报表")
    industry: IndustryData | None = None
    valuation: list[StockPrice] = Field(default_factory=list, description="估值数据(含PE/PB)")

    # Phase 8: 新增数据源
    announcements: list[Announcement] = Field(default_factory=list, description="公告披露")
    governance: GovernanceData | None = Field(default=None, description="公司治理数据")
    research_reports: list[ResearchReportSummary] = Field(default_factory=list, description="研报摘要")
    shareholders: ShareholderData | None = Field(default=None, description="股东数据")
    industry_enhanced: IndustryEnhancedData | None = Field(default=None, description="行业增强数据")
    valuation_percentile: ValuationPercentile | None = Field(default=None, description="估值分位数据")
    news: list[NewsData] = Field(default_factory=list, description="新闻数据")
    sentiment: SentimentData | None = Field(default=None, description="舆情情绪数据")
    policy_documents: list[PolicyDocument] = Field(default_factory=list, description="政策原文资料")

    compliance_events: list[ComplianceEvent] = Field(default_factory=list, description="官方合规/监管事件")
    patents: list[PatentRecord] = Field(default_factory=list, description="官方专利/技术资料")

    cross_verification: DataCrossVerification | None = Field(default=None, description="澶氭簮浜ゅ弶楠岃瘉缁撴灉")

    collection_status: dict[str, str] = Field(
        default_factory=dict,
        description="采集状态: data_type -> ok/partial/failed",
    )
    module_profiles: dict[str, ModuleQualityProfile] = Field(default_factory=dict, description="模块数据质量画像")
    field_contracts: dict[str, FieldContract] = Field(default_factory=dict, description="字段级质量契约")
    field_statuses: dict[str, FieldCollectionStatus] = Field(default_factory=dict, description="字段采集状态")
    field_quality: dict[str, FieldQualityTrace] = Field(default_factory=dict, description="字段质量追踪")
    quality_gate: QualityGateDecision | None = Field(default=None, description="双闸门结果")
    status: DataQualityStatus = Field(default=DataQualityStatus.PARTIAL, description="整体数据状态")
    completeness: float = Field(default=0.0, ge=0, le=1, description="整体关键字段完整度")
    missing_fields: list[str] = Field(default_factory=list, description="整体关键缺失字段")
    evidence_refs: list[EvidenceRef] = Field(default_factory=list, description="整体关键证据")
    source_priority: list[str] = Field(default_factory=list, description="整体来源优先级")
    coverage_ratio: float = Field(default=0.0, ge=0, le=1, description="覆盖率0-1")
    errors: list[str] = Field(default_factory=list)
    collected_at: datetime = Field(default_factory=datetime.now)


# ============================================================
# Agent I/O 模型
# ============================================================


class AgentInput(BaseModel):
    """通用Agent输入"""
    stock_code: str = Field(description="股票代码")
    stock_name: str | None = Field(default=None, description="股票名称")
    context: dict[str, Any] = Field(default_factory=dict, description="上下文数据")
    depth: str = Field(default="standard", description="分析深度: quick/standard/deep")


class AgentExecutionRecord(BaseModel):
    """Normalized runtime metadata for one agent execution."""

    agent_name: str = Field(description="Agent name")
    status: str = Field(default="", description="success/failed")
    execution_mode: str = Field(default="deterministic", description="deterministic/llm/hybrid")
    configured_model: str | None = Field(default=None, description="Configured model alias if present")
    model_used: str | None = Field(default=None, description="Model alias requested at runtime")
    llm_invoked: bool = Field(default=False, description="Whether an LLM call was attempted")
    summary: str = Field(default="", description="Execution summary")
    confidence: float | None = Field(default=None, description="Agent confidence")
    errors: list[str] = Field(default_factory=list, description="Execution errors")


class AgentOutput(BaseModel):
    """通用Agent输出"""
    agent_name: str = Field(description="Agent名称")
    status: AgentStatus = Field(default=AgentStatus.SUCCESS)
    data: dict[str, Any] = Field(default_factory=dict, description="输出数据")
    errors: list[str] = Field(default_factory=list, description="错误列表")
    data_sources: list[str] = Field(default_factory=list, description="数据来源")
    confidence: float = Field(ge=0, le=1, default=0.5, description="置信度")
    summary: str = Field(default="", description="结论摘要")
    execution_mode: str = Field(default="", description="Actual execution mode")
    configured_model: str | None = Field(default=None, description="Configured model alias")
    model_used: str | None = Field(default=None, description="Model alias used for runtime call")
    llm_invoked: bool = Field(default=False, description="Whether an LLM call was attempted")
    timestamp: datetime = Field(default_factory=datetime.now)


# ============================================================
# 研究流程模型
# ============================================================


class UserProfile(BaseModel):
    """用户画像"""
    investment_style: str = Field(default="价值投资")
    horizon: str = Field(default="中长期")
    risk_tolerance: str = Field(default="中等")
    competence_industries: list[str] = Field(default_factory=list)


class ResearchRequest(BaseModel):
    """研究请求"""
    stock_codes: list[str] = Field(min_length=1, description="标的代码列表")
    user_profile: UserProfile = Field(default_factory=UserProfile)
    depth: str = Field(default="standard", description="研究深度")
    focus_areas: list[str] | None = Field(default=None)


class ResearchState(BaseModel):
    """全流程研究状态"""
    request: ResearchRequest

    # 数据层输出
    raw_data: dict[str, Any] = Field(default_factory=dict)
    cleaned_data: dict[str, Any] = Field(default_factory=dict)

    # 分析层输出
    screening_result: dict[str, Any] = Field(default_factory=dict)
    industry_analysis: dict[str, Any] = Field(default_factory=dict)
    business_analysis: dict[str, Any] = Field(default_factory=dict)
    governance_analysis: dict[str, Any] = Field(default_factory=dict)
    financial_analysis: dict[str, Any] = Field(default_factory=dict)
    valuation_analysis: dict[str, Any] = Field(default_factory=dict)
    risk_analysis: dict[str, Any] = Field(default_factory=dict)

    # 决策层输出
    conclusion: dict[str, Any] = Field(default_factory=dict)
    report: str | None = None

    # 系统状态
    current_step: str = "init"
    errors: list[str] = Field(default_factory=list)
    completed_agents: list[str] = Field(default_factory=list)


# ============================================================
# 分析层模型
# ============================================================


class ScreeningCheckItem(BaseModel):
    """单个筛选检查项"""
    item: str = Field(description="检查项目名称")
    status: Literal["pass", "warning", "reject"] = Field(description="检查结果")
    detail: str = Field(description="检查详情和依据")
    evidence: str = Field(default="", description="数据支撑依据")


class ScreeningResult(BaseModel):
    """初筛结果"""
    verdict: str = Field(description="综合判定: 通过/重点警示/刚性剔除")
    checks: list[ScreeningCheckItem] = Field(description="各检查项结果")
    key_risks: list[str] = Field(default_factory=list, description="核心风险点")
    recommendation: str = Field(description="是否建议继续深度研究")
    confidence: float = Field(ge=0, le=1, default=0.5, description="判定置信度")


class FinancialDimensionScore(BaseModel):
    """单个财务维度评分"""
    dimension: str = Field(description="维度名称: 盈利能力/成长性/偿债能力/运营效率/现金流质量")
    score: float = Field(ge=0, le=10, description="评分 0-10")
    trend: str = Field(description="趋势: 改善/稳定/恶化")
    key_metrics: dict[str, Any] = Field(default_factory=dict, description="关键指标数值")
    analysis: str = Field(description="分析说明")
    concerns: list[str] = Field(default_factory=list, description="关注点")


class FinancialAnalysisResult(BaseModel):
    """财务分析结果"""
    overall_score: float = Field(ge=0, le=10, description="综合评分 0-10")
    dimensions: list[FinancialDimensionScore] = Field(description="5个维度评分")
    trend_summary: str = Field(description="3-5年趋势总结")
    cashflow_verification: str = Field(description="现金流验证结论")
    anomaly_flags: list[str] = Field(default_factory=list, description="财务异常标记")
    peer_comparison: str = Field(default="", description="同行对比结论")
    conclusion: str = Field(description="综合财务健康度结论")


class ValuationMethodResult(BaseModel):
    """单个估值方法结果"""
    method: str = Field(description="估值方法: PE/PB/DCF/PEG/PS")
    intrinsic_value: float | None = Field(default=None, description="内在价值估算")
    upside_pct: float | None = Field(default=None, description="上行空间百分比")
    assumptions: list[str] = Field(default_factory=list, description="核心假设")
    limitations: list[str] = Field(default_factory=list, description="方法局限性")


class ValuationResult(BaseModel):
    """估值结果"""
    methods: list[ValuationMethodResult] = Field(description="各方法估值结果")
    pe_percentile: float | None = Field(default=None, description="当前PE历史分位(0-100)")
    pb_percentile: float | None = Field(default=None, description="当前PB历史分位(0-100)")
    reasonable_range_low: float | None = Field(default=None, description="合理估值下限")
    reasonable_range_high: float | None = Field(default=None, description="合理估值上限")
    current_price: float | None = Field(default=None, description="当前股价")
    margin_of_safety: float | None = Field(default=None, description="安全边际百分比")
    valuation_level: str = Field(description="估值水平: 低估/合理/高估/严重高估")
    conclusion: str = Field(description="估值综合结论")


# ============================================================
# Phase 4: 分析层扩展模型
# ============================================================


class RevenueSegment(BaseModel):
    """收入结构项"""
    segment_name: str = Field(description="业务/产品/地区名称")
    revenue: float | None = Field(default=None, description="收入金额(元)")
    ratio: float | None = Field(default=None, description="占比(%)")
    growth: float | None = Field(default=None, description="增速(%)")
    gross_margin: float | None = Field(default=None, description="毛利率(%)")


class MoatAssessment(BaseModel):
    """护城河评估"""
    moat_type: str = Field(description="护城河类型: 品牌/网络效应/转换成本/成本优势/规模效应/专利/无")
    strength: str = Field(description="强度: 强/中/弱/无")
    evidence: str = Field(description="支撑证据")
    sustainability: str = Field(description="可持续性判断")


class BusinessModelResult(BaseModel):
    """商业模式分析结果"""
    model_score: float = Field(ge=0, le=10, description="商业模式综合评分 0-10")
    revenue_structure: list[RevenueSegment] = Field(default_factory=list, description="收入结构拆解")
    profit_driver: str = Field(description="核心盈利驱动力说明")
    asset_model: str = Field(description="资产模式: 轻/重/混合")
    client_concentration: str = Field(default="", description="客户集中度评估")
    moats: list[MoatAssessment] = Field(default_factory=list, description="护城河评估")
    moat_overall: str = Field(description="护城河综合判断: 宽/窄/无")
    negative_view: str = Field(description="反证视角: 为什么这个商业模式可能失败")
    conclusion: str = Field(description="商业模式综合结论")


class CompetitorInfo(BaseModel):
    """竞争对手信息"""
    name: str = Field(description="公司名称")
    market_share: float | None = Field(default=None, description="市场份额(%)")
    advantage: str = Field(default="", description="竞争优势")
    threat_level: str = Field(default="中", description="威胁程度: 高/中/低")


class IndustryAnalysisResult(BaseModel):
    """行业分析结果"""
    lifecycle: str = Field(description="行业生命周期: 初创期/成长期/成熟期/衰退期")
    lifecycle_evidence: str = Field(description="生命周期判断依据")
    market_size: float | None = Field(default=None, description="市场规模(亿元)")
    market_growth: float | None = Field(default=None, description="行业增速(%)")
    competition_pattern: str = Field(description="竞争格局: 寡头垄断/寡头竞争/垄断竞争/完全竞争")
    cr5: float | None = Field(default=None, description="CR5集中度(%)")
    top_competitors: list[CompetitorInfo] = Field(default_factory=list, description="主要竞争对手")
    prosperity_indicators: list[str] = Field(default_factory=list, description="景气度指标列表")
    prosperity_direction: str = Field(description="景气方向: 上行/平稳/下行")
    policy_stance: str = Field(default="", description="政策态度")
    company_position: str = Field(description="公司在行业中的地位")
    conclusion: str = Field(description="行业综合结论")


class GovernanceResult(BaseModel):
    """治理分析结果"""
    governance_score: float = Field(ge=0, le=10, description="治理评分 0-10")
    management_assessment: str = Field(description="管理层评估")
    management_integrity: str = Field(description="管理层诚信: 优/良/中/差")
    controller_analysis: str = Field(description="实控人分析")
    related_transactions: str = Field(default="", description="关联交易评估")
    equity_pledge: str = Field(default="", description="股权质押情况")
    capital_allocation: str = Field(description="资本配置效率评估")
    dividend_policy: str = Field(default="", description="分红政策评估")
    incentive_plan: str = Field(default="", description="股权激励评估")
    conclusion: str = Field(description="治理综合结论")


class ScenarioResult(BaseModel):
    """情景分析结果"""
    scenario: str = Field(description="情景: 乐观/中性/悲观")
    target_price: float | None = Field(default=None, description="目标价")
    upside_pct: float | None = Field(default=None, description="上行空间(%)")
    assumptions: list[str] = Field(default_factory=list, description="核心假设")
    probability: float | None = Field(default=None, description="发生概率(%)")


class RiskItem(BaseModel):
    """单个风险项"""
    category: str = Field(description="风险类别: 行业/经营/财务/治理/市场/政策")
    risk_name: str = Field(description="风险名称")
    severity: str = Field(description="严重程度: 高/中/低")
    probability: str = Field(description="发生概率: 高/中/低")
    impact: str = Field(description="影响说明")
    mitigation: str = Field(default="", description="缓解措施")


class RiskAnalysisResult(BaseModel):
    """风险分析结果"""
    overall_risk_level: str = Field(description="整体风险等级: 低/中/高/极高")
    risk_score: float = Field(ge=0, le=10, description="风险评分(越高越危险) 0-10")
    risks: list[RiskItem] = Field(default_factory=list, description="风险清单")
    scenarios: list[ScenarioResult] = Field(default_factory=list, description="三情景测算")
    fatal_risks: list[str] = Field(default_factory=list, description="致命风险(可能否定投资逻辑)")
    monitoring_points: list[str] = Field(default_factory=list, description="需持续跟踪的指标")
    conclusion: str = Field(description="风险综合结论")


# ============================================================
# Phase 5: 决策层模型
# ============================================================


class InvestmentConclusion(BaseModel):
    """投资结论卡片"""
    model_config = ConfigDict(populate_by_name=True)

    recommendation: str = Field(description="投资建议: 买入(强烈)/买入(谨慎)/持有/观望/卖出")
    confidence_level: str = Field(description="置信度: 高/中/低")
    target_price_low: float | None = Field(default=None, description="目标价下限")
    target_price_high: float | None = Field(default=None, description="目标价上限")
    current_price: float | None = Field(default=None, description="当前股价")
    upside_pct: float | None = Field(default=None, description="上行空间(%)")
    risk_level: str = Field(description="风险等级: 低/中/高/极高")
    key_reasons_buy: list[str] = Field(default_factory=list, description="买入理由")
    key_reasons_sell: list[str] = Field(default_factory=list, description="卖出/观望理由")
    core_thesis: list[str] = Field(default_factory=list, description="核心投资逻辑")
    expectation_gap: str = Field(default="", description="预期差判断")
    catalysts: list[str] = Field(default_factory=list, description="催化剂")
    key_assumptions: list[str] = Field(default_factory=list, description="核心假设")
    valuation_range: str = Field(default="", description="估值区间说明")
    return_breakdown: list[str] = Field(default_factory=list, description="收益来源拆解")
    major_risks: list[str] = Field(default_factory=list, description="主要风险")
    failure_conditions: list[str] = Field(default_factory=list, description="逻辑失效条件")
    monitoring_points: list[str] = Field(default_factory=list, description="需跟踪的指标")
    monitoring_plan: list[MonitoringPlanItem] = Field(default_factory=list, description="分层跟踪计划")
    position_advice: str = Field(default="", description="仓位建议")
    holding_period: str = Field(default="", description="建议持有周期")
    stop_loss_price: float | None = Field(default=None, description="止损价")
    consistency_notes: list[str] = Field(default_factory=list, description="流程一致性说明")
    evidence_refs: list[EvidenceRef] = Field(default_factory=list, description="结论证据")
    execution_trace: list[AgentExecutionRecord] = Field(default_factory=list, description="Agent执行轨迹")
    conclusion_summary: str = Field(description="一段话结论")


class ResearchReport(BaseModel):
    """研究报告元数据"""
    model_config = ConfigDict(populate_by_name=True)

    stock_code: str = Field(description="股票代码")
    stock_name: str = Field(default="", description="股票名称")
    report_date: datetime = Field(default_factory=datetime.now, description="报告日期")
    depth: str = Field(default="standard", description="研究深度")
    markdown: str = Field(default="", description="完整Markdown报告内容")
    conclusion: InvestmentConclusion | None = Field(default=None, description="投资结论")
    chart_pack: list[ChartPackItem] = Field(default_factory=list, description="图表包")
    evidence_pack: list[EvidencePackItem] = Field(default_factory=list, description="证据包")
    quality_gate: QualityGateDecision | None = Field(default=None, description="质量闸门结果")
    baseline_snapshot: RegressionBaselineSnapshot | None = Field(default=None, description="结构化回归基线")
    execution_trace: list[AgentExecutionRecord] = Field(default_factory=list, description="Agent执行轨迹")
    agents_completed: list[str] = Field(default_factory=list, description="已完成的Agent")
    agents_skipped: list[str] = Field(default_factory=list, description="跳过的Agent")
    errors: list[str] = Field(default_factory=list, description="错误列表")


# ============================================================
# Phase 7: 知识库与增量更新模型
# ============================================================


class KnowledgeCategory(str, Enum):
    """知识库分类"""
    STOCK = "stock"         # 个股研究
    INDUSTRY = "industry"   # 行业分析
    MACRO = "macro"         # 宏观环境
    REPORT = "report"       # 研究报告
    RISK = "risk"           # 风险分析
    DECISION = "decision"   # 投资结论


class UpdateFrequency(str, Enum):
    """更新频率"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


class UpdateRecord(BaseModel):
    """增量更新记录"""
    stock_code: str
    update_type: str  # full / incremental
    previous_collected_at: datetime | None = None
    new_collected_at: datetime = Field(default_factory=datetime.now)
    data_changes: dict[str, Any] = Field(default_factory=dict)
    coverage_ratio: float = Field(default=0.0, ge=0, le=1)
    duration_seconds: float = 0.0
    errors: list[str] = Field(default_factory=list)


class MonitoringAlert(BaseModel):
    """监控预警"""
    stock_code: str
    stock_name: str = ""
    alert_type: str  # threshold / trend / event
    severity: str  # info / warning / critical
    metric_name: str
    current_value: str
    threshold_value: str | None = None
    message: str
    triggered_at: datetime = Field(default_factory=datetime.now)


class WatchListItem(BaseModel):
    """跟踪列表项"""
    stock_code: str
    stock_name: str = ""
    recommendation: str = ""  # 最近一次投资建议
    added_at: datetime = Field(default_factory=datetime.now)
    last_updated_at: datetime | None = None
    last_report_date: datetime | None = None
    update_frequency: UpdateFrequency = UpdateFrequency.WEEKLY
    monitoring_points: list[str] = Field(default_factory=list)
    alert_thresholds: dict[str, Any] = Field(default_factory=dict)
    status: str = "normal"  # normal / warning / critical
    notes: str = ""


class WatchList(BaseModel):
    """跟踪列表"""
    items: list[WatchListItem] = Field(default_factory=list)
    updated_at: datetime = Field(default_factory=datetime.now)


class ResearchHistoryEntry(BaseModel):
    """研究历史条目"""
    stock_code: str
    stock_name: str = ""
    research_date: datetime
    depth: str = "standard"
    recommendation: str | None = None
    risk_level: str | None = None
    target_price_low: float | None = None
    target_price_high: float | None = None
    current_price: float | None = None
    report_path: str | None = None
    chart_pack_path: str | None = None
    evidence_pack_path: str | None = None
    agents_completed: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


# ============================================================
# Phase 8: 数据源补齐模型
# ============================================================


# --- Sprint 1: 公告与合规 ---


class Announcement(BaseModel):
    """公告披露记录"""
    title: str = Field(description="公告标题")
    announcement_type: str = Field(default="", description="公告类型: 年报/季报/半年报/临时公告/问询函等")
    announcement_date: date | None = Field(default=None, description="公告日期")
    announcement_id: str = Field(default="", description="公告ID")
    source: str = Field(default="cninfo", description="来源")
    url: str = Field(default="", description="原文链接")
    pdf_url: str = Field(default="", description="PDF原文链接")
    summary: str = Field(default="", description="摘要内容")
    excerpt: str = Field(default="", description="原文摘录")
    highlights: list[str] = Field(default_factory=list, description="原文关键信息")
    structured_fields: dict[str, Any] = Field(default_factory=dict, description="结构化抽取字段")
    page_count: int | None = Field(default=None, description="PDF页数")
    full_text_available: bool = Field(default=False, description="是否已提取原文")


class GovernanceData(BaseModel):
    """公司治理专项数据"""
    # 实控人
    actual_controller: str | None = Field(default=None, description="实际控制人")
    controller_type: str | None = Field(default=None, description="实控人性质")
    # 股权质押
    equity_pledge_ratio: float | None = Field(default=None, description="股权质押比例(%)")
    pledge_details: str = Field(default="", description="质押详情")
    # 关联交易
    related_transaction: str = Field(default="", description="关联交易评估")
    # 担保
    guarantee_info: str = Field(default="", description="担保信息")
    # 诉讼
    lawsuit_info: str = Field(default="", description="诉讼/仲裁信息")
    # 高管增减持
    management_changes: list[dict[str, Any]] = Field(default_factory=list, description="高管增减持记录")
    dividend_history: list[dict[str, Any]] = Field(default_factory=list, description="分红历史")
    buyback_history: list[dict[str, Any]] = Field(default_factory=list, description="回购历史")
    refinancing_history: list[dict[str, Any]] = Field(default_factory=list, description="再融资历史")


# --- Sprint 2: 研报与股东 ---


class ResearchReportSummary(BaseModel):
    """研报摘要（轻量）"""
    title: str = Field(description="研报标题")
    institution: str = Field(default="", description="研究机构")
    rating: str = Field(default="", description="评级: 买入/增持/中性/减持")
    target_price: float | None = Field(default=None, description="目标价")
    publish_date: date | None = Field(default=None, description="发布日期")
    industry: str = Field(default="", description="所属行业")
    pdf_url: str = Field(default="", description="PDF原文链接")
    summary: str = Field(default="", description="核心观点摘要(200字内)")
    excerpt: str = Field(default="", description="原文摘录")
    highlights: list[str] = Field(default_factory=list, description="原文关键信息")
    page_count: int | None = Field(default=None, description="PDF页数")


class PolicyDocument(BaseModel):
    """政策原文资料"""
    title: str = Field(description="政策标题")
    source: str = Field(default="gov.cn", description="来源站点")
    policy_date: date | None = Field(default=None, description="发布日期")
    issuing_body: str = Field(default="", description="发布机构")
    document_type: str = Field(default="", description="文种/分类")
    url: str = Field(default="", description="原文链接")
    summary: str = Field(default="", description="摘要内容")
    excerpt: str = Field(default="", description="原文摘录")
    highlights: list[str] = Field(default_factory=list, description="原文关键信息")
    matched_keywords: list[str] = Field(default_factory=list, description="命中关键词")


class ComplianceEvent(BaseModel):
    """官方合规/监管事件"""
    title: str = Field(description="事件标题")
    source: str = Field(default="", description="来源站点")
    publish_date: date | None = Field(default=None, description="发布日期")
    event_type: str = Field(default="", description="事件类型")
    severity: str = Field(default="", description="风险等级提示")
    related_party: str = Field(default="", description="关联主体")
    url: str = Field(default="", description="原文链接")
    summary: str = Field(default="", description="摘要")
    excerpt: str = Field(default="", description="原文摘录")
    raw_tags: list[str] = Field(default_factory=list, description="结构化标签")


class PatentRecord(BaseModel):
    """官方专利/技术资料"""
    title: str = Field(description="专利名称或技术标题")
    source: str = Field(default="", description="来源站点")
    publish_date: date | None = Field(default=None, description="公开/发布时间")
    patent_type: str = Field(default="", description="专利类型")
    application_no: str = Field(default="", description="申请号")
    patent_no: str = Field(default="", description="专利号")
    legal_status: str = Field(default="", description="法律状态")
    assignee: str = Field(default="", description="申请人/权利人")
    inventors: list[str] = Field(default_factory=list, description="发明人")
    summary: str = Field(default="", description="摘要")
    excerpt: str = Field(default="", description="原文摘录")
    url: str = Field(default="", description="原文链接")
    keywords: list[str] = Field(default_factory=list, description="技术关键词")


class ShareholderData(BaseModel):
    """股东结构数据"""
    top_shareholders: list[dict[str, Any]] = Field(default_factory=list, description="前十大股东")
    fund_holders: list[dict[str, Any]] = Field(default_factory=list, description="基金持仓")
    shareholder_count: int | None = Field(default=None, description="股东户数")
    shareholder_count_change: float | None = Field(default=None, description="股东户数变化率(%)")
    management_share_ratio: float | None = Field(default=None, description="管理层持股比例(%)")
    locked_shares_release: list[dict[str, Any]] = Field(default_factory=list, description="限售股解禁计划")


# --- Sprint 3: 行业增强与估值分位 ---


class IndustryEnhancedData(BaseModel):
    """行业增强数据"""
    industry_name: str = Field(description="行业名称")
    industry_code: str | None = Field(default=None, description="行业代码")
    industry_level: str | None = Field(default=None, description="行业分级: 一级行业/二级行业/三级行业")
    industry_description: str = Field(default="", description="行业简介")
    # 行业指数
    industry_index_close: float | None = Field(default=None, description="行业指数收盘价")
    industry_change_pct: float | None = Field(default=None, description="行业涨跌幅(%)")
    industry_pe: float | None = Field(default=None, description="行业整体PE")
    industry_pb: float | None = Field(default=None, description="行业整体PB")
    industry_turnover_volume: float | None = Field(default=None, description="行业成交量")
    industry_turnover_amount: float | None = Field(default=None, description="行业成交额(亿)")
    industry_fund_flow: float | None = Field(default=None, description="行业资金净流入(亿)")
    industry_rank: str = Field(default="", description="行业涨幅排名")
    rising_count: int | None = Field(default=None, description="上涨家数")
    falling_count: int | None = Field(default=None, description="下跌家数")
    industry_ytd_change_pct: float | None = Field(default=None, description="行业年初至今涨跌幅(%)")
    industry_1y_change_pct: float | None = Field(default=None, description="行业近一年涨跌幅(%)")
    # 行业排名
    stock_rank_in_industry: int | None = Field(default=None, description="个股在行业中排名")
    total_in_industry: int | None = Field(default=None, description="行业内公司总数")
    # 领涨/领跌
    industry_leaders: list[str] = Field(default_factory=list, description="行业龙头/领涨股")
    data_points: list[str] = Field(default_factory=list, description="关键行业数据点")


class ValuationPercentile(BaseModel):
    """估值历史分位数据"""
    pe_ttm_current: float | None = Field(default=None, description="当前PE(TTM)")
    pe_ttm_percentile: float | None = Field(default=None, description="PE历史分位(0-100)")
    pb_mrq_current: float | None = Field(default=None, description="当前PB(MRQ)")
    pb_mrq_percentile: float | None = Field(default=None, description="PB历史分位(0-100)")
    pe_3y_avg: float | None = Field(default=None, description="近3年PE均值")
    pe_5y_avg: float | None = Field(default=None, description="近5年PE均值")
    valuation_level: str = Field(default="", description="估值水平: 低估/合理/偏高/极高估")


# --- Sprint 4: 新闻舆情 ---


class NewsData(BaseModel):
    """新闻数据"""
    title: str = Field(description="新闻标题")
    content: str = Field(default="", description="新闻内容/摘要")
    source: str = Field(default="", description="来源")
    publish_time: str = Field(default="", description="发布时间")
    sentiment: str = Field(default="neutral", description="情绪: positive/neutral/negative")
    relevance: str = Field(default="", description="与标的关联度: high/medium/low")


class SentimentData(BaseModel):
    """舆情情绪数据"""
    news_count_7d: int = Field(default=0, description="近7天相关新闻数")
    positive_count: int = Field(default=0, description="正面新闻数")
    negative_count: int = Field(default=0, description="负面新闻数")
    neutral_count: int = Field(default=0, description="中性新闻数")
    sentiment_score: float | None = Field(default=None, description="情绪评分(-1到1)")
    hot_topics: list[str] = Field(default_factory=list, description="热门话题关键词")


# ============================================================
# Phase 9: 行业同业批量采集 & 交叉验证
# ============================================================


class PeerCompany(BaseModel):
    """同业公司"""
    stock_code: str = Field(description="同业股票代码, 如 002475")
    stock_name: str = Field(default="", description="同业股票名称")
    industry_sw: str = Field(default="", description="申万行业分类名称")
    industry_sw_code: str | None = Field(default=None, description="申万行业代码")
    industry_level: str = Field(default="二级行业", description="使用的行业级别: 一级行业/二级行业/三级行业")
    market_cap: float | None = Field(default=None, description="总市值(元)")
    rank_in_industry: int | None = Field(default=None, description="行业内市值排名")


class IndustryDataPoint(BaseModel):
    """从年报中提取的行业数据点"""
    metric_name: str = Field(description="指标: market_size/cagr/cr5/market_share")
    metric_value: float | None = Field(default=None, description="数值")
    metric_unit: str = Field(default="", description="单位: 亿元/%/家")
    year: str = Field(default="", description="参考年份, 如 2024")
    source_company: str = Field(default="", description="来源公司名称")
    source_company_code: str = Field(default="", description="来源公司代码")
    source_type: str = Field(default="annual_report", description="来源类型: annual_report/consulting/self_reported")
    consulting_firm: str = Field(default="", description="引用的咨询机构: IDC/沙利文/...")
    excerpt: str = Field(default="", description="原文摘录")
    pdf_url: str = Field(default="", description="来源PDF链接")


class CrossVerifiedMetric(BaseModel):
    """交叉验证后的单个指标"""
    metric_name: str = Field(description="指标名称")
    values: list[float] = Field(default_factory=list, description="各来源的值")
    sources: list[str] = Field(default_factory=list, description="来源公司名称列表")
    mean_value: float | None = Field(default=None, description="均值")
    median_value: float | None = Field(default=None, description="中位数")
    std_dev: float | None = Field(default=None, description="标准差")
    min_value: float | None = Field(default=None, description="最小值")
    max_value: float | None = Field(default=None, description="最大值")
    source_count: int = Field(default=0, description="独立来源数")
    confidence_score: float = Field(default=0.0, ge=0, le=1, description="置信度评分 0-1")
    consistency_flag: str = Field(default="insufficient", description="consistent/divergent/insufficient")
    consulting_sources: list[str] = Field(default_factory=list, description="引用的咨询机构列表")
    recommended_value: float | None = Field(default=None, description="推荐取值")
    evidence_refs: list[EvidenceRef] = Field(default_factory=list, description="证据引用")


class MetricSourceValue(BaseModel):
    """Generic numeric observation from one source for cross verification."""

    metric_name: str = Field(description="Metric name")
    metric_value: float | None = Field(default=None, description="Observed numeric value")
    metric_unit: str = Field(default="", description="Metric unit")
    source_name: str = Field(default="", description="Source label")
    source_type: str = Field(default="unknown", description="Source type")
    category: str = Field(default="", description="financial/realtime/valuation")
    reference_date: str = Field(default="", description="Observation date")
    excerpt: str = Field(default="", description="Short evidence excerpt")


class DataCrossVerification(BaseModel):
    """Cross verification result for company-level financial and market data."""

    stock_code: str = Field(default="", description="Target stock code")
    latest_report_date: str = Field(default="", description="Latest financial report date used")
    verified_metrics: list[CrossVerifiedMetric] = Field(default_factory=list, description="Cross-verified metrics")
    consistent_metrics: list[str] = Field(default_factory=list, description="Metrics with consistent multi-source observations")
    divergent_metrics: list[str] = Field(default_factory=list, description="Metrics with divergent multi-source observations")
    insufficient_metrics: list[str] = Field(default_factory=list, description="Metrics without enough cross-source support")
    overall_confidence: float = Field(default=0.0, ge=0, le=1, description="Overall confidence")
    summary: str = Field(default="", description="Human-readable summary")


class IndustryCrossVerification(BaseModel):
    """完整行业交叉验证结果"""
    industry_name: str = Field(default="", description="行业名称")
    industry_level: str = Field(default="", description="使用的SW行业级别")
    target_stock_code: str = Field(default="", description="目标股票代码")
    peer_count: int = Field(default=0, description="同业公司数量")
    peers: list[PeerCompany] = Field(default_factory=list, description="识别出的同业公司")
    data_points: list[IndustryDataPoint] = Field(default_factory=list, description="原始提取数据点")
    verified_metrics: list[CrossVerifiedMetric] = Field(default_factory=list, description="交叉验证指标")
    overall_confidence: float = Field(default=0.0, ge=0, le=1, description="整体置信度")
    collection_errors: list[str] = Field(default_factory=list, description="采集错误记录")
