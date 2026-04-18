"""Data quality, evidence, and degradation helpers."""

from __future__ import annotations

import math
import re
from typing import Any

from pydantic import BaseModel

from .models import (
    BlockingLevel,
    DataQualityStatus,
    EvidenceRef,
    FieldCollectionStatus,
    FieldContract,
    FieldEvidenceState,
    FieldPeriodType,
    FieldQualityTrace,
    FieldSourceValue,
    FieldValueState,
    ModuleQualityProfile,
    QualityGateDecision,
    RegressionBaselineSnapshot,
)

MODULE_SOURCE_PRIORITY: dict[str, list[str]] = {
    "stock_info": ["exchange", "company_profile", "akshare", "baostock"],
    "prices": ["exchange", "eastmoney", "akshare", "baostock"],
    "realtime": ["exchange", "eastmoney", "sina"],
    "financials": ["eastmoney", "akshare", "baostock", "ths", "annual_report"],
    "valuation": ["exchange", "baostock", "akshare"],
    "announcements": ["annual_report", "semi_annual_report", "quarterly_report", "announcement", "inquiry_letter"],
    "governance": ["cninfo", "csrc", "announcement", "company_profile"],
    "research_reports": ["research_report"],
    "shareholders": ["cninfo", "annual_report", "announcement", "akshare"],
    "industry_enhanced": ["stats_gov", "ministry", "industry_association", "policy_document", "peer_cross_verification"],
    "valuation_percentile": ["exchange", "baostock"],
    "news": ["news"],
    "sentiment": ["news"],
    "policy_documents": ["gov.cn", "ministry", "association"],
    "compliance_events": ["csrc", "credit_china", "national_enterprise_credit"],
    "patents": ["cnipa", "annual_report", "company_announcement"],
    "industry_peers": ["annual_report", "cninfo", "akshare"],
    "cross_verification": ["annual_report", "consulting", "cninfo"],
}

MODULE_MIN_COMPLETENESS: dict[str, float] = {
    "stock_info": 0.6,
    "prices": 0.5,
    "realtime": 0.5,
    "financials": 0.6,
    "valuation": 0.5,
    "announcements": 0.5,
    "governance": 0.4,
    "research_reports": 0.4,
    "shareholders": 0.4,
    "industry_enhanced": 0.4,
    "valuation_percentile": 0.5,
    "news": 0.4,
    "sentiment": 0.4,
    "policy_documents": 0.4,
    "compliance_events": 0.4,
    "patents": 0.4,
    "industry_peers": 0.3,
    "cross_verification": 0.3,
}

MODULE_AGGREGATION_WEIGHTS: dict[str, float] = {
    "stock_info": 0.9,
    "prices": 0.6,
    "realtime": 0.8,
    "financials": 2.5,
    "valuation": 1.0,
    "announcements": 1.2,
    "governance": 1.7,
    "research_reports": 0.4,
    "shareholders": 1.1,
    "industry_enhanced": 1.4,
    "valuation_percentile": 0.9,
    "news": 0.3,
    "sentiment": 0.2,
    "policy_documents": 0.8,
    "compliance_events": 1.2,
    "patents": 0.3,
    "cross_verification": 1.8,
}

_CONSISTENT_TOLERANCE = 0.15


def _contract(
    field: str,
    *,
    label: str,
    allowed_sources: list[str],
    unit: str,
    period_type: FieldPeriodType,
    blocking_level: BlockingLevel,
    notes: str = "",
) -> FieldContract:
    return FieldContract(
        field=field,
        label=label,
        allowed_sources=allowed_sources,
        unit=unit,
        period_type=period_type,
        blocking_level=blocking_level,
        notes=notes,
    )


FIELD_CONTRACTS: dict[str, FieldContract] = {
    "stock_info.main_business": _contract(
        "stock_info.main_business",
        label="主营业务",
        allowed_sources=["annual_report_structured", "annual_report_text", "stock_info"],
        unit="text",
        period_type=FieldPeriodType.ANNUAL,
        blocking_level=BlockingLevel.CRITICAL,
    ),
    "stock_info.business_model": _contract(
        "stock_info.business_model",
        label="商业模式标签",
        allowed_sources=["annual_report_structured", "annual_report_text"],
        unit="text",
        period_type=FieldPeriodType.ANNUAL,
        blocking_level=BlockingLevel.CORE,
    ),
    "stock_info.asset_model": _contract(
        "stock_info.asset_model",
        label="资产模式",
        allowed_sources=["annual_report_structured", "annual_report_text"],
        unit="label",
        period_type=FieldPeriodType.ANNUAL,
        blocking_level=BlockingLevel.CORE,
    ),
    "stock_info.client_type": _contract(
        "stock_info.client_type",
        label="客户类型",
        allowed_sources=["annual_report_structured", "annual_report_text"],
        unit="label",
        period_type=FieldPeriodType.ANNUAL,
        blocking_level=BlockingLevel.WARNING,
    ),
    "financials.latest.revenue": _contract(
        "financials.latest.revenue",
        label="最新营收",
        allowed_sources=["eastmoney_profit", "akshare_financial_abstract", "baostock_profit"],
        unit="cny",
        period_type=FieldPeriodType.CUMULATIVE,
        blocking_level=BlockingLevel.CRITICAL,
    ),
    "financials.latest.net_profit": _contract(
        "financials.latest.net_profit",
        label="最新归母净利润",
        allowed_sources=["eastmoney_profit", "akshare_financial_abstract", "baostock_profit"],
        unit="cny",
        period_type=FieldPeriodType.CUMULATIVE,
        blocking_level=BlockingLevel.CRITICAL,
    ),
    "financials.latest.deduct_net_profit": _contract(
        "financials.latest.deduct_net_profit",
        label="最新扣非净利润",
        allowed_sources=["eastmoney_profit", "akshare_financial_abstract", "annual_report_structured"],
        unit="cny",
        period_type=FieldPeriodType.CUMULATIVE,
        blocking_level=BlockingLevel.CORE,
    ),
    "financials.latest.operating_cashflow": _contract(
        "financials.latest.operating_cashflow",
        label="最新经营现金流",
        allowed_sources=["eastmoney_cashflow", "akshare_financial_abstract", "baostock_cashflow"],
        unit="cny",
        period_type=FieldPeriodType.CUMULATIVE,
        blocking_level=BlockingLevel.CRITICAL,
        notes="公告/PDF 抽取仅可作为旁证，不能覆盖核心现金流口径。",
    ),
    "financials.latest.free_cashflow": _contract(
        "financials.latest.free_cashflow",
        label="最新自由现金流",
        allowed_sources=["eastmoney_cashflow", "derived_per_share", "annual_report_structured"],
        unit="cny",
        period_type=FieldPeriodType.CUMULATIVE,
        blocking_level=BlockingLevel.CORE,
    ),
    "financials.latest.equity": _contract(
        "financials.latest.equity",
        label="最新归母权益",
        allowed_sources=["eastmoney_balance", "akshare_financial_abstract", "derived_per_share"],
        unit="cny",
        period_type=FieldPeriodType.QUARTER,
        blocking_level=BlockingLevel.CRITICAL,
    ),
    "financials.latest.goodwill_ratio": _contract(
        "financials.latest.goodwill_ratio",
        label="商誉/净资产",
        allowed_sources=["eastmoney_balance", "ths_goodwill_detail"],
        unit="%",
        period_type=FieldPeriodType.QUARTER,
        blocking_level=BlockingLevel.CORE,
    ),
    "financials.latest.contract_liabilities": _contract(
        "financials.latest.contract_liabilities",
        label="合同负债",
        allowed_sources=["eastmoney_balance", "annual_report_structured"],
        unit="cny",
        period_type=FieldPeriodType.QUARTER,
        blocking_level=BlockingLevel.CORE,
    ),
    "governance.actual_controller": _contract(
        "governance.actual_controller",
        label="实控人",
        allowed_sources=["stock_info", "stock_hold_control_cninfo", "annual_report_text"],
        unit="text",
        period_type=FieldPeriodType.LATEST,
        blocking_level=BlockingLevel.CORE,
    ),
    "governance.equity_pledge_ratio": _contract(
        "governance.equity_pledge_ratio",
        label="股权质押比例",
        allowed_sources=["stock_cg_equity_mortgage_cninfo", "annual_report_text"],
        unit="%",
        period_type=FieldPeriodType.LATEST,
        blocking_level=BlockingLevel.CORE,
    ),
    "governance.guarantee_info": _contract(
        "governance.guarantee_info",
        label="担保信息",
        allowed_sources=["stock_cg_guarantee_cninfo", "annual_report_text"],
        unit="records",
        period_type=FieldPeriodType.EVENT,
        blocking_level=BlockingLevel.CORE,
    ),
    "governance.lawsuit_info": _contract(
        "governance.lawsuit_info",
        label="诉讼信息",
        allowed_sources=["stock_cg_lawsuit_cninfo", "official_compliance", "annual_report_text"],
        unit="records",
        period_type=FieldPeriodType.EVENT,
        blocking_level=BlockingLevel.CORE,
    ),
    "governance.management_changes": _contract(
        "governance.management_changes",
        label="董监高变动/增减持",
        allowed_sources=["stock_hold_change_cninfo", "annual_report_text", "announcement"],
        unit="records",
        period_type=FieldPeriodType.EVENT,
        blocking_level=BlockingLevel.CORE,
    ),
    "shareholders.shareholder_count": _contract(
        "shareholders.shareholder_count",
        label="股东户数",
        allowed_sources=["stock_hold_num_cninfo"],
        unit="count",
        period_type=FieldPeriodType.QUARTER,
        blocking_level=BlockingLevel.CORE,
    ),
    "shareholders.management_share_ratio": _contract(
        "shareholders.management_share_ratio",
        label="董监高持股比例",
        allowed_sources=["annual_report_text", "stock_hold_change_cninfo"],
        unit="%",
        period_type=FieldPeriodType.ANNUAL,
        blocking_level=BlockingLevel.CORE,
    ),
    "industry_enhanced.industry_pe": _contract(
        "industry_enhanced.industry_pe",
        label="行业PE",
        allowed_sources=["stock_industry_pe_ratio_cninfo", "peer_cross_verification"],
        unit="x",
        period_type=FieldPeriodType.LATEST,
        blocking_level=BlockingLevel.WARNING,
    ),
    "industry_enhanced.industry_pb": _contract(
        "industry_enhanced.industry_pb",
        label="行业PB",
        allowed_sources=["stock_industry_pe_ratio_cninfo", "peer_cross_verification"],
        unit="x",
        period_type=FieldPeriodType.LATEST,
        blocking_level=BlockingLevel.WARNING,
    ),
    "industry_enhanced.industry_leaders": _contract(
        "industry_enhanced.industry_leaders",
        label="行业龙头",
        allowed_sources=["peer_cross_verification", "industry_board_info"],
        unit="list",
        period_type=FieldPeriodType.LATEST,
        blocking_level=BlockingLevel.CORE,
    ),
    "industry_enhanced.data_points": _contract(
        "industry_enhanced.data_points",
        label="行业数据点",
        allowed_sources=["peer_cross_verification", "industry_board_info", "stats_gov"],
        unit="list",
        period_type=FieldPeriodType.LATEST,
        blocking_level=BlockingLevel.CORE,
    ),
    "policy_documents.latest": _contract(
        "policy_documents.latest",
        label="最新官方政策",
        allowed_sources=["gov.cn"],
        unit="document",
        period_type=FieldPeriodType.EVENT,
        blocking_level=BlockingLevel.WARNING,
    ),
    "compliance_events.latest": _contract(
        "compliance_events.latest",
        label="最新官方合规事件",
        allowed_sources=["csrc", "credit_china", "national_enterprise_credit"],
        unit="event",
        period_type=FieldPeriodType.EVENT,
        blocking_level=BlockingLevel.WARNING,
    ),
    "patents.latest": _contract(
        "patents.latest",
        label="最新官方专利",
        allowed_sources=["cnipa"],
        unit="patent",
        period_type=FieldPeriodType.EVENT,
        blocking_level=BlockingLevel.WARNING,
    ),
}

FIELD_GATE_WEIGHTS: dict[str, float] = {
    "stock_info.main_business": 1.4,
    "stock_info.business_model": 0.8,
    "stock_info.asset_model": 0.7,
    "financials.latest.revenue": 1.8,
    "financials.latest.net_profit": 1.8,
    "financials.latest.deduct_net_profit": 1.0,
    "financials.latest.operating_cashflow": 1.8,
    "financials.latest.free_cashflow": 1.0,
    "financials.latest.equity": 1.6,
    "financials.latest.goodwill_ratio": 1.0,
    "financials.latest.contract_liabilities": 0.8,
    "governance.actual_controller": 0.8,
    "governance.equity_pledge_ratio": 1.0,
    "governance.guarantee_info": 1.0,
    "governance.lawsuit_info": 1.0,
    "governance.management_changes": 0.9,
    "shareholders.shareholder_count": 0.6,
    "shareholders.management_share_ratio": 0.9,
    "industry_enhanced.industry_leaders": 0.8,
    "industry_enhanced.data_points": 1.0,
}


def model_to_data(value: Any) -> Any:
    """Convert nested pydantic models into plain JSON-like data."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        return [model_to_data(item) for item in value]
    if isinstance(value, dict):
        return {key: model_to_data(item) for key, item in value.items()}
    return value


def is_meaningful(value: Any) -> bool:
    """Return True when a value contains non-empty substantive content."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, BaseModel):
        return is_meaningful(model_to_data(value))
    if isinstance(value, (list, tuple, set)):
        return any(is_meaningful(item) for item in value)
    if isinstance(value, dict):
        return any(is_meaningful(item) for item in value.values())
    return True


def _resolve_field(payload: Any, field_path: str) -> Any:
    current = model_to_data(payload)
    for part in field_path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _list_item_completeness(items: list[Any], required_fields: list[str], min_count: int) -> tuple[float, list[str]]:
    if not items:
        return 0.0, required_fields

    meaningful_items = [item for item in items if is_meaningful(item)]
    count_score = min(len(meaningful_items) / float(min_count), 1.0)

    first_item = meaningful_items[0] if meaningful_items else {}
    missing_fields = [field for field in required_fields if not is_meaningful(_resolve_field(first_item, field))]
    detail_score = 1.0
    if required_fields:
        detail_score = 1 - (len(missing_fields) / len(required_fields))

    completeness = round((count_score * 0.5) + (detail_score * 0.5), 2)
    return max(0.0, completeness), missing_fields


def build_module_profile(module_name: str, payload: Any) -> ModuleQualityProfile:
    """Build a standardized quality profile for one collected module."""
    data = model_to_data(payload)
    min_completeness = MODULE_MIN_COMPLETENESS.get(module_name, 0.5)
    source_priority = MODULE_SOURCE_PRIORITY.get(module_name, [])

    completeness = 0.0
    missing_fields: list[str] = []
    notes: list[str] = []

    if module_name == "stock_info":
        required = ["name", "industry_sw", "listing_date", "actual_controller", "main_business"]
        missing_fields = [field for field in required if not is_meaningful(_resolve_field(data, field))]
        completeness = round(1 - (len(missing_fields) / len(required)), 2)
    elif module_name == "prices":
        count = len(data or [])
        completeness = round(min(count / 250.0, 1.0), 2)
        if count < 60:
            missing_fields.append("price_history")
    elif module_name == "realtime":
        required = ["close", "market_cap", "pe_ttm"]
        missing_fields = [field for field in required if not is_meaningful(_resolve_field(data, field))]
        completeness = round(1 - (len(missing_fields) / len(required)), 2)
    elif module_name == "financials":
        required = ["report_date", "revenue", "net_profit", "operating_cashflow", "equity", "goodwill_ratio"]
        completeness, missing_fields = _list_item_completeness(data or [], required, min_count=4)
    elif module_name == "valuation":
        required = ["date", "pe_ttm", "pb_mrq"]
        completeness, missing_fields = _list_item_completeness(data or [], required, min_count=12)
    elif module_name == "announcements":
        required = ["title", "announcement_date", "excerpt"]
        completeness, missing_fields = _list_item_completeness(data or [], required, min_count=3)
    elif module_name == "governance":
        required = [
            "actual_controller",
            "equity_pledge_ratio",
            "related_transaction",
            "guarantee_info",
            "lawsuit_info",
            "management_changes",
            "dividend_history",
            "buyback_history",
            "refinancing_history",
        ]
        missing_fields = [field for field in required if not is_meaningful(_resolve_field(data, field))]
        completeness = round(1 - (len(missing_fields) / len(required)), 2)
        if is_meaningful(_resolve_field(data, "actual_controller")) and completeness < min_completeness:
            notes.append("仅有实控人/股东层信息，治理证据不足")
    elif module_name == "research_reports":
        required = ["title", "institution", "publish_date", "summary"]
        completeness, missing_fields = _list_item_completeness(data or [], required, min_count=3)
    elif module_name == "shareholders":
        required = ["top_shareholders", "shareholder_count", "management_share_ratio"]
        missing_fields = [field for field in required if not is_meaningful(_resolve_field(data, field))]
        completeness = round(1 - (len(missing_fields) / len(required)), 2)
    elif module_name == "industry_enhanced":
        required = ["industry_name", "industry_pe", "industry_pb", "industry_leaders", "data_points"]
        missing_fields = [field for field in required if not is_meaningful(_resolve_field(data, field))]
        completeness = round(1 - (len(missing_fields) / len(required)), 2)
        if not is_meaningful(_resolve_field(data, "industry_name")):
            notes.append("行业增强信息仅包含空壳成功，缺少实质字段")
    elif module_name == "valuation_percentile":
        required = ["pe_ttm_current", "pe_ttm_percentile", "pb_mrq_current", "pb_mrq_percentile"]
        missing_fields = [field for field in required if not is_meaningful(_resolve_field(data, field))]
        completeness = round(1 - (len(missing_fields) / len(required)), 2)
    elif module_name == "news":
        required = ["title", "publish_time", "content"]
        completeness, missing_fields = _list_item_completeness(data or [], required, min_count=5)
    elif module_name == "sentiment":
        required = ["news_count_7d", "sentiment_score"]
        missing_fields = [field for field in required if not is_meaningful(_resolve_field(data, field))]
        completeness = round(1 - (len(missing_fields) / len(required)), 2)
    elif module_name == "policy_documents":
        required = ["title", "policy_date", "issuing_body", "excerpt"]
        completeness, missing_fields = _list_item_completeness(data or [], required, min_count=2)
    elif module_name == "compliance_events":
        required = ["title", "publish_date", "source", "summary"]
        completeness, missing_fields = _list_item_completeness(data or [], required, min_count=1)
    elif module_name == "patents":
        required = ["title", "publish_date", "source", "patent_type"]
        completeness, missing_fields = _list_item_completeness(data or [], required, min_count=1)
    elif module_name == "cross_verification":
        metrics = list(_resolve_field(data, "verified_metrics") or [])
        if not metrics:
            missing_fields = ["verified_metrics"]
            completeness = 0.0
        else:
            consistent = sum(1 for item in metrics if _resolve_field(item, "consistency_flag") == "consistent")
            divergent = sum(1 for item in metrics if _resolve_field(item, "consistency_flag") == "divergent")
            insufficient = sum(1 for item in metrics if _resolve_field(item, "consistency_flag") == "insufficient")
            metric_count = len(metrics)
            count_score = min(metric_count / 6.0, 1.0)
            consistency_score = (consistent + 0.5 * insufficient) / float(metric_count)
            completeness = round((count_score * 0.4) + (consistency_score * 0.6), 2)
            missing_fields = [f"divergent:{_resolve_field(item, 'metric_name')}" for item in metrics if _resolve_field(item, "consistency_flag") == "divergent"][:5]
            if divergent:
                notes.append("关键指标存在多源分歧，需回溯源表复核")
            if not consistent:
                notes.append("暂未形成充分的多源一致性证据")
    else:
        completeness = 1.0 if is_meaningful(data) else 0.0

    if not is_meaningful(data):
        status = DataQualityStatus.FAILED
        completeness = 0.0
    elif completeness >= min_completeness:
        status = DataQualityStatus.OK
    else:
        status = DataQualityStatus.PARTIAL

    return ModuleQualityProfile(
        status=status,
        completeness=round(completeness, 2),
        missing_fields=missing_fields,
        source_priority=source_priority,
        evidence_refs=extract_evidence_refs(module_name, data),
        notes=notes,
    )


def build_module_profiles(data: dict[str, Any]) -> dict[str, ModuleQualityProfile]:
    """Build quality profiles for all known modules."""
    modules = [
        "stock_info",
        "prices",
        "realtime",
        "financials",
        "valuation",
        "announcements",
        "governance",
        "research_reports",
        "shareholders",
        "industry_enhanced",
        "valuation_percentile",
        "news",
        "sentiment",
        "policy_documents",
        "compliance_events",
        "patents",
        "cross_verification",
    ]
    return {module: build_module_profile(module, data.get(module)) for module in modules}


def aggregate_quality(profiles: dict[str, ModuleQualityProfile]) -> tuple[DataQualityStatus, float, float, list[str], list[EvidenceRef], list[str]]:
    """Aggregate module profiles into overall data quality metrics."""
    if not profiles:
        return DataQualityStatus.FAILED, 0.0, 0.0, [], [], []

    total_weight = sum(MODULE_AGGREGATION_WEIGHTS.get(name, 1.0) for name in profiles) or 1.0
    completeness = round(
        sum(profile.completeness * MODULE_AGGREGATION_WEIGHTS.get(name, 1.0) for name, profile in profiles.items()) / total_weight,
        2,
    )
    ok_weight = sum(
        MODULE_AGGREGATION_WEIGHTS.get(name, 1.0)
        for name, profile in profiles.items()
        if profile.status == DataQualityStatus.OK
    )
    coverage_ratio = round(ok_weight / total_weight, 2)

    missing_fields: list[str] = []
    evidence_refs: list[EvidenceRef] = []
    source_priority: list[str] = []
    for module_name, profile in profiles.items():
        missing_fields.extend([f"{module_name}.{field}" for field in profile.missing_fields[:5]])
        evidence_refs.extend(profile.evidence_refs[:2])
        for source in profile.source_priority:
            if source not in source_priority:
                source_priority.append(source)

    if coverage_ratio >= 0.78 and completeness >= 0.7:
        status = DataQualityStatus.OK
    elif coverage_ratio >= 0.35 or completeness >= 0.35:
        status = DataQualityStatus.PARTIAL
    else:
        status = DataQualityStatus.FAILED

    return status, completeness, coverage_ratio, missing_fields[:30], evidence_refs[:20], source_priority


def _latest_financial(data: dict[str, Any]) -> dict[str, Any]:
    financials = [item for item in list(data.get("financials", []) or []) if isinstance(item, dict)]
    return financials[0] if financials else {}


def _field_module(field_name: str) -> str:
    return field_name.split(".", 1)[0]


def _normalize_collection_status(raw_statuses: Any) -> dict[str, FieldCollectionStatus]:
    normalized: dict[str, FieldCollectionStatus] = {}
    for key, value in dict(raw_statuses or {}).items():
        if isinstance(value, FieldCollectionStatus):
            normalized[key] = value
        elif isinstance(value, dict):
            payload = dict(value)
            payload.setdefault("field", key)
            normalized[key] = FieldCollectionStatus(**payload)
    return normalized


def _extract_contract_value(field_name: str, data: dict[str, Any]) -> Any:
    if field_name.startswith("financials.latest."):
        return _latest_financial(data).get(field_name.rsplit(".", 1)[-1])
    if field_name == "policy_documents.latest":
        items = [item for item in data.get("policy_documents", []) if isinstance(item, dict)]
        return items[0] if items else None
    if field_name == "compliance_events.latest":
        items = [item for item in data.get("compliance_events", []) if isinstance(item, dict)]
        return items[0] if items else None
    if field_name == "patents.latest":
        items = [item for item in data.get("patents", []) if isinstance(item, dict)]
        return items[0] if items else None
    return _resolve_field(data, field_name)


def _extract_report_period(field_name: str, data: dict[str, Any]) -> str:
    if field_name.startswith("financials.latest."):
        return str(_latest_financial(data).get("report_date") or "")
    if field_name == "policy_documents.latest":
        item = _extract_contract_value(field_name, data) or {}
        return str(item.get("policy_date") or "")
    if field_name == "compliance_events.latest":
        item = _extract_contract_value(field_name, data) or {}
        return str(item.get("publish_date") or "")
    if field_name == "patents.latest":
        item = _extract_contract_value(field_name, data) or {}
        return str(item.get("publish_date") or "")
    if field_name.startswith(("governance.", "shareholders.", "industry_enhanced.")):
        return str(_latest_financial(data).get("report_date") or "")
    return ""


def _cross_metric_name(field_name: str) -> str | None:
    mapping = {
        "financials.latest.revenue": "revenue",
        "financials.latest.net_profit": "net_profit",
        "financials.latest.operating_cashflow": "operating_cashflow",
        "financials.latest.free_cashflow": "free_cashflow",
        "financials.latest.equity": "equity",
        "financials.latest.goodwill_ratio": "goodwill_ratio",
    }
    return mapping.get(field_name)


def _main_business_announcement_priority(item: dict[str, Any]) -> tuple[int, str]:
    text = f"{item.get('announcement_type', '')} {item.get('title', '')}"
    if "年报" in text or ("年度报告" in text and "半年度报告" not in text):
        return 0, str(item.get("announcement_date") or "")
    if "半年" in text or "半年报" in text or "中期报告" in text:
        return 1, str(item.get("announcement_date") or "")
    if "季度" in text or "季报" in text:
        return 2, str(item.get("announcement_date") or "")
    return 3, str(item.get("announcement_date") or "")


def _is_plausible_main_business_text(value: Any) -> bool:
    text = str(value or "").strip()
    if not is_meaningful(text):
        return False
    noise_markers = (
        "相关内容",
        "数据统计口径",
        "主营业务数据",
        "主营业务变化情况",
        "营业收入",
        "净利润",
    )
    if any(marker in text for marker in noise_markers):
        return False
    if re.search(r"\d{4}年\d{1,2}月", text):
        return False
    return True


def _normalize_source_values(field_name: str, data: dict[str, Any], contract: FieldContract, value: Any) -> list[FieldSourceValue]:
    source_values: list[FieldSourceValue] = []
    if field_name.startswith("financials.latest."):
        latest = _latest_financial(data)
        raw_data = dict(latest.get("raw_data") or {})
        field_key = field_name.rsplit(".", 1)[-1]
        for source_name, payload in dict(raw_data.get("source_values") or {}).items():
            if not isinstance(payload, dict):
                continue
            metrics = dict(payload.get("metrics") or {})
            if field_key not in metrics:
                continue
            source_values.append(
                FieldSourceValue(
                    source_name=source_name,
                    source_type=str(payload.get("source_type") or ""),
                    reference_date=str(payload.get("reference_date") or latest.get("report_date") or ""),
                    value=metrics.get(field_key),
                    unit=contract.unit,
                    period_type=contract.period_type.value,
                )
            )
        return source_values

    if field_name == "stock_info.main_business":
        stock_info = data.get("stock_info", {}) or {}
        if is_meaningful(stock_info.get("main_business")):
            source_values.append(
                FieldSourceValue(
                    source_name="stock_info",
                    source_type="company_profile",
                    value=stock_info.get("main_business"),
                    unit=contract.unit,
                    period_type=contract.period_type.value,
                )
            )
        for item in sorted(
            [entry for entry in data.get("announcements", []) if isinstance(entry, dict)],
            key=_main_business_announcement_priority,
        ):
            structured = dict(item.get("structured_fields") or {})
            stock_section = dict(structured.get("stock_info") or {})
            candidate = stock_section.get("main_business")
            if not _is_plausible_main_business_text(candidate):
                continue
            source_values.append(
                FieldSourceValue(
                    source_name="annual_report_structured",
                    source_type="annual_report",
                    reference_date=str(item.get("announcement_date") or ""),
                    value=candidate,
                    unit=contract.unit,
                    period_type=contract.period_type.value,
                    excerpt=str(item.get("title") or ""),
                )
            )
            break
        return source_values

    if field_name == "policy_documents.latest":
        item = value if isinstance(value, dict) else {}
        if item:
            source_values.append(
                FieldSourceValue(
                    source_name=str(item.get("source") or "gov.cn"),
                    source_type="official_document",
                    reference_date=str(item.get("policy_date") or ""),
                    value=item.get("title"),
                    unit=contract.unit,
                    period_type=contract.period_type.value,
                    excerpt=str(item.get("excerpt") or item.get("summary") or "")[:180],
                )
            )
        return source_values

    if field_name == "compliance_events.latest":
        item = value if isinstance(value, dict) else {}
        if item:
            source_values.append(
                FieldSourceValue(
                    source_name=str(item.get("source") or "official"),
                    source_type="official_event",
                    reference_date=str(item.get("publish_date") or ""),
                    value=item.get("title"),
                    unit=contract.unit,
                    period_type=contract.period_type.value,
                    excerpt=str(item.get("excerpt") or item.get("summary") or "")[:180],
                )
            )
        return source_values

    if field_name == "patents.latest":
        item = value if isinstance(value, dict) else {}
        if item:
            source_values.append(
                FieldSourceValue(
                    source_name=str(item.get("source") or "cnipa"),
                    source_type="official_patent",
                    reference_date=str(item.get("publish_date") or ""),
                    value=item.get("title"),
                    unit=contract.unit,
                    period_type=contract.period_type.value,
                    excerpt=str(item.get("excerpt") or item.get("summary") or "")[:180],
                )
            )
        return source_values

    if field_name in {"industry_enhanced.data_points", "industry_enhanced.industry_leaders"}:
        if isinstance(value, list) and value:
            source_values.append(
                FieldSourceValue(
                    source_name="industry_enhanced",
                    source_type="industry_context",
                    reference_date=_extract_report_period(field_name, data),
                    value=value[:6],
                    unit=contract.unit,
                    period_type=contract.period_type.value,
                )
            )
        return source_values

    if is_meaningful(value):
        source_values.append(
            FieldSourceValue(
                source_name=_field_module(field_name),
                source_type="structured",
                reference_date=_extract_report_period(field_name, data),
                value=value,
                unit=contract.unit,
                period_type=contract.period_type.value,
            )
        )
    return source_values


def _resolve_value_state(
    field_name: str,
    contract: FieldContract,
    value: Any,
    data: dict[str, Any],
    field_statuses: dict[str, FieldCollectionStatus],
) -> FieldValueState:
    field_status = field_statuses.get(field_name)
    if field_status and field_status.value_state != FieldValueState.MISSING:
        return field_status.value_state
    if is_meaningful(value):
        return FieldValueState.PRESENT
    module_status = str((data.get("collection_status") or {}).get(_field_module(field_name)) or "").strip().lower()
    if module_status == "failed":
        return FieldValueState.COLLECTION_FAILED
    if contract.blocking_level == BlockingLevel.NONE and field_name.endswith(".latest") and not is_meaningful(value):
        return FieldValueState.MISSING
    return FieldValueState.MISSING


def _normalize_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _numeric_consistency(source_values: list[FieldSourceValue]) -> FieldEvidenceState:
    numeric_values = [_normalize_number(item.value) for item in source_values]
    numeric_values = [value for value in numeric_values if value is not None]
    if len(numeric_values) < 2:
        return FieldEvidenceState.SINGLE_SOURCE if numeric_values else FieldEvidenceState.UNKNOWN
    mean_value = sum(numeric_values) / len(numeric_values)
    if mean_value == 0:
        spread = max(abs(value) for value in numeric_values)
        return FieldEvidenceState.CONSISTENT if spread == 0 else FieldEvidenceState.DIVERGENT
    variance = sum((value - mean_value) ** 2 for value in numeric_values) / len(numeric_values)
    std_dev = math.sqrt(variance)
    return FieldEvidenceState.CONSISTENT if std_dev / abs(mean_value) <= _CONSISTENT_TOLERANCE else FieldEvidenceState.DIVERGENT


def _evidence_state_from_cross(field_name: str, data: dict[str, Any]) -> FieldEvidenceState | None:
    metric_name = _cross_metric_name(field_name)
    if not metric_name:
        return None
    cross = data.get("cross_verification") if isinstance(data.get("cross_verification"), dict) else {}
    for item in list(cross.get("verified_metrics") or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("metric_name") or "") != metric_name:
            continue
        flag = str(item.get("consistency_flag") or "").strip().lower()
        return {
            "consistent": FieldEvidenceState.CONSISTENT,
            "divergent": FieldEvidenceState.DIVERGENT,
            "insufficient": FieldEvidenceState.INSUFFICIENT,
        }.get(flag, FieldEvidenceState.UNKNOWN)
    return None


def _resolve_evidence_state(
    field_name: str,
    value_state: FieldValueState,
    source_values: list[FieldSourceValue],
    data: dict[str, Any],
) -> FieldEvidenceState:
    if value_state == FieldValueState.VERIFIED_ABSENT:
        return FieldEvidenceState.VERIFIED_ABSENT
    if value_state == FieldValueState.COLLECTION_FAILED:
        return FieldEvidenceState.COLLECTION_FAILED
    if value_state == FieldValueState.NOT_APPLICABLE:
        return FieldEvidenceState.NOT_APPLICABLE
    if value_state != FieldValueState.PRESENT:
        return FieldEvidenceState.INSUFFICIENT

    from_cross = _evidence_state_from_cross(field_name, data)
    if from_cross is not None:
        return from_cross
    if len(source_values) <= 1:
        return FieldEvidenceState.SINGLE_SOURCE
    if all(_normalize_number(item.value) is not None for item in source_values):
        return _numeric_consistency(source_values)
    unique_values = {str(item.value).strip() for item in source_values if is_meaningful(item.value)}
    return FieldEvidenceState.CONSISTENT if len(unique_values) == 1 else FieldEvidenceState.DIVERGENT


def _score_field_quality(value_state: FieldValueState, evidence_state: FieldEvidenceState) -> float:
    value_scores = {
        FieldValueState.PRESENT: 1.0,
        FieldValueState.VERIFIED_ABSENT: 0.85,
        FieldValueState.NOT_APPLICABLE: 1.0,
        FieldValueState.COLLECTION_FAILED: 0.0,
        FieldValueState.MISSING: 0.0,
    }
    evidence_scores = {
        FieldEvidenceState.CONSISTENT: 1.0,
        FieldEvidenceState.SINGLE_SOURCE: 0.72,
        FieldEvidenceState.INSUFFICIENT: 0.45,
        FieldEvidenceState.DIVERGENT: 0.2,
        FieldEvidenceState.VERIFIED_ABSENT: 0.85,
        FieldEvidenceState.COLLECTION_FAILED: 0.0,
        FieldEvidenceState.NOT_APPLICABLE: 1.0,
        FieldEvidenceState.UNKNOWN: 0.35,
    }
    return round((value_scores.get(value_state, 0.0) * 0.55) + (evidence_scores.get(evidence_state, 0.0) * 0.45), 2)


def build_field_quality_map(
    data: dict[str, Any],
    *,
    contracts: dict[str, FieldContract] | None = None,
) -> dict[str, FieldQualityTrace]:
    """Build field-level quality traces from cleaned/raw data."""
    normalized_data = model_to_data(data)
    contracts = contracts or FIELD_CONTRACTS
    field_statuses = _normalize_collection_status(normalized_data.get("field_statuses"))

    traces: dict[str, FieldQualityTrace] = {}
    for field_name, contract in contracts.items():
        value = _extract_contract_value(field_name, normalized_data)
        report_period = _extract_report_period(field_name, normalized_data)
        source_values = _normalize_source_values(field_name, normalized_data, contract, value)
        value_state = _resolve_value_state(field_name, contract, value, normalized_data, field_statuses)
        evidence_state = _resolve_evidence_state(field_name, value_state, source_values, normalized_data)
        notes: list[str] = []
        field_status = field_statuses.get(field_name)
        if field_status and field_status.note:
            notes.append(field_status.note)
        if contract.notes:
            notes.append(contract.notes)
        if value_state == FieldValueState.MISSING:
            notes.append("字段缺失，未形成稳定证据。")

        traces[field_name] = FieldQualityTrace(
            field=field_name,
            label=contract.label,
            value=value,
            allowed_sources=list(contract.allowed_sources),
            unit=contract.unit,
            period_type=contract.period_type,
            blocking_level=contract.blocking_level,
            report_period=report_period,
            value_state=value_state,
            evidence_state=evidence_state,
            source_count=len(source_values),
            confidence_score=_score_field_quality(value_state, evidence_state),
            source_values=source_values,
            notes=notes[:4],
        )
    return traces


def contract_dicts(contracts: dict[str, FieldContract] | None = None) -> dict[str, dict[str, Any]]:
    """Serialize field contracts into plain dicts."""
    contracts = contracts or FIELD_CONTRACTS
    return {key: value.model_dump(mode="json") for key, value in contracts.items()}


def contract_models(contracts: dict[str, FieldContract] | None = None) -> dict[str, FieldContract]:
    """Return deep-copied field contract models for typed payloads."""
    contracts = contracts or FIELD_CONTRACTS
    return {key: value.model_copy(deep=True) for key, value in contracts.items()}


def field_quality_dicts(traces: dict[str, FieldQualityTrace]) -> dict[str, dict[str, Any]]:
    """Serialize field quality traces into plain dicts."""
    return {key: value.model_dump(mode="json") for key, value in traces.items()}


def normalize_field_quality_map(raw_map: dict[str, Any] | None) -> dict[str, FieldQualityTrace]:
    """Convert raw dicts into FieldQualityTrace models."""
    traces: dict[str, FieldQualityTrace] = {}
    for key, value in dict(raw_map or {}).items():
        if isinstance(value, FieldQualityTrace):
            traces[key] = value
        elif isinstance(value, dict):
            payload = dict(value)
            payload.setdefault("field", key)
            traces[key] = FieldQualityTrace(**payload)
    return traces


def get_field_contract(field_name: str) -> FieldContract | None:
    return FIELD_CONTRACTS.get(field_name)


def get_field_quality_trace(context: dict[str, Any], field_name: str) -> FieldQualityTrace | None:
    traces = normalize_field_quality_map(context.get("field_quality"))
    if field_name in traces:
        return traces[field_name]
    if field_name in FIELD_CONTRACTS:
        return build_field_quality_map(context).get(field_name)
    return None


def core_evidence_score(traces: dict[str, FieldQualityTrace] | dict[str, Any] | None) -> float:
    """Weighted core evidence score used by the quality gate."""
    field_map = normalize_field_quality_map(traces if isinstance(traces, dict) else {})
    if not field_map:
        return 0.0

    weighted_sum = 0.0
    total_weight = 0.0
    for field_name, trace in field_map.items():
        if trace.blocking_level == BlockingLevel.NONE:
            continue
        weight = FIELD_GATE_WEIGHTS.get(field_name, 0.6 if trace.blocking_level == BlockingLevel.WARNING else 1.0)
        weighted_sum += float(trace.confidence_score) * weight
        total_weight += weight
    return round(weighted_sum / total_weight, 2) if total_weight else 0.0


def _blocking_trace(trace: FieldQualityTrace) -> bool:
    if trace.blocking_level not in {BlockingLevel.CORE, BlockingLevel.CRITICAL}:
        return False
    if trace.value_state in {FieldValueState.COLLECTION_FAILED, FieldValueState.MISSING}:
        return True
    return trace.evidence_state in {FieldEvidenceState.COLLECTION_FAILED, FieldEvidenceState.DIVERGENT}


def _weak_trace(trace: FieldQualityTrace) -> bool:
    if trace.blocking_level not in {BlockingLevel.CORE, BlockingLevel.CRITICAL}:
        return False
    if trace.value_state != FieldValueState.PRESENT:
        return False
    return trace.evidence_state in {FieldEvidenceState.SINGLE_SOURCE, FieldEvidenceState.INSUFFICIENT}


def build_quality_gate_decision(
    *,
    cleaned_data: dict[str, Any],
    peer_cross_verification: dict[str, Any],
    depth: str,
) -> QualityGateDecision:
    """Build the dual-gate decision using core evidence score plus blocking fields."""
    traces = normalize_field_quality_map(cleaned_data.get("field_quality"))
    if not traces:
        traces = build_field_quality_map(cleaned_data)

    score = core_evidence_score(traces)
    coverage = float(cleaned_data.get("coverage_ratio", 0.0) or 0.0)
    company_cross = cleaned_data.get("cross_verification", {}) if isinstance(cleaned_data, dict) else {}
    company_confidence = float(company_cross.get("overall_confidence", 0.0) or 0.0)
    divergent_metrics = [str(item) for item in company_cross.get("divergent_metrics", [])[:5]]
    peer_status = str(peer_cross_verification.get("status") or "").strip().lower()
    peer_verified = len(peer_cross_verification.get("verified_metrics", []) or [])
    peer_points = len(peer_cross_verification.get("data_points", []) or [])
    peer_count = int(peer_cross_verification.get("peer_count") or 0)

    blocking_fields = [field for field, trace in traces.items() if _blocking_trace(trace)]
    weak_fields = [field for field, trace in traces.items() if _weak_trace(trace)]
    reasons: list[str] = []
    consistency_notes: list[str] = []

    threshold = 0.72 if depth == "deep" else 0.7 if depth == "standard" else 0.6
    if score < threshold:
        reasons.append(f"核心证据分仅 {score:.0%}，低于 {depth} 深度阈值 {threshold:.0%}")
    if blocking_fields:
        reasons.append(f"存在阻断字段未满足证据要求: {', '.join(blocking_fields[:6])}")
    if coverage < 0.55:
        reasons.append(f"整体覆盖率仅 {coverage:.0%}，说明非核心证据层仍存在明显缺口")
    if company_cross and company_confidence < 0.55:
        detail = f"公司多源交叉验证置信度仅 {company_confidence:.0%}"
        if divergent_metrics:
            detail += f"，且存在分歧指标: {', '.join(divergent_metrics)}"
        reasons.append(detail)
    if weak_fields:
        consistency_notes.append(f"以下核心字段仅单源或弱证据，后续评分需自动降级: {', '.join(weak_fields[:6])}")

    industry_sw = str((cleaned_data.get("stock_info") or {}).get("industry_sw") or "").strip()
    if depth in {"standard", "deep"} and industry_sw:
        if peer_status in {"insufficient", "failed"}:
            reasons.append(f"同业交叉验证未形成有效结果（{peer_count}家同业，{peer_points}个数据点，{peer_verified}个验证指标）")
        elif peer_count > 0 and peer_verified == 0:
            reasons.append(f"同业交叉验证未形成有效指标（{peer_count}家同业，{peer_points}个数据点，{peer_verified}个验证指标）")

    return QualityGateDecision(
        blocked=bool(reasons),
        core_evidence_score=score,
        blocking_fields=blocking_fields,
        weak_fields=weak_fields,
        reasons=reasons,
        consistency_notes=consistency_notes,
        coverage_ratio=coverage,
        company_cross_confidence=company_confidence,
        peer_verified=peer_verified,
    )


def build_process_consistency_notes(
    *,
    screening: dict[str, Any] | None = None,
    valuation: dict[str, Any] | None = None,
    conclusion: dict[str, Any] | None = None,
) -> list[str]:
    """Generate explicit explanations when upstream and downstream judgments diverge."""
    screening = screening or {}
    valuation = valuation or {}
    conclusion = conclusion or {}

    verdict = str(screening.get("verdict") or "").strip()
    valuation_level = str(valuation.get("valuation_level") or "").strip()
    recommendation = str(conclusion.get("recommendation") or "").strip()
    notes: list[str] = []

    if verdict == "通过" and valuation_level in {"高估", "严重高估"} and recommendation in {"观望", "卖出"}:
        notes.append("初筛通过仅代表未发现刚性硬伤，不代表当前估值具备买点；由于估值已高估，最终结论下调为观望/卖出。")
    if verdict == "重点警示" and recommendation.startswith("买入"):
        notes.append("初筛存在重点警示，但后续仍给出买入判断，必须持续跟踪警示项是否被新的证据消化。")
    if valuation_level == "低估" and recommendation == "卖出":
        notes.append("估值虽显示低估，但治理/财务/合规风险已压倒估值吸引力，因此最终结论仍为卖出。")
    return notes


def build_regression_baseline_snapshot(
    *,
    stock_code: str,
    stock_name: str,
    depth: str,
    cleaned_data: dict[str, Any],
    quality_gate: QualityGateDecision | dict[str, Any] | None = None,
    screening: dict[str, Any] | None = None,
    valuation: dict[str, Any] | None = None,
    conclusion: dict[str, Any] | None = None,
    warning_count: int = 0,
) -> RegressionBaselineSnapshot:
    """Build the structured baseline snapshot saved for each run."""
    gate = quality_gate if isinstance(quality_gate, QualityGateDecision) else QualityGateDecision(**dict(quality_gate or {}))
    screening = screening or {}
    valuation = valuation or {}
    conclusion = conclusion or {}
    cross = cleaned_data.get("cross_verification", {}) if isinstance(cleaned_data, dict) else {}
    consistency_notes = build_process_consistency_notes(
        screening=screening,
        valuation=valuation,
        conclusion=conclusion,
    )
    return RegressionBaselineSnapshot(
        stock_code=stock_code,
        stock_name=stock_name,
        depth=depth,
        coverage_ratio=float(cleaned_data.get("coverage_ratio", 0.0) or 0.0),
        completeness=float(cleaned_data.get("completeness", 0.0) or 0.0),
        core_evidence_score=float(gate.core_evidence_score),
        missing_fields=list(cleaned_data.get("missing_fields", []) or []),
        blocking_fields=list(gate.blocking_fields or []),
        divergent_fields=list(cross.get("divergent_metrics", []) or []),
        warning_count=warning_count,
        initial_verdict=str(screening.get("verdict") or ""),
        final_recommendation=str(conclusion.get("recommendation") or ""),
        quality_gate_blocked=bool(gate.blocked),
        quality_gate_reasons=list(gate.reasons or []),
        consistency_notes=consistency_notes + list(gate.consistency_notes or []),
    )


def extract_evidence_refs(module_name: str, payload: Any) -> list[EvidenceRef]:
    """Extract a small set of traceable evidence refs from a module payload."""
    data = model_to_data(payload)
    refs: list[EvidenceRef] = []

    if not is_meaningful(data):
        return refs

    if module_name in {"announcements", "research_reports", "policy_documents", "news", "compliance_events", "patents"} and isinstance(data, list):
        for item in data[:3]:
            if not isinstance(item, dict):
                continue
            refs.append(
                EvidenceRef(
                    source=str(item.get("source") or item.get("institution") or module_name),
                    source_priority=0,
                    title=str(item.get("title") or ""),
                    field="excerpt",
                    excerpt=str(item.get("excerpt") or item.get("summary") or item.get("content") or "")[:280],
                    url=str(item.get("url") or item.get("pdf_url") or ""),
                    reference_date=str(
                        item.get("announcement_date")
                        or item.get("publish_date")
                        or item.get("policy_date")
                        or item.get("publish_time")
                        or item.get("published_at")
                        or ""
                    ),
                )
            )
        return refs

    if module_name == "financials" and isinstance(data, list) and data:
        latest = data[0] if isinstance(data[0], dict) else {}
        refs.append(
            EvidenceRef(
                source=str(latest.get("source") or "financials"),
                source_priority=0,
                title="latest_financial_statement",
                field="report_date",
                excerpt=(
                    f"报告期={latest.get('report_date')}；营收={latest.get('revenue')}；"
                    f"净利润={latest.get('net_profit')}；经营现金流={latest.get('operating_cashflow')}"
                ),
                reference_date=str(latest.get("report_date") or ""),
            )
        )
        return refs

    if module_name == "cross_verification" and isinstance(data, dict):
        for metric in list(data.get("verified_metrics") or [])[:3]:
            if not isinstance(metric, dict):
                continue
            metric_name = str(metric.get("metric_name") or "")
            excerpt = (
                f"consistency={metric.get('consistency_flag')} | "
                f"recommended={metric.get('recommended_value')} | "
                f"sources={len(metric.get('sources') or [])}"
            )
            refs.append(
                EvidenceRef(
                    source="cross_verification",
                    source_priority=0,
                    title=f"cross_check_{metric_name}",
                    field=metric_name,
                    excerpt=excerpt[:240],
                )
            )
        return refs

    if module_name in {"stock_info", "governance", "shareholders", "industry_enhanced", "valuation_percentile", "sentiment", "realtime"} and isinstance(data, dict):
        for field, value in list(data.items())[:8]:
            if not is_meaningful(value):
                continue
            refs.append(
                EvidenceRef(
                    source=module_name,
                    source_priority=0,
                    title=module_name,
                    field=str(field),
                    excerpt=str(value)[:240],
                )
            )
            if len(refs) >= 3:
                break
    return refs


def profile_dicts(profiles: dict[str, ModuleQualityProfile]) -> dict[str, dict[str, Any]]:
    """Serialize module profiles into plain dicts."""
    return {key: value.model_dump(mode="json") for key, value in profiles.items()}


def normalize_profile_map(raw_profiles: dict[str, Any] | None) -> dict[str, ModuleQualityProfile]:
    """Convert raw dicts into ModuleQualityProfile models."""
    profiles: dict[str, ModuleQualityProfile] = {}
    for key, value in (raw_profiles or {}).items():
        if isinstance(value, ModuleQualityProfile):
            profiles[key] = value
        elif isinstance(value, dict):
            profiles[key] = ModuleQualityProfile(**value)
    return profiles


def get_module_profile(context: dict[str, Any], module_name: str) -> ModuleQualityProfile:
    """Get a module profile from cleaned/raw context, building one on the fly if needed."""
    raw_profiles = context.get("module_profiles")
    profiles = normalize_profile_map(raw_profiles)
    if module_name in profiles:
        return profiles[module_name]
    return build_module_profile(module_name, context.get(module_name))


def has_minimum_support(context: dict[str, Any], module_name: str, *, min_completeness: float | None = None) -> bool:
    """Return True when a module meets its minimum completeness threshold."""
    profile = get_module_profile(context, module_name)
    required = MODULE_MIN_COMPLETENESS.get(module_name, 0.5) if min_completeness is None else min_completeness
    return profile.status == DataQualityStatus.OK and profile.completeness >= required


def merge_evidence_refs(*groups: Any, limit: int = 8) -> list[EvidenceRef]:
    """Merge multiple evidence lists while deduplicating by title/field/url."""
    merged: list[EvidenceRef] = []
    seen: set[tuple[str, str, str]] = set()
    for group in groups:
        for item in group or []:
            ref = item if isinstance(item, EvidenceRef) else EvidenceRef(**item)
            key = (ref.title, ref.field, ref.url)
            if key in seen:
                continue
            seen.add(key)
            merged.append(ref)
            if len(merged) >= limit:
                return merged
    return merged
