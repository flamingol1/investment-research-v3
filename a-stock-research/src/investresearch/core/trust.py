"""Data quality, evidence, and degradation helpers."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from .models import DataQualityStatus, EvidenceRef, ModuleQualityProfile

MODULE_SOURCE_PRIORITY: dict[str, list[str]] = {
    "stock_info": ["exchange", "company_profile", "akshare", "baostock"],
    "prices": ["exchange", "eastmoney", "akshare", "baostock"],
    "realtime": ["exchange", "eastmoney", "sina"],
    "financials": ["annual_report", "semi_annual_report", "quarterly_report", "akshare", "baostock"],
    "valuation": ["exchange", "baostock", "akshare"],
    "announcements": ["annual_report", "semi_annual_report", "quarterly_report", "announcement", "inquiry_letter"],
    "governance": ["csrc", "exchange", "announcement", "cninfo", "company_profile"],
    "research_reports": ["research_report"],
    "shareholders": ["company_announcement", "cninfo", "akshare"],
    "industry_enhanced": ["national_bureau_of_statistics", "ministry", "industry_association", "policy_document"],
    "valuation_percentile": ["exchange", "baostock"],
    "news": ["news"],
    "sentiment": ["news"],
    "policy_documents": ["gov.cn", "ministry", "association"],
    "compliance_events": ["csrc", "credit_china", "national_enterprise_credit"],
    "patents": ["cnipa", "annual_report", "company_announcement"],
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
        required = [
            "report_date",
            "revenue",
            "net_profit",
            "operating_cashflow",
            "equity",
            "goodwill_ratio",
        ]
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
    ]
    return {module: build_module_profile(module, data.get(module)) for module in modules}


def aggregate_quality(profiles: dict[str, ModuleQualityProfile]) -> tuple[DataQualityStatus, float, float, list[str], list[EvidenceRef], list[str]]:
    """Aggregate module profiles into overall data quality metrics."""
    if not profiles:
        return DataQualityStatus.FAILED, 0.0, 0.0, [], [], []

    completeness = round(sum(profile.completeness for profile in profiles.values()) / len(profiles), 2)
    ok_count = sum(1 for profile in profiles.values() if profile.status == DataQualityStatus.OK)
    coverage_ratio = round(ok_count / len(profiles), 2)

    missing_fields: list[str] = []
    evidence_refs: list[EvidenceRef] = []
    source_priority: list[str] = []
    for module_name, profile in profiles.items():
        missing_fields.extend([f"{module_name}.{field}" for field in profile.missing_fields[:5]])
        evidence_refs.extend(profile.evidence_refs[:2])
        for source in profile.source_priority:
            if source not in source_priority:
                source_priority.append(source)

    if coverage_ratio >= 0.8 and completeness >= 0.7:
        status = DataQualityStatus.OK
    elif coverage_ratio >= 0.35 or completeness >= 0.35:
        status = DataQualityStatus.PARTIAL
    else:
        status = DataQualityStatus.FAILED

    return status, completeness, coverage_ratio, missing_fields[:30], evidence_refs[:20], source_priority


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
