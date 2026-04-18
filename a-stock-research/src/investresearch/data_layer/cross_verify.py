"""Cross-verification utilities for industry peers and company-level metrics."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

from investresearch.core.logging import get_logger
from investresearch.core.models import (
    CrossVerifiedMetric,
    DataCrossVerification,
    EvidenceRef,
    FinancialStatement,
    IndustryCrossVerification,
    IndustryDataPoint,
    MetricSourceValue,
    PeerCompany,
    StockPrice,
)

logger = get_logger("agent.cross_verify")

_CONSISTENCY_THRESHOLD = 0.15

_METRIC_NAME_MAP: dict[str, str] = {
    "市场规模": "market_size",
    "行业规模": "market_size",
    "市场空间": "market_size",
    "行业市场空间": "market_size",
    "行业市场总额": "market_size",
    "复合增长率": "cagr",
    "年复合增长率": "cagr",
    "CAGR": "cagr",
    "年均增长率": "cagr",
    "年均增速": "cagr",
    "年化增长率": "cagr",
    "CR5": "cr5",
    "前五名集中度": "cr5",
    "行业集中度": "cr5",
    "市场份额": "market_share",
    "市占率": "market_share",
    "市场占有率": "market_share",
}

_SOURCE_QUALITY: dict[str, float] = {
    "consulting": 1.0,
    "annual_report": 0.85,
    "official_statement": 0.9,
    "market_quote": 0.8,
    "self_reported": 0.6,
    "derived": 0.45,
    "unknown": 0.5,
}

_RELEVANT_CROSS_METRICS = {
    "close",
    "revenue",
    "net_profit",
    "equity",
    "operating_cashflow",
    "investing_cashflow",
    "financing_cashflow",
    "free_cashflow",
    "cash_to_profit",
    "goodwill_ratio",
    "market_cap",
    "pe_ttm",
    "pb_mrq",
}


class CrossVerificationEngine:
    """Cross-source verification engine."""

    def verify(
        self,
        data_points: list[IndustryDataPoint],
    ) -> list[CrossVerifiedMetric]:
        """Verify industry data points extracted from peer annual reports."""
        if not data_points:
            return []

        grouped = self._group_industry_points(data_points)
        results: list[CrossVerifiedMetric] = []

        for metric_name, points in grouped.items():
            try:
                verified = self._verify_industry_metric(metric_name, points)
                if verified is not None:
                    results.append(verified)
            except Exception as exc:
                logger.warning(f"楠岃瘉琛屼笟鎸囨爣 {metric_name} 澶辫触: {exc}")

        return sorted(results, key=lambda item: item.source_count, reverse=True)

    def build_result(
        self,
        data_points: list[IndustryDataPoint],
        peers: list[PeerCompany],
        industry_name: str = "",
        industry_level: str = "",
        target_stock_code: str = "",
        errors: list[str] | None = None,
    ) -> IndustryCrossVerification:
        """Build a complete industry peer cross-verification result."""
        verified = self.verify(data_points)
        overall = round(sum(item.confidence_score for item in verified) / len(verified), 2) if verified else 0.0
        return IndustryCrossVerification(
            industry_name=industry_name,
            industry_level=industry_level,
            target_stock_code=target_stock_code,
            peer_count=len(peers),
            peers=peers,
            data_points=data_points,
            verified_metrics=verified,
            overall_confidence=overall,
            collection_errors=errors or [],
        )

    def verify_metric_sources(
        self,
        observations: list[MetricSourceValue],
    ) -> list[CrossVerifiedMetric]:
        """Verify generic company-level observations from multiple sources."""
        if not observations:
            return []

        grouped: dict[str, list[MetricSourceValue]] = defaultdict(list)
        for item in observations:
            if item.metric_value is None:
                continue
            grouped[str(item.metric_name).strip()].append(item)

        verified: list[CrossVerifiedMetric] = []
        for metric_name, points in grouped.items():
            try:
                metric = self._verify_generic_metric(metric_name, points)
                if metric is not None:
                    verified.append(metric)
            except Exception as exc:
                logger.warning(f"楠岃瘉鍏徃绾ф寚鏍?{metric_name} 澶辫触: {exc}")

        return sorted(
            verified,
            key=lambda item: (item.consistency_flag != "divergent", item.source_count, item.confidence_score),
            reverse=True,
        )

    def build_data_cross_verification(
        self,
        stock_code: str,
        financials: list[FinancialStatement],
        realtime: StockPrice | dict[str, Any] | None = None,
        valuation_percentile: dict[str, Any] | None = None,
    ) -> DataCrossVerification:
        """Build company-level cross verification for financial and realtime metrics."""
        latest = self._latest_substantive_financial(financials)
        observations = self._collect_company_observations(
            latest=latest,
            realtime=realtime,
            valuation_percentile=valuation_percentile or {},
        )
        verified = self.verify_metric_sources(observations)

        consistent = [item.metric_name for item in verified if item.consistency_flag == "consistent"]
        divergent = [item.metric_name for item in verified if item.consistency_flag == "divergent"]
        insufficient = [item.metric_name for item in verified if item.consistency_flag == "insufficient"]
        overall = round(sum(item.confidence_score for item in verified) / len(verified), 2) if verified else 0.0
        summary = self._build_data_summary(verified, consistent, divergent, insufficient)

        return DataCrossVerification(
            stock_code=stock_code,
            latest_report_date=latest.report_date.isoformat() if latest and latest.report_date else "",
            verified_metrics=verified,
            consistent_metrics=consistent,
            divergent_metrics=divergent,
            insufficient_metrics=insufficient,
            overall_confidence=overall,
            summary=summary,
        )

    def _group_industry_points(
        self,
        data_points: list[IndustryDataPoint],
    ) -> dict[str, list[IndustryDataPoint]]:
        grouped: dict[str, list[IndustryDataPoint]] = defaultdict(list)
        for item in data_points:
            normalized = self._normalize_metric_name(item.metric_name)
            if item.metric_value is not None:
                grouped[normalized].append(item)
        return dict(grouped)

    def _normalize_metric_name(self, raw_name: str) -> str:
        name = str(raw_name or "").strip()
        if name in _METRIC_NAME_MAP:
            return _METRIC_NAME_MAP[name]
        if name in {"market_size", "cagr", "cr5", "market_share"}:
            return name
        for cn_name, en_name in _METRIC_NAME_MAP.items():
            if cn_name and cn_name in name:
                return en_name
        return name

    def _verify_industry_metric(
        self,
        metric_name: str,
        points: list[IndustryDataPoint],
    ) -> CrossVerifiedMetric | None:
        deduped = self._deduplicate_industry_points(points)
        if not deduped:
            return None

        values = [item.metric_value for item in deduped if item.metric_value is not None]
        if not values:
            return None

        stats = self._compute_statistics(values)
        source_names = [item.source_company for item in deduped]
        consulting_sources = list({item.consulting_firm for item in deduped if item.consulting_firm})
        source_types = [item.source_type for item in deduped]
        confidence = self._compute_confidence(values=values, source_types=source_types, consulting_count=len(consulting_sources))
        consistency = self._determine_consistency(values, stats.get("std_dev"))
        recommended = self._select_industry_recommended_value(values, deduped, consulting_sources)
        evidence = self._build_industry_evidence_refs(deduped)

        return CrossVerifiedMetric(
            metric_name=metric_name,
            values=values,
            sources=source_names,
            mean_value=stats.get("mean"),
            median_value=stats.get("median"),
            std_dev=stats.get("std_dev"),
            min_value=stats.get("min"),
            max_value=stats.get("max"),
            source_count=len(values),
            confidence_score=round(confidence, 2),
            consistency_flag=consistency,
            consulting_sources=consulting_sources,
            recommended_value=recommended,
            evidence_refs=evidence,
        )

    def _verify_generic_metric(
        self,
        metric_name: str,
        observations: list[MetricSourceValue],
    ) -> CrossVerifiedMetric | None:
        deduped = self._deduplicate_generic_points(observations)
        values = [item.metric_value for item in deduped if item.metric_value is not None]
        if not values:
            return None

        stats = self._compute_statistics(values)
        source_names = [item.source_name for item in deduped]
        source_types = [item.source_type for item in deduped]
        confidence = self._compute_confidence(values=values, source_types=source_types, consulting_count=0)
        consistency = self._determine_consistency(values, stats.get("std_dev"))
        recommended = self._select_generic_recommended_value(values, deduped)
        evidence = self._build_generic_evidence_refs(metric_name, deduped)

        return CrossVerifiedMetric(
            metric_name=metric_name,
            values=values,
            sources=source_names,
            mean_value=stats.get("mean"),
            median_value=stats.get("median"),
            std_dev=stats.get("std_dev"),
            min_value=stats.get("min"),
            max_value=stats.get("max"),
            source_count=len(values),
            confidence_score=round(confidence, 2),
            consistency_flag=consistency,
            recommended_value=recommended,
            evidence_refs=evidence,
        )

    @staticmethod
    def _deduplicate_industry_points(
        points: list[IndustryDataPoint],
    ) -> list[IndustryDataPoint]:
        by_source: dict[str, list[IndustryDataPoint]] = defaultdict(list)
        for item in points:
            key = item.source_company_code or item.source_company
            by_source[key].append(item)

        result: list[IndustryDataPoint] = []
        for source_points in by_source.values():
            consulting = [item for item in source_points if item.consulting_firm]
            result.append(consulting[0] if consulting else source_points[0])
        return result

    @staticmethod
    def _deduplicate_generic_points(
        observations: list[MetricSourceValue],
    ) -> list[MetricSourceValue]:
        by_source: dict[str, MetricSourceValue] = {}
        priority = {"official_statement": 3, "market_quote": 2, "unknown": 1, "derived": 0}
        for item in observations:
            key = item.source_name or "unknown"
            existing = by_source.get(key)
            if existing is None or priority.get(item.source_type, 1) > priority.get(existing.source_type, 1):
                by_source[key] = item
        return list(by_source.values())

    @staticmethod
    def _compute_statistics(values: list[float]) -> dict[str, float | None]:
        if not values:
            return {"mean": None, "median": None, "std_dev": None, "min": None, "max": None}

        mean = sum(values) / len(values)
        sorted_vals = sorted(values)
        if len(sorted_vals) % 2 == 0:
            median = (sorted_vals[len(sorted_vals) // 2 - 1] + sorted_vals[len(sorted_vals) // 2]) / 2
        else:
            median = sorted_vals[len(sorted_vals) // 2]

        if len(sorted_vals) >= 2:
            variance = sum((value - mean) ** 2 for value in sorted_vals) / (len(sorted_vals) - 1)
            std_dev = math.sqrt(variance)
        else:
            std_dev = 0.0

        return {
            "mean": round(mean, 2),
            "median": round(median, 2),
            "std_dev": round(std_dev, 2),
            "min": round(min(values), 2),
            "max": round(max(values), 2),
        }

    def _compute_confidence(
        self,
        values: list[float],
        source_types: list[str],
        consulting_count: int,
    ) -> float:
        n = len(values)
        if n == 0:
            return 0.0

        mean = sum(values) / n
        if n >= 2 and mean != 0:
            variance = sum((value - mean) ** 2 for value in values) / (n - 1)
            std_dev = math.sqrt(variance)
            consistency = max(0.0, 1.0 - (std_dev / abs(mean)))
        else:
            consistency = 0.3

        count_score = min(n / 5.0, 1.0)
        quality_scores = [_SOURCE_QUALITY.get(source_type, 0.5) for source_type in source_types]
        quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0.5
        if consulting_count > 0:
            quality = min(quality + 0.15, 1.0)

        confidence = 0.5 * consistency + 0.3 * count_score + 0.2 * quality
        return max(0.0, min(1.0, confidence))

    def _determine_consistency(
        self,
        values: list[float],
        std_dev: float | None,
    ) -> str:
        if len(values) < 2:
            return "insufficient"

        mean = sum(values) / len(values)
        if mean == 0 or std_dev is None:
            return "insufficient"

        cv = std_dev / abs(mean)
        return "consistent" if cv <= _CONSISTENCY_THRESHOLD else "divergent"

    @staticmethod
    def _select_industry_recommended_value(
        values: list[float],
        points: list[IndustryDataPoint],
        consulting_sources: list[str],
    ) -> float | None:
        if not values:
            return None

        if consulting_sources:
            for item in points:
                if item.consulting_firm in consulting_sources and item.metric_value is not None:
                    return round(item.metric_value, 2)

        if len(values) >= 3:
            sorted_vals = sorted(values)
            mid = len(sorted_vals) // 2
            if len(sorted_vals) % 2 == 0:
                return round((sorted_vals[mid - 1] + sorted_vals[mid]) / 2, 2)
            return round(sorted_vals[mid], 2)

        if len(values) == 2:
            return round(sum(values) / len(values), 2)

        return round(values[0], 2)

    @staticmethod
    def _select_generic_recommended_value(
        values: list[float],
        observations: list[MetricSourceValue],
    ) -> float | None:
        if not values:
            return None

        for preferred_type in ("official_statement", "market_quote"):
            for item in observations:
                if item.source_type == preferred_type and item.metric_value is not None:
                    return round(item.metric_value, 2)

        if len(values) >= 3:
            sorted_vals = sorted(values)
            mid = len(sorted_vals) // 2
            if len(sorted_vals) % 2 == 0:
                return round((sorted_vals[mid - 1] + sorted_vals[mid]) / 2, 2)
            return round(sorted_vals[mid], 2)

        if len(values) == 2:
            return round(sum(values) / len(values), 2)

        return round(values[0], 2)

    @staticmethod
    def _build_industry_evidence_refs(
        points: list[IndustryDataPoint],
    ) -> list[EvidenceRef]:
        refs: list[EvidenceRef] = []
        for item in points:
            refs.append(
                EvidenceRef(
                    source=item.source_company_code or item.source_company,
                    title=f"{item.source_company} annual report - {item.metric_name}",
                    field=item.metric_name,
                    excerpt=item.excerpt[:200] if item.excerpt else "",
                    url=item.pdf_url,
                )
            )
        return refs

    @staticmethod
    def _build_generic_evidence_refs(
        metric_name: str,
        observations: list[MetricSourceValue],
    ) -> list[EvidenceRef]:
        refs: list[EvidenceRef] = []
        for item in observations:
            refs.append(
                EvidenceRef(
                    source=item.source_name,
                    title=f"{metric_name} cross-check",
                    field=metric_name,
                    excerpt=(item.excerpt or f"{item.source_name}: {item.metric_value}")[:220],
                    reference_date=item.reference_date,
                )
            )
        return refs

    def _collect_company_observations(
        self,
        *,
        latest: FinancialStatement | None,
        realtime: StockPrice | dict[str, Any] | None,
        valuation_percentile: dict[str, Any],
    ) -> list[MetricSourceValue]:
        observations: list[MetricSourceValue] = []

        if latest is not None:
            raw_data = dict(latest.raw_data or {})
            source_values = raw_data.get("source_values")
            if isinstance(source_values, dict):
                for source_name, payload in source_values.items():
                    if not isinstance(payload, dict):
                        continue
                    metrics = payload.get("metrics")
                    if not isinstance(metrics, dict):
                        continue
                    source_type = str(payload.get("source_type") or "unknown")
                    reference_date = str(payload.get("reference_date") or (latest.report_date.isoformat() if latest.report_date else ""))
                    for metric_name, metric_value in metrics.items():
                        if metric_name not in _RELEVANT_CROSS_METRICS:
                            continue
                        numeric = self._safe_float(metric_value)
                        if numeric is None:
                            continue
                        observations.append(
                            MetricSourceValue(
                                metric_name=metric_name,
                                metric_value=numeric,
                                metric_unit=self._metric_unit(metric_name),
                                source_name=str(source_name),
                                source_type=source_type,
                                category="financial",
                                reference_date=reference_date,
                                excerpt=f"{source_name}: {metric_name}={numeric}",
                            )
                        )

        realtime_data = realtime if isinstance(realtime, dict) else realtime.model_dump(mode="json") if realtime else {}
        if isinstance(realtime_data, dict):
            realtime_source_values = realtime_data.get("raw_data", {}).get("source_values", {}) if isinstance(realtime_data.get("raw_data"), dict) else {}
            if isinstance(realtime_source_values, dict):
                for source_name, payload in realtime_source_values.items():
                    if not isinstance(payload, dict):
                        continue
                    metrics = payload.get("metrics")
                    if not isinstance(metrics, dict):
                        continue
                    source_type = str(payload.get("source_type") or "unknown")
                    reference_date = str(payload.get("reference_date") or realtime_data.get("date") or "")
                    for metric_name, metric_value in metrics.items():
                        if metric_name not in _RELEVANT_CROSS_METRICS:
                            continue
                        numeric = self._safe_float(metric_value)
                        if numeric is None:
                            continue
                        observations.append(
                            MetricSourceValue(
                                metric_name=metric_name,
                                metric_value=numeric,
                                metric_unit=self._metric_unit(metric_name),
                                source_name=str(source_name),
                                source_type=source_type,
                                category="realtime",
                                reference_date=reference_date,
                                excerpt=f"{source_name}: {metric_name}={numeric}",
                            )
                        )

            for field_name, source_name in (
                ("close", "realtime_quote"),
                ("market_cap", "realtime_quote"),
                ("pe_ttm", "realtime_quote"),
                ("pb_mrq", "realtime_quote"),
            ):
                numeric = self._safe_float(realtime_data.get(field_name))
                if numeric is None:
                    continue
                observations.append(
                    MetricSourceValue(
                        metric_name=field_name,
                        metric_value=numeric,
                        metric_unit=self._metric_unit(field_name),
                        source_name=source_name,
                        source_type="market_quote",
                        category="realtime",
                        reference_date=str(realtime_data.get("date") or ""),
                        excerpt=f"{field_name}={numeric}",
                    )
                )

        for metric_name, field_name in (("pe_ttm", "pe_ttm_current"), ("pb_mrq", "pb_mrq_current")):
            numeric = self._safe_float(valuation_percentile.get(field_name))
            if numeric is None:
                continue
            observations.append(
                MetricSourceValue(
                    metric_name=metric_name,
                    metric_value=numeric,
                    metric_unit=self._metric_unit(metric_name),
                    source_name="valuation_percentile",
                    source_type="official_statement",
                    category="valuation",
                    excerpt=f"{field_name}={numeric}",
                )
            )

        return observations

    @staticmethod
    def _latest_substantive_financial(
        financials: list[FinancialStatement],
    ) -> FinancialStatement | None:
        for item in financials:
            if any(
                getattr(item, field, None) is not None
                for field in ("revenue", "net_profit", "operating_cashflow", "equity")
            ):
                return item
        return financials[0] if financials else None

    @staticmethod
    def _build_data_summary(
        verified: list[CrossVerifiedMetric],
        consistent: list[str],
        divergent: list[str],
        insufficient: list[str],
    ) -> str:
        if not verified:
            return "未找到可用的多来源交叉验证指标"
        if divergent:
            return (
                f"多来源交叉验证已覆盖 {len(verified)} 个指标，"
                f"其中 {len(divergent)} 个存在分歧: {', '.join(divergent[:5])}"
            )
        if consistent:
            return (
                f"多来源交叉验证已覆盖 {len(verified)} 个指标，"
                f"其中 {len(consistent)} 个基本一致: {', '.join(consistent[:5])}"
            )
        return (
            f"多来源交叉验证已覆盖 {len(verified)} 个指标，"
            f"但大多仍为单源或需继续补证: {', '.join(insufficient[:5])}"
        )

    @staticmethod
    def _metric_unit(metric_name: str) -> str:
        if metric_name in {"goodwill_ratio"}:
            return "%"
        if metric_name in {"cash_to_profit", "pe_ttm", "pb_mrq"}:
            return "x"
        if metric_name in {"close"}:
            return "price"
        if metric_name in {"market_cap", "revenue", "net_profit", "equity", "operating_cashflow", "investing_cashflow", "financing_cashflow", "free_cashflow"}:
            return "cny"
        return ""

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value in (None, "", "-"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
