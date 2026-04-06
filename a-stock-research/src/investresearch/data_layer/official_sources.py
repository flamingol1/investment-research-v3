"""Official-source adapter registry.

This module provides a free-first adapter layer so the collector can query
official/public sources behind a stable interface without hard-coding every
source directly into the collector flow.
"""

from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
from datetime import date, datetime
from html import unescape
from typing import Any, Callable
from urllib.parse import urljoin


def _coerce_date_value(value: Any) -> date | None:
    if value in (None, "", "-"):
        return None

    text = str(value).strip()
    normalized = (
        text.replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
        .replace("/", "-")
        .replace(".", "-")
    )
    normalized = re.sub(r"\s+", " ", normalized)

    for candidate in (normalized[:10], text[:10]):
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed.date()
        except ValueError:
            continue

    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(re.sub(r"[^0-9-]", "", normalized[:10]), fmt).date()
        except ValueError:
            continue
    return None


def _strip_html(value: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", value, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _safe_excerpt(value: str, limit: int = 280) -> str:
    return _strip_html(value)[:limit]


class OfficialSourceAdapter(ABC):
    """Base adapter for official/public sources."""

    source_name: str = "official"

    def __init__(self, request_fn: Callable[..., Any]) -> None:
        self._request = request_fn

    @abstractmethod
    def search_policy_documents(self, keyword: str, *, limit: int = 5) -> list[dict[str, Any]]:
        """Search official policy/public documents."""

    def search_company_events(
        self,
        *,
        stock_code: str = "",
        company_name: str = "",
        keywords: list[str] | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """Search company-related official events. Default is no-op."""
        del stock_code, company_name, keywords, limit
        return []


class GovCnPolicyAdapter(OfficialSourceAdapter):
    """China gov policy library adapter."""

    source_name = "gov_cn"

    def search_policy_documents(self, keyword: str, *, limit: int = 5) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            "https://sousuo.www.gov.cn/search-gov/data",
            params={
                "t": "zhengcelibrary",
                "q": keyword,
                "sort": "score",
                "sortType": "1",
                "searchfield": "title",
                "p": "1",
                "n": str(limit),
                "type": "gwyzcwjk",
            },
            headers={"Referer": "https://sousuo.www.gov.cn/zcwjk/policyDocumentLibrary"},
            timeout=30,
        )
        data = response.json()
        search_vo = data.get("searchVO") or {}
        cat_map = search_vo.get("catMap") or {}
        records: list[dict[str, Any]] = []
        for bucket in ("gongwen", "bumenfile"):
            for item in (cat_map.get(bucket) or {}).get("listVO") or []:
                published = item.get("pubtimeStr") or item.get("pubtime")
                records.append(
                    {
                        "title": str(item.get("title", "") or ""),
                        "source": "gov.cn",
                        "policy_date": _coerce_date_value(published),
                        "issuing_body": str(item.get("puborg", "") or ""),
                        "document_type": bucket,
                        "url": str(item.get("url", "") or ""),
                        "summary": str(item.get("summary", "") or "")[:280],
                        "matched_keywords": [keyword],
                    }
                )
        return records


class CsrcHtmlSearchAdapter(OfficialSourceAdapter):
    """China Securities Regulatory Commission search adapter.

    The official website returns HTML rather than a stable JSON endpoint, so we
    parse search-result links conservatively and only keep records that look
    like enforcement or compliance-related disclosures.
    """

    source_name = "csrc"

    def __init__(self, request_fn: Callable[..., Any]) -> None:
        super().__init__(request_fn)
        self._search_url = os.environ.get(
            "INVESTRESEARCH_CSRC_BASE_URL",
            "https://www.csrc.gov.cn/searchList/a1a078ee0bc54721ab6b148884c784a2",
        ).strip()

    def search_policy_documents(self, keyword: str, *, limit: int = 5) -> list[dict[str, Any]]:
        del keyword, limit
        return []

    def search_company_events(
        self,
        *,
        stock_code: str = "",
        company_name: str = "",
        keywords: list[str] | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        query_candidates = [company_name, stock_code, *(keywords or [])]
        seen_queries: set[str] = set()
        records: list[dict[str, Any]] = []

        for query in query_candidates:
            query = str(query or "").strip()
            if not query or query in seen_queries:
                continue
            seen_queries.add(query)

            try:
                response = self._request(
                    "GET",
                    self._search_url,
                    params={"keyword": query},
                    headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.csrc.gov.cn/"},
                    timeout=20,
                )
            except Exception:
                continue

            html_text = getattr(response, "text", "") or ""
            records.extend(self._parse_search_html(html_text, query=query, limit=limit))
            if len(records) >= limit:
                break

        return records[:limit]

    @classmethod
    def _parse_search_html(cls, html_text: str, *, query: str, limit: int = 5) -> list[dict[str, Any]]:
        if not html_text:
            return []

        matches = list(
            re.finditer(
                r'<a[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<title>[\s\S]*?)</a>',
                html_text,
                flags=re.I,
            )
        )

        records: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for match in matches:
            href = str(match.group("href") or "").strip()
            title = _strip_html(match.group("title") or "")
            if not href or not title:
                continue

            normalized_href = urljoin("https://www.csrc.gov.cn/", href)
            if "javascript:" in normalized_href.lower():
                continue

            looks_like_detail = any(
                token in normalized_href
                for token in ("/content.shtml", "/csrc/", "/files/")
            )
            looks_like_enforcement = any(
                token in f"{title} {normalized_href}"
                for token in ("处罚", "立案", "监管", "决定书", "禁入", "问询", "违法", "违规")
            )
            if not looks_like_detail or not looks_like_enforcement:
                continue

            context_start = max(0, match.start() - 240)
            context_end = min(len(html_text), match.end() + 520)
            context_html = html_text[context_start:context_end]
            context_text = _strip_html(context_html)

            if query and query not in context_text and query not in title:
                # Keep pure enforcement results even when the official search page
                # omits the raw keyword from visible snippets.
                if len(query) >= 4:
                    continue

            publish_date = cls._extract_date(context_text)
            summary = cls._extract_summary(context_html, title=title)
            event_type = cls._infer_event_type(f"{title} {summary}")

            dedupe_key = (title, normalized_href)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            records.append(
                {
                    "title": title,
                    "source": "csrc",
                    "url": normalized_href,
                    "summary": summary,
                    "published_at": publish_date.isoformat() if publish_date else "",
                    "date": publish_date.isoformat() if publish_date else "",
                    "event_type": event_type,
                    "type": event_type,
                    "raw": {
                        "query": query,
                        "context": context_text[:400],
                    },
                }
            )
            if len(records) >= limit:
                break

        return records

    @staticmethod
    def _extract_date(text: str) -> date | None:
        patterns = [
            r"(20\d{2}[-/.年]\d{1,2}[-/.月]\d{1,2}日?)",
            r"(20\d{2}\d{2}\d{2})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return _coerce_date_value(match.group(1))
        return None

    @staticmethod
    def _extract_summary(context_html: str, *, title: str) -> str:
        paragraph_matches = re.findall(r"<p[^>]*>([\s\S]*?)</p>", context_html, flags=re.I)
        for paragraph in paragraph_matches:
            text = _safe_excerpt(paragraph)
            if text and text != title:
                return text

        text = _safe_excerpt(context_html, limit=420)
        text = text.replace(title, "", 1).strip()
        return text[:280]

    @staticmethod
    def _infer_event_type(text: str) -> str:
        if any(token in text for token in ("行政处罚", "处罚决定", "决定书")):
            return "administrative_penalty"
        if any(token in text for token in ("市场禁入", "禁入")):
            return "market_ban"
        if any(token in text for token in ("立案", "调查")):
            return "investigation"
        if any(token in text for token in ("问询", "监管函", "监管措施")):
            return "regulatory_measure"
        return "official_event"


class ConfigurableOfficialSearchAdapter(OfficialSourceAdapter):
    """Environment-driven adapter scaffold for future official sources.

    By default this is a no-op. If the corresponding environment variables are
    provided, it can issue a generic GET request and normalize a simple JSON
    response into the collector's internal document format.
    """

    def __init__(self, request_fn: Callable[..., Any], *, source_name: str, env_prefix: str) -> None:
        super().__init__(request_fn)
        self.source_name = source_name
        self._base_url = os.environ.get(f"{env_prefix}_BASE_URL", "").strip()
        self._query_param = os.environ.get(f"{env_prefix}_QUERY_PARAM", "q").strip() or "q"
        self._type_param = os.environ.get(f"{env_prefix}_TYPE_PARAM", "").strip()

    def search_policy_documents(self, keyword: str, *, limit: int = 5) -> list[dict[str, Any]]:
        del keyword, limit
        return []

    def search_company_events(
        self,
        *,
        stock_code: str = "",
        company_name: str = "",
        keywords: list[str] | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        if not self._base_url:
            return []

        query = company_name or stock_code or (keywords[0] if keywords else "")
        if not query:
            return []

        params: dict[str, Any] = {self._query_param: query, "limit": limit}
        if self._type_param:
            params["type"] = self._type_param

        response = self._request("GET", self._base_url, params=params, timeout=20)
        payload = response.json()
        items = payload.get("results") or payload.get("data") or []

        normalized: list[dict[str, Any]] = []
        for item in items[:limit]:
            if not isinstance(item, dict):
                continue
            normalized.append(
                {
                    "title": str(item.get("title", "") or item.get("name", "") or ""),
                    "source": self.source_name,
                    "url": str(item.get("url", "") or ""),
                    "summary": str(item.get("summary", "") or item.get("excerpt", "") or "")[:280],
                    "published_at": str(item.get("date", "") or item.get("published_at", "") or ""),
                    "type": str(item.get("type", "") or item.get("event_type", "") or item.get("patent_type", "") or ""),
                    "application_no": str(item.get("application_no", "") or item.get("applicationNo", "") or ""),
                    "patent_no": str(item.get("patent_no", "") or item.get("patentNo", "") or ""),
                    "status": str(item.get("status", "") or item.get("legal_status", "") or ""),
                    "assignee": str(item.get("assignee", "") or item.get("applicant", "") or ""),
                    "inventors": list(item.get("inventors") or item.get("inventor_list") or []),
                    "keywords": list(item.get("keywords") or item.get("tags") or []),
                    "raw": item,
                }
            )
        return normalized


class OfficialSourceRegistry:
    """Registry for free-first official/public source adapters."""

    def __init__(self, request_fn: Callable[..., Any]) -> None:
        self._adapters: dict[str, OfficialSourceAdapter] = {
            "gov_cn": GovCnPolicyAdapter(request_fn),
            "csrc": CsrcHtmlSearchAdapter(request_fn),
            "credit_china": ConfigurableOfficialSearchAdapter(
                request_fn,
                source_name="credit_china",
                env_prefix="INVESTRESEARCH_CREDIT_CHINA",
            ),
            "national_enterprise_credit": ConfigurableOfficialSearchAdapter(
                request_fn,
                source_name="national_enterprise_credit",
                env_prefix="INVESTRESEARCH_NECIPS",
            ),
            "cnipa": ConfigurableOfficialSearchAdapter(
                request_fn,
                source_name="cnipa",
                env_prefix="INVESTRESEARCH_CNIPA",
            ),
            "stats_gov": ConfigurableOfficialSearchAdapter(
                request_fn,
                source_name="stats_gov",
                env_prefix="INVESTRESEARCH_STATS_GOV",
            ),
        }

    @property
    def adapters(self) -> dict[str, OfficialSourceAdapter]:
        return self._adapters

    def search_policy_documents(self, keyword: str, *, limit: int = 5) -> list[dict[str, Any]]:
        return self._adapters["gov_cn"].search_policy_documents(keyword, limit=limit)

    def search_company_compliance_events(
        self,
        *,
        stock_code: str = "",
        company_name: str = "",
        keywords: list[str] | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for key in ("csrc", "credit_china", "national_enterprise_credit"):
            events.extend(
                self._adapters[key].search_company_events(
                    stock_code=stock_code,
                    company_name=company_name,
                    keywords=keywords,
                    limit=limit,
                )
            )
        return self._dedupe_and_sort(events, limit=limit, date_fields=("date", "published_at"))

    def search_patents(
        self,
        *,
        stock_code: str = "",
        company_name: str = "",
        keywords: list[str] | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        records = self._adapters["cnipa"].search_company_events(
            stock_code=stock_code,
            company_name=company_name,
            keywords=keywords,
            limit=limit,
        )
        return self._dedupe_and_sort(records, limit=limit, date_fields=("date", "published_at"))

    @staticmethod
    def _dedupe_and_sort(
        records: list[dict[str, Any]],
        *,
        limit: int,
        date_fields: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        unique: dict[tuple[str, str], dict[str, Any]] = {}
        for item in records:
            if not isinstance(item, dict):
                continue
            key = (
                str(item.get("source") or ""),
                str(item.get("url") or item.get("title") or ""),
            )
            if not key[1]:
                continue
            if key not in unique:
                unique[key] = item

        def sort_key(item: dict[str, Any]) -> tuple[int, str]:
            for field in date_fields:
                parsed = _coerce_date_value(item.get(field))
                if parsed:
                    return (1, parsed.isoformat())
            return (0, "")

        ordered = sorted(unique.values(), key=sort_key, reverse=True)
        return ordered[:limit]
