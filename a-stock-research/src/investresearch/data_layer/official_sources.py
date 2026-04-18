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


def _query_candidates(stock_code: str = "", company_name: str = "", keywords: list[str] | None = None) -> list[str]:
    ordered: list[str] = []
    for raw in (company_name, stock_code, *(keywords or [])):
        text = str(raw or "").strip()
        if not text or text in ordered:
            continue
        ordered.append(text)
    return ordered


def _safe_json(response: Any) -> Any | None:
    try:
        return response.json()
    except Exception:
        return None


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


class HybridHtmlJsonSearchAdapter(OfficialSourceAdapter):
    """Generic adapter that tries official candidate URLs and parses JSON/HTML search results."""

    source_name = "official_search"

    def __init__(
        self,
        request_fn: Callable[..., Any],
        *,
        source_name: str,
        env_prefix: str,
        default_candidates: list[dict[str, Any]],
        required_tokens: tuple[str, ...] = (),
    ) -> None:
        super().__init__(request_fn)
        self.source_name = source_name
        self._env_prefix = env_prefix
        self._default_candidates = default_candidates
        self._required_tokens = required_tokens

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
        records: list[dict[str, Any]] = []
        for query in _query_candidates(stock_code=stock_code, company_name=company_name, keywords=keywords):
            records.extend(self._search_one_query(query, limit=limit))
            if len(records) >= limit:
                break
        deduped: dict[tuple[str, str], dict[str, Any]] = {}
        for item in records:
            key = (str(item.get("title") or ""), str(item.get("url") or ""))
            if not key[0] and not key[1]:
                continue
            deduped.setdefault(key, item)
        return list(deduped.values())[:limit]

    def _search_one_query(self, query: str, *, limit: int) -> list[dict[str, Any]]:
        for candidate in self._candidate_definitions(query, limit):
            try:
                response = self._request(
                    candidate.get("method", "GET"),
                    candidate["url"],
                    params=candidate.get("params"),
                    headers=candidate.get("headers") or self._request_headers(candidate["url"]),
                    timeout=int(candidate.get("timeout", 20)),
                )
            except Exception:
                continue

            payload = _safe_json(response)
            if payload is not None:
                records = self._parse_json_records(payload, query=query, limit=limit)
                if records:
                    return records[:limit]

            html_text = getattr(response, "text", "") or ""
            records = self._parse_html_records(html_text, query=query, limit=limit)
            if records:
                return records[:limit]
        return []

    def _candidate_definitions(self, query: str, limit: int) -> list[dict[str, Any]]:
        env_base = os.environ.get(f"{self._env_prefix}_BASE_URL", "").strip()
        env_param = os.environ.get(f"{self._env_prefix}_QUERY_PARAM", "").strip() or "keyword"
        candidates: list[dict[str, Any]] = []
        if env_base:
            candidates.append({"url": env_base, "params": {env_param: query, "limit": limit}})
        for item in self._default_candidates:
            params = dict(item.get("params") or {})
            query_param = str(item.get("query_param") or "keyword")
            params.setdefault(query_param, query)
            if "limit" not in params and not item.get("omit_limit", False):
                params["limit"] = limit
            candidates.append({"url": item["url"], "params": params, "headers": item.get("headers"), "timeout": item.get("timeout", 20)})
        return candidates

    def _request_headers(self, url: str) -> dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/json,application/xhtml+xml",
            "Referer": url,
        }

    def _parse_json_records(self, payload: Any, *, query: str, limit: int) -> list[dict[str, Any]]:
        items = self._extract_candidate_items(payload)
        records: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or item.get("name") or item.get("subject") or "").strip()
            summary = _safe_excerpt(str(item.get("summary") or item.get("excerpt") or item.get("content") or ""))
            text = f"{title} {summary}"
            if not self._match_query(text, query=query):
                continue
            if self._required_tokens and not any(token in text for token in self._required_tokens):
                continue
            records.append(
                {
                    "title": title or query,
                    "source": self.source_name,
                    "url": str(item.get("url") or item.get("link") or item.get("detailUrl") or ""),
                    "summary": summary[:280],
                    "published_at": str(item.get("date") or item.get("publishDate") or item.get("published_at") or ""),
                    "event_type": self._infer_result_type(text),
                    "raw": item,
                }
            )
            if len(records) >= limit:
                break
        return records

    def _parse_html_records(self, html_text: str, *, query: str, limit: int) -> list[dict[str, Any]]:
        if not html_text:
            return []
        records: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for match in re.finditer(r'<a[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<title>[\s\S]*?)</a>', html_text, flags=re.I):
            href = str(match.group("href") or "").strip()
            title = _strip_html(match.group("title") or "")
            if not href or not title:
                continue
            context_start = max(0, match.start() - 240)
            context_end = min(len(html_text), match.end() + 520)
            context_html = html_text[context_start:context_end]
            context_text = _strip_html(context_html)
            if not self._match_query(f"{title} {context_text}", query=query):
                continue
            if self._required_tokens and not any(token in f"{title} {context_text}" for token in self._required_tokens):
                continue
            url = href if href.startswith("http") else urljoin(self._default_candidates[0]["url"], href)
            key = (title, url)
            if key in seen:
                continue
            seen.add(key)
            records.append(
                {
                    "title": title,
                    "source": self.source_name,
                    "url": url,
                    "summary": self._extract_html_summary(context_html, title=title),
                    "published_at": self._extract_html_date(context_text),
                    "event_type": self._infer_result_type(f"{title} {context_text}"),
                    "raw": {"query": query, "context": context_text[:400]},
                }
            )
            if len(records) >= limit:
                break
        return records

    @staticmethod
    def _extract_candidate_items(payload: Any) -> list[Any]:
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        for key in ("results", "data", "list", "rows", "items", "hits", "docs"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return candidate
            if isinstance(candidate, dict):
                nested = HybridHtmlJsonSearchAdapter._extract_candidate_items(candidate)
                if nested:
                    return nested
        return []

    @staticmethod
    def _match_query(text: str, *, query: str) -> bool:
        query = str(query or "").strip()
        haystack = str(text or "")
        if not query:
            return False
        if query in haystack:
            return True
        return len(query) <= 3

    @staticmethod
    def _extract_html_summary(context_html: str, *, title: str) -> str:
        paragraph_matches = re.findall(r"<p[^>]*>([\s\S]*?)</p>", context_html, flags=re.I)
        for paragraph in paragraph_matches:
            text = _safe_excerpt(paragraph)
            if text and text != title:
                return text
        return _safe_excerpt(context_html, limit=280).replace(title, "", 1).strip()[:280]

    @staticmethod
    def _extract_html_date(text: str) -> str:
        patterns = [r"(20\d{2}[-/.年]\d{1,2}[-/.月]\d{1,2}日?)", r"(20\d{2}\d{2}\d{2})"]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                parsed = _coerce_date_value(match.group(1))
                if parsed:
                    return parsed.isoformat()
        return ""

    def _infer_result_type(self, text: str) -> str:
        del text
        return "official_event"


class CreditChinaSearchAdapter(HybridHtmlJsonSearchAdapter):
    """信用中国搜索适配器。"""

    def __init__(self, request_fn: Callable[..., Any]) -> None:
        super().__init__(
            request_fn,
            source_name="credit_china",
            env_prefix="INVESTRESEARCH_CREDIT_CHINA",
            default_candidates=[
                {"url": "https://www.creditchina.gov.cn/search_all", "query_param": "keyword"},
                {"url": "https://www.creditchina.gov.cn/home/xygsNew/xzxk/xzxk_list.shtml", "query_param": "keyword"},
                {"url": "https://www.creditchina.gov.cn/home/xygsNew/xzcf/xzcf_list.shtml", "query_param": "keyword"},
            ],
            required_tokens=("处罚", "失信", "异常", "监管", "决定", "违法", "违规", "信用"),
        )

    def _infer_result_type(self, text: str) -> str:
        if any(token in text for token in ("行政处罚", "处罚决定", "处罚信息")):
            return "administrative_penalty"
        if any(token in text for token in ("失信", "黑名单", "严重违法")):
            return "dishonesty_record"
        if any(token in text for token in ("异常", "监管", "提醒")):
            return "regulatory_measure"
        return "official_event"


class NationalEnterpriseCreditSearchAdapter(HybridHtmlJsonSearchAdapter):
    """国家企业信用信息公示系统适配器。"""

    def __init__(self, request_fn: Callable[..., Any]) -> None:
        super().__init__(
            request_fn,
            source_name="national_enterprise_credit",
            env_prefix="INVESTRESEARCH_NECIPS",
            default_candidates=[
                {"url": "https://www.gsxt.gov.cn/index.html", "query_param": "keyword"},
                {"url": "https://www.gsxt.gov.cn/corp-query-search-1.html", "query_param": "searchword"},
                {"url": "https://bt.gsxt.gov.cn/affiche-query-info-searchTest.html", "query_param": "searchword"},
            ],
            required_tokens=("行政处罚", "经营异常", "严重违法", "抽查检查", "企业信用"),
        )

    def _infer_result_type(self, text: str) -> str:
        if "经营异常" in text:
            return "business_abnormality"
        if "严重违法" in text:
            return "serious_illegality"
        if "行政处罚" in text:
            return "administrative_penalty"
        if "抽查检查" in text:
            return "inspection_result"
        return "enterprise_credit_record"


class CnipaPatentSearchAdapter(HybridHtmlJsonSearchAdapter):
    """国家知识产权局专利搜索适配器。"""

    def __init__(self, request_fn: Callable[..., Any]) -> None:
        super().__init__(
            request_fn,
            source_name="cnipa",
            env_prefix="INVESTRESEARCH_CNIPA",
            default_candidates=[
                {"url": "https://ggfw.cnipa.gov.cn/searchResult", "query_param": "searchWord"},
                {"url": "https://www.cnipa.gov.cn/module/search/index.jsp", "query_param": "keyword"},
            ],
            required_tokens=("专利", "申请号", "专利号", "发明", "实用新型", "外观设计"),
        )

    def _parse_json_records(self, payload: Any, *, query: str, limit: int) -> list[dict[str, Any]]:
        records = super()._parse_json_records(payload, query=query, limit=limit)
        for item in records:
            raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
            text = f"{item.get('title', '')} {item.get('summary', '')}"
            item["application_no"] = str(raw.get("application_no") or raw.get("applicationNo") or self._extract_patent_no(text, "application") or "")
            item["patent_no"] = str(raw.get("patent_no") or raw.get("patentNo") or self._extract_patent_no(text, "patent") or "")
            item["patent_type"] = str(raw.get("patent_type") or raw.get("type") or self._extract_patent_type(text) or "")
            item["status"] = str(raw.get("status") or raw.get("legal_status") or self._extract_patent_status(text) or "")
            item["assignee"] = str(raw.get("assignee") or raw.get("applicant") or self._extract_assignee(text) or "")
            item["inventors"] = list(raw.get("inventors") or raw.get("inventor_list") or [])
            item["event_type"] = item["patent_type"] or "patent_record"
        return records

    def _parse_html_records(self, html_text: str, *, query: str, limit: int) -> list[dict[str, Any]]:
        records = super()._parse_html_records(html_text, query=query, limit=limit)
        for item in records:
            raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
            text = f"{item.get('title', '')} {item.get('summary', '')} {raw.get('context', '')}"
            item["application_no"] = self._extract_patent_no(text, "application") or ""
            item["patent_no"] = self._extract_patent_no(text, "patent") or ""
            item["patent_type"] = self._extract_patent_type(text) or ""
            item["status"] = self._extract_patent_status(text) or ""
            item["assignee"] = self._extract_assignee(text) or ""
            item["inventors"] = []
            item["event_type"] = item["patent_type"] or "patent_record"
        return records

    def _infer_result_type(self, text: str) -> str:
        return self._extract_patent_type(text) or "patent_record"

    @staticmethod
    def _extract_patent_no(text: str, kind: str) -> str | None:
        patterns = {
            "application": [r"申请号[:：]?\s*([A-Z]{0,2}\d[\d.]+)", r"\b(CN\d{8,}\.\d)\b"],
            "patent": [r"专利号[:：]?\s*([A-Z]{0,2}\d[\d.]+)", r"\b(ZL\d{8,}\.\d)\b"],
        }
        for pattern in patterns.get(kind, []):
            match = re.search(pattern, text, flags=re.I)
            if match:
                return str(match.group(1)).strip()
        return None

    @staticmethod
    def _extract_patent_type(text: str) -> str | None:
        for token in ("发明专利", "实用新型", "外观设计"):
            if token in text:
                return token
        return None

    @staticmethod
    def _extract_patent_status(text: str) -> str | None:
        for token in ("授权", "有效", "审中", "公开", "失效", "驳回"):
            if token in text:
                return token
        return None

    @staticmethod
    def _extract_assignee(text: str) -> str | None:
        match = re.search(r"(?:申请人|专利权人)[:：]?\s*([\u4e00-\u9fa5A-Za-z0-9()（）\-]{2,80})", text)
        if match:
            return str(match.group(1)).strip()
        return None


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
            "credit_china": CreditChinaSearchAdapter(request_fn),
            "national_enterprise_credit": NationalEnterpriseCreditSearchAdapter(request_fn),
            "cnipa": CnipaPatentSearchAdapter(request_fn),
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
