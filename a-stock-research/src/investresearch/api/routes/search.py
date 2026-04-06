"""Knowledge-base search routes."""

from __future__ import annotations

import asyncio
import io
import json
import os
import re
import unicodedata
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query

from investresearch.api.schemas import (
    SearchItemResponse,
    SearchRequest,
    SearchResponse,
    SecurityLookupItemResponse,
    SecurityLookupResponse,
)
from investresearch.core.exceptions import (
    KnowledgeBaseConnectionError,
    KnowledgeBaseQueryError,
)
from investresearch.core.logging import get_logger
from investresearch.data_layer.cache import FileCache

try:
    from pypinyin import Style, lazy_pinyin
except ImportError:  # pragma: no cover - optional dependency fallback
    Style = None
    lazy_pinyin = None

logger = get_logger("api.routes.search")

router = APIRouter(prefix="/api", tags=["search"])

REPORTS_DIR = Path("output/reports")
SECURITY_CACHE = FileCache(default_ttl=60 * 60 * 12)
SECURITY_CACHE_KEY = "security_lookup_universe_v2"
WHITESPACE_RE = re.compile(r"\s+")


def _distance_to_similarity(distance: float | int | None) -> float:
    if distance is None:
        return 0.0

    distance_value = max(float(distance), 0.0)
    return round(1.0 / (1.0 + distance_value), 4)


@router.post("/search", response_model=SearchResponse)
async def search_knowledge_base(req: SearchRequest) -> SearchResponse:
    """Search similar content from the local knowledge base."""
    try:
        from investresearch.knowledge_base.chroma_store import ChromaKnowledgeStore

        store = ChromaKnowledgeStore()
        results = store.search_similar(
            query=req.query,
            category=req.category,
            n=req.num_results,
        )
    except (KnowledgeBaseConnectionError, KnowledgeBaseQueryError, ImportError) as exc:
        logger.warning(f"Knowledge base search unavailable: {exc}")
        return SearchResponse(
            query=req.query,
            results=[],
            total=0,
            warning=f"知识库当前不可用: {exc}",
        )
    except Exception as exc:
        logger.error(f"Knowledge base search failed unexpectedly: {exc}", exc_info=True)
        return SearchResponse(
            query=req.query,
            results=[],
            total=0,
            warning="知识库搜索暂时不可用，请稍后重试",
        )

    items: list[SearchItemResponse] = []
    for result in results:
        meta = result.get("metadata", {})
        items.append(
            SearchItemResponse(
                document=result.get("document", "")[:500],
                stock_code=meta.get("stock_code", ""),
                stock_name=meta.get("stock_name", ""),
                category=meta.get("category", ""),
                date=meta.get("date", ""),
                similarity=_distance_to_similarity(result.get("distance")),
            )
        )

    return SearchResponse(query=req.query, results=items, total=len(items))


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "").upper()
    return re.sub(r"[\s._\-·]+", "", normalized)


def _clean_display_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "").strip()
    return WHITESPACE_RE.sub("", normalized)


def _exchange_from_code(stock_code: str) -> str:
    if stock_code.startswith(("6", "9")):
        return "SH"
    if stock_code.startswith(("0", "3")):
        return "SZ"
    if stock_code.startswith(("4", "8")):
        return "BJ"
    return ""


def _build_name_aliases(stock_name: str) -> set[str]:
    cleaned_name = _clean_display_name(stock_name)
    if not cleaned_name:
        return set()

    aliases = {_normalize_text(cleaned_name)}

    if lazy_pinyin is None or Style is None:
        return aliases

    try:
        full_pinyin = "".join(lazy_pinyin(cleaned_name))
        initials = "".join(lazy_pinyin(cleaned_name, style=Style.FIRST_LETTER))
    except Exception as exc:
        logger.warning(f"Failed to build pinyin aliases for {stock_name}: {exc}")
        return aliases

    if full_pinyin:
        aliases.add(_normalize_text(full_pinyin))
    if initials:
        aliases.add(_normalize_text(initials))

    return {alias for alias in aliases if alias}


def _build_security_item(
    stock_code: str,
    stock_name: str = "",
    *,
    has_report: bool = False,
    in_watchlist: bool = False,
    source: str = "local",
) -> dict[str, Any]:
    cleaned_name = _clean_display_name(stock_name)
    exchange = _exchange_from_code(stock_code)
    aliases = {stock_code}
    if exchange:
        aliases.update(
            {
                f"{exchange}{stock_code}",
                f"{exchange}.{stock_code}",
            }
        )
    aliases.update(_build_name_aliases(cleaned_name))

    return {
        "stock_code": stock_code,
        "stock_name": cleaned_name,
        "normalized_name": _normalize_text(cleaned_name),
        "exchange": exchange,
        "aliases": sorted({_normalize_text(alias) for alias in aliases if alias}),
        "has_report": has_report,
        "in_watchlist": in_watchlist,
        "source": source,
    }


def _merge_security_items(
    base: dict[str, dict[str, Any]],
    incoming: dict[str, Any],
) -> None:
    existing = base.get(incoming["stock_code"])
    if existing is None:
        base[incoming["stock_code"]] = incoming
        return

    if incoming.get("stock_name") and not existing.get("stock_name"):
        existing["stock_name"] = incoming["stock_name"]
        existing["normalized_name"] = incoming["normalized_name"]

    existing["has_report"] = existing["has_report"] or incoming.get("has_report", False)
    existing["in_watchlist"] = existing["in_watchlist"] or incoming.get("in_watchlist", False)
    existing["aliases"] = sorted(set(existing["aliases"]) | set(incoming.get("aliases", [])))

    if existing.get("source") != "market" and incoming.get("source") == "market":
        existing["source"] = "market"


def _load_local_security_items() -> dict[str, dict[str, Any]]:
    items: dict[str, dict[str, Any]] = {}

    watch_path = Path("data/watch_list.json")
    if watch_path.exists():
        try:
            payload = json.loads(watch_path.read_text(encoding="utf-8"))
            for row in payload.get("items", []):
                stock_code = str(row.get("stock_code", "")).strip()
                if not stock_code:
                    continue
                _merge_security_items(
                    items,
                    _build_security_item(
                        stock_code,
                        str(row.get("stock_name", "")),
                        in_watchlist=True,
                    ),
                )
        except Exception as exc:
            logger.warning(f"Failed to load watch list for security lookup: {exc}")

    if REPORTS_DIR.exists():
        for path in REPORTS_DIR.glob("*_meta.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                stock_code = str(payload.get("stock_code", "")).strip()
                if not stock_code:
                    continue
                _merge_security_items(
                    items,
                    _build_security_item(
                        stock_code,
                        str(payload.get("stock_name", "")),
                        has_report=True,
                    ),
                )
            except Exception as exc:
                logger.warning(f"Failed to parse report meta for security lookup: {path} | {exc}")

    return items


def _load_market_security_items() -> list[dict[str, Any]]:
    for proxy_key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]:
        os.environ.pop(proxy_key, None)
    os.environ["no_proxy"] = "*"

    import akshare as ak

    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        df = ak.stock_info_a_code_name()

    items: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        stock_code = str(row.get("code", "")).strip()
        stock_name = str(row.get("name", "")).strip()
        if not stock_code:
            continue
        items.append(
            _build_security_item(
                stock_code,
                stock_name,
                source="market",
            )
        )

    return items


def _serialize_security_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for item in items:
        serialized.append(
            {
                "stock_code": item["stock_code"],
                "stock_name": item.get("stock_name", ""),
                "exchange": item.get("exchange", ""),
                "aliases": list(item.get("aliases", [])),
                "has_report": bool(item.get("has_report", False)),
                "in_watchlist": bool(item.get("in_watchlist", False)),
                "source": item.get("source", "local"),
            }
        )
    return serialized


def _deserialize_security_items(raw_items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    items: dict[str, dict[str, Any]] = {}
    for raw in raw_items:
        stock_code = str(raw.get("stock_code", "")).strip()
        if not stock_code:
            continue

        item = _build_security_item(
            stock_code,
            str(raw.get("stock_name", "")),
            has_report=bool(raw.get("has_report", False)),
            in_watchlist=bool(raw.get("in_watchlist", False)),
            source=str(raw.get("source", "local")),
        )
        if raw.get("exchange"):
            item["exchange"] = str(raw["exchange"])
        if raw.get("aliases"):
            item["aliases"] = sorted({_normalize_text(alias) for alias in raw["aliases"]})
        _merge_security_items(items, item)

    return items


def _load_security_universe(force_refresh: bool = False) -> tuple[list[dict[str, Any]], str, bool]:
    local_items = _load_local_security_items()
    cached = None if force_refresh else SECURITY_CACHE.get(SECURITY_CACHE_KEY)
    if isinstance(cached, dict) and isinstance(cached.get("items"), list):
        cached_items = _deserialize_security_items(cached["items"])
        for local_item in local_items.values():
            _merge_security_items(cached_items, local_item)
        return list(cached_items.values()), "cache", bool(cached.get("fallback", False))

    try:
        market_items = _load_market_security_items()
        merged: dict[str, dict[str, Any]] = {
            item["stock_code"]: item
            for item in market_items
        }
        for local_item in local_items.values():
            _merge_security_items(merged, local_item)

        serialized_items = _serialize_security_items(list(merged.values()))
        SECURITY_CACHE.set(
            SECURITY_CACHE_KEY,
            {
                "items": serialized_items,
                "fallback": False,
            },
            ttl=60 * 60 * 12,
        )
        return list(merged.values()), "market", False
    except Exception as exc:
        logger.warning(f"Security universe refresh failed, using local fallback: {exc}")
        return list(local_items.values()), "local", True


def _match_security_score(item: dict[str, Any], query: str) -> float | None:
    normalized_query = _normalize_text(query)
    if not normalized_query:
        return None

    stock_code = item["stock_code"]
    normalized_name = item.get("normalized_name", "")
    aliases = item.get("aliases", [])
    score = 0.0

    if stock_code == normalized_query:
        score = max(score, 120.0)
    elif stock_code.startswith(normalized_query):
        score = max(score, 104.0 - len(stock_code) * 0.1)
    elif normalized_query in stock_code:
        score = max(score, 84.0)

    if normalized_name == normalized_query:
        score = max(score, 118.0)
    elif normalized_name.startswith(normalized_query):
        score = max(score, 96.0 - len(normalized_name) * 0.05)
    elif normalized_query in normalized_name:
        score = max(score, 80.0)

    for alias in aliases:
        if alias == normalized_query:
            score = max(score, 100.0)
        elif alias.startswith(normalized_query):
            score = max(score, 92.0)
        elif normalized_query in alias:
            score = max(score, 78.0)

    if score == 0.0:
        return None

    if item.get("has_report"):
        score += 6.0
    if item.get("in_watchlist"):
        score += 4.0

    return score


def _score_security_items(
    universe: list[dict[str, Any]],
    query: str,
    limit: int,
) -> list[dict[str, Any]]:
    scored: list[tuple[float, dict[str, Any]]] = []

    for item in universe:
        score = _match_security_score(item, query)
        if score is None:
            continue
        scored.append((score, item))

    scored.sort(
        key=lambda entry: (
            -entry[0],
            not entry[1].get("has_report", False),
            not entry[1].get("in_watchlist", False),
            entry[1]["stock_code"],
        )
    )

    return [item for _, item in scored[:limit]]


def _search_security_items(query: str, limit: int) -> tuple[list[dict[str, Any]], str, bool]:
    universe, source, fallback = _load_security_universe()
    matches = _score_security_items(universe, query, limit)

    # Cache can become stale or incomplete; retry once with a fresh market snapshot.
    if not matches and source == "cache":
        universe, source, fallback = _load_security_universe(force_refresh=True)
        matches = _score_security_items(universe, query, limit)

    return matches, source, fallback


@router.get("/securities/search", response_model=SecurityLookupResponse)
async def search_securities(
    q: str = Query(..., min_length=1, description="Security code or name"),
    limit: int = Query(8, ge=1, le=20, description="Maximum result count"),
) -> SecurityLookupResponse:
    """Search stock code and name suggestions for the research input."""
    query = q.strip()
    if not query:
        return SecurityLookupResponse(query=q, items=[], total=0)

    matches, source, fallback = await asyncio.to_thread(_search_security_items, query, limit)
    items = [
        SecurityLookupItemResponse(
            stock_code=item["stock_code"],
            stock_name=item.get("stock_name", ""),
            exchange=item.get("exchange", ""),
            has_report=bool(item.get("has_report", False)),
            in_watchlist=bool(item.get("in_watchlist", False)),
        )
        for item in matches
    ]
    return SecurityLookupResponse(
        query=query,
        items=items,
        total=len(items),
        source=source,
        fallback=fallback,
    )
