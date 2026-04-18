from __future__ import annotations

import pandas as pd

from investresearch.core.models import PeerCompany
from investresearch.data_layer.cache import FileCache
from investresearch.data_layer.industry_peers import (
    PeerIdentifier,
    PeerReportCollector,
    _normalize_pdf_text,
)


def test_peer_identifier_filters_inactive_codes_and_fills_names(
    tmp_path,
    monkeypatch,
) -> None:
    identifier = PeerIdentifier(cache=FileCache(str(tmp_path)))

    industry_df = pd.DataFrame(
        [
            {"symbol": "002558", "start_date": "2021-07-30", "industry_code": "720401", "update_time": "2025-10-15"},
            {"symbol": "000835", "start_date": "2021-07-30", "industry_code": "720401", "update_time": "2021-07-31"},
            {"symbol": "002113", "start_date": "2021-07-30", "industry_code": "720401", "update_time": "2021-07-31"},
            {"symbol": "002174", "start_date": "2021-07-30", "industry_code": "720401", "update_time": "2025-10-15"},
            {"symbol": "002425", "start_date": "2021-07-30", "industry_code": "720401", "update_time": "2025-10-15"},
            {"symbol": "002447", "start_date": "2021-07-30", "industry_code": "720401", "update_time": "2021-07-31"},
            {"symbol": "002464", "start_date": "2021-07-30", "industry_code": "720401", "update_time": "2021-07-31"},
            {"symbol": "002517", "start_date": "2021-07-30", "industry_code": "720401", "update_time": "2025-10-15"},
            {"symbol": "002555", "start_date": "2021-07-30", "industry_code": "720401", "update_time": "2025-10-15"},
        ]
    )
    active_df = pd.DataFrame(
        [
            {"code": "002174", "name": "游族网络"},
            {"code": "002425", "name": "凯撒文化"},
            {"code": "002517", "name": "恺英网络"},
            {"code": "002555", "name": "三七互娱"},
            {"code": "002558", "name": "巨人网络"},
        ]
    )

    def fake_akshare_call(func_name: str, **kwargs):
        del kwargs
        if func_name == "stock_industry_clf_hist_sw":
            return industry_df
        if func_name == "stock_info_a_code_name":
            return active_df
        if func_name == "stock_zh_a_spot_em":
            return pd.DataFrame(columns=["代码", "总市值"])
        raise AssertionError(f"unexpected akshare function: {func_name}")

    monkeypatch.setattr(identifier, "_akshare_call", fake_akshare_call)

    peers = identifier._find_peers_by_code(
        stock_code="002558",
        code_prefix="720401",
        industry_sw_name="游戏Ⅱ",
        level_name="三级行业",
        max_peers=8,
    )

    assert [peer.stock_code for peer in peers] == ["002174", "002425", "002517", "002555"]
    assert [peer.stock_name for peer in peers] == ["游族网络", "凯撒文化", "恺英网络", "三七互娱"]


def test_identify_peers_refreshes_stale_cached_peer_list(tmp_path, monkeypatch) -> None:
    cache = FileCache(str(tmp_path))
    cache.set(
        "peers_002558",
        [
            {"stock_code": "000835", "stock_name": "", "industry_sw": "游戏Ⅱ", "industry_level": "三级行业"},
            {"stock_code": "002174", "stock_name": "", "industry_sw": "游戏Ⅱ", "industry_level": "三级行业"},
        ],
        ttl=86400,
    )
    identifier = PeerIdentifier(cache=cache)

    monkeypatch.setattr(
        identifier,
        "_get_active_stock_name_map",
        lambda: {
            "002174": "游族网络",
            "002425": "凯撒文化",
            "002517": "恺英网络",
            "002555": "三七互娱",
            "002558": "巨人网络",
        },
    )
    monkeypatch.setattr(identifier, "_get_latest_industry_code", lambda stock_code: "720401")
    monkeypatch.setattr(
        identifier,
        "_find_peers_by_code",
        lambda **kwargs: [
            PeerCompany(stock_code="002174", stock_name="游族网络", industry_sw="游戏Ⅱ", industry_level="三级行业"),
            PeerCompany(stock_code="002425", stock_name="凯撒文化", industry_sw="游戏Ⅱ", industry_level="三级行业"),
            PeerCompany(stock_code="002517", stock_name="恺英网络", industry_sw="游戏Ⅱ", industry_level="三级行业"),
            PeerCompany(stock_code="002555", stock_name="三七互娱", industry_sw="游戏Ⅱ", industry_level="三级行业"),
        ],
    )

    peers = identifier.identify_peers("002558", industry_sw_name="游戏Ⅱ", min_peers=3)

    assert [peer.stock_code for peer in peers] == ["002174", "002425", "002517", "002555"]
    assert [peer.stock_name for peer in peers] == ["游族网络", "凯撒文化", "恺英网络", "三七互娱"]


def test_extract_industry_data_points_handles_cjk_spacing_and_sales_revenue_phrase(
    tmp_path,
) -> None:
    collector = PeerReportCollector(cache=FileCache(str(tmp_path)))
    peer = PeerCompany(
        stock_code="002555",
        stock_name="三七互娱",
        industry_sw="游戏Ⅱ",
        industry_level="三级行业",
    )
    raw_text = (
        "根据《2025年中国游戏产业报告》显示，2025年，国内游戏市 场实际销售收入3507.89亿元，"
        "同比增长7.68%；用户规模6.83亿。"
    )

    points = collector._extract_industry_data_points(
        text=_normalize_pdf_text(raw_text),
        peer=peer,
        pdf_url="https://example.com/report.pdf",
        industry_name="游戏Ⅱ",
    )

    market_size_points = [item for item in points if item.metric_name == "market_size"]
    assert len(market_size_points) == 1
    assert market_size_points[0].metric_value == 3507.89
    assert market_size_points[0].source_company == "三七互娱"


def test_detect_consulting_sources_recognizes_newzoo(tmp_path) -> None:
    collector = PeerReportCollector(cache=FileCache(str(tmp_path)))
    text = _normalize_pdf_text(
        "根据Newzoo发布的《2024年全球游戏市场报告》，2024年全球游戏市场将实现1877亿美元收入。"
    )

    refs = collector._detect_consulting_sources(text)

    assert refs
    assert refs[0][0] == "Newzoo"
