"""行业同业识别与批量年报采集模块.

从申万行业分类中识别同业公司，批量采集年报PDF，
提取行业市场数据（市场规模/CAGR/CR5/市占率）和咨询机构引用。
"""

from __future__ import annotations

import io
import os
import re
import time
from datetime import date, datetime
from typing import Any, Callable

import pandas as pd
import requests

from investresearch.core.logging import get_logger
from investresearch.core.models import (
    EvidenceRef,
    IndustryDataPoint,
    PeerCompany,
)

from .cache import FileCache

logger = get_logger("agent.industry_peers")

# 速率限制
_MIN_REQUEST_INTERVAL = 0.6


# ============================================================
# 同业识别
# ============================================================


class PeerIdentifier:
    """基于申万行业分类的同业公司识别器."""

    def __init__(self, cache: FileCache | None = None) -> None:
        self._cache = cache or FileCache()
        self._logger = get_logger("peer_identifier")
        self._last_request_time: float = 0.0

    def _rate_limit(self) -> None:
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()

    def _akshare_call(self, func_name: str, **kwargs: Any) -> pd.DataFrame:
        import akshare as ak

        self._rate_limit()
        func = getattr(ak, func_name, None)
        if func is None:
            raise ValueError(f"AKShare函数不存在: {func_name}")
        return func(**kwargs)

    def identify_peers(
        self,
        stock_code: str,
        industry_sw_name: str = "",
        *,
        max_peers: int = 8,
        min_peers: int = 3,
    ) -> list[PeerCompany]:
        """识别同业公司.

        策略:
        1. 从 stock_industry_clf_hist_sw 获取标的最新SW分类代码
        2. 找到相同三级行业的所有股票
        3. 三级不够3家 → 降级到二级(代码前4位)
        4. 二级不够 → 降级到一级(代码前2位)
        5. 按市值排序取 TOP N
        """
        cache_key = f"peers_{stock_code}"
        cached = self._cache.get(cache_key)
        if cached:
            cached_peers = [PeerCompany(**p) for p in cached]
            active_stock_names = self._get_active_stock_name_map()
            if not active_stock_names:
                return cached_peers

            normalized_cached: list[PeerCompany] = []
            for peer in cached_peers:
                if peer.stock_code not in active_stock_names:
                    continue
                peer_payload = peer.model_dump()
                peer_payload["stock_name"] = active_stock_names.get(peer.stock_code, peer.stock_name or "")
                normalized_cached.append(PeerCompany(**peer_payload))

            if len(normalized_cached) >= min_peers:
                result = normalized_cached[:max_peers]
                self._cache.set(cache_key, [p.model_dump() for p in result], ttl=86400)
                return result

        # 获取SW行业分类数据
        industry_code = self._get_latest_industry_code(stock_code)
        if not industry_code:
            self._logger.warning(f"未找到 {stock_code} 的SW行业分类")
            return []

        self._logger.info(f"{stock_code} SW行业代码: {industry_code}")

        # 逐级查找同业: 三级 → 二级 → 一级
        for level, code_prefix, level_name in [
            (3, industry_code, "三级行业"),
            (2, industry_code[:4], "二级行业"),
            (1, industry_code[:2], "一级行业"),
        ]:
            peers = self._find_peers_by_code(
                stock_code=stock_code,
                code_prefix=code_prefix,
                industry_sw_name=industry_sw_name,
                level_name=level_name,
                max_peers=max_peers,
            )
            if len(peers) >= min_peers:
                result = peers[:max_peers]
                self._cache.set(cache_key, [p.model_dump() for p in result], ttl=86400)
                self._logger.info(
                    f"找到 {len(result)} 家同业({level_name}), "
                    f"代码前缀={code_prefix}"
                )
                return result

        self._logger.warning(f"未找到足够同业(最少{min_peers}家)")
        return []

    def _get_latest_industry_code(self, stock_code: str) -> str:
        """获取标的最新的SW行业分类代码."""
        try:
            df = self._akshare_call("stock_industry_clf_hist_sw")
            stock_rows = df[df["symbol"] == stock_code]
            if stock_rows.empty:
                return ""
            latest = stock_rows.sort_values("start_date", ascending=False).iloc[0]
            return str(latest["industry_code"])
        except Exception as e:
            self._logger.warning(f"获取SW分类失败: {e}")
            return ""

    def _get_active_stock_name_map(self) -> dict[str, str]:
        """获取当前仍在A股列表中的股票代码与名称映射."""
        cache_key = "active_a_stock_name_map"
        cached = self._cache.get(cache_key)
        if isinstance(cached, dict) and cached:
            return {str(code): str(name) for code, name in cached.items()}

        try:
            df = self._akshare_call("stock_info_a_code_name")
            if df is None or df.empty:
                return {}

            code_col = "code" if "code" in df.columns else None
            name_col = "name" if "name" in df.columns else None
            if not code_col or not name_col:
                return {}

            result = {
                str(row[code_col]): str(row[name_col]).strip()
                for _, row in df.iterrows()
                if row.get(code_col) and row.get(name_col)
            }
            if result:
                self._cache.set(cache_key, result, ttl=86400)
            return result
        except Exception as e:
            self._logger.warning(f"获取A股代码名称映射失败: {e}")
            return {}

    def _find_peers_by_code(
        self,
        stock_code: str,
        code_prefix: str,
        industry_sw_name: str,
        level_name: str,
        max_peers: int,
    ) -> list[PeerCompany]:
        """按行业代码前缀查找同业公司."""
        try:
            df = self._akshare_call("stock_industry_clf_hist_sw")
        except Exception as e:
            self._logger.warning(f"获取行业分类失败: {e}")
            return []

        # 每只股票取最新行业代码
        latest_per_stock: dict[str, str] = {}
        for _, row in df.iterrows():
            sym = str(row["symbol"])
            code = str(row["industry_code"])
            # 只保留最新(数据已按日期排序)
            latest_per_stock[sym] = code

        # 筛选同行业
        peer_codes = [
            sym for sym, code in latest_per_stock.items()
            if code.startswith(code_prefix) and sym != stock_code
        ]

        if not peer_codes:
            return []

        active_stock_names = self._get_active_stock_name_map()
        if active_stock_names:
            peer_codes = [code for code in peer_codes if code in active_stock_names]
            if not peer_codes:
                return []

        # 获取市值排序
        market_caps = self._get_market_caps(peer_codes)
        peer_codes_with_cap = [
            (code, market_caps.get(code))
            for code in peer_codes
        ]
        peer_codes_with_cap.sort(
            key=lambda x: x[1] if x[1] is not None else 0,
            reverse=True,
        )

        peers: list[PeerCompany] = []
        for rank, (code, cap) in enumerate(peer_codes_with_cap[:max_peers], 1):
            peers.append(PeerCompany(
                stock_code=code,
                stock_name=active_stock_names.get(code, ""),
                industry_sw=industry_sw_name,
                industry_level=level_name,
                market_cap=cap,
                rank_in_industry=rank,
            ))

        return peers

    def _get_market_caps(self, stock_codes: list[str]) -> dict[str, float]:
        """批量获取市值."""
        try:
            df = self._akshare_call("stock_zh_a_spot_em")
            if df is None or df.empty:
                return {}
            result: dict[str, float] = {}
            for code in stock_codes:
                row = df[df["代码"] == code]
                if not row.empty:
                    cap = _safe_float(row.iloc[0].get("总市值"))
                    if cap is not None:
                        result[code] = cap
            return result
        except Exception as e:
            self._logger.warning(f"获取市值失败: {e}")
            return {}


# ============================================================
# 同业年报采集
# ============================================================


# 行业数据提取正则模式
_MARKET_SIZE_PATTERNS = [
    r"(?:全球|中国|国内)([^\s,，。；:：]{0,12})(?:市场|行业)(?:实际销售收入|销售收入|收入规模|市场收入|市场总收入|规模|空间|总额)[:：]?\s*(?:约为|约|达到|达|为|将实现|实现|突破)?\s*([0-9,.]+)\s*(万亿元|亿元|万元|亿)",
    r"([^\s,，。；:：]{0,12})(?:市场|行业)(?:实际销售收入|销售收入|收入规模|市场收入|市场总收入|规模)(?:约为|约|达到|达|为|将实现|实现|突破)?[:：]?\s*([0-9,.]+)\s*(万亿元|亿元|万元|亿)",
    r"(?:全球|中国|国内)([^\s,，。；:：]{0,12})(?:市场|行业)(?:需求|容量)[:：]?\s*(?:约为|约|达到|达|为)?\s*([0-9,.]+)\s*(万亿元|亿元|万元|亿)",
]

_CAGR_PATTERNS = [
    r"(?:复合增长率|年复合增长率|CAGR)[:：]?\s*约?\s*([0-9,.]+)\s*(%|％)",
    r"(?:年均增长率|年均增速|年化增长率|年化增速)[:：]?\s*([0-9,.]+)\s*(%|％)",
    r"(?:预计|预计未来(?:\d+)年)(?:年均|复合)(?:增长|增速)[:：]?\s*([0-9,.]+)\s*(%|％)",
]

_CR5_PATTERNS = [
    r"CR5[:：]?\s*(?:约为|约|达到)?\s*([0-9,.]+)\s*(%|％)",
    r"(?:行业|市场)(?:前五|前五名|前5名)(?:集中度|市占率|份额合计)[:：]?\s*([0-9,.]+)\s*(%|％)",
    r"(?:行业|市场)(?:集中度|CR5)[:：]?\s*(?:约为|约|达到)?\s*([0-9,.]+)\s*(%|％)",
]

_MARKET_SHARE_PATTERNS = [
    r"(?:公司|本公司)(?:在|于)(?:{industry})?(?:行业|市场|领域|细分)(?:市占率|市场份额|市场占有率)[:：]?\s*(?:约为|约)?\s*([0-9,.]+)\s*(%|％)",
    r"(?:公司|本公司)(?:全球|中国|国内)(?:市场|行业)(?:份额|占有率)[:：]?\s*([0-9,.]+)\s*(%|％)",
    r"(?:市占率|市场份额|市场占有率)[:：]?\s*(?:约为|约|达到|突破)?\s*([0-9,.]+)\s*(%|％)",
]

# 咨询机构检测模式
_CONSULTING_FIRM_PATTERNS = [
    (r"(?:根据|据|引用|参考)(IDC)(?:的|报告|数据|统计|发布)", "IDC"),
    (r"(?:根据|据|引用|参考)(Gartner)(?:的|报告|数据|统计)", "Gartner"),
    (r"(?:根据|据|引用|参考)(Newzoo)(?:的|报告|数据|统计|发布)", "Newzoo"),
    (r"(?:根据|据|引用|参考)(Frost\s*&?\s*Sullivan|弗若斯特沙利文|沙利文)(?:的|报告|数据|统计)", "Frost & Sullivan"),
    (r"(?:根据|据|引用|参考)(Euromonitor|欧睿)(?:的|报告|数据|统计)", "Euromonitor"),
    (r"(?:根据|据|引用|参考)(IHS|Markit)(?:的|报告|数据|统计)", "IHS Markit"),
    (r"(?:根据|据|引用|参考)(赛迪|赛迪顾问)(?:的|报告|数据|统计)", "赛迪"),
    (r"(?:根据|据|引用|参考)(艾瑞|艾瑞咨询)(?:的|报告|数据|统计)", "艾瑞"),
    (r"(?:根据|据|引用|参考)(智研咨询|智研)(?:的|报告|数据|统计)", "智研咨询"),
    (r"(?:根据|据|引用|参考)(前瞻产业研究院|前瞻)(?:的|报告|数据|统计)", "前瞻产业研究院"),
    (r"(?:根据|据|引用|参考)(头豹研究院|头豹)(?:的|报告|数据|统计)", "头豹研究院"),
    (r"(?:根据|据|引用|参考)(灼识咨询|灼识)(?:的|报告|数据|统计)", "灼识咨询"),
    (r"(?:根据|据|引用|参考)(中商产业研究院|中商)(?:的|报告|数据|统计)", "中商产业研究院"),
    (r"(?:根据|据|引用|参考)(观研天下|观研)(?:的|报告|数据|统计)", "观研天下"),
    (r"(?:根据|据|引用|参考)(Frost\s*China)(?:的|报告|数据|统计)", "Frost & Sullivan"),
]


class PeerReportCollector:
    """批量采集同业公司年报并提取行业数据."""

    def __init__(self, cache: FileCache | None = None) -> None:
        self._cache = cache or FileCache()
        self._logger = get_logger("peer_report_collector")
        self._session = requests.Session()
        self._session.trust_env = False
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
        })
        self._last_request_time: float = 0.0

    def _rate_limit(self) -> None:
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()

    def collect_peer_reports(
        self,
        peers: list[PeerCompany],
        industry_name: str = "",
        progress_callback: Callable[[str, str], None] | None = None,
    ) -> list[IndustryDataPoint]:
        """批量采集同业年报并提取行业数据.

        对每家同业公司:
        1. 从巨潮查找最新年报
        2. 下载PDF并提取文本
        3. 正则提取行业数据点
        4. 检测咨询机构引用
        """
        all_data_points: list[IndustryDataPoint] = []

        for i, peer in enumerate(peers):
            msg = f"[{i + 1}/{len(peers)}] 采集 {peer.stock_name or peer.stock_code} 年报..."
            self._logger.info(msg)
            if progress_callback:
                progress_callback("industry_peers", msg)

            try:
                points = self._collect_single_peer(peer, industry_name)
                all_data_points.extend(points)
                self._logger.info(
                    f"  {peer.stock_code}: 提取到 {len(points)} 个行业数据点"
                )
            except Exception as e:
                self._logger.warning(
                    f"  {peer.stock_code} 采集失败(跳过): {e}"
                )

        self._logger.info(
            f"同业年报采集完成: {len(peers)}家, "
            f"共提取 {len(all_data_points)} 个数据点"
        )
        return all_data_points

    def _collect_single_peer(
        self,
        peer: PeerCompany,
        industry_name: str,
    ) -> list[IndustryDataPoint]:
        """采集单个同业的年报数据."""
        # 查找年报PDF
        annual_report = self._find_annual_report(peer.stock_code)
        if not annual_report:
            self._logger.debug(f"  {peer.stock_code}: 未找到年报")
            return []

        pdf_url = annual_report.get("pdf_url", "")
        if not pdf_url:
            return []

        # 下载并提取PDF文本
        text = self._download_and_extract_pdf(pdf_url, peer.stock_code)
        if not text:
            return []

        # 提取行业数据点
        data_points = self._extract_industry_data_points(
            text=text,
            peer=peer,
            pdf_url=pdf_url,
            industry_name=industry_name,
        )

        # 检测咨询机构引用并标注
        consulting_refs = self._detect_consulting_sources(text)
        if consulting_refs:
            for dp in data_points:
                if not dp.consulting_firm:
                    # 为没有明确标注的数据点关联咨询机构
                    for firm_name, _excerpt in consulting_refs[:2]:
                        if dp.excerpt and _excerpt[:20] in dp.excerpt:
                            dp.consulting_firm = firm_name
                            dp.source_type = "consulting"
                            break

        return data_points

    def _find_annual_report(self, stock_code: str) -> dict[str, Any] | None:
        """从巨潮查找最新年报."""
        cache_key = f"peer_annual_report_{stock_code}"
        cached = self._cache.get(cache_key)
        if cached:
            return cached

        try:
            from akshare.stock_feature import stock_disclosure_cninfo as cninfo_module

            stock_id_map = getattr(cninfo_module, "__get_stock_json")("沪深京")
            org_id = stock_id_map.get(stock_code)
            if not org_id:
                return None

            category_dict = getattr(cninfo_module, "__get_category_dict")()
            category = category_dict.get("年报", "")

            from datetime import timedelta
            end_date = date.today()
            start_date = end_date - timedelta(days=730)

            payload = {
                "pageNum": "1",
                "pageSize": "5",
                "column": "szse",
                "tabName": "fulltext",
                "plate": "",
                "stock": f"{stock_code},{org_id}",
                "searchkey": "",
                "secid": "",
                "category": category,
                "trade": "",
                "seDate": (
                    f"{start_date.strftime('%Y-%m-%d')}~"
                    f"{end_date.strftime('%Y-%m-%d')}"
                ),
                "sortName": "",
                "sortType": "",
                "isHLtitle": "true",
            }

            self._rate_limit()
            response = self._session.post(
                "http://www.cninfo.com.cn/new/hisAnnouncement/query",
                data=payload,
                headers={
                    "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            for item in data.get("announcements", []):
                title = re.sub(r"<[^>]+>", "", str(item.get("announcementTitle", "")))
                if "年度报告" in title and "摘要" not in title:
                    pdf_path = str(item.get("adjunctUrl", "") or "")
                    pdf_url = f"https://static.cninfo.com.cn/{pdf_path.lstrip('/')}" if pdf_path else ""
                    result = {"title": title, "pdf_url": pdf_url}
                    self._cache.set(cache_key, result, ttl=86400 * 7)
                    return result

        except Exception as e:
            self._logger.debug(f"查找 {stock_code} 年报失败: {e}")

        return None

    def _download_and_extract_pdf(
        self,
        pdf_url: str,
        stock_code: str,
        max_pages: int = 50,
    ) -> str:
        """下载PDF并提取文本."""
        cache_key = f"peer_pdf_text_v2_{_cache_digest(pdf_url)}"
        cached = self._cache.get(cache_key)
        if cached:
            return str(cached)

        try:
            self._rate_limit()
            response = self._session.get(
                pdf_url,
                headers={"Referer": "https://www.cninfo.com.cn/"},
                timeout=60,
            )
            content = response.content
            if not content.startswith(b"%PDF"):
                return ""

            import pdfplumber

            page_texts: list[str] = []
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page in pdf.pages[:max_pages]:
                    text = page.extract_text() or ""
                    if text:
                        page_texts.append(text)

            full_text = "\n".join(page_texts)
            full_text = _normalize_pdf_text(full_text)

            if full_text:
                self._cache.set(cache_key, full_text, ttl=86400 * 7)
            return full_text

        except Exception as e:
            self._logger.debug(f"PDF提取失败 {pdf_url}: {e}")
            return ""

    def _extract_industry_data_points(
        self,
        text: str,
        peer: PeerCompany,
        pdf_url: str,
        industry_name: str = "",
    ) -> list[IndustryDataPoint]:
        """从年报文本中提取行业数据点."""
        data_points: list[IndustryDataPoint] = []
        seen: set[tuple[str, float | None]] = set()

        # 市场规模
        for pattern in _MARKET_SIZE_PATTERNS:
            for match in re.finditer(pattern, text):
                industry_kw = str(match.group(1)).strip() if match.lastindex and match.lastindex >= 1 else ""
                value_str = match.group(2) if match.lastindex and match.lastindex >= 2 else ""
                unit = match.group(3) if match.lastindex and match.lastindex >= 3 else ""
                value = _parse_chinese_amount(value_str, unit)
                key = ("market_size", value)
                if key not in seen and value is not None:
                    seen.add(key)
                    excerpt = _extract_context(text, match.start(), match.end(), 120)
                    data_points.append(IndustryDataPoint(
                        metric_name="market_size",
                        metric_value=value,
                        metric_unit="亿元",
                        source_company=peer.stock_name,
                        source_company_code=peer.stock_code,
                        excerpt=excerpt,
                        pdf_url=pdf_url,
                    ))

        # CAGR
        for pattern in _CAGR_PATTERNS:
            for match in re.finditer(pattern, text):
                value = _safe_float(match.group(1))
                key = ("cagr", value)
                if key not in seen and value is not None:
                    seen.add(key)
                    excerpt = _extract_context(text, match.start(), match.end(), 120)
                    data_points.append(IndustryDataPoint(
                        metric_name="cagr",
                        metric_value=value,
                        metric_unit="%",
                        source_company=peer.stock_name,
                        source_company_code=peer.stock_code,
                        excerpt=excerpt,
                        pdf_url=pdf_url,
                    ))

        # CR5
        for pattern in _CR5_PATTERNS:
            for match in re.finditer(pattern, text):
                value = _safe_float(match.group(1))
                key = ("cr5", value)
                if key not in seen and value is not None:
                    seen.add(key)
                    excerpt = _extract_context(text, match.start(), match.end(), 120)
                    data_points.append(IndustryDataPoint(
                        metric_name="cr5",
                        metric_value=value,
                        metric_unit="%",
                        source_company=peer.stock_name,
                        source_company_code=peer.stock_code,
                        excerpt=excerpt,
                        pdf_url=pdf_url,
                    ))

        # 市场份额
        for raw_pattern in _MARKET_SHARE_PATTERNS:
            pattern = raw_pattern.replace("{industry}", re.escape(industry_name))
            for match in re.finditer(pattern, text):
                value = _safe_float(match.group(1))
                key = ("market_share", value)
                if key not in seen and value is not None:
                    seen.add(key)
                    excerpt = _extract_context(text, match.start(), match.end(), 120)
                    data_points.append(IndustryDataPoint(
                        metric_name="market_share",
                        metric_value=value,
                        metric_unit="%",
                        source_company=peer.stock_name,
                        source_company_code=peer.stock_code,
                        excerpt=excerpt,
                        pdf_url=pdf_url,
                    ))

        return data_points

    def _detect_consulting_sources(
        self,
        text: str,
    ) -> list[tuple[str, str]]:
        """检测咨询机构引用.

        Returns: [(咨询机构名, 摘录), ...]
        """
        results: list[tuple[str, str]] = []
        seen: set[str] = set()

        for pattern, firm_name in _CONSULTING_FIRM_PATTERNS:
            for match in re.finditer(pattern, text):
                if firm_name not in seen:
                    seen.add(firm_name)
                    excerpt = _extract_context(text, match.start(), match.end(), 180)
                    results.append((firm_name, excerpt))

        return results


# ============================================================
# 工具函数
# ============================================================


def _safe_float(v: Any) -> float | None:
    if v is None or v == "" or v == "-":
        return None
    s = str(v).strip().replace(",", "").replace("，", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _normalize_pdf_text(text: str) -> str:
    """规范化 PDF 提取文本，修复中文词组被空格打断的问题."""
    normalized = str(text or "").replace("\u3000", " ").replace("\xa0", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", normalized)
    normalized = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[0-9A-Za-z])", "", normalized)
    normalized = re.sub(r"(?<=[0-9A-Za-z])\s+(?=[\u4e00-\u9fff])", "", normalized)
    return normalized.strip()


def _parse_chinese_amount(number_text: str, unit_text: str = "") -> float | None:
    """解析中文金额并统一转为亿元."""
    if not number_text:
        return None
    try:
        numeric = float(str(number_text).replace(",", "").replace("，", ""))
    except (ValueError, TypeError):
        return None

    unit = str(unit_text or "").strip()
    multiplier = {
        "万亿元": 10000.0,
        "亿": 1.0,
        "亿元": 1.0,
        "万": 0.0001,
        "万元": 0.0001,
    }.get(unit, 1.0)

    return round(numeric * multiplier, 2)


def _extract_context(text: str, start: int, end: int, radius: int = 120) -> str:
    """提取匹配点前后的上下文."""
    ctx_start = max(0, start - radius)
    ctx_end = min(len(text), end + radius)
    context = text[ctx_start:ctx_end].strip()
    return context[:280]


def _cache_digest(value: str) -> str:
    import hashlib
    return hashlib.md5(value.encode("utf-8")).hexdigest()
