"""AKShare 数据源适配器 - 从现有 collector.py 提取的核心采集逻辑"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

# 清除代理设置
for _proxy_key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]:
    os.environ.pop(_proxy_key, None)
os.environ["no_proxy"] = "*"

from .base import DataSourceAdapter, SourceHealth, CollectionResult, SUPPORTED_DATA_TYPES

# 速率限制
MIN_REQUEST_INTERVAL = 0.5


class AKShareAdapter(DataSourceAdapter):
    """AKShare 数据源适配器

    封装 AKShare 接口调用，统一返回 CollectionResult。
    """

    def __init__(self) -> None:
        self._last_request_time: float = 0.0

    @property
    def name(self) -> str:
        return "akshare"

    @property
    def display_name(self) -> str:
        return "AKShare (东方财富/同花顺/巨潮)"

    @property
    def priority(self) -> int:
        return 1

    def get_supported_types(self) -> list[str]:
        return [
            "stock_info",
            "daily_prices",
            "realtime_quote",
            "financials",
            "announcements",
            "governance",
            "research_reports",
            "shareholders",
            "industry",
            "valuation_pct",
            "news",
        ]

    def health_check(self) -> SourceHealth:
        start = time.time()
        try:
            import akshare as ak
            self._rate_limit()
            # 简单测试：获取上证指数最新一条
            df = ak.stock_zh_index_daily(symbol="sh000001")
            if df is not None and not df.empty:
                return SourceHealth(
                    name=self.name,
                    status="healthy",
                    latency_ms=int((time.time() - start) * 1000),
                )
            return SourceHealth(
                name=self.name,
                status="degraded",
                latency_ms=int((time.time() - start) * 1000),
                error="返回空数据",
            )
        except Exception as e:
            return SourceHealth(
                name=self.name,
                status="down",
                latency_ms=int((time.time() - start) * 1000),
                error=str(e),
            )

    def collect(self, data_type: str, target: str, **kwargs: Any) -> CollectionResult:
        start = time.time()
        try:
            handler = getattr(self, f"_collect_{data_type}", None)
            if handler is None:
                return CollectionResult(
                    target=target,
                    data_type=data_type,
                    source_name=self.name,
                    status="failed",
                    error=f"不支持的数据类型: {data_type}",
                )

            data = handler(target, **kwargs)
            elapsed = int((time.time() - start) * 1000)

            record_count = 0
            if isinstance(data, dict):
                record_count = len(data)
            elif isinstance(data, list):
                record_count = len(data)

            return CollectionResult(
                target=target,
                data_type=data_type,
                source_name=self.name,
                status="success",
                records_fetched=record_count,
                data=data if isinstance(data, dict) else {"items": data},
                duration_ms=elapsed,
            )
        except Exception as e:
            elapsed = int((time.time() - start) * 1000)
            return CollectionResult(
                target=target,
                data_type=data_type,
                source_name=self.name,
                status="failed",
                error=str(e),
                duration_ms=elapsed,
            )

    # ================================================================
    # 速率控制
    # ================================================================

    def _rate_limit(self) -> None:
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def _akshare_call(self, func_name: str, **kwargs: Any) -> pd.DataFrame:
        import akshare as ak
        self._rate_limit()
        func = getattr(ak, func_name, None)
        if func is None:
            raise ValueError(f"AKShare 函数不存在: {func_name}")
        result = func(**kwargs)
        if result is None:
            raise ValueError(f"{func_name} 返回 None")
        return result

    # ================================================================
    # 各类型采集方法
    # ================================================================

    def _collect_stock_info(self, stock_code: str, **kwargs: Any) -> dict[str, Any]:
        df = self._akshare_call("stock_individual_info_em", symbol=stock_code)
        info: dict[str, str] = {}
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                info[str(row.iloc[0])] = str(row.iloc[1])

        return {
            "code": stock_code,
            "name": info.get("股票简称", ""),
            "exchange": info.get("上市板块"),
            "listing_date": info.get("上市时间"),
            "industry_sw": info.get("行业"),
            "actual_controller": info.get("实际控制人"),
            "main_business": (info.get("经营范围", ""))[:500],
        }

    def _collect_daily_prices(self, stock_code: str, **kwargs: Any) -> list[dict[str, Any]]:
        end_date = kwargs.get("end_date", date.today().strftime("%Y%m%d"))
        start_date = kwargs.get(
            "start_date",
            (date.today() - timedelta(days=365 * 3)).strftime("%Y%m%d"),
        )

        df = self._akshare_call(
            "stock_zh_a_hist",
            symbol=stock_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )

        if df is None or df.empty:
            return []

        prices = []
        for _, row in df.iterrows():
            prices.append({
                "date": str(row.get("日期", "")),
                "open": self._safe_float(row.get("开盘")),
                "close": self._safe_float(row.get("收盘")),
                "high": self._safe_float(row.get("最高")),
                "low": self._safe_float(row.get("最低")),
                "volume": self._safe_float(row.get("成交量")),
                "amount": self._safe_float(row.get("成交额")),
                "turnover_rate": self._safe_float(row.get("换手率")),
            })
        return prices

    def _collect_realtime_quote(self, stock_code: str, **kwargs: Any) -> dict[str, Any]:
        df = self._akshare_call("stock_zh_a_spot_em")
        if df is None or df.empty:
            return {}

        row = df[df["代码"] == stock_code]
        if row.empty:
            return {}

        r = row.iloc[0]
        return {
            "code": stock_code,
            "date": str(date.today()),
            "open": self._safe_float(r.get("今开")),
            "close": self._safe_float(r.get("最新价")),
            "high": self._safe_float(r.get("最高")),
            "low": self._safe_float(r.get("最低")),
            "volume": self._safe_float(r.get("成交量")),
            "amount": self._safe_float(r.get("成交额")),
            "turnover_rate": self._safe_float(r.get("换手率")),
            "pe_ttm": self._safe_float(r.get("市盈率-动态")),
            "pb_mrq": self._safe_float(r.get("市净率")),
            "market_cap": self._safe_float(r.get("总市值")),
        }

    def _collect_financials(self, stock_code: str, **kwargs: Any) -> list[dict[str, Any]]:
        import akshare as ak
        self._rate_limit()
        try:
            df = ak.stock_financial_abstract_ths(symbol=stock_code)
        except Exception:
            df = None

        if df is None or df.empty:
            return []

        results = []
        for _, row in df.iterrows():
            report_date_str = str(row.get("报告期", ""))
            if not report_date_str or report_date_str == "None":
                continue
            results.append({
                "report_date": report_date_str,
                "revenue": self._safe_float(row.get("营业总收入")),
                "net_profit": self._safe_float(row.get("净利润")),
                "total_assets": self._safe_float(row.get("总资产")),
                "equity": self._safe_float(row.get("所有者权益合计")),
                "operating_cashflow": self._safe_float(row.get("经营活动产生的现金流量净额")),
                "roe": self._safe_float(row.get("净资产收益率(%)")),
                "gross_margin": self._safe_float(row.get("销售毛利率(%)")),
                "net_margin": self._safe_float(row.get("销售净利率(%)")),
            })
        return results

    def _collect_announcements(self, stock_code: str, **kwargs: Any) -> list[dict[str, Any]]:
        df = self._akshare_call("stock_notice_report", symbol=stock_code)
        if df is None or df.empty:
            return []
        results = []
        for _, row in df.head(20).iterrows():
            results.append({
                "title": str(row.get("公告标题", "")),
                "type": str(row.get("公告类型", "")),
                "date": str(row.get("公告日期", "")),
            })
        return results

    def _collect_governance(self, stock_code: str, **kwargs: Any) -> dict[str, Any]:
        result: dict[str, Any] = {"code": stock_code}

        # 股权质押
        try:
            df = self._akshare_call("stock_cg_equity_mortgage_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                result["pledge_count"] = len(df)
        except Exception:
            pass

        # 担保信息
        try:
            df = self._akshare_call("stock_cg_guarantee_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                result["guarantee_count"] = len(df)
        except Exception:
            pass

        # 诉讼信息
        try:
            df = self._akshare_call("stock_cg_lawsuit_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                result["lawsuit_count"] = len(df)
        except Exception:
            pass

        return result

    def _collect_research_reports(self, stock_code: str, **kwargs: Any) -> list[dict[str, Any]]:
        df = self._akshare_call("stock_research_report_em", symbol=stock_code)
        if df is None or df.empty:
            return []
        results = []
        for _, row in df.head(10).iterrows():
            results.append({
                "title": str(row.get("title", row.get("标题", ""))),
                "institution": str(row.get("org", row.get("机构", ""))),
                "rating": str(row.get("em_rating", row.get("评级", ""))),
                "publish_date": str(row.get("publish_date", row.get("日期", ""))),
            })
        return results

    def _collect_shareholders(self, stock_code: str, **kwargs: Any) -> dict[str, Any]:
        result: dict[str, Any] = {"code": stock_code}

        # 十大流通股东
        try:
            df = self._akshare_call("stock_circulate_stock_holder", symbol=stock_code)
            if df is not None and not df.empty:
                top10 = []
                for _, row in df.head(10).iterrows():
                    top10.append({
                        "name": str(row.iloc[0]) if len(row) > 0 else "",
                        "shares": self._safe_float(row.iloc[1]) if len(row) > 1 else None,
                        "ratio": self._safe_float(row.iloc[2]) if len(row) > 2 else None,
                    })
                result["top_shareholders"] = top10
        except Exception:
            pass

        return result

    def _collect_industry(self, stock_code: str, **kwargs: Any) -> dict[str, Any]:
        result: dict[str, Any] = {"code": stock_code}

        try:
            df = self._akshare_call("stock_industry_clf_hist_sw", symbol=stock_code)
            if df is not None and not df.empty:
                row = df.iloc[0]
                result["industry_name"] = str(row.get("行业名称", ""))
                result["industry_level"] = str(row.get("行业级别", ""))
        except Exception:
            pass

        return result

    def _collect_valuation_pct(self, stock_code: str, **kwargs: Any) -> dict[str, Any]:
        result: dict[str, Any] = {"code": stock_code}

        try:
            df = self._akshare_call("stock_index_pe_lg", symbol=stock_code)
            if df is not None and not df.empty:
                row = df.iloc[-1]
                result["pe_ttm_current"] = self._safe_float(row.get("pe", row.iloc[0] if len(row) > 0 else None))
                result["pe_ttm_percentile"] = self._safe_float(row.get("percentile", row.iloc[1] if len(row) > 1 else None))
        except Exception:
            pass

        try:
            df = self._akshare_call("stock_index_pb_lg", symbol=stock_code)
            if df is not None and not df.empty:
                row = df.iloc[-1]
                result["pb_mrq_current"] = self._safe_float(row.get("pb", row.iloc[0] if len(row) > 0 else None))
                result["pb_mrq_percentile"] = self._safe_float(row.get("percentile", row.iloc[1] if len(row) > 1 else None))
        except Exception:
            pass

        return result

    def _collect_news(self, stock_code: str, **kwargs: Any) -> list[dict[str, Any]]:
        results = []

        # 东方财富新闻
        try:
            df = self._akshare_call("stock_news_em", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(20).iterrows():
                    results.append({
                        "title": str(row.get("新闻标题", row.iloc[0] if len(row) > 0 else "")),
                        "content": str(row.get("新闻内容", row.iloc[1] if len(row) > 1 else ""))[:500],
                        "source": str(row.get("来源", row.iloc[2] if len(row) > 2 else "")),
                        "publish_time": str(row.get("发布时间", row.iloc[3] if len(row) > 3 else "")),
                    })
        except Exception:
            pass

        # 财联社要闻
        try:
            df = self._akshare_call("stock_news_main_cx", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(10).iterrows():
                    results.append({
                        "title": str(row.iloc[0]) if len(row) > 0 else "",
                        "content": str(row.iloc[1])[:500] if len(row) > 1 else "",
                        "source": "财联社",
                        "publish_time": str(row.iloc[2]) if len(row) > 2 else "",
                    })
        except Exception:
            pass

        return results

    # ================================================================
    # 工具方法
    # ================================================================

    @staticmethod
    def _safe_float(v: Any) -> float | None:
        if v is None or v == "" or v == "-" or v is False:
            return None
        s = str(v).strip().replace(",", "")
        if s.endswith("%"):
            try:
                return float(s[:-1])
            except ValueError:
                return None
        multipliers = {"亿": 1e8, "万": 1e4}
        for suffix, mult in multipliers.items():
            if s.endswith(suffix):
                try:
                    return float(s[:-1]) * mult
                except ValueError:
                    return None
        try:
            return float(s)
        except (ValueError, TypeError):
            return None
