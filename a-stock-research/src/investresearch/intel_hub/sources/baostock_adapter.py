"""BaoStock 数据源适配器"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta
from typing import Any

from .base import DataSourceAdapter, SourceHealth, CollectionResult


class BaoStockAdapter(DataSourceAdapter):
    """BaoStock 数据源适配器

    主要提供估值数据和财务数据备源。
    """

    @property
    def name(self) -> str:
        return "baostock"

    @property
    def display_name(self) -> str:
        return "BaoStock (免费A股历史数据)"

    @property
    def priority(self) -> int:
        return 2

    def get_supported_types(self) -> list[str]:
        return ["stock_info", "daily_prices", "financials", "valuation"]

    def health_check(self) -> SourceHealth:
        start = time.time()
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code != "0":
                return SourceHealth(
                    name=self.name,
                    status="down",
                    latency_ms=int((time.time() - start) * 1000),
                    error=f"登录失败: {lg.error_msg}",
                )
            bs.logout()
            return SourceHealth(
                name=self.name,
                status="healthy",
                latency_ms=int((time.time() - start) * 1000),
            )
        except ImportError:
            return SourceHealth(
                name=self.name,
                status="down",
                error="BaoStock 未安装",
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
            import baostock as bs
        except ImportError:
            return CollectionResult(
                target=target,
                data_type=data_type,
                source_name=self.name,
                status="failed",
                error="BaoStock 未安装",
            )

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

            data = handler(bs, target, **kwargs)
            elapsed = int((time.time() - start) * 1000)

            records = data if isinstance(data, (list, dict)) else {}
            count = len(records) if isinstance(records, (list, dict)) else 0

            return CollectionResult(
                target=target,
                data_type=data_type,
                source_name=self.name,
                status="success",
                records_fetched=count,
                data={"items": records} if isinstance(records, list) else records,
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
    # 各类型采集方法
    # ================================================================

    def _collect_stock_info(self, bs: Any, stock_code: str, **kwargs: Any) -> dict[str, Any]:
        bs_code = self._to_baostock_code(stock_code)
        if not bs_code:
            return {}

        lg = bs.login()
        if lg.error_code != "0":
            raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")

        try:
            rs = bs.query_stock_basic(code=bs_code)
            if rs.error_code != "0":
                return {}

            while rs.next():
                row = rs.get_row_data()
                if len(row) < 3:
                    continue
                return {
                    "code": stock_code,
                    "name": str(row[1]),
                    "listing_date": str(row[2]),
                    "type": str(row[4]) if len(row) > 4 else "",
                    "status": str(row[5]) if len(row) > 5 else "",
                }
            return {}
        finally:
            bs.logout()

    def _collect_daily_prices(self, bs: Any, stock_code: str, **kwargs: Any) -> list[dict[str, Any]]:
        bs_code = self._to_baostock_code(stock_code)
        if not bs_code:
            return []

        end_date = kwargs.get("end_date", date.today().strftime("%Y-%m-%d"))
        start_date = kwargs.get(
            "start_date",
            (date.today() - timedelta(days=365 * 3)).strftime("%Y-%m-%d"),
        )

        lg = bs.login()
        if lg.error_code != "0":
            raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")

        try:
            fields = "date,open,high,low,close,volume,amount,turn,pctChg"
            rs = bs.query_history_k_data_plus(
                bs_code, fields,
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2",  # 前复权
            )

            prices = []
            while rs.error_code == "0" and rs.next():
                row = rs.get_row_data()
                prices.append({
                    "date": str(row[0]),
                    "open": self._safe_float(row[1]),
                    "high": self._safe_float(row[2]),
                    "low": self._safe_float(row[3]),
                    "close": self._safe_float(row[4]),
                    "volume": self._safe_float(row[5]),
                    "amount": self._safe_float(row[6]),
                    "turnover_rate": self._safe_float(row[7]),
                    "change_pct": self._safe_float(row[8]),
                })
            return prices
        finally:
            bs.logout()

    def _collect_financials(self, bs: Any, stock_code: str, **kwargs: Any) -> list[dict[str, Any]]:
        bs_code = self._to_baostock_code(stock_code)
        if not bs_code:
            return []

        lg = bs.login()
        if lg.error_code != "0":
            raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")

        try:
            current_year = date.today().year
            start_year = current_year - 4
            merged: dict[str, dict[str, Any]] = {}

            # 盈利能力
            for year in range(start_year, current_year + 1):
                for quarter in range(1, 5):
                    try:
                        rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)
                        while rs.next():
                            row = rs.get_row_data()
                            if len(row) < 9:
                                continue
                            stat_date = str(row[2])
                            if stat_date:
                                merged.setdefault(stat_date, {})
                                merged[stat_date].update({
                                    "roe": self._safe_float(row[3]),
                                    "net_margin": self._safe_float(row[4]),
                                    "gross_margin": self._safe_float(row[5]),
                                    "net_profit": self._safe_float(row[6]),
                                    "revenue": self._safe_float(row[8]),
                                })
                    except Exception:
                        continue

            # 偿债能力
            for year in range(start_year, current_year + 1):
                for quarter in range(1, 5):
                    try:
                        rs = bs.query_balance_data(code=bs_code, year=year, quarter=quarter)
                        while rs.next():
                            row = rs.get_row_data()
                            if len(row) < 8:
                                continue
                            stat_date = str(row[2])
                            if stat_date:
                                merged.setdefault(stat_date, {})
                                merged[stat_date].update({
                                    "current_ratio": self._safe_float(row[3]),
                                    "quick_ratio": self._safe_float(row[4]),
                                    "debt_ratio": self._safe_float(row[7]),
                                })
                    except Exception:
                        continue

            # 成长能力
            for year in range(start_year, current_year + 1):
                for quarter in range(1, 5):
                    try:
                        rs = bs.query_growth_data(code=bs_code, year=year, quarter=quarter)
                        while rs.next():
                            row = rs.get_row_data()
                            if len(row) < 7:
                                continue
                            stat_date = str(row[2])
                            if stat_date:
                                merged.setdefault(stat_date, {})
                                merged[stat_date].update({
                                    "revenue_yoy": self._safe_float(row[6]),
                                    "net_profit_yoy": self._safe_float(row[5]),
                                })
                    except Exception:
                        continue

            results = []
            for stat_date, fields in sorted(merged.items(), reverse=True):
                results.append({"report_date": stat_date, **fields})
            return results
        finally:
            bs.logout()

    def _collect_valuation(self, bs: Any, stock_code: str, **kwargs: Any) -> list[dict[str, Any]]:
        bs_code = self._to_baostock_code(stock_code)
        if not bs_code:
            return []

        lg = bs.login()
        if lg.error_code != "0":
            raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")

        try:
            end_date = date.today().strftime("%Y-%m-%d")
            start_date = (date.today() - timedelta(days=365 * 3)).strftime("%Y-%m-%d")

            fields = "date,peTTM,pbMRQ"
            rs = bs.query_history_k_data_plus(
                bs_code, fields,
                start_date=start_date,
                end_date=end_date,
                frequency="d",
            )

            all_rows = []
            while rs.error_code == "0" and rs.next():
                row = rs.get_row_data()
                all_rows.append(row)

            # 月度采样
            valuations = []
            seen_months: set[str] = set()
            for row in reversed(all_rows):
                d_str = str(row[0])
                if not d_str:
                    continue
                try:
                    d = datetime.strptime(d_str, "%Y-%m-%d").date()
                except ValueError:
                    continue
                month_key = f"{d.year}-{d.month:02d}"
                if month_key in seen_months:
                    continue
                seen_months.add(month_key)
                valuations.append({
                    "date": d_str,
                    "pe_ttm": self._safe_float(row[1]),
                    "pb_mrq": self._safe_float(row[2]),
                })

            valuations.reverse()
            return valuations
        finally:
            bs.logout()

    # ================================================================
    # 工具方法
    # ================================================================

    @staticmethod
    def _to_baostock_code(code: str) -> str:
        if code.startswith(("sh.", "sz.")):
            return code
        if code.startswith(("6", "9")):
            return f"sh.{code}"
        elif code.startswith(("0", "3")):
            return f"sz.{code}"
        return ""

    @staticmethod
    def _safe_float(v: Any) -> float | None:
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
