"""多源数据采集Agent - AKShare主源 + BaoStock备源

采集数据类型:
1. 股票基础信息 (stock_individual_info_em)
2. 历史行情 (stock_zh_a_hist, 前复权)
3. 实时行情 (stock_zh_a_spot_em)
4. 财务报表三大表 (stock_financial_report_ths)
5. 估值数据含PE/PB (baostock query_history_k_data_plus)
"""

from __future__ import annotations

import hashlib
import io
import math
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import requests
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# 在模块加载时清除代理设置，避免系统代理干扰AKShare/BaoStock
for _proxy_key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]:
    os.environ.pop(_proxy_key, None)
os.environ["no_proxy"] = "*"

from investresearch.core.agent_base import AgentBase
from investresearch.core.exceptions import (
    AgentError,
    DataCollectionError,
)
from investresearch.core.logging import get_logger
from investresearch.core.models import (
    AgentInput,
    AgentOutput,
    AgentStatus,
    CollectorOutput,
    DataSource,
    FinancialStatement,
    IndustryData,
    StockBasicInfo,
    StockPrice,
    Announcement,
    GovernanceData,
    ResearchReportSummary,
    ShareholderData,
    IndustryEnhancedData,
    ValuationPercentile,
    NewsData,
    SentimentData,
    PolicyDocument,
    ComplianceEvent,
    PatentRecord,
)
from investresearch.core.trust import aggregate_quality, build_module_profiles, profile_dicts

from .cache import FileCache
from .official_sources import OfficialSourceRegistry

logger = get_logger("agent.collector")

# 速率限制间隔(秒)
MIN_REQUEST_INTERVAL = 0.5


class DataCollectorAgent(AgentBase[AgentInput, AgentOutput]):
    """多源数据采集Agent

    AKShare为主数据源，BaoStock为备份数据源。
    支持缓存、重试、速率限制。
    """

    agent_name: str = "data_collector"

    def __init__(self, cache: FileCache | None = None) -> None:
        super().__init__()
        self._cache = cache or FileCache()
        self._last_request_time: float = 0.0
        self._session = requests.Session()
        self._session.trust_env = False
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
        })
        self._official_sources = OfficialSourceRegistry(self._request)

    def _log_failure(self, data_type: str, source: str, error: Exception, context: str = "") -> None:
        """结构化记录采集失败"""
        msg = f"[{source}] {data_type} 采集失败: {type(error).__name__}: {error}"
        if context:
            msg += f" | {context}"
        self.logger.warning(msg)

    # ================================================================
    # 主入口
    # ================================================================

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行数据采集"""
        stock_code = input_data.stock_code
        self.logger.info(f"开始采集数据 | code={stock_code}")

        result = CollectorOutput()
        depth = input_data.depth

        # 采集任务列表（原有5类 + Phase 8新增9类）
        tasks = [
            ("stock_info", self._get_stock_info),
            ("daily_prices", self._get_daily_prices),
            ("realtime_quote", self._get_realtime_quote),
            ("financials", self._get_financial_statements),
            ("valuation", self._get_valuation_data),
            # Sprint 1: 公告与治理
            ("announcements", self._get_announcements),
            ("compliance_events", self._get_compliance_events),
            ("governance", self._get_governance_data),
            # Sprint 2: 研报与股东
            ("research_reports", self._get_research_reports),
            ("shareholders", self._get_shareholder_data),
            ("patents", self._get_patents),
            # Sprint 3: 行业增强与估值分位
            ("industry_enhanced", self._get_industry_enhanced),
            ("valuation_percentile", self._get_valuation_percentile),
            # Sprint 4: 新闻舆情
            ("news", self._get_news),
            ("sentiment", self._get_sentiment_data),
            ("policy_documents", self._get_policy_documents),
        ]

        for data_type, fetch_fn in tasks:
            try:
                self.logger.info(f"采集 {data_type}...")
                fetch_fn(stock_code, result)
                self.logger.info(f"采集 {data_type} 完成")
            except Exception as e:
                self.logger.warning(f"采集 {data_type} 失败: {e}")
                result.collection_status[data_type] = "failed"
                result.errors.append(f"{data_type}: {e}")

        # 跨源填补缺失字段
        self._fill_missing_fields(stock_code, result)

        profiles = build_module_profiles(result.model_dump(mode="json"))
        (
            result.status,
            result.completeness,
            result.coverage_ratio,
            result.missing_fields,
            result.evidence_refs,
            result.source_priority,
        ) = aggregate_quality(profiles)
        result.module_profiles = profiles
        result.collection_status = {name: profile.status.value for name, profile in profiles.items()}

        self.logger.info(
            f"采集完成 | 状态={result.status.value} | 覆盖率={result.coverage_ratio:.0%} | 完整度={result.completeness:.0%}"
        )

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data=result.model_dump(),
            errors=list(result.errors),
            data_sources=result.source_priority[:8] or ["akshare", "baostock"],
            confidence=result.coverage_ratio,
            summary=(
                f"采集{len(result.collection_status)}类数据，状态={result.status.value}，"
                f"覆盖率{result.coverage_ratio:.0%}，完整度{result.completeness:.0%}"
            ),
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验输出"""
        if output.status != AgentStatus.SUCCESS:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, [f"状态异常: {output.status}"])

        data = output.data
        ratio = data.get("coverage_ratio", 0)
        if ratio < 0.2:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(
                self.agent_name,
                [f"覆盖率过低({ratio:.0%})，数据采集可能大面积失败"],
            )

    # ================================================================
    # 速率限制 + AKShare统一调用
    # ================================================================

    def _rate_limit(self) -> None:
        """简单速率限制"""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def _akshare_call(self, func_name: str, **kwargs: Any) -> pd.DataFrame:
        """统一AKShare调用包装器"""
        import akshare as ak

        self._rate_limit()

        func = getattr(ak, func_name, None)
        if func is None:
            raise DataCollectionError("akshare", f"函数不存在: {func_name}")

        self.logger.debug(f"AKShare调用: {func_name}({kwargs})")
        result = func(**kwargs)

        if result is None:
            raise DataCollectionError("akshare", f"{func_name} 返回None")

        return result

    def _get_from_cache(self, key: str) -> Any | None:
        return self._cache.get(key)

    def _save_to_cache(self, key: str, value: Any, ttl: int | None = None) -> None:
        self._cache.set(key, value, ttl=ttl)

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
        **kwargs: Any,
    ) -> requests.Response:
        """统一 HTTP 请求封装，禁用系统代理并附带默认 UA。"""
        self._rate_limit()
        merged_headers = dict(self._session.headers)
        if headers:
            merged_headers.update(headers)
        response = self._session.request(
            method=method,
            url=url,
            headers=merged_headers,
            timeout=timeout,
            **kwargs,
        )
        response.raise_for_status()
        return response

    # ================================================================
    # 1. 股票基础信息
    # ================================================================

    def _get_stock_info(self, stock_code: str, result: CollectorOutput) -> None:
        """采集股票基础信息 - 东方财富主源/新浪备源"""
        cache_key = f"stock_info_{stock_code}"
        cached = self._get_from_cache(cache_key)
        if cached:
            result.stock_info = StockBasicInfo(**cached)
            return

        # 主源: 东方财富
        try:
            df = self._akshare_call("stock_individual_info_em", symbol=stock_code)
            if df is not None and not df.empty:
                info = {}
                for _, row in df.iterrows():
                    info[str(row.iloc[0])] = str(row.iloc[1])

                stock_info = StockBasicInfo(
                    code=stock_code,
                    name=info.get("股票简称", ""),
                    exchange=info.get("上市板块"),
                    listing_date=self._parse_date(info.get("上市时间")),
                    industry_sw=info.get("行业"),
                    actual_controller=info.get("实际控制人"),
                    main_business=info.get("经营范围", "")[:500] if info.get("经营范围") else None,
                )
                result.stock_info = stock_info
                self._save_to_cache(cache_key, stock_info.model_dump(), ttl=86400)
                return
        except Exception as e:
            self.logger.warning(f"东方财富股票信息失败，切换新浪源: {e}")

        # 备源: 新浪财经获取基本名称
        self._get_stock_info_sina(stock_code, result)
        if result.stock_info:
            self._save_to_cache(cache_key, result.stock_info.model_dump(), ttl=86400)

        # 备源2: BaoStock stock_basic 补充 listing_date 等缺失字段
        if result.stock_info and not result.stock_info.listing_date:
            self._get_stock_info_baostock(stock_code, result)
            if result.stock_info:
                self._save_to_cache(cache_key, result.stock_info.model_dump(), ttl=86400)

    def _get_stock_info_sina(self, stock_code: str, result: CollectorOutput) -> None:
        """通过新浪获取股票名称"""
        import requests
        self._rate_limit()
        sina_code = self._to_sina_code(stock_code)
        if not sina_code:
            return
        try:
            s = requests.Session()
            s.trust_env = False
            resp = s.get(
                f"https://hq.sinajs.cn/list={sina_code}",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"},
                timeout=10,
            )
            resp.encoding = "gbk"
            line = resp.text.strip()
            if '="' not in line:
                return
            data_str = line.split('="')[1].rstrip('";')
            name = data_str.split(",")[0]
            if name:
                result.stock_info = StockBasicInfo(
                    code=stock_code,
                    name=name,
                    exchange="SZSE" if stock_code.startswith(("0", "3")) else "SSE",
                )
                self.logger.info(f"新浪获取股票名称: {name}")
        except Exception as e:
            self.logger.warning(f"新浪股票信息失败: {e}")

    def _get_stock_info_baostock(self, stock_code: str, result: CollectorOutput) -> None:
        """通过BaoStock query_stock_basic 补充股票基础信息"""
        import baostock as bs

        bs_code = self._to_baostock_code(stock_code)
        if not bs_code:
            return

        try:
            lg = bs.login()
            if lg.error_code != "0":
                self._log_failure("stock_info", "baostock", Exception(f"登录失败: {lg.error_msg}"))
                return

            try:
                rs = bs.query_stock_basic(code=bs_code)
                if rs.error_code != "0":
                    self._log_failure("stock_info", "baostock", Exception(f"查询失败: {rs.error_msg}"))
                    return

                while rs.next():
                    row = rs.get_row_data()
                    # row: [code, code_name, ipoDate, outDate, type, status]
                    if len(row) < 3:
                        continue

                    name = str(row[1]) if len(row) >= 2 else ""
                    listing_date = self._parse_date(str(row[2]))

                    existing = result.stock_info
                    if existing:
                        updated = existing.model_copy(update={
                            "name": existing.name or name,
                            "listing_date": existing.listing_date or listing_date,
                        })
                        result.stock_info = updated
                    self.logger.info(f"BaoStock 补充股票信息: name={name}, listing_date={listing_date}")
                    return
            finally:
                bs.logout()
        except ImportError:
            self.logger.warning("BaoStock未安装，跳过stock_info补充")
        except Exception as e:
            self._log_failure("stock_info", "baostock", e)

    # ================================================================
    # 2. 历史行情
    # ================================================================

    def _get_daily_prices(self, stock_code: str, result: CollectorOutput) -> None:
        """采集历史日线行情 (前复权) - 东方财富主源/腾讯备源"""
        cache_key = f"daily_prices_{stock_code}"
        cached = self._get_from_cache(cache_key)
        if cached:
            result.prices = [StockPrice(**p) for p in cached]
            return

        end_date = date.today().strftime("%Y%m%d")
        start_date = (date.today() - timedelta(days=365 * 3)).strftime("%Y%m%d")

        # 主源: 东方财富
        try:
            df = self._akshare_call(
                "stock_zh_a_hist",
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            if df is not None and not df.empty:
                prices = self._parse_em_prices(df, stock_code)
                if prices:
                    result.prices = prices
                    self._save_to_cache(cache_key, [p.model_dump() for p in prices], ttl=86400)
                    self.logger.info(f"采集行情(东方财富) {len(prices)} 条")
                    return
        except Exception as e:
            self.logger.warning(f"东方财富行情失败，切换腾讯源: {e}")

        # 备源: 腾讯证券
        df = self._get_daily_prices_tx(stock_code, start_date, end_date)
        if df is None or df.empty:
            raise DataCollectionError("akshare", "历史行情为空(双源均失败)", stock_code=stock_code)

        prices = self._parse_tx_prices(df, stock_code)
        result.prices = prices
        self._save_to_cache(cache_key, [p.model_dump() for p in prices], ttl=86400)
        self.logger.info(f"采集行情(腾讯源) {len(prices)} 条")

    # ================================================================
    # 3. 实时行情
    # ================================================================

    def _get_realtime_quote(self, stock_code: str, result: CollectorOutput) -> None:
        """采集实时行情 - 东方财富主源/新浪备源"""
        # 主源: 东方财富
        try:
            df = self._akshare_call("stock_zh_a_spot_em")
            if df is not None and not df.empty:
                row = df[df["代码"] == stock_code]
                if not row.empty:
                    row = row.iloc[0]
                    result.realtime = StockPrice(
                        code=stock_code,
                        date=date.today(),
                        open=self._safe_float(row.get("今开")),
                        close=self._safe_float(row.get("最新价")),
                        high=self._safe_float(row.get("最高")),
                        low=self._safe_float(row.get("最低")),
                        volume=self._safe_float(row.get("成交量")),
                        amount=self._safe_float(row.get("成交额")),
                        turnover_rate=self._safe_float(row.get("换手率")),
                        pe_ttm=self._safe_float(row.get("市盈率-动态")),
                        pb_mrq=self._safe_float(row.get("市净率")),
                        market_cap=self._safe_float(row.get("总市值")),
                    )
                    return
        except Exception as e:
            self.logger.warning(f"东方财富实时行情失败，切换新浪源: {e}")

        # 备源: 新浪财经
        self._get_realtime_quote_sina(stock_code, result)

    # ================================================================
    # 4. 财务报表
    # ================================================================

    def _get_financial_statements(self, stock_code: str, result: CollectorOutput) -> None:
        """采集财务报表 - 三级备源链: AKShare详细→AKShare简化→BaoStock"""
        cache_key = f"financials_{stock_code}_v2"
        cached = self._get_from_cache(cache_key)
        if cached:
            result.financials = [FinancialStatement(**f) for f in cached]
            return

        # 第一级: AKShare详细报表
        for stmt_type in ["income", "balance", "cash"]:
            try:
                df = self._akshare_call(
                    "stock_financial_report_ths",
                    symbol=stock_code,
                    indicator=stmt_type,
                )
                if df is not None and not df.empty:
                    self.logger.info(f"财务报表 {stmt_type}: {len(df)} 行")
            except Exception as e:
                self._log_failure(f"financials_{stmt_type}", "akshare", e, "stock_financial_report_ths")

        # 第二级: AKShare简化采集
        if not result.financials:
            self._get_financial_simplified(stock_code, result)

        # 第三级: BaoStock财务数据备源（填补缺失字段或全部补充）
        self._get_financial_baostock(stock_code, result)

        if result.financials:
            self._save_to_cache(
                cache_key,
                [f.model_dump() for f in result.financials],
                ttl=86400 * 7,
            )

    def _get_financial_simplified(self, stock_code: str, result: CollectorOutput) -> None:
        """简化财务数据采集 (主要指标)"""
        try:
            import akshare as ak
            self._rate_limit()

            df = ak.stock_financial_abstract_ths(symbol=stock_code)
            if df is None or df.empty:
                return

            for _, row in df.iterrows():
                try:
                    report_date = self._parse_date(str(row.get("报告期", "")))
                    if report_date is None:
                        continue

                    fs = FinancialStatement(
                        code=stock_code,
                        report_date=report_date,
                        report_type="annual",
                        source=DataSource.AKSHARE,
                        revenue=self._safe_float(row.get("营业总收入")),
                        net_profit=self._safe_float(row.get("净利润")),
                        total_assets=self._safe_float(row.get("总资产")),
                        equity=self._safe_float(row.get("所有者权益合计")),
                        operating_cashflow=self._safe_float(row.get("经营活动产生的现金流量净额")),
                        roe=self._safe_percent_value(row.get("净资产收益率(%)")),
                        gross_margin=self._safe_percent_value(row.get("销售毛利率(%)")),
                        net_margin=self._safe_percent_value(row.get("销售净利率(%)")),
                    )
                    # 计算派生指标
                    if fs.total_assets and fs.total_liabilities is None and fs.equity:
                        fs.debt_ratio = self._safe_pct(fs.total_assets - fs.equity, fs.total_assets)

                    result.financials.append(fs)
                except (ValueError, TypeError):
                    continue

            self.logger.info(f"简化财务数据: {len(result.financials)} 期")
        except Exception as e:
            self.logger.warning(f"简化财务采集失败: {e}")

    def _get_financial_baostock(self, stock_code: str, result: CollectorOutput) -> None:
        """BaoStock财务数据备源 - 查询profit/balance/growth/operation四表合并

        BaoStock API字段映射(已验证):
        profit:  [code, pubDate, statDate, roeAvg, npMargin, gpMargin, netProfit, epsTTM, MBRevenue, totalShare, liquidShare]
        balance: [code, pubDate, statDate, currentRatio, quickRatio, cashRatio, YOYLiability, liabilityToAsset, YOYAsset]
        growth:  [code, pubDate, statDate, YOYEquity, YOYAsset, YOYNI, YOYProfit, YOYRevenue, YOYEPSBasic]
        operation: [code, pubDate, statDate, NRTurnRatio, NRTurnDays, ARTurnRatio, ARTurnDays, INVTurnRatio, INVTurnDays]
        """
        bs_code = self._to_baostock_code(stock_code)
        if not bs_code:
            return

        try:
            import baostock as bs
        except ImportError:
            self.logger.warning("BaoStock未安装，跳过财务数据备源")
            return

        try:
            lg = bs.login()
            if lg.error_code != "0":
                self._log_failure("financials", "baostock", Exception(f"登录失败: {lg.error_msg}"))
                return

            try:
                # 查询最近4年(16个季度)的数据
                current_year = date.today().year
                start_year = current_year - 4

                # 按报告期聚合: {statDate: {field: value}}
                merged: dict[str, dict[str, Any]] = {}

                # 1. 盈利能力 (profit_data)
                self._baostock_query_merge(
                    bs, bs_code, start_year, merged,
                    "query_profit_data",
                    lambda row: {
                        "roe": self._safe_percent_value(row[3]),
                        "net_margin": self._safe_percent_value(row[4]),
                        "gross_margin": self._safe_percent_value(row[5]),
                        "net_profit": self._safe_float(row[6]),
                        "revenue": self._safe_float(row[8]),
                    },
                )

                # 2. 偿债能力 (balance_data)
                self._baostock_query_merge(
                    bs, bs_code, start_year, merged,
                    "query_balance_data",
                    lambda row: {
                        "current_ratio": self._safe_float(row[3]),
                        "quick_ratio": self._safe_float(row[4]),
                        "debt_ratio": self._resolve_baostock_debt_ratio(row[7], row[8]),
                    },
                )

                # 3. 成长能力 (growth_data)
                self._baostock_query_merge(
                    bs, bs_code, start_year, merged,
                    "query_growth_data",
                    lambda row: {
                        "revenue_yoy": self._safe_percent_value(row[6]),
                        "net_profit_yoy": self._safe_percent_value(row[5]),
                    },
                )

                # 4. 营运能力 (operation_data)
                self._baostock_query_merge(
                    bs, bs_code, start_year, merged,
                    "query_operation_data",
                    lambda row: {
                        "receivable_turnover": self._safe_float(row[3]),
                        "inventory_turnover": self._safe_float(row[5]),
                    },
                )

                # 构建已有的报告期集合，避免重复
                existing_dates: set[str] = set()
                for fs in result.financials:
                    if fs.report_date:
                        existing_dates.add(fs.report_date.strftime("%Y-%m-%d"))

                # 合并到 result.financials
                new_count = 0
                filled_count = 0
                for stat_date_str, fields in sorted(merged.items(), reverse=True):
                    report_date = self._parse_date(stat_date_str)
                    if report_date is None:
                        continue

                    # 尝试匹配已有的FinancialStatement进行字段补充
                    matched_fs = None
                    for fs in result.financials:
                        if fs.report_date and fs.report_date.strftime("%Y-%m-%d") == stat_date_str:
                            matched_fs = fs
                            break

                    if matched_fs:
                        # 补充缺失字段
                        updated = False
                        update_dict = {}
                        for key, val in fields.items():
                            if val is not None and getattr(matched_fs, key, None) is None:
                                update_dict[key] = val
                                updated = True
                        if updated:
                            idx = result.financials.index(matched_fs)
                            result.financials[idx] = matched_fs.model_copy(update=update_dict)
                            filled_count += 1
                    elif stat_date_str not in existing_dates:
                        # 新增报告期
                        quarter = self._guess_quarter(stat_date_str)
                        fs = FinancialStatement(
                            code=stock_code,
                            report_date=report_date,
                            report_type=quarter,
                            source=DataSource.BAOSTOCK,
                            **{k: v for k, v in fields.items() if v is not None},
                        )
                        result.financials.append(fs)
                        new_count += 1

                # 按日期倒序排列
                result.financials.sort(key=lambda f: f.report_date or date.min, reverse=True)

                self.logger.info(
                    f"BaoStock财务备源: 合并{len(merged)}期, "
                    f"补充{filled_count}期字段, 新增{new_count}期"
                )

            finally:
                bs.logout()

        except Exception as e:
            self._log_failure("financials", "baostock", e)

    def _baostock_query_merge(
        self,
        bs: Any,
        bs_code: str,
        start_year: int,
        merged: dict[str, dict[str, Any]],
        query_method: str,
        field_extractor: Any,
    ) -> None:
        """执行BaoStock查询并合并到merged字典"""
        current_year = date.today().year
        for year in range(start_year, current_year + 1):
            for quarter in range(1, 5):
                try:
                    rs = getattr(bs, query_method)(
                        code=bs_code,
                        year=year,
                        quarter=quarter,
                    )
                    if rs.error_code != "0":
                        continue
                    while rs.next():
                        row = rs.get_row_data()
                        if len(row) < 3:
                            continue
                        stat_date = str(row[2])
                        if not stat_date or stat_date == "":
                            continue
                        fields = field_extractor(row)
                        if stat_date not in merged:
                            merged[stat_date] = {}
                        merged[stat_date].update(fields)
                except Exception:
                    continue

    @staticmethod
    def _guess_quarter(date_str: str) -> str:
        """根据报告期日期猜测报表类型"""
        if "03-31" in date_str:
            return "Q1"
        elif "06-30" in date_str:
            return "Q2"
        elif "09-30" in date_str:
            return "Q3"
        elif "12-31" in date_str:
            return "annual"
        return "quarterly"

    # ================================================================
    # 5. 估值数据 (BaoStock)
    # ================================================================

    def _get_valuation_data(self, stock_code: str, result: CollectorOutput) -> None:
        """通过BaoStock采集估值数据 (PE/PB) - 日频采样"""
        cache_key = f"valuation_{stock_code}"
        cached = self._get_from_cache(cache_key)
        if cached:
            result.valuation = [StockPrice(**p) for p in cached]
            return

        bs_code = self._to_baostock_code(stock_code)
        if not bs_code:
            return

        try:
            import baostock as bs

            lg = bs.login()
            if lg.error_code != "0":
                raise DataCollectionError("baostock", f"登录失败: {lg.error_msg}")

            try:
                end_date = date.today().strftime("%Y-%m-%d")
                start_date = (date.today() - timedelta(days=365 * 3)).strftime("%Y-%m-%d")

                # 日频采集估值指标（月频不支持估值字段）
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

                # 每月采样一条（取月末），减少数据量
                valuations = []
                seen_months: set[str] = set()
                for row in reversed(all_rows):
                    d = self._parse_date(row[0])
                    if d is None:
                        continue
                    month_key = f"{d.year}-{d.month:02d}"
                    if month_key in seen_months:
                        continue
                    seen_months.add(month_key)
                    valuations.append(StockPrice(
                        code=stock_code,
                        date=d,
                        pe_ttm=self._safe_float(row[1]),
                        pb_mrq=self._safe_float(row[2]),
                    ))

                valuations.reverse()
                result.valuation = valuations
                self._save_to_cache(
                    cache_key,
                    [v.model_dump() for v in valuations],
                    ttl=86400 * 7,
                )
                self.logger.info(f"BaoStock估值数据: {len(valuations)} 条(月度采样)")

            finally:
                bs.logout()

        except ImportError:
            self.logger.warning("BaoStock未安装，跳过估值数据")
        except Exception as e:
            self.logger.warning(f"BaoStock估值采集失败: {e}")

    # ================================================================
    # 备源采集方法
    # ================================================================

    def _get_daily_prices_tx(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        """腾讯源历史行情"""
        import akshare as ak
        self._rate_limit()
        tx_code = self._to_tencent_code(stock_code)
        if not tx_code:
            return None
        try:
            return ak.stock_zh_a_hist_tx(
                symbol=tx_code,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
        except Exception as e:
            self.logger.warning(f"腾讯源行情失败: {e}")
            return None

    def _get_realtime_quote_sina(self, stock_code: str, result: CollectorOutput) -> None:
        """新浪源实时行情"""
        import requests
        self._rate_limit()
        sina_code = self._to_sina_code(stock_code)
        if not sina_code:
            return
        try:
            s = requests.Session()
            s.trust_env = False
            resp = s.get(
                f"https://hq.sinajs.cn/list={sina_code}",
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://finance.sina.com.cn/",
                },
                timeout=10,
            )
            resp.encoding = "gbk"
            line = resp.text.strip()
            if '="' not in line:
                return
            data_str = line.split('="')[1].rstrip('";')
            fields = data_str.split(",")
            if len(fields) < 32:
                return
            result.realtime = StockPrice(
                code=stock_code,
                date=date.today(),
                open=self._safe_float(fields[1]),
                close=self._safe_float(fields[3]),
                high=self._safe_float(fields[4]),
                low=self._safe_float(fields[5]),
                volume=self._safe_float(fields[8]),
                amount=self._safe_float(fields[9]),
            )
            self.logger.info("实时行情(新浪源)采集成功")
        except Exception as e:
            self.logger.warning(f"新浪实时行情失败: {e}")

    def _parse_em_prices(self, df: pd.DataFrame, stock_code: str) -> list[StockPrice]:
        """解析东方财富行情DataFrame"""
        prices = []
        for _, row in df.iterrows():
            try:
                prices.append(StockPrice(
                    code=stock_code,
                    date=self._parse_date(str(row.get("日期", ""))),
                    open=self._safe_float(row.get("开盘")),
                    close=self._safe_float(row.get("收盘")),
                    high=self._safe_float(row.get("最高")),
                    low=self._safe_float(row.get("最低")),
                    volume=self._safe_float(row.get("成交量")),
                    amount=self._safe_float(row.get("成交额")),
                    turnover_rate=self._safe_float(row.get("换手率")),
                ))
            except (ValueError, TypeError):
                continue
        return prices

    def _parse_tx_prices(self, df: pd.DataFrame, stock_code: str) -> list[StockPrice]:
        """解析腾讯源行情DataFrame"""
        prices = []
        for _, row in df.iterrows():
            try:
                prices.append(StockPrice(
                    code=stock_code,
                    date=self._parse_date(str(row.get("date", ""))),
                    open=self._safe_float(row.get("open")),
                    close=self._safe_float(row.get("close")),
                    high=self._safe_float(row.get("high")),
                    low=self._safe_float(row.get("low")),
                    amount=self._safe_float(row.get("amount")),
                ))
            except (ValueError, TypeError):
                continue
        return prices

    # ================================================================
    # 工具方法
    # ================================================================

    @staticmethod
    def _to_baostock_code(code: str) -> str:
        """股票代码转BaoStock格式: 600519 -> sh.600519"""
        if code.startswith(("sh.", "sz.")):
            return code
        if code.startswith(("6", "9")):
            return f"sh.{code}"
        elif code.startswith(("0", "3")):
            return f"sz.{code}"
        return ""

    @staticmethod
    def _to_tencent_code(code: str) -> str:
        """股票代码转腾讯格式: 300358 -> sz300358"""
        if code.startswith(("sh", "sz")):
            return code
        if code.startswith(("6", "9")):
            return f"sh{code}"
        elif code.startswith(("0", "3")):
            return f"sz{code}"
        return ""

    @staticmethod
    def _to_sina_code(code: str) -> str:
        """股票代码转新浪格式: 300358 -> sz300358"""
        return DataCollectorAgent._to_tencent_code(code)

    @staticmethod
    def _parse_date(s: str | None) -> date | None:
        """安全解析日期"""
        if not s:
            return None
        s = str(s).strip()
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y年%m月%d日"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    @staticmethod
    def _safe_float(v: Any) -> float | None:
        """安全转float，支持中文数量级(万/亿)和百分比"""
        if v is None or v == "" or v == "-" or v is False:
            return None
        s = str(v).strip().replace(",", "")
        # 处理百分比
        if s.endswith("%"):
            try:
                return float(s[:-1])
            except ValueError:
                return None
        # 处理中文数量级
        multipliers = {"亿": 1e8, "万": 1e4, "千": 1e3, "百": 1e2}
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

    @staticmethod
    def _safe_pct(numerator: float | None, denominator: float | None) -> float | None:
        """安全百分比计算"""
        if numerator is None or denominator is None or denominator == 0:
            return None
        return round(numerator / denominator * 100, 2)

    @staticmethod
    def _normalize_stock_code(value: Any) -> str:
        """Normalize codes like sh.600519 / 600519.SH / 600519 to 600519."""
        text = str(value or "").strip()
        digits = "".join(ch for ch in text if ch.isdigit())
        return digits[-6:] if len(digits) >= 6 else text

    @staticmethod
    def _filter_stock_rows(
        df: pd.DataFrame,
        stock_code: str,
        code_columns: tuple[str, ...] = ("代码", "证券代码", "股票代码", "symbol"),
    ) -> pd.DataFrame:
        """Return rows whose code column matches the target stock code."""
        normalized = DataCollectorAgent._normalize_stock_code(stock_code)
        for column in code_columns:
            if column not in df.columns:
                continue
            series = df[column].apply(DataCollectorAgent._normalize_stock_code)
            return df[series == normalized]
        return df.iloc[0:0]

    @staticmethod
    def _coerce_date_value(value: Any) -> date | None:
        """Convert timestamps / strings / pandas dates into ``date``."""
        if value in (None, "", "-"):
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            numeric = float(value)
            if math.isfinite(numeric) and numeric > 0:
                try:
                    if numeric >= 1e17:
                        return datetime.fromtimestamp(numeric / 1_000_000_000).date()
                    if numeric >= 1e14:
                        return datetime.fromtimestamp(numeric / 1_000_000).date()
                    if numeric >= 1e11:
                        return datetime.fromtimestamp(numeric / 1_000).date()
                    if numeric >= 1e9:
                        return datetime.fromtimestamp(numeric).date()
                except (OverflowError, OSError, ValueError):
                    pass
        try:
            parsed = pd.to_datetime(value, errors="coerce")
            if pd.notna(parsed):
                return parsed.date()
        except Exception:
            pass
        return DataCollectorAgent._parse_date(str(value))

    @staticmethod
    def _quarter_end_candidates(reference: date | None = None, count: int = 4) -> list[str]:
        """Return recent quarter-end dates in YYYYMMDD format."""
        ref = reference or date.today()
        candidates: list[str] = []
        year = ref.year
        while len(candidates) < count:
            for month, day in ((12, 31), (9, 30), (6, 30), (3, 31)):
                current = date(year, month, day)
                if current <= ref:
                    candidates.append(current.strftime("%Y%m%d"))
                    if len(candidates) >= count:
                        break
            year -= 1
        return candidates

    @staticmethod
    def _safe_int(v: Any) -> int | None:
        """安全转 int。"""
        value = DataCollectorAgent._safe_float(v)
        if value is None:
            return None
        try:
            return int(round(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_percent_value(v: Any) -> float | None:
        """统一把比例字段转成常见百分数表示。"""
        value = DataCollectorAgent._safe_float(v)
        if value is None:
            return None
        if abs(value) <= 1.2:
            value *= 100
        return round(value, 2)

    @staticmethod
    def _resolve_baostock_debt_ratio(
        liability_to_asset: Any,
        asset_to_equity: Any,
    ) -> float | None:
        """修正 BaoStock 资产负债率口径异常，统一返回百分数表示。"""
        direct_ratio = DataCollectorAgent._safe_percent_value(liability_to_asset)
        asset_to_equity_value = DataCollectorAgent._safe_float(asset_to_equity)
        inferred_ratio = None
        if asset_to_equity_value is not None and asset_to_equity_value > 1:
            try:
                inferred_ratio = round((1 - 1 / asset_to_equity_value) * 100, 2)
            except ZeroDivisionError:
                inferred_ratio = None

        if direct_ratio is None:
            return inferred_ratio
        if inferred_ratio is None:
            return direct_ratio
        if 0 <= direct_ratio <= 5 and inferred_ratio >= 10:
            return inferred_ratio
        return direct_ratio

    @staticmethod
    def _strip_html(text: str) -> str:
        return re.sub(r"<[^>]+>", "", text or "")

    @staticmethod
    def _clean_text(text: str) -> str:
        """清理网页/PDF文本中的多余空白。"""
        if not text:
            return ""
        cleaned = str(text).replace("\u3000", " ").replace("\xa0", " ")
        cleaned = re.sub(r"[ \t]+", " ", cleaned)
        cleaned = re.sub(r"\s*\n\s*", "\n", cleaned)
        cleaned = re.sub(r"\n{2,}", "\n", cleaned)
        return cleaned.strip()

    @staticmethod
    def _extract_highlights(text: str, *, keywords: list[str] | None = None, limit: int = 6) -> list[str]:
        """从长文本里提取和关键词最相关的句子。"""
        normalized = DataCollectorAgent._clean_text(text)
        if not normalized:
            return []

        sentences = [
            item.strip("；;，, ")
            for item in re.split(r"[。！？；\n\r]+", normalized)
            if item and len(item.strip()) >= 12
        ]
        highlights: list[str] = []
        seen: set[str] = set()

        for keyword in keywords or []:
            for sentence in sentences:
                if keyword and keyword in sentence and sentence not in seen:
                    highlights.append(sentence[:140])
                    seen.add(sentence)
                    break

        for sentence in sentences:
            if sentence in seen:
                continue
            highlights.append(sentence[:140])
            seen.add(sentence)
            if len(highlights) >= limit:
                break

        return highlights[:limit]

    @staticmethod
    def _build_excerpt(text: str, highlights: list[str], max_chars: int = 900) -> str:
        """生成适合报告引用的摘录。"""
        if highlights:
            excerpt = "；".join(highlights)
            if excerpt:
                return excerpt[:max_chars]
        normalized = DataCollectorAgent._clean_text(text).replace("\n", " ")
        return normalized[:max_chars]

    @staticmethod
    def _parse_chinese_amount(number_text: str, unit_text: str = "") -> float | None:
        """Parse Chinese amount strings like 12.3亿元 / 4,500万元 into Yuan."""
        if not number_text:
            return None
        text = str(number_text).replace(",", "").replace("，", "").strip()
        negative = False
        if text.startswith("(") and text.endswith(")"):
            negative = True
            text = text[1:-1]
        if text.startswith("（") and text.endswith("）"):
            negative = True
            text = text[1:-1]
        if text.startswith("-"):
            negative = True
            text = text[1:]
        try:
            numeric = float(text)
        except (ValueError, TypeError):
            return None
        unit = str(unit_text or "").strip()
        multiplier = {
            "元": 1.0,
            "万元": 1e4,
            "万": 1e4,
            "亿元": 1e8,
            "亿": 1e8,
        }.get(unit, 1.0)
        value = numeric * multiplier
        return -value if negative else value

    @staticmethod
    def _extract_text_by_patterns(text: str, patterns: list[str], *, max_chars: int = 220) -> str:
        """Extract the first text span matching any pattern."""
        if not text:
            return ""
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.S)
            if not match:
                continue
            value = DataCollectorAgent._clean_text(match.group(1))
            if value:
                return value[:max_chars]
        return ""

    @staticmethod
    def _extract_amount_by_keywords(text: str, keywords: list[str]) -> float | None:
        """Extract amount after a set of keywords."""
        for keyword in keywords:
            pattern = rf"{re.escape(keyword)}[^\n:：]{{0,24}}[:：]?\s*([\-（(]?\d[\d,\.]*)\s*(亿元|万元|元|亿|万)?"
            match = re.search(pattern, text)
            if not match:
                continue
            value = DataCollectorAgent._parse_chinese_amount(match.group(1), match.group(2) or "")
            if value is not None:
                return round(value, 2)
        return None

    @staticmethod
    def _extract_percent_by_keywords(text: str, keywords: list[str]) -> float | None:
        """Extract percentage after a set of keywords."""
        for keyword in keywords:
            pattern = rf"{re.escape(keyword)}[^\n:：]{{0,24}}[:：]?\s*([\-（(]?\d[\d,\.]*)\s*(%|％)?"
            match = re.search(pattern, text)
            if not match:
                continue
            raw = DataCollectorAgent._safe_float(match.group(1))
            if raw is None:
                continue
            if abs(raw) <= 1.2:
                raw *= 100
            return round(raw, 2)
        return None

    @staticmethod
    def _infer_report_period_from_title(title: str) -> str:
        """Infer report date from titles like 2025年年度报告."""
        text = str(title or "")
        year_match = re.search(r"(\d{4})年", text)
        if not year_match:
            return ""
        year = year_match.group(1)
        if "半年度" in text or "半年报" in text:
            return f"{year}-06-30"
        if "第三季度" in text or "三季报" in text:
            return f"{year}-09-30"
        if "第一季度" in text or "一季报" in text:
            return f"{year}-03-31"
        if "年度报告" in text or "年报" in text:
            return f"{year}-12-31"
        return ""

    @staticmethod
    def _infer_business_model_from_text(text: str) -> str:
        lowered = str(text or "")
        if any(token in lowered for token in ("订阅", "SaaS", "平台", "软件服务")):
            return "订阅/平台服务"
        if any(token in lowered for token in ("直销", "经销", "渠道", "代理")):
            return "直销+渠道分销"
        if any(token in lowered for token in ("项目", "工程", "交付")):
            return "项目制交付"
        if any(token in lowered for token in ("制造", "生产", "产能", "工厂")):
            return "制造销售一体化"
        return ""

    @staticmethod
    def _infer_asset_model_from_text(text: str) -> str:
        lowered = str(text or "")
        if any(token in lowered for token in ("工厂", "生产线", "设备", "产能", "制造基地")):
            return "重"
        if any(token in lowered for token in ("软件", "平台", "咨询", "服务", "研发设计")):
            return "轻"
        return ""

    @staticmethod
    def _infer_client_type_from_text(text: str) -> str:
        lowered = str(text or "")
        has_b = any(token in lowered for token in ("企业客户", "下游客户", "B端", "经销商"))
        has_c = any(token in lowered for token in ("消费者", "终端客户", "零售", "C端"))
        has_g = any(token in lowered for token in ("政府", "医院", "学校", "财政"))
        if has_b and has_c:
            return "ToB/ToC"
        if has_b and has_g:
            return "ToB/ToG"
        if has_b:
            return "ToB"
        if has_c:
            return "ToC"
        if has_g:
            return "ToG"
        return ""

    @staticmethod
    def _extract_structured_announcement_fields(
        text: str,
        *,
        title: str = "",
        announcement_date: date | None = None,
    ) -> dict[str, Any]:
        """Extract normalized fields from report/notice text without using LLM."""
        if not text:
            return {}

        normalized = DataCollectorAgent._clean_text(text)
        if not normalized:
            return {}

        main_business = DataCollectorAgent._extract_text_by_patterns(
            normalized,
            [
                r"(?:主营业务|公司主要从事|主要业务为|核心业务为|主要从事的业务)[:：]\s*([^。；\n]{12,220})",
                r"(?:公司是.*?)(?:从事|聚焦于)([^。；\n]{12,220})",
            ],
        )
        if not main_business:
            fallback_match = re.search(r"公司主要从事([^。；\n]{6,220})", normalized)
            if fallback_match:
                main_business = DataCollectorAgent._clean_text(fallback_match.group(1))[:180]
        business_model = DataCollectorAgent._infer_business_model_from_text(normalized)
        asset_model = DataCollectorAgent._infer_asset_model_from_text(normalized)
        client_type = DataCollectorAgent._infer_client_type_from_text(normalized)
        report_period = DataCollectorAgent._infer_report_period_from_title(title)
        risk_factors = DataCollectorAgent._extract_highlights(
            normalized,
            keywords=["风险", "不确定", "波动", "减值", "竞争", "政策"],
            limit=5,
        )

        financial_snapshot = {
            "report_date": report_period or (announcement_date.isoformat() if announcement_date else ""),
            "operating_cashflow": DataCollectorAgent._extract_amount_by_keywords(
                normalized,
                ["经营活动产生的现金流量净额", "经营现金流量净额", "经营活动现金流量净额"],
            ),
            "investing_cashflow": DataCollectorAgent._extract_amount_by_keywords(
                normalized,
                ["投资活动产生的现金流量净额", "投资现金流量净额"],
            ),
            "financing_cashflow": DataCollectorAgent._extract_amount_by_keywords(
                normalized,
                ["筹资活动产生的现金流量净额", "筹资现金流量净额"],
            ),
            "goodwill_ratio": DataCollectorAgent._extract_percent_by_keywords(
                normalized,
                ["商誉占净资产比例", "商誉/净资产", "商誉占归母净资产比例"],
            ),
            "non_recurring_profit": DataCollectorAgent._extract_amount_by_keywords(
                normalized,
                ["非经常性损益", "非经常性损益金额"],
            ),
            "contract_liabilities": DataCollectorAgent._extract_amount_by_keywords(
                normalized,
                ["合同负债", "合同负债期末余额"],
            ),
            "top5_customer_ratio": DataCollectorAgent._extract_percent_by_keywords(
                normalized,
                ["前五大客户销售额占年度销售总额比例", "前五名客户销售额占比", "前五大客户销售占比"],
            ),
            "top5_supplier_ratio": DataCollectorAgent._extract_percent_by_keywords(
                normalized,
                ["前五大供应商采购额占年度采购总额比例", "前五名供应商采购额占比", "前五大供应商采购占比"],
            ),
        }
        op_cf = financial_snapshot.get("operating_cashflow")
        inv_cf = financial_snapshot.get("investing_cashflow")
        if op_cf is not None and inv_cf is not None:
            financial_snapshot["free_cashflow"] = round(op_cf + inv_cf, 2)

        dividend_plan = DataCollectorAgent._extract_text_by_patterns(
            normalized,
            [
                r"(?:利润分配预案|利润分配方案|分红方案)[:：]\s*([^。；\n]{8,120})",
                r"(每10股[^。；\n]{4,80})",
            ],
            max_chars=120,
        )

        structured = {
            "stock_info": {
                "main_business": main_business,
                "business_model": business_model,
                "asset_model": asset_model,
                "client_type": client_type,
            },
            "financial_snapshot": {key: value for key, value in financial_snapshot.items() if value not in (None, "", [], {})},
            "risk_factors": risk_factors,
            "dividend_plan": dividend_plan,
        }

        if not any(
            value
            for value in [
                main_business,
                business_model,
                asset_model,
                client_type,
                structured["financial_snapshot"],
                risk_factors,
                dividend_plan,
            ]
        ):
            return {}
        return structured

    @staticmethod
    def _cache_digest(value: str) -> str:
        return hashlib.md5(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _extract_eastmoney_cookie_values(script_text: str) -> tuple[str | None, str | None]:
        """从东财 PDF 反爬脚本中还原 cookie。"""
        if not script_text:
            return None, None
        status_parts = re.findall(r"(?:WTKkN|bOYDu|wyeCN):(\d+)", script_text)
        status_value = None
        if len(status_parts) >= 3:
            status_value = str(sum(int(part) for part in status_parts[:3]))

        bot_match = re.search(
            r'EO_Bot_Ssid=.*?case"3":t=.*?,(\d+)\)',
            script_text,
            flags=re.S,
        )
        if not bot_match:
            bot_match = re.search(r"iTyzs\(t,(\d+)\)", script_text)
        if not status_value or not bot_match:
            return None, None
        return status_value, bot_match.group(1)

    def _download_pdf_bytes(self, pdf_url: str, *, referer: str = "") -> bytes:
        """下载 PDF，并处理东财 PDF 的简单反爬 cookie。"""
        headers = {}
        if referer:
            headers["Referer"] = referer
        elif "dfcfw.com" in pdf_url:
            headers["Referer"] = "https://data.eastmoney.com/report/stock.jshtml"
        elif "cninfo.com.cn" in pdf_url:
            headers["Referer"] = "https://www.cninfo.com.cn/"

        response = self._request("GET", pdf_url, headers=headers, timeout=60)
        content = response.content
        if content.startswith(b"%PDF"):
            return content

        if "dfcfw.com" in pdf_url:
            status_cookie, bot_cookie = self._extract_eastmoney_cookie_values(response.text)
            if status_cookie and bot_cookie:
                self._session.cookies.set("__tst_status", f"{status_cookie}#", path="/")
                self._session.cookies.set("EO_Bot_Ssid", bot_cookie, path="/")
                response = self._request("GET", pdf_url, headers=headers, timeout=60)
                content = response.content

        return content if content.startswith(b"%PDF") else b""

    def _extract_pdf_material(
        self,
        pdf_url: str,
        *,
        cache_prefix: str,
        highlight_keywords: list[str] | None = None,
        referer: str = "",
        document_title: str = "",
        document_date: date | None = None,
        max_pages: int = 25,
    ) -> dict[str, Any]:
        """下载并提取 PDF 摘录，结果缓存到本地。"""
        cache_key = f"{cache_prefix}_{self._cache_digest(pdf_url)}"
        cached = self._get_from_cache(cache_key)
        if cached:
            payload = dict(cached)
            payload.setdefault("structured_fields", {})
            return payload

        payload = {
            "summary": "",
            "excerpt": "",
            "highlights": [],
            "page_count": None,
            "full_text_available": False,
            "structured_fields": {},
        }
        try:
            pdf_bytes = self._download_pdf_bytes(pdf_url, referer=referer)
            if not pdf_bytes:
                self._save_to_cache(cache_key, payload, ttl=86400 * 7)
                return payload

            import pdfplumber

            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                page_count = len(pdf.pages)
                page_texts: list[str] = []
                for page in pdf.pages[:max_pages]:
                    text = page.extract_text() or ""
                    if text:
                        page_texts.append(text)

            full_text = self._clean_text("\n".join(page_texts))
            highlights = self._extract_highlights(
                full_text,
                keywords=highlight_keywords,
                limit=6,
            )
            excerpt = self._build_excerpt(full_text, highlights, max_chars=900)
            structured_fields = self._extract_structured_announcement_fields(
                full_text,
                title=document_title,
                announcement_date=document_date,
            )
            payload = {
                "summary": excerpt[:260],
                "excerpt": excerpt,
                "highlights": highlights,
                "page_count": page_count,
                "full_text_available": bool(full_text),
                "structured_fields": structured_fields,
            }
        except Exception as exc:
            self.logger.debug(f"PDF解析失败 {pdf_url}: {exc}")

        self._save_to_cache(cache_key, payload, ttl=86400 * 7)
        return payload

    def _extract_html_material(
        self,
        url: str,
        *,
        cache_prefix: str,
        highlight_keywords: list[str] | None = None,
    ) -> dict[str, Any]:
        """提取官方网页正文摘要。"""
        cache_key = f"{cache_prefix}_{self._cache_digest(url)}"
        cached = self._get_from_cache(cache_key)
        if cached:
            return dict(cached)

        payload = {"summary": "", "excerpt": "", "highlights": []}
        try:
            response = self._request("GET", url, headers={"Referer": "https://www.gov.cn/"}, timeout=30)
            if not response.encoding or response.encoding.lower() == "iso-8859-1":
                response.encoding = response.apparent_encoding or "utf-8"

            from bs4 import BeautifulSoup

            soup = BeautifulSoup(response.text, "html.parser")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()

            candidates = [
                "#UCAP-CONTENT",
                ".pages_content",
                ".TRS_Editor",
                ".content",
                "article",
                ".arti_content",
            ]
            content_text = ""
            for selector in candidates:
                node = soup.select_one(selector)
                if not node:
                    continue
                paragraphs = [
                    item.get_text(" ", strip=True)
                    for item in node.find_all(["p", "li"])
                    if item.get_text(" ", strip=True)
                ]
                if paragraphs:
                    content_text = "\n".join(paragraphs)
                    break

            if not content_text:
                content_text = soup.get_text("\n", strip=True)

            cleaned = self._clean_text(content_text)
            highlights = self._extract_highlights(cleaned, keywords=highlight_keywords, limit=5)
            excerpt = self._build_excerpt(cleaned, highlights, max_chars=900)
            payload = {
                "summary": excerpt[:260],
                "excerpt": excerpt,
                "highlights": highlights,
            }
        except Exception as exc:
            self.logger.debug(f"网页正文提取失败 {url}: {exc}")

        self._save_to_cache(cache_key, payload, ttl=86400 * 7)
        return payload

    @staticmethod
    def _percentile_rank(values: list[float], current: float | None) -> float | None:
        """Compute percentile rank for the current value against historical samples."""
        if current is None:
            return None
        valid = sorted(float(value) for value in values if value is not None)
        if len(valid) < 6:
            return None
        below_or_equal = sum(value <= current for value in valid)
        return round(below_or_equal / len(valid) * 100, 2)

    @staticmethod
    def _valuation_level_from_percentile(percentile: float | None) -> str:
        """Map percentile rank into a valuation bucket."""
        if percentile is None:
            return ""
        if percentile <= 20:
            return "低估"
        if percentile <= 50:
            return "合理"
        if percentile <= 80:
            return "偏高"
        return "极高估"

    # ================================================================
    # Sprint 1: 公告与治理数据采集
    # ================================================================

    @staticmethod
    def _infer_announcement_type(title: str, fallback: str = "") -> str:
        text = str(title or "")
        if "半年度报告" in text or "半年报" in text or "中期报告" in text:
            return "半年报"
        if "第三季度报告" in text or "三季报" in text:
            return "三季报"
        if "第一季度报告" in text or "一季报" in text:
            return "一季报"
        if "年报" in text or "年度报告" in text:
            return "年报"
        return fallback

    def _query_cninfo_announcements(
        self,
        stock_code: str,
        *,
        category: str = "",
        start_date: str,
        end_date: str,
        max_pages: int = 3,
    ) -> list[dict[str, Any]]:
        """直接调用巨潮披露接口，保留 PDF 附件链接。"""
        from akshare.stock_feature import stock_disclosure_cninfo as cninfo_module

        stock_id_map = getattr(cninfo_module, "__get_stock_json")("沪深京")
        org_id = stock_id_map.get(stock_code)
        if not org_id:
            return []

        category_dict = getattr(cninfo_module, "__get_category_dict")()
        payload = {
            "pageNum": "1",
            "pageSize": "30",
            "column": "szse",
            "tabName": "fulltext",
            "plate": "",
            "stock": f"{stock_code},{org_id}",
            "searchkey": "",
            "secid": "",
            "category": category_dict.get(category, "") if category else "",
            "trade": "",
            "seDate": (
                f"{'-'.join([start_date[:4], start_date[4:6], start_date[6:]])}~"
                f"{'-'.join([end_date[:4], end_date[4:6], end_date[6:]])}"
            ),
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }

        response = self._request(
            "POST",
            "http://www.cninfo.com.cn/new/hisAnnouncement/query",
            data=payload,
            headers={
                "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=30,
        )
        data = response.json()
        total_pages = max(1, math.ceil(int(data.get("totalAnnouncement") or 0) / 30))
        pages = min(total_pages, max_pages)

        records: list[dict[str, Any]] = []
        for page in range(1, pages + 1):
            payload["pageNum"] = str(page)
            page_response = self._request(
                "POST",
                "http://www.cninfo.com.cn/new/hisAnnouncement/query",
                data=payload,
                headers={
                    "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=30,
            )
            for item in page_response.json().get("announcements", []):
                title = self._strip_html(str(item.get("announcementTitle", "") or ""))
                pdf_path = str(item.get("adjunctUrl", "") or "")
                pdf_url = f"https://static.cninfo.com.cn/{pdf_path.lstrip('/')}" if pdf_path else ""
                notice_date = self._coerce_date_value(item.get("announcementTime"))
                record = {
                    "announcement_id": str(item.get("announcementId", "") or ""),
                    "title": title,
                    "announcement_type": self._infer_announcement_type(title, fallback=category),
                    "announcement_date": notice_date,
                    "source": "cninfo",
                    "url": (
                        "http://www.cninfo.com.cn/new/disclosure/detail"
                        f"?stockCode={stock_code}&announcementId={item.get('announcementId', '')}"
                        f"&orgId={item.get('orgId', '')}&announcementTime={notice_date}"
                    ),
                    "pdf_url": pdf_url,
                }
                records.append(record)

        records.sort(key=lambda item: str(item.get("announcement_date") or ""), reverse=True)
        return records

    def _get_announcements(self, stock_code: str, result: CollectorOutput) -> None:
        """采集公告原文，优先补齐年报/半年报/季报 PDF 摘录。"""
        cache_key = f"announcements_{stock_code}_v4"
        cached = self._get_from_cache(cache_key)
        if cached:
            result.announcements = [Announcement(**item) for item in cached]
            return

        start_date = (date.today() - timedelta(days=730)).strftime("%Y%m%d")
        end_date = date.today().strftime("%Y%m%d")
        selected: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        def append_unique(items: list[dict[str, Any]], limit: int = 1) -> None:
            for item in items:
                key = item.get("announcement_id") or item.get("title")
                if not key or key in seen_ids:
                    continue
                seen_ids.add(str(key))
                selected.append(item)
                if limit and len([entry for entry in selected if entry.get("announcement_type") == item.get("announcement_type")]) >= limit:
                    break

        for category in ["年报", "半年报", "三季报", "一季报"]:
            try:
                append_unique(
                    self._query_cninfo_announcements(
                        stock_code,
                        category=category,
                        start_date=start_date,
                        end_date=end_date,
                        max_pages=2,
                    ),
                    limit=1,
                )
            except Exception as exc:
                self._log_failure("announcements", "cninfo", exc, category)

        for category in ["日常经营", "公司治理", "风险提示", "股权变动"]:
            try:
                items = self._query_cninfo_announcements(
                    stock_code,
                    category=category,
                    start_date=start_date,
                    end_date=end_date,
                    max_pages=1,
                )
                for item in items[:2]:
                    key = item.get("announcement_id") or item.get("title")
                    if not key or key in seen_ids:
                        continue
                    seen_ids.add(str(key))
                    selected.append(item)
            except Exception as exc:
                self._log_failure("announcements", "cninfo", exc, category)

        highlight_keywords = [
            "主营业务",
            "管理层讨论",
            "核心竞争力",
            "风险因素",
            "研发投入",
            "产能",
            "客户",
            "现金流",
            "毛利率",
            "海外",
        ]
        fallback_keywords = ["担保", "质押", "回购", "增持", "减持", "问询", "风险", "股东"]
        announcements: list[Announcement] = []
        for item in selected[:8]:
            pdf_url = str(item.get("pdf_url") or "")
            extracted = (
                self._extract_pdf_material(
                    pdf_url,
                    cache_prefix="cninfo_pdf_v3",
                    highlight_keywords=highlight_keywords if item.get("announcement_type") in {"年报", "半年报", "三季报", "一季报"} else fallback_keywords,
                    referer=str(item.get("url") or "https://www.cninfo.com.cn/"),
                    document_title=str(item.get("title") or ""),
                    document_date=item.get("announcement_date"),
                    max_pages=60 if item.get("announcement_type") in {"年报", "半年报"} else 18,
                )
                if pdf_url
                else {
                    "summary": "",
                    "excerpt": "",
                    "highlights": [],
                    "page_count": None,
                    "full_text_available": False,
                    "structured_fields": {},
                }
            )
            announcements.append(
                Announcement(
                    title=str(item.get("title") or ""),
                    announcement_type=str(item.get("announcement_type") or ""),
                    announcement_date=item.get("announcement_date"),
                    announcement_id=str(item.get("announcement_id") or ""),
                    source="cninfo",
                    url=str(item.get("url") or ""),
                    pdf_url=pdf_url,
                    summary=str(extracted.get("summary") or "") or str(item.get("title") or "")[:200],
                    excerpt=str(extracted.get("excerpt") or ""),
                    highlights=list(extracted.get("highlights") or []),
                    structured_fields=dict(extracted.get("structured_fields") or {}),
                    page_count=self._safe_int(extracted.get("page_count")),
                    full_text_available=bool(extracted.get("full_text_available")),
                )
            )

        result.announcements = announcements
        if announcements:
            self._save_to_cache(cache_key, [item.model_dump() for item in announcements], ttl=86400 * 3)
        """采集公司治理数据 - 股权质押/担保/诉讼"""
        governance = GovernanceData()

        # 1. 实控人信息（从stock_info已有，补充详情）
        try:
            df = self._akshare_call("stock_hold_control_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                row = df.iloc[0]
                governance.actual_controller = str(row.iloc[0]) if len(df.columns) > 0 else None
        except Exception as e:
            self._log_failure("governance_controller", "akshare", e, "stock_hold_control_cninfo")

        # 2. 股权质押
        try:
            df = self._akshare_call("stock_cg_equity_mortgage_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                governance.pledge_details = f"记录数:{len(df)}"
                # 尝试提取质押比例
                for col in df.columns:
                    if "比例" in str(col) or "质押" in str(col):
                        val = df[col].iloc[0] if len(df) > 0 else None
                        if val is not None:
                            governance.equity_pledge_ratio = self._safe_float(val)
                        break
        except Exception as e:
            self._log_failure("governance_pledge", "akshare", e, "stock_cg_equity_mortgage_cninfo")

        # 3. 担保信息
        try:
            df = self._akshare_call("stock_cg_guarantee_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                governance.guarantee_info = f"担保记录:{len(df)}条"
        except Exception as e:
            self._log_failure("governance_guarantee", "akshare", e, "stock_cg_guarantee_cninfo")

        # 4. 诉讼信息
        try:
            df = self._akshare_call("stock_cg_lawsuit_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                governance.lawsuit_info = f"诉讼记录:{len(df)}条"
        except Exception as e:
            self._log_failure("governance_lawsuit", "akshare", e, "stock_cg_lawsuit_cninfo")

        # 5. 股东增减持
        try:
            df = self._akshare_call("stock_hold_change_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(10).iterrows():
                    governance.management_changes.append({
                        "name": str(row.iloc[0]) if len(row) > 0 else "",
                        "change_type": str(row.iloc[1]) if len(row) > 1 else "",
                        "change_date": str(row.iloc[2]) if len(row) > 2 else "",
                    })
        except Exception as e:
            self._log_failure("governance_hold_change", "akshare", e, "stock_hold_change_cninfo")

        result.governance = governance

    # ================================================================
    # Sprint 2: 研报与股东数据采集
    # ================================================================

    def _get_research_reports(self, stock_code: str, result: CollectorOutput) -> None:
        """采集卖方研报，并尽量补充 PDF 原文摘录。"""
        cache_key = f"research_reports_{stock_code}_v3"
        cached = self._get_from_cache(cache_key)
        if cached:
            result.research_reports = [ResearchReportSummary(**item) for item in cached]
            return

        try:
            df = self._akshare_call("stock_research_report_em", symbol=stock_code)
            if df is not None and not df.empty:
                first_row = df.iloc[0]
                if result.stock_info and not result.stock_info.industry_sw:
                    result.stock_info = result.stock_info.model_copy(
                        update={"industry_sw": str(first_row.get("行业", "") or "")}
                    )
                reports: list[ResearchReportSummary] = []
                for index, (_, row) in enumerate(df.head(10).iterrows()):
                    title = str(row.get("报告名称", "")).strip()
                    if not title:
                        continue
                    pdf_url = str(row.get("报告PDF链接", "") or "")
                    highlights = (
                        self._extract_pdf_material(
                            pdf_url,
                            cache_prefix="eastmoney_report_pdf_v2",
                            highlight_keywords=["业绩", "盈利预测", "毛利率", "产能", "出货", "海外", "隔膜", "负极", "设备"],
                            referer="https://data.eastmoney.com/report/stock.jshtml",
                            max_pages=16,
                        )
                        if pdf_url and index < 3
                        else {"summary": "", "excerpt": "", "highlights": [], "page_count": None}
                    )
                    reports.append(
                        ResearchReportSummary(
                            title=title,
                            institution=str(row.get("机构", "")),
                            rating=str(row.get("东财评级", "")),
                            publish_date=self._coerce_date_value(row.get("日期")),
                            industry=str(row.get("行业", "") or ""),
                            pdf_url=pdf_url,
                            summary=str(highlights.get("summary") or "") or title[:200],
                            excerpt=str(highlights.get("excerpt") or ""),
                            highlights=list(highlights.get("highlights") or []),
                            page_count=self._safe_int(highlights.get("page_count")),
                        )
                    )
                result.research_reports = reports
                if reports:
                    self._save_to_cache(cache_key, [item.model_dump() for item in reports], ttl=86400 * 3)
        except Exception as exc:
            self._log_failure("research_reports", "akshare", exc, "stock_research_report_em")
        """采集股东结构数据 - 十大股东/基金持仓/股东户数"""
        shareholders = ShareholderData()

        # 1. 十大流通股东
        try:
            df = self._akshare_call("stock_circulate_stock_holder", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(10).iterrows():
                    shareholders.top_shareholders.append({
                        "name": str(row.iloc[0]) if len(row) > 0 else "",
                        "shares": self._safe_float(row.iloc[1]) if len(row) > 1 else None,
                        "ratio": self._safe_float(row.iloc[2]) if len(row) > 2 else None,
                    })
        except Exception as e:
            self._log_failure("shareholders_top10", "akshare", e, "stock_circulate_stock_holder")

        # 2. 基金持仓
        try:
            df = self._akshare_call("stock_report_fund_hold", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(10).iterrows():
                    shareholders.fund_holders.append({
                        "name": str(row.iloc[0]) if len(row) > 0 else "",
                        "shares": self._safe_float(row.iloc[1]) if len(row) > 1 else None,
                    })
        except Exception as e:
            self._log_failure("shareholders_fund", "akshare", e, "stock_report_fund_hold")

        # 3. 股东户数变化
        try:
            df = self._akshare_call("stock_hold_num_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                row = df.iloc[0]
                shareholders.shareholder_count = self._safe_int(row.iloc[0]) if len(row) > 0 else None
                if len(df) > 1:
                    shareholders.shareholder_count_change = self._safe_float(df.iloc[1].iloc[0]) if len(df.columns) > 0 else None
        except Exception as e:
            self._log_failure("shareholders_count", "akshare", e, "stock_hold_num_cninfo")

        result.shareholders = shareholders

    # ================================================================
    # Sprint 3: 行业增强与估值分位
    # ================================================================

    def _get_industry_enhanced(self, stock_code: str, result: CollectorOutput) -> None:
        """补充可直接用于研究的行业行情和关键数据点。"""
        cache_key = f"industry_enhanced_{stock_code}_v2"
        cached = self._get_from_cache(cache_key)
        if cached:
            result.industry_enhanced = IndustryEnhancedData(**cached)
            return

        industry_name = ""
        industry_code = None

        if result.stock_info and result.stock_info.industry_sw:
            industry_name = result.stock_info.industry_sw
            industry_code = result.stock_info.industry_sw_code
        elif result.industry:
            industry_name = result.industry.industry_name
            industry_code = result.industry.industry_code

        enhanced = IndustryEnhancedData(
            industry_name=industry_name,
            industry_code=industry_code,
        )

        try:
            df = self._akshare_call("stock_industry_clf_hist_sw")
            matched = self._filter_stock_rows(df, stock_code, ("symbol",))
            if matched is not None and not matched.empty:
                latest = matched.sort_values("update_time", ascending=False).iloc[0]
                enhanced.industry_code = str(latest.get("industry_code", enhanced.industry_code or ""))
        except Exception as exc:
            self._log_failure("industry_sw", "akshare", exc, "stock_industry_clf_hist_sw")

        if industry_name:
            try:
                df = self._akshare_call("stock_board_industry_name_ths")
                matched = df[df["name"] == industry_name]
                if not matched.empty and not enhanced.industry_code:
                    enhanced.industry_code = str(matched.iloc[0]["code"])
            except Exception as exc:
                self._log_failure("industry_name_ths", "akshare", exc, "stock_board_industry_name_ths")

            try:
                info_df = self._akshare_call("stock_board_industry_info_ths", symbol=industry_name)
                if info_df is not None and not info_df.empty:
                    info_map = {
                        str(row.iloc[0]).strip(): str(row.iloc[1]).strip()
                        for _, row in info_df.iterrows()
                        if len(row) >= 2
                    }
                    enhanced.industry_index_close = self._safe_float(info_map.get("昨收")) or self._safe_float(info_map.get("今开"))
                    enhanced.industry_change_pct = self._safe_percent_value(info_map.get("板块涨幅"))
                    enhanced.industry_turnover_volume = self._safe_float(info_map.get("成交量(万手)"))
                    enhanced.industry_turnover_amount = self._safe_float(info_map.get("成交额(亿)"))
                    enhanced.industry_fund_flow = self._safe_float(info_map.get("资金净流入(亿)"))
                    enhanced.industry_rank = info_map.get("涨幅排名", "")
                    ratio_text = info_map.get("涨跌家数", "")
                    if "/" in ratio_text:
                        rise_text, fall_text = ratio_text.split("/", 1)
                        enhanced.rising_count = self._safe_int(rise_text)
                        enhanced.falling_count = self._safe_int(fall_text)
                    enhanced.data_points = [
                        item
                        for item in [
                            f"板块涨幅 {enhanced.industry_change_pct}%" if enhanced.industry_change_pct is not None else "",
                            f"成交额 {enhanced.industry_turnover_amount} 亿" if enhanced.industry_turnover_amount is not None else "",
                            f"资金净流入 {enhanced.industry_fund_flow} 亿" if enhanced.industry_fund_flow is not None else "",
                            f"涨跌家数 {enhanced.rising_count}/{enhanced.falling_count}" if enhanced.rising_count is not None and enhanced.falling_count is not None else "",
                            f"涨幅排名 {enhanced.industry_rank}" if enhanced.industry_rank else "",
                        ]
                        if item
                    ]
            except Exception as exc:
                self._log_failure("industry_info_ths", "akshare", exc, "stock_board_industry_info_ths")

            try:
                start_date = (date.today() - timedelta(days=400)).strftime("%Y%m%d")
                end_date = date.today().strftime("%Y%m%d")
                index_df = self._akshare_call(
                    "stock_board_industry_index_ths",
                    symbol=industry_name,
                    start_date=start_date,
                    end_date=end_date,
                )
                if index_df is not None and not index_df.empty:
                    latest = index_df.iloc[-1]
                    latest_close = self._safe_float(latest.get("收盘价"))
                    if latest_close is not None:
                        enhanced.industry_index_close = latest_close

                    latest_date = pd.to_datetime(latest.get("日期"), errors="coerce")
                    current_year = date.today().year
                    year_df = index_df[pd.to_datetime(index_df["日期"], errors="coerce").dt.year == current_year]
                    ytd_base = self._safe_float(year_df.iloc[0].get("收盘价")) if not year_df.empty else None
                    one_year_base = self._safe_float(index_df.iloc[0].get("收盘价"))
                    if latest_close is not None and ytd_base not in (None, 0):
                        enhanced.industry_ytd_change_pct = round((latest_close - ytd_base) / ytd_base * 100, 2)
                    if latest_close is not None and one_year_base not in (None, 0):
                        enhanced.industry_1y_change_pct = round((latest_close - one_year_base) / one_year_base * 100, 2)
                    if latest_date is not None and pd.notna(latest_date) and enhanced.data_points is not None:
                        if enhanced.industry_ytd_change_pct is not None:
                            enhanced.data_points.append(f"年初至今 {enhanced.industry_ytd_change_pct}%")
                        if enhanced.industry_1y_change_pct is not None:
                            enhanced.data_points.append(f"近一年 {enhanced.industry_1y_change_pct}%")
            except Exception as exc:
                self._log_failure("industry_index_ths", "akshare", exc, "stock_board_industry_index_ths")

        try:
            df = self._akshare_call("stock_industry_pe_ratio_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                row = df.iloc[0]
                enhanced.industry_pe = self._safe_float(row.get("市盈率", row.iloc[0] if len(row) > 0 else None))
                enhanced.industry_pb = self._safe_float(row.get("市净率", row.iloc[1] if len(row) > 1 else None))
        except Exception as exc:
            self._log_failure("industry_pe", "akshare", exc, "stock_industry_pe_ratio_cninfo")

        result.industry_enhanced = enhanced
        if self._has_substantive_model_data(enhanced):
            self._save_to_cache(cache_key, enhanced.model_dump(), ttl=86400)

    def _get_valuation_percentile(self, stock_code: str, result: CollectorOutput) -> None:
        """Compute stock valuation percentile directly from collected valuation history."""
        percentile = ValuationPercentile()

        pe_values = [float(item.pe_ttm) for item in result.valuation if item.pe_ttm is not None]
        pb_values = [float(item.pb_mrq) for item in result.valuation if item.pb_mrq is not None]

        latest_valuation = result.valuation[-1] if result.valuation else None
        current_pe = (
            result.realtime.pe_ttm
            if result.realtime and result.realtime.pe_ttm is not None
            else latest_valuation.pe_ttm if latest_valuation else None
        )
        current_pb = (
            result.realtime.pb_mrq
            if result.realtime and result.realtime.pb_mrq is not None
            else latest_valuation.pb_mrq if latest_valuation else None
        )

        percentile.pe_ttm_current = self._safe_float(current_pe)
        percentile.pb_mrq_current = self._safe_float(current_pb)
        percentile.pe_ttm_percentile = self._percentile_rank(pe_values, percentile.pe_ttm_current)
        percentile.pb_mrq_percentile = self._percentile_rank(pb_values, percentile.pb_mrq_current)

        if pe_values:
            percentile.pe_3y_avg = round(sum(pe_values[-36:]) / len(pe_values[-36:]), 2)
            percentile.pe_5y_avg = round(sum(pe_values[-60:]) / len(pe_values[-60:]), 2)

        percentile.valuation_level = self._valuation_level_from_percentile(
            percentile.pe_ttm_percentile
        )
        result.valuation_percentile = percentile
        return None
        """采集估值历史分位数据 - 理杏仁"""
        percentile = ValuationPercentile()

        try:
            df = self._akshare_call("stock_index_pe_lg", symbol=stock_code)
            if df is not None and not df.empty:
                # 取最新一行
                row = df.iloc[-1]
                percentile.pe_ttm_current = self._safe_float(row.get("pe", row.iloc[0] if len(row) > 0 else None))
                percentile.pe_ttm_percentile = self._safe_float(row.get("percentile", row.iloc[1] if len(row) > 1 else None))
                # 计算3年/5年均值
                if len(df) >= 750:  # ~3年交易日
                    percentile.pe_3y_avg = df.tail(750)["pe"].mean() if "pe" in df.columns else df.tail(750).iloc[:, 0].astype(float).mean()
                if len(df) >= 1250:  # ~5年交易日
                    percentile.pe_5y_avg = df.tail(1250)["pe"].mean() if "pe" in df.columns else df.tail(1250).iloc[:, 0].astype(float).mean()
        except Exception as e:
            self._log_failure("valuation_pe_percentile", "akshare", e, "stock_index_pe_lg")

        try:
            df = self._akshare_call("stock_index_pb_lg", symbol=stock_code)
            if df is not None and not df.empty:
                row = df.iloc[-1]
                percentile.pb_mrq_current = self._safe_float(row.get("pb", row.iloc[0] if len(row) > 0 else None))
                percentile.pb_mrq_percentile = self._safe_float(row.get("percentile", row.iloc[1] if len(row) > 1 else None))
        except Exception as e:
            self._log_failure("valuation_pb_percentile", "akshare", e, "stock_index_pb_lg")

        # 判定估值水平
        if percentile.pe_ttm_percentile is not None:
            if percentile.pe_ttm_percentile <= 20:
                percentile.valuation_level = "低估"
            elif percentile.pe_ttm_percentile <= 50:
                percentile.valuation_level = "合理"
            elif percentile.pe_ttm_percentile <= 80:
                percentile.valuation_level = "偏高"
            else:
                percentile.valuation_level = "极高估"

        result.valuation_percentile = percentile

    # ================================================================
    # Sprint 4: 新闻舆情数据采集
    # ================================================================

    def _get_news(self, stock_code: str, result: CollectorOutput) -> None:
        """Collect stock-related news from Eastmoney and optionally Caixin."""
        try:
            df = self._akshare_call("stock_news_em", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(20).iterrows():
                    result.news.append(
                        NewsData(
                            title=str(row.get("新闻标题", "")),
                            content=str(row.get("新闻内容", ""))[:500],
                            source=str(row.get("文章来源", "")),
                            publish_time=str(row.get("发布时间", "")),
                        )
                    )
        except Exception as exc:
            self._log_failure("news_em", "akshare", exc, "stock_news_em")

        if result.stock_info and result.stock_info.name:
            try:
                df = self._akshare_call("stock_news_main_cx")
                if df is not None and not df.empty:
                    stock_name = result.stock_info.name
                    matched = df[
                        df["summary"].astype(str).str.contains(stock_name, na=False)
                        | df["tag"].astype(str).str.contains(stock_name, na=False)
                    ]
                    for _, row in matched.head(5).iterrows():
                        result.news.append(
                            NewsData(
                                title=str(row.get("tag", stock_name)),
                                content=str(row.get("summary", ""))[:500],
                                source="财新",
                                publish_time="",
                            )
                        )
            except Exception as exc:
                self._log_failure("news_cx", "akshare", exc, "stock_news_main_cx")
        return None
        """采集个股新闻 - 东方财富新闻"""
        try:
            df = self._akshare_call("stock_news_em", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(20).iterrows():
                    result.news.append(NewsData(
                        title=str(row.get("新闻标题", row.iloc[0] if len(row) > 0 else "")),
                        content=str(row.get("新闻内容", row.iloc[1] if len(row) > 1 else ""))[:500],
                        source=str(row.get("来源", row.iloc[2] if len(row) > 2 else "")),
                        publish_time=str(row.get("发布时间", row.iloc[3] if len(row) > 3 else "")),
                    ))
        except Exception as e:
            self._log_failure("news_em", "akshare", e, "stock_news_em")

        # 备源: 财联社要闻
        try:
            df = self._akshare_call("stock_news_main_cx", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(10).iterrows():
                    result.news.append(NewsData(
                        title=str(row.iloc[0]) if len(row) > 0 else "",
                        content=str(row.iloc[1])[:500] if len(row) > 1 else "",
                        source="财联社",
                        publish_time=str(row.iloc[2]) if len(row) > 2 else "",
                    ))
        except Exception as e:
            self._log_failure("news_cx", "akshare", e, "stock_news_main_cx")
        """根据新闻数据计算舆情情绪"""
        sentiment = SentimentData()

        if result.news:
            sentiment.news_count_7d = len(result.news)
            for n in result.news:
                if n.sentiment == "positive":
                    sentiment.positive_count += 1
                elif n.sentiment == "negative":
                    sentiment.negative_count += 1
                else:
                    sentiment.neutral_count += 1

            # 计算情绪评分
            total = sentiment.positive_count + sentiment.negative_count + sentiment.neutral_count
            if total > 0:
                sentiment.sentiment_score = round(
                    (sentiment.positive_count - sentiment.negative_count) / total, 2
                )

        result.sentiment = sentiment

    def _get_governance_data(self, stock_code: str, result: CollectorOutput) -> None:
        """Derive stable governance basics from stock info."""
        governance = GovernanceData()
        if result.stock_info:
            governance.actual_controller = result.stock_info.actual_controller
            governance.controller_type = result.stock_info.controller_type
        official_events = result.compliance_events or []

        if official_events:
            titles = []
            for item in official_events[:3]:
                if isinstance(item, ComplianceEvent):
                    title = str(item.title or "").strip()
                else:
                    title = str(item.get("title") or "").strip()
                if title:
                    titles.append(title)
            if titles and not governance.lawsuit_info:
                governance.lawsuit_info = f"官方公开合规事件{len(official_events)}条: {'；'.join(titles[:3])}"
        result.governance = governance

    @staticmethod
    def _severity_from_compliance_text(title: str, summary: str) -> str:
        text = f"{title} {summary}"
        if any(token in text for token in ("立案", "处罚", "失信", "执行", "虚假", "违规")):
            return "high"
        if any(token in text for token in ("监管", "关注", "问询", "异常")):
            return "medium"
        return "low"

    def _get_compliance_events(self, stock_code: str, result: CollectorOutput) -> None:
        """Collect official compliance/regulatory events from free-first adapters."""
        cache_key = f"compliance_events_{stock_code}_v1"
        cached = self._get_from_cache(cache_key)
        if cached:
            result.compliance_events = [ComplianceEvent(**item) for item in cached]
            return

        company_name = result.stock_info.name if result.stock_info else ""
        keywords = [stock_code]
        if company_name:
            keywords.append(company_name)

        try:
            records = self._official_sources.search_company_compliance_events(
                stock_code=stock_code,
                company_name=company_name,
                keywords=keywords,
                limit=6,
            )
        except Exception as exc:
            self._log_failure("compliance_events", "official_sources", exc, company_name or stock_code)
            return

        events: list[ComplianceEvent] = []
        seen_keys: set[str] = set()
        for item in records:
            title = self._clean_text(str(item.get("title") or ""))
            if not title:
                continue
            dedupe_key = f"{item.get('source','')}_{title}"
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            summary = self._clean_text(str(item.get("summary") or item.get("excerpt") or ""))[:280]
            events.append(
                ComplianceEvent(
                    title=title,
                    source=str(item.get("source") or ""),
                    publish_date=self._coerce_date_value(item.get("publish_date") or item.get("published_at") or item.get("date")),
                    event_type=str(item.get("event_type") or item.get("type") or "official_event"),
                    severity=self._severity_from_compliance_text(title, summary),
                    related_party=company_name or stock_code,
                    url=str(item.get("url") or ""),
                    summary=summary,
                    excerpt=summary,
                    raw_tags=list(item.get("tags") or item.get("keywords") or []),
                )
            )

        result.compliance_events = events
        if events:
            self._save_to_cache(cache_key, [item.model_dump() for item in events], ttl=86400 * 3)

    def _get_shareholder_data(self, stock_code: str, result: CollectorOutput) -> None:
        """Collect shareholder structure and latest shareholder count."""
        shareholders = ShareholderData()

        try:
            df = self._akshare_call("stock_circulate_stock_holder", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(10).iterrows():
                    shareholders.top_shareholders.append(
                        {
                            "name": str(row.get("股东名称", "")),
                            "shares": self._safe_float(row.get("持股数量")),
                            "ratio": self._safe_float(row.get("占流通股比例")),
                            "share_nature": str(row.get("股本性质", "")),
                            "report_date": str(self._coerce_date_value(row.get("截止日期")) or ""),
                        }
                    )
        except Exception as exc:
            self._log_failure("shareholders_top10", "akshare", exc, "stock_circulate_stock_holder")

        for quarter_end in self._quarter_end_candidates(count=2):
            try:
                df = self._akshare_call("stock_hold_num_cninfo", date=quarter_end)
                matched = self._filter_stock_rows(df, stock_code, ("证券代码",))
                if matched.empty:
                    continue
                row = matched.iloc[0]
                shareholders.shareholder_count = self._safe_int(row.get("本期股东人数"))
                shareholders.shareholder_count_change = self._safe_float(row.get("股东人数增幅"))
                break
            except Exception as exc:
                self.logger.debug(f"shareholders_count fallback skipped for {quarter_end}: {exc}")
                continue

        result.shareholders = shareholders

    def _get_patents(self, stock_code: str, result: CollectorOutput) -> None:
        """Collect official patent/technology records from free-first adapters."""
        cache_key = f"patents_{stock_code}_v1"
        cached = self._get_from_cache(cache_key)
        if cached:
            result.patents = [PatentRecord(**item) for item in cached]
            return

        company_name = result.stock_info.name if result.stock_info else ""
        keywords = [stock_code]
        if company_name:
            keywords.append(company_name)
        if result.stock_info and result.stock_info.main_business:
            keywords.extend(
                [
                    token.strip()
                    for token in re.split(r"[、，,；; ]+", str(result.stock_info.main_business))
                    if 2 <= len(token.strip()) <= 12
                ]
            )

        try:
            records = self._official_sources.search_patents(
                stock_code=stock_code,
                company_name=company_name,
                keywords=keywords[:6],
                limit=6,
            )
        except Exception as exc:
            self._log_failure("patents", "official_sources", exc, company_name or stock_code)
            return

        patents: list[PatentRecord] = []
        seen_keys: set[str] = set()
        for item in records:
            title = self._clean_text(str(item.get("title") or item.get("name") or ""))
            if not title:
                continue
            application_no = str(item.get("application_no") or item.get("applicationNo") or item.get("id") or "")
            dedupe_key = application_no or title
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
            keywords_value = item.get("keywords") or raw.get("keywords") if isinstance(raw, dict) else item.get("keywords")
            patents.append(
                PatentRecord(
                    title=title,
                    source=str(item.get("source") or ""),
                    publish_date=self._coerce_date_value(item.get("publish_date") or item.get("published_at") or item.get("date")),
                    patent_type=str(item.get("patent_type") or item.get("type") or (raw.get("type") if isinstance(raw, dict) else "") or ""),
                    application_no=application_no,
                    patent_no=str(item.get("patent_no") or item.get("patentNo") or (raw.get("patent_no") if isinstance(raw, dict) else "") or ""),
                    legal_status=str(item.get("legal_status") or item.get("status") or (raw.get("status") if isinstance(raw, dict) else "") or ""),
                    assignee=str(item.get("assignee") or company_name or (raw.get("assignee") if isinstance(raw, dict) else "") or ""),
                    inventors=list(item.get("inventors") or (raw.get("inventors") if isinstance(raw, dict) else []) or []),
                    summary=self._clean_text(str(item.get("summary") or item.get("excerpt") or ""))[:280],
                    excerpt=self._clean_text(str(item.get("excerpt") or item.get("summary") or ""))[:280],
                    url=str(item.get("url") or ""),
                    keywords=[str(keyword) for keyword in list(keywords_value or [])[:6]],
                )
            )

        result.patents = patents
        if patents:
            self._save_to_cache(cache_key, [item.model_dump() for item in patents], ttl=86400 * 3)

    def _get_sentiment_data(self, stock_code: str, result: CollectorOutput) -> None:
        """Derive sentiment counts from collected news."""
        sentiment = SentimentData()
        if result.news:
            sentiment.news_count_7d = len(result.news)
            sentiment.neutral_count = len(result.news)
            sentiment.sentiment_score = 0.0
        result.sentiment = sentiment

    @staticmethod
    def _build_policy_keywords(result: CollectorOutput) -> list[str]:
        """根据标的行业和主营业务生成政策检索关键词。"""
        info = result.stock_info
        industry = str(info.industry_sw or "") if info else ""
        business = str(info.main_business or "") if info else ""
        keyword_map = {
            "电池": ["锂电池", "动力电池", "储能", "负极材料", "新能源汽车", "新材料"],
            "半导体": ["半导体", "集成电路", "算力", "芯片", "新型显示"],
            "光伏": ["光伏", "储能", "新能源", "硅料", "绿色制造"],
            "医药": ["医药", "创新药", "医疗器械", "集采", "生物医药"],
        }

        keywords: list[str] = []
        if industry:
            keywords.append(industry)
        for token in re.split(r"[、，,；;。/ ]+", business):
            token = token.strip()
            if 2 <= len(token) <= 12:
                keywords.append(token)
        for anchor, extra_keywords in keyword_map.items():
            if anchor and anchor in industry:
                keywords.extend(extra_keywords)
        if info and info.name:
            keywords.append(str(info.name))

        deduped: list[str] = []
        seen: set[str] = set()
        for keyword in keywords:
            if keyword and keyword not in seen:
                deduped.append(keyword)
                seen.add(keyword)
        return deduped[:6]

    def _query_policy_documents(self, keyword: str, *, page_size: int = 5) -> list[dict[str, Any]]:
        """从中国政府网政策文件库检索与关键词相关的政策文件。"""
        records = self._official_sources.search_policy_documents(keyword, limit=page_size)
        normalized: list[dict[str, Any]] = []
        for item in records:
            normalized.append(
                {
                    "title": self._strip_html(str(item.get("title", "") or "")),
                    "source": str(item.get("source", "gov.cn") or "gov.cn"),
                    "policy_date": self._coerce_date_value(item.get("policy_date")),
                    "issuing_body": str(item.get("issuing_body", "") or ""),
                    "document_type": str(item.get("document_type", "") or ""),
                    "url": str(item.get("url", "") or ""),
                    "summary": self._clean_text(self._strip_html(str(item.get("summary", "") or "")))[:280],
                    "matched_keywords": list(item.get("matched_keywords") or [keyword]),
                }
            )
        return normalized

    def _get_policy_documents(self, stock_code: str, result: CollectorOutput) -> None:
        """采集与标的行业相关的官方政策原文。"""
        cache_key = f"policy_documents_{stock_code}_v1"
        cached = self._get_from_cache(cache_key)
        if cached:
            result.policy_documents = [PolicyDocument(**item) for item in cached]
            return

        keywords = self._build_policy_keywords(result)
        if not keywords:
            return

        candidates: list[dict[str, Any]] = []
        seen_urls: dict[str, dict[str, Any]] = {}
        for keyword in keywords:
            try:
                for item in self._query_policy_documents(keyword):
                    url = str(item.get("url") or "")
                    if not url:
                        continue
                    existing = seen_urls.get(url)
                    if existing:
                        existing_keywords = set(existing.get("matched_keywords") or [])
                        existing_keywords.update(item.get("matched_keywords") or [])
                        existing["matched_keywords"] = sorted(existing_keywords)
                        continue
                    seen_urls[url] = item
                    candidates.append(item)
            except Exception as exc:
                self._log_failure("policy_documents", "gov.cn", exc, keyword)

        policy_documents: list[PolicyDocument] = []
        for item in sorted(
            candidates,
            key=lambda record: (
                len(record.get("matched_keywords") or []),
                str(record.get("policy_date") or ""),
            ),
            reverse=True,
        )[:6]:
            material = self._extract_html_material(
                str(item.get("url") or ""),
                cache_prefix="policy_html",
                highlight_keywords=list(item.get("matched_keywords") or []),
            )
            policy_documents.append(
                PolicyDocument(
                    title=str(item.get("title") or ""),
                    source=str(item.get("source") or "gov.cn"),
                    policy_date=item.get("policy_date"),
                    issuing_body=str(item.get("issuing_body") or ""),
                    document_type=str(item.get("document_type") or ""),
                    url=str(item.get("url") or ""),
                    summary=str(material.get("summary") or "") or str(item.get("summary") or ""),
                    excerpt=str(material.get("excerpt") or ""),
                    highlights=list(material.get("highlights") or []),
                    matched_keywords=list(item.get("matched_keywords") or []),
                )
            )

        result.policy_documents = policy_documents
        if policy_documents:
            self._save_to_cache(cache_key, [item.model_dump() for item in policy_documents], ttl=86400 * 3)

    # ================================================================
    # 跨源填补
    # ================================================================

    @staticmethod
    def _extract_main_business_from_announcements(announcements: list[Announcement]) -> str | None:
        """从定期报告摘录中提取主营业务描述。"""
        for item in announcements:
            structured = item.structured_fields if isinstance(item.structured_fields, dict) else {}
            stock_info = structured.get("stock_info") if isinstance(structured, dict) else None
            if isinstance(stock_info, dict):
                main_business = str(stock_info.get("main_business") or "").strip()
                if len(main_business) >= 12:
                    return main_business[:180]

        patterns = [
            r"(?:主营业务|公司主要从事|主要业务为|核心业务为)[:：]\s*([^。；\n]{12,220})",
            r"(?:从事|聚焦于)([^。；\n]{12,220})",
        ]
        for item in announcements:
            text = " ".join(
                [
                    str(item.title or ""),
                    str(item.excerpt or ""),
                    "；".join(item.highlights or []),
                ]
            )
            if not text:
                continue
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    candidate = DataCollectorAgent._clean_text(match.group(1))[:180]
                    if len(candidate) >= 12:
                        return candidate
        return None

    @staticmethod
    def _derive_governance_histories_from_announcements(
        announcements: list[Announcement],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """从公告标题提炼分红、回购、再融资历史。"""
        dividend_history: list[dict[str, Any]] = []
        buyback_history: list[dict[str, Any]] = []
        refinancing_history: list[dict[str, Any]] = []

        for item in announcements:
            title = str(item.title or "")
            if not title:
                continue
            payload = {
                "date": item.announcement_date.isoformat() if item.announcement_date else "",
                "title": title,
                "source": item.source,
                "url": item.url or item.pdf_url,
            }
            if any(keyword in title for keyword in ("分红", "利润分配", "派息", "现金红利")):
                dividend_history.append(payload)
            if any(keyword in title for keyword in ("回购", "股份回购")):
                buyback_history.append(payload)
            if any(keyword in title for keyword in ("定增", "配股", "可转债", "再融资", "募集说明书", "发行股份")):
                refinancing_history.append(payload)
        return dividend_history[:10], buyback_history[:10], refinancing_history[:10]

    @staticmethod
    def _announcement_priority(item: Announcement) -> tuple[int, str]:
        text = f"{item.announcement_type} {item.title}"
        if "骞存姤" in text or "骞村害鎶ュ憡" in text:
            return 0, str(item.announcement_date or "")
        if "鍗婂勾" in text or "鍗婂勾鎶?" in text or "涓湡鎶ュ憡" in text:
            return 1, str(item.announcement_date or "")
        if "瀛ｅ害" in text or "瀛ｆ姤" in text:
            return 2, str(item.announcement_date or "")
        if "闂" in text:
            return 4, str(item.announcement_date or "")
        return 3, str(item.announcement_date or "")

    @staticmethod
    def _merge_structured_stock_info(
        stock_info: StockBasicInfo | None,
        announcements: list[Announcement],
    ) -> StockBasicInfo | None:
        if stock_info is None:
            return None

        updates: dict[str, Any] = {}
        target_fields = ("main_business", "business_model", "asset_model", "client_type")
        for item in sorted(announcements, key=DataCollectorAgent._announcement_priority):
            structured = item.structured_fields if isinstance(item.structured_fields, dict) else {}
            structured_stock_info = structured.get("stock_info") if isinstance(structured, dict) else None
            if not isinstance(structured_stock_info, dict):
                continue
            for field_name in target_fields:
                if updates.get(field_name) or getattr(stock_info, field_name):
                    continue
                value = str(structured_stock_info.get(field_name) or "").strip()
                if value:
                    updates[field_name] = value[:240]

        return stock_info.model_copy(update=updates) if updates else stock_info

    @staticmethod
    def _merge_structured_financials(
        stock_code: str,
        financials: list[FinancialStatement],
        announcements: list[Announcement],
    ) -> list[FinancialStatement]:
        if not announcements:
            return financials

        allowed_fields = {
            name
            for name in FinancialStatement.model_fields
            if name not in {"code", "report_date", "report_type", "source", "raw_data"}
        }
        merged = list(financials)
        index_by_date = {
            item.report_date.strftime("%Y-%m-%d"): idx
            for idx, item in enumerate(merged)
            if item.report_date
        }

        for item in sorted(announcements, key=DataCollectorAgent._announcement_priority):
            structured = item.structured_fields if isinstance(item.structured_fields, dict) else {}
            snapshot = structured.get("financial_snapshot") if isinstance(structured, dict) else None
            if not isinstance(snapshot, dict) or not snapshot:
                continue

            report_date = DataCollectorAgent._parse_date(str(snapshot.get("report_date") or ""))
            if report_date is None:
                continue

            normalized_snapshot = {
                key: value
                for key, value in snapshot.items()
                if key in allowed_fields and value not in (None, "", [], {})
            }
            extra_snapshot = {
                key: value
                for key, value in snapshot.items()
                if key not in normalized_snapshot and value not in (None, "", [], {})
            }
            if not normalized_snapshot and not extra_snapshot:
                continue

            report_date_key = report_date.strftime("%Y-%m-%d")
            if report_date_key in index_by_date:
                existing = merged[index_by_date[report_date_key]]
                updates = {
                    key: value
                    for key, value in normalized_snapshot.items()
                    if getattr(existing, key, None) in (None, "", [], {})
                }
                raw_data = dict(existing.raw_data or {})
                if extra_snapshot:
                    extracts = list(raw_data.get("announcement_extracts") or [])
                    extracts.append({"title": item.title, "snapshot": extra_snapshot})
                    raw_data["announcement_extracts"] = extracts[-5:]
                if raw_data != (existing.raw_data or {}):
                    updates["raw_data"] = raw_data
                if updates:
                    merged[index_by_date[report_date_key]] = existing.model_copy(update=updates)
                continue

            merged.append(
                FinancialStatement(
                    code=stock_code,
                    report_date=report_date,
                    report_type=item.announcement_type or "announcement_extract",
                    source=DataSource.CNINFO,
                    raw_data={"announcement_title": item.title, "announcement_snapshot_extra": extra_snapshot},
                    **normalized_snapshot,
                )
            )
            index_by_date[report_date_key] = len(merged) - 1

        merged.sort(key=lambda value: value.report_date or date.min, reverse=True)
        return merged

    @staticmethod
    def _merge_structured_governance(
        governance: GovernanceData | None,
        announcements: list[Announcement],
    ) -> GovernanceData | None:
        if governance is None or not announcements:
            return governance

        dividend_history = list(governance.dividend_history or [])
        seen_titles = {str(item.get("title") or "") for item in dividend_history if isinstance(item, dict)}

        for item in sorted(announcements, key=DataCollectorAgent._announcement_priority):
            structured = item.structured_fields if isinstance(item.structured_fields, dict) else {}
            dividend_plan = str(structured.get("dividend_plan") or "").strip() if isinstance(structured, dict) else ""
            if not dividend_plan or item.title in seen_titles:
                continue
            dividend_history.append(
                {
                    "date": item.announcement_date.isoformat() if item.announcement_date else "",
                    "title": item.title,
                    "source": item.source,
                    "url": item.url or item.pdf_url,
                    "plan": dividend_plan,
                }
            )
            seen_titles.add(item.title)

        return governance.model_copy(update={"dividend_history": dividend_history[:10]}) if dividend_history else governance

    @staticmethod
    def _enrich_financial_statements(financials: list[FinancialStatement]) -> list[FinancialStatement]:
        """补齐自由现金流和净现比等可直接派生字段。"""
        enriched: list[FinancialStatement] = []
        for item in financials:
            updates: dict[str, Any] = {}
            if item.free_cashflow is None and item.operating_cashflow is not None and item.investing_cashflow is not None:
                updates["free_cashflow"] = round(item.operating_cashflow + item.investing_cashflow, 2)
            if item.cash_to_profit is None and item.operating_cashflow is not None and item.net_profit not in (None, 0):
                updates["cash_to_profit"] = round(item.operating_cashflow / item.net_profit, 2)
            enriched.append(item.model_copy(update=updates) if updates else item)
        return enriched

    def _fill_missing_fields(self, stock_code: str, result: CollectorOutput) -> None:
        """Cross-source backfill for lightweight but useful fields."""
        if (not result.valuation or len(result.valuation) == 0) and result.realtime:
            rt = result.realtime
            if rt.pe_ttm is not None or rt.pb_mrq is not None:
                result.valuation = [
                    StockPrice(
                        code=stock_code,
                        date=date.today(),
                        pe_ttm=rt.pe_ttm,
                        pb_mrq=rt.pb_mrq,
                    )
                ]

        if (not result.governance or not self._has_substantive_model_data(result.governance)) and result.stock_info:
            top_holder_name = ""
            if result.shareholders and result.shareholders.top_shareholders:
                top_holder = result.shareholders.top_shareholders[0]
                if isinstance(top_holder, dict):
                    top_holder_name = str(top_holder.get("name", "") or "")
            result.governance = GovernanceData(
                actual_controller=result.stock_info.actual_controller or top_holder_name or None,
                controller_type=result.stock_info.controller_type,
            )

        if (
            result.stock_info
            and result.governance
            and result.governance.actual_controller
            and not result.stock_info.actual_controller
        ):
            result.stock_info = result.stock_info.model_copy(
                update={"actual_controller": result.governance.actual_controller}
            )

        if result.financials:
            result.financials = self._enrich_financial_statements(result.financials)

        if result.announcements:
            if result.stock_info:
                result.stock_info = self._merge_structured_stock_info(result.stock_info, result.announcements)
            result.financials = self._merge_structured_financials(stock_code, result.financials, result.announcements)
            if result.governance:
                result.governance = self._merge_structured_governance(result.governance, result.announcements)
            if result.financials:
                result.financials = self._enrich_financial_statements(result.financials)

        if result.stock_info and not result.stock_info.main_business and result.announcements:
            main_business = self._extract_main_business_from_announcements(result.announcements)
            if main_business:
                result.stock_info = result.stock_info.model_copy(update={"main_business": main_business})

        if result.announcements:
            dividend_history, buyback_history, refinancing_history = self._derive_governance_histories_from_announcements(
                result.announcements
            )
            if result.governance:
                result.governance = result.governance.model_copy(
                    update={
                        "dividend_history": result.governance.dividend_history or dividend_history,
                        "buyback_history": result.governance.buyback_history or buyback_history,
                        "refinancing_history": result.governance.refinancing_history or refinancing_history,
                    }
                )

        if (not result.industry_enhanced or not self._has_substantive_model_data(result.industry_enhanced)):
            industry_name = ""
            industry_code = None
            if result.stock_info and result.stock_info.industry_sw:
                industry_name = result.stock_info.industry_sw
                industry_code = result.stock_info.industry_sw_code
            elif result.industry:
                industry_name = result.industry.industry_name
                industry_code = result.industry.industry_code
            if industry_name or industry_code:
                result.industry_enhanced = IndustryEnhancedData(
                    industry_name=industry_name,
                    industry_code=industry_code,
                )

        if not result.valuation_percentile or not self._has_substantive_model_data(result.valuation_percentile):
            self._get_valuation_percentile(stock_code, result)

        if (not result.sentiment or result.sentiment.news_count_7d == 0) and result.news:
            result.sentiment = SentimentData(
                news_count_7d=len(result.news),
                neutral_count=len(result.news),
                sentiment_score=0.0,
            )

        if not result.announcements and result.news:
            keywords = ("公告", "年报", "季报", "回购", "增持", "减持", "分红", "业绩")
            fallback_items: list[Announcement] = []
            for news in result.news:
                title = str(news.title or "")
                if not title or not any(keyword in title for keyword in keywords):
                    continue
                fallback_items.append(
                    Announcement(
                        title=title,
                        announcement_type="news_fallback",
                        announcement_date=self._coerce_date_value(news.publish_time),
                        source="news_fallback",
                        summary=str(news.content or "")[:200],
                    )
                )
            if fallback_items:
                result.announcements = fallback_items[:10]
        return None

    # ================================================================
    # 覆盖率计算
    # ================================================================

    @staticmethod
    def _has_substantive_model_data(value: BaseModel | None) -> bool:
        if value is None:
            return False

        for field_name, field in value.__class__.model_fields.items():
            field_value = getattr(value, field_name)
            if field.default_factory is not None:
                default_value = field.default_factory()
            else:
                default_value = field.default

            if isinstance(field_value, BaseModel):
                if DataCollectorAgent._has_substantive_model_data(field_value):
                    return True
                continue

            if isinstance(field_value, (list, dict, set, tuple)):
                if field_value != default_value:
                    return True
                continue

            if field_value != default_value:
                return True

        return False

    @staticmethod
    def _calc_coverage(result: CollectorOutput) -> float:
        """计算数据覆盖率"""
        profiles = build_module_profiles(result.model_dump(mode="json"))
        _, _, coverage_ratio, _, _, _ = aggregate_quality(profiles)
        return coverage_ratio
