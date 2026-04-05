"""多源数据采集Agent - AKShare主源 + BaoStock备源

采集数据类型:
1. 股票基础信息 (stock_individual_info_em)
2. 历史行情 (stock_zh_a_hist, 前复权)
3. 实时行情 (stock_zh_a_spot_em)
4. 财务报表三大表 (stock_financial_report_ths)
5. 估值数据含PE/PB (baostock query_history_k_data_plus)
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
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
)

from .cache import FileCache

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
            ("governance", self._get_governance_data),
            # Sprint 2: 研报与股东
            ("research_reports", self._get_research_reports),
            ("shareholders", self._get_shareholder_data),
            # Sprint 3: 行业增强与估值分位
            ("industry_enhanced", self._get_industry_enhanced),
            ("valuation_percentile", self._get_valuation_percentile),
            # Sprint 4: 新闻舆情
            ("news", self._get_news),
            ("sentiment", self._get_sentiment_data),
        ]

        for data_type, fetch_fn in tasks:
            try:
                self.logger.info(f"采集 {data_type}...")
                fetch_fn(stock_code, result)
                result.collection_status[data_type] = "ok"
                self.logger.info(f"采集 {data_type} 完成")
            except Exception as e:
                self.logger.warning(f"采集 {data_type} 失败: {e}")
                result.collection_status[data_type] = "failed"
                result.errors.append(f"{data_type}: {e}")

        # 跨源填补缺失字段
        self._fill_missing_fields(stock_code, result)

        # 计算覆盖率
        result.coverage_ratio = self._calc_coverage(result)
        self.logger.info(f"采集完成 | 覆盖率={result.coverage_ratio:.0%}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data=result.model_dump(),
            data_sources=["akshare", "baostock"],
            confidence=result.coverage_ratio,
            summary=f"采集{len(result.collection_status)}类数据，覆盖率{result.coverage_ratio:.0%}",
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验输出"""
        if output.status != AgentStatus.SUCCESS:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, [f"状态异常: {output.status}"])

        data = output.data
        ratio = data.get("coverage_ratio", 0)
        if ratio < 0.3:
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
        cache_key = f"financials_{stock_code}"
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
                        roe=self._safe_float(row.get("净资产收益率(%)")),
                        gross_margin=self._safe_float(row.get("销售毛利率(%)")),
                        net_margin=self._safe_float(row.get("销售净利率(%)")),
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
                        "roe": self._safe_float(row[3]),
                        "net_margin": self._safe_float(row[4]),
                        "gross_margin": self._safe_float(row[5]),
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
                        "debt_ratio": self._safe_float(row[7]),
                    },
                )

                # 3. 成长能力 (growth_data)
                self._baostock_query_merge(
                    bs, bs_code, start_year, merged,
                    "query_growth_data",
                    lambda row: {
                        "revenue_yoy": self._safe_float(row[6]),
                        "net_profit_yoy": self._safe_float(row[5]),
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
                name=fields[0],
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

    # ================================================================
    # Sprint 1: 公告与治理数据采集
    # ================================================================

    def _get_announcements(self, stock_code: str, result: CollectorOutput) -> None:
        """采集公告披露数据 - 巨潮资讯网"""
        try:
            df = self._akshare_call("stock_notice_report", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(20).iterrows():
                    result.announcements.append(Announcement(
                        title=str(row.get("公告标题", "")),
                        announcement_type=str(row.get("公告类型", "")),
                        announcement_date=self._parse_date(str(row.get("公告日期", ""))),
                        source="cninfo",
                    ))
        except Exception as e:
            self._log_failure("announcements", "akshare", e, "stock_notice_report")
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
                    result.governance.management_changes.append({
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
        """采集研报摘要 - 东方财富研报中心"""
        try:
            df = self._akshare_call("stock_research_report_em", symbol=stock_code)
            if df is not None and not df.empty:
                for _, row in df.head(10).iterrows():
                    result.research_reports.append(ResearchReportSummary(
                        title=str(row.get("title", row.get("标题", ""))),
                        institution=str(row.get("org", row.get("机构", ""))),
                        rating=str(row.get("em_rating", row.get("评级", ""))),
                        publish_date=self._parse_date(str(row.get("publish_date", row.get("日期", "")))),
                    ))
        except Exception as e:
            self._log_failure("research_reports", "akshare", e, "stock_research_report_em")
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
                shareholders.shareholder_count = self._safe_float(row.iloc[0]) if len(row) > 0 else None
                if len(df) > 1:
                    shareholders.shareholder_count_change = self._safe_float(df.iloc[1].iloc[0]) if len(df.columns) > 0 else None
        except Exception as e:
            self._log_failure("shareholders_count", "akshare", e, "stock_hold_num_cninfo")

        result.shareholders = shareholders

    # ================================================================
    # Sprint 3: 行业增强与估值分位
    # ================================================================

    def _get_industry_enhanced(self, stock_code: str, result: CollectorOutput) -> None:
        """采集行业增强数据 - 申万行业/东方财富行业板块"""
        enhanced = IndustryEnhancedData(industry_name=result.industry.industry_name if result.industry else "")

        # 1. 申万行业分类
        try:
            df = self._akshare_call("stock_industry_clf_hist_sw", symbol=stock_code)
            if df is not None and not df.empty:
                row = df.iloc[0]
                enhanced.industry_name = str(row.get("行业名称", result.industry.industry_name if result.industry else ""))
                enhanced.industry_level = str(row.get("行业级别", ""))
        except Exception as e:
            self._log_failure("industry_sw", "akshare", e, "stock_industry_clf_hist_sw")

        # 2. 东方财富行业板块行情
        try:
            df = self._akshare_call("stock_board_industry_spot_em")
            if df is not None and not df.empty:
                industry_name = result.industry.industry_name if result.industry else ""
                if industry_name:
                    row = df[df["板块名称"] == industry_name]
                    if not row.empty:
                        row = row.iloc[0]
                        enhanced.industry_index_close = self._safe_float(row.get("最新价"))
                        enhanced.industry_change_pct = self._safe_float(row.get("涨跌幅"))
        except Exception as e:
            self._log_failure("industry_board", "akshare", e, "stock_board_industry_spot_em")

        # 3. 行业PE/PB
        try:
            df = self._akshare_call("stock_industry_pe_ratio_cninfo", symbol=stock_code)
            if df is not None and not df.empty:
                row = df.iloc[0]
                enhanced.industry_pe = self._safe_float(row.get("市盈率", row.iloc[0] if len(row) > 0 else None))
                enhanced.industry_pb = self._safe_float(row.get("市净率", row.iloc[1] if len(row) > 1 else None))
        except Exception as e:
            self._log_failure("industry_pe", "akshare", e, "stock_industry_pe_ratio_cninfo")

        result.industry_enhanced = enhanced

    def _get_valuation_percentile(self, stock_code: str, result: CollectorOutput) -> None:
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

    # ================================================================
    # 跨源填补
    # ================================================================

    def _fill_missing_fields(self, stock_code: str, result: CollectorOutput) -> None:
        """跨源填补缺失字段"""
        # 1. 估值数据空时从realtime提取PE/PB
        if (not result.valuation or len(result.valuation) == 0) and result.realtime:
            rt = result.realtime
            if rt.pe_ttm is not None or rt.pb_mrq is not None:
                result.valuation = [StockPrice(
                    code=stock_code,
                    date=date.today(),
                    pe_ttm=rt.pe_ttm,
                    pb_mrq=rt.pb_mrq,
                )]
                self.logger.info("跨源填补: 从实时行情提取估值数据")

        # 2. 舆情空但有新闻时计算基础中性情绪
        if (not result.sentiment or result.sentiment.news_count_7d == 0) and result.news:
            from investresearch.core.models import SentimentData
            result.sentiment = SentimentData(
                news_count_7d=len(result.news),
                neutral_count=len(result.news),
                sentiment_score=0.0,
            )
            self.logger.info(f"跨源填补: 从{len(result.news)}条新闻生成基础舆情")

    # ================================================================
    # 覆盖率计算
    # ================================================================

    @staticmethod
    def _calc_coverage(result: CollectorOutput) -> float:
        """计算数据覆盖率"""
        total = 14  # 原5类 + 新增9类
        filled = 0
        if result.stock_info and result.stock_info.name:
            filled += 1
        if result.prices and len(result.prices) > 0:
            filled += 1
        if result.realtime:
            filled += 1
        if result.financials and len(result.financials) > 0:
            filled += 1
        if result.valuation and len(result.valuation) > 0:
            filled += 1
        # Sprint 1-4 新增
        if result.announcements and len(result.announcements) > 0:
            filled += 1
        if result.governance:
            filled += 1
        if result.research_reports and len(result.research_reports) > 0:
            filled += 1
        if result.shareholders:
            filled += 1
        if result.industry_enhanced:
            filled += 1
        if result.valuation_percentile:
            filled += 1
        if result.news and len(result.news) > 0:
            filled += 1
        if result.sentiment:
            filled += 1
        return round(filled / total, 2)
