"""数据清洗Agent - 标准化、去重、缺失标注、派生指标计算

清洗流程:
1. 价格数据: 去重/排序/类型转换/缺失标注
2. 财务数据: 派生指标(ROE/净利率/资产负债率/营收增速)/缺失标注
3. 股票信息: 字段标准化
4. 完整性检查: 覆盖率低于80%时告警
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd


def _check_governance(d: dict) -> bool:
    """检查治理数据是否有实际内容（非空壳）"""
    gov = d.get("governance")
    if not gov:
        return False
    gov_keys = ["actual_controller", "equity_pledge_ratio", "guarantee_info", "lawsuit_info", "management_changes"]
    filled = [k for k in gov_keys if gov.get(k)]
    return len(filled) > 0

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import (
    AgentInput,
    AgentOutput,
    AgentStatus,
    CollectorOutput,
)

logger = get_logger("agent.cleaner")


class DataCleanerAgent(AgentBase[AgentInput, AgentOutput]):
    """数据清洗Agent

    从AgentInput.context["raw_data"]中取原始采集数据，
    清洗后输出到AgentOutput.data["cleaned"]。
    """

    agent_name: str = "data_cleaner"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行数据清洗"""
        raw = input_data.context.get("raw_data", {})
        if not raw:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无原始数据"],
            )

        self.logger.info("开始数据清洗...")
        cleaned: dict[str, Any] = {}
        warnings: list[str] = []

        # === 原始4类 ===
        prices_raw = raw.get("prices", [])
        if prices_raw:
            cleaned["prices"] = self._clean_prices(prices_raw)
            self.logger.info(f"价格数据清洗完成: {len(cleaned['prices'])} 条")
        else:
            warnings.append("无价格数据")

        financials_raw = raw.get("financials", [])
        if financials_raw:
            cleaned["financials"] = self._clean_financials(financials_raw)
            self.logger.info(f"财务数据清洗完成: {len(cleaned['financials'])} 期")
        else:
            warnings.append("无财务数据")

        stock_info_raw = raw.get("stock_info")
        if stock_info_raw:
            cleaned["stock_info"] = self._clean_stock_info(stock_info_raw)
            self.logger.info("股票信息清洗完成")
        else:
            warnings.append("无股票基础信息")

        valuation_raw = raw.get("valuation", [])
        if valuation_raw:
            cleaned["valuation"] = self._clean_valuation(valuation_raw)
            self.logger.info(f"估值数据清洗完成: {len(cleaned['valuation'])} 条")
        else:
            warnings.append("无估值数据")

        realtime_raw = raw.get("realtime")
        if realtime_raw:
            cleaned["realtime"] = realtime_raw if isinstance(realtime_raw, dict) else self._clean_stock_info(realtime_raw)

        # === Sprint 1: 公告与治理 ===
        announcements_raw = raw.get("announcements", [])
        if announcements_raw:
            cleaned["announcements"] = self._clean_announcements(announcements_raw)
            self.logger.info(f"公告数据清洗完成: {len(cleaned['announcements'])} 条")
        else:
            warnings.append("无公告数据")

        governance_raw = raw.get("governance")
        if governance_raw and isinstance(governance_raw, dict):
            gov = self._clean_governance(governance_raw)
            if gov.get("_fields_available"):
                cleaned["governance"] = gov
                self.logger.info(f"治理数据清洗完成: {', '.join(gov['_fields_available'])}")
            else:
                warnings.append("治理数据为空壳")
        else:
            warnings.append("无治理数据")

        # === Sprint 2: 研报与股东 ===
        reports_raw = raw.get("research_reports", [])
        if reports_raw:
            cleaned["research_reports"] = self._clean_research_reports(reports_raw)
            self.logger.info(f"研报数据清洗完成: {len(cleaned['research_reports'])} 条")
        else:
            warnings.append("无研报数据")

        shareholders_raw = raw.get("shareholders")
        if shareholders_raw and isinstance(shareholders_raw, dict):
            sh = self._clean_shareholders(shareholders_raw)
            if sh.get("_fields_available"):
                cleaned["shareholders"] = sh
                self.logger.info(f"股东数据清洗完成: {', '.join(sh['_fields_available'])}")
            else:
                warnings.append("股东数据为空壳")
        else:
            warnings.append("无股东数据")

        # === Sprint 3: 行业增强与估值分位 ===
        industry_raw = raw.get("industry_enhanced")
        if industry_raw and isinstance(industry_raw, dict):
            ie = self._clean_industry_enhanced(industry_raw)
            if ie.get("industry_name") or ie.get("industry_pe") is not None:
                cleaned["industry_enhanced"] = ie
                self.logger.info("行业增强数据清洗完成")
            else:
                warnings.append("行业增强数据为空壳")
        else:
            warnings.append("无行业增强数据")

        vp_raw = raw.get("valuation_percentile")
        if vp_raw and isinstance(vp_raw, dict):
            vp = self._clean_valuation_percentile(vp_raw)
            if vp.get("pe_ttm_current") is not None or vp.get("pb_mrq_current") is not None:
                cleaned["valuation_percentile"] = vp
                self.logger.info("估值分位数据清洗完成")
            else:
                warnings.append("估值分位数据为空壳")
        else:
            warnings.append("无估值分位数据")

        # === Sprint 4: 新闻舆情 ===
        news_raw = raw.get("news", [])
        if news_raw:
            cleaned["news"] = self._clean_news(news_raw)
            self.logger.info(f"新闻数据清洗完成: {len(cleaned['news'])} 条")
        else:
            warnings.append("无新闻数据")

        sentiment_raw = raw.get("sentiment")
        if sentiment_raw and isinstance(sentiment_raw, dict):
            sent = self._clean_sentiment(sentiment_raw)
            if sent.get("news_count_7d", 0) > 0:
                cleaned["sentiment"] = sent
                self.logger.info(f"舆情数据清洗完成: {sent.get('news_count_7d')} 条新闻")
            else:
                warnings.append("舆情数据为空壳")
        else:
            warnings.append("无舆情数据")

        # === 完整性检查 ===
        coverage = self._calc_cleaned_coverage(cleaned)
        cleaned["coverage_ratio"] = coverage

        if coverage < 0.8:
            warnings.append(f"数据覆盖率偏低({coverage:.0%})")

        self.logger.info(
            f"清洗完成 | 覆盖率={coverage:.0%} | 警告={len(warnings)}"
        )

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"cleaned": cleaned, "warnings": warnings},
            data_sources=raw.get("data_sources", []),
            confidence=coverage,
            summary=f"清洗完成，覆盖率{coverage:.0%}，{len(warnings)}个警告",
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验输出"""
        if output.status != AgentStatus.SUCCESS:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, [f"状态异常: {output.status}"])

    # ================================================================
    # 清洗方法
    # ================================================================

    def _clean_prices(self, prices: list[dict]) -> list[dict]:
        """清洗价格数据: 去重/排序/类型转换"""
        if not prices:
            return []

        df = pd.DataFrame(prices)

        # 类型转换
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        numeric_cols = ["open", "close", "high", "low", "volume", "amount",
                        "turnover_rate", "pe_ttm", "pb_mrq", "ps_ttm", "market_cap"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 去重 (按code+date，保留最后一条)
        if "date" in df.columns:
            df = df.sort_values("date").drop_duplicates(
                subset=["code", "date"] if "code" in df.columns else ["date"],
                keep="last",
            )

        # 标注缺失
        df["_has_price"] = df[["open", "close", "high", "low"]].notna().all(axis=1)
        df["_has_volume"] = df["volume"].notna()

        # 转回list[dict]
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        return df.to_dict(orient="records")

    def _clean_financials(self, financials: list[dict]) -> list[dict]:
        """清洗财务数据: 计算派生指标，按日期倒序排列"""
        if not financials:
            return []

        # 按日期倒序排列（最新在前）
        def _get_date(f: dict) -> str:
            d = f.get("report_date", "")
            return str(d) if d else ""

        financials_sorted = sorted(financials, key=_get_date, reverse=True)

        cleaned = []
        for f in financials_sorted:
            item = dict(f)  # 浅拷贝

            # 计算派生指标（如果原始数据有缺失）
            if item.get("net_margin") is None:
                revenue = self._safe_float(item.get("revenue"))
                net_profit = self._safe_float(item.get("net_profit"))
                if revenue and net_profit is not None and revenue != 0:
                    item["net_margin"] = round(net_profit / revenue * 100, 2)

            if item.get("debt_ratio") is None:
                total_assets = self._safe_float(item.get("total_assets"))
                total_liabilities = self._safe_float(item.get("total_liabilities"))
                if total_assets and total_liabilities is not None and total_assets != 0:
                    item["debt_ratio"] = round(total_liabilities / total_assets * 100, 2)

            if item.get("roe") is None:
                equity = self._safe_float(item.get("equity"))
                net_profit = self._safe_float(item.get("net_profit"))
                if equity and net_profit is not None and equity != 0:
                    item["roe"] = round(net_profit / equity * 100, 2)

            # 标注数据质量
            has_revenue = item.get("revenue") is not None
            has_profit = item.get("net_profit") is not None
            has_balance = item.get("total_assets") is not None
            item["_quality_score"] = sum([has_revenue, has_profit, has_balance]) / 3

            cleaned.append(item)

        return cleaned

    def _clean_stock_info(self, info: dict) -> dict:
        """清洗股票基础信息"""
        cleaned = dict(info)

        # 标准化交易所
        exchange = cleaned.get("exchange", "")
        if "上海" in str(exchange) or "主板" in str(exchange):
            cleaned["exchange_normalized"] = "SSE"
        elif "深圳" in str(exchange) or "创业板" in str(exchange):
            cleaned["exchange_normalized"] = "SZSE"
        elif "北京" in str(exchange):
            cleaned["exchange_normalized"] = "BSE"

        return cleaned

    def _clean_valuation(self, valuations: list[dict]) -> list[dict]:
        """清洗估值数据"""
        if not valuations:
            return []

        df = pd.DataFrame(valuations)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date").drop_duplicates(
                subset=["code", "date"] if "code" in df.columns else ["date"],
                keep="last",
            )

        for col in ["pe_ttm", "pb_mrq", "ps_ttm"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 计算估值分位
        if "pe_ttm" in df.columns:
            valid_pe = df["pe_ttm"].dropna()
            if len(valid_pe) > 0:
                latest_pe = valid_pe.iloc[-1]
                df["pe_percentile"] = (valid_pe <= latest_pe).mean()

        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        return df.to_dict(orient="records")

    # ================================================================
    # 工具方法
    # ================================================================

    @staticmethod
    def _safe_float(v: Any) -> float | None:
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _calc_cleaned_coverage(cleaned: dict) -> float:
        """计算清洗后数据覆盖率（全部13类）"""
        checks: list[tuple[str, Any]] = [
            ("stock_info", lambda d: bool(d.get("stock_info") and d["stock_info"].get("name"))),
            ("prices", lambda d: bool(d.get("prices") and len(d["prices"]) > 10)),
            ("realtime", lambda d: bool(d.get("realtime"))),
            ("financials", lambda d: bool(d.get("financials") and len(d["financials"]) > 0)),
            ("valuation", lambda d: bool(d.get("valuation") and len(d["valuation"]) > 0)),
            ("announcements", lambda d: bool(d.get("announcements") and len(d["announcements"]) > 0)),
            ("governance", lambda d: _check_governance(d)),
            ("research_reports", lambda d: bool(d.get("research_reports") and len(d["research_reports"]) > 0)),
            ("shareholders", lambda d: bool(d.get("shareholders"))),
            ("industry_enhanced", lambda d: (
                bool(d.get("industry_enhanced"))
                and d["industry_enhanced"].get("industry_name")
            )),
            ("valuation_percentile", lambda d: (
                bool(d.get("valuation_percentile"))
                and (d["valuation_percentile"].get("pe_ttm_percentile") is not None
                     or d["valuation_percentile"].get("pb_mrq_percentile") is not None)
            )),
            ("news", lambda d: bool(d.get("news") and len(d["news"]) > 0)),
            ("sentiment", lambda d: bool(d.get("sentiment") and d.get("sentiment", {}).get("news_count_7d", 0) > 0)),
        ]
        filled = sum(1 for _, check in checks if check(cleaned))
        return round(filled / len(checks), 2)

    # ================================================================
    # Sprint 1-4 清洗方法
    # ================================================================

    def _clean_announcements(self, announcements: list[dict]) -> list[dict]:
        """清洗公告数据： 去重、标准化类型、 按日期排序"""
        if not announcements:
            return []

        cleaned: list[dict] = []
        seen_titles: set[str] = set()
        for ann in announcements:
            title = ann.get("title", "")
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)

            item = dict(ann)
            # 标准化公告类型
            atype = str(item.get("announcement_type", ""))
            if "年报" in atype or "年度报告" in atype:
                item["announcement_type_normalized"] = "annual_report"
            elif "季报" in atype or "季度报告" in atype:
                item["announcement_type_normalized"] = "quarterly_report"
            elif "半年报" in atype or "中期报告" in atype:
                item["announcement_type_normalized"] = "semi_annual"
            elif "问询" in atype:
                item["announcement_type_normalized"] = "inquiry_letter"
            elif "临时" in atype:
                item["announcement_type_normalized"] = "temporary"
            else:
                item["announcement_type_normalized"] = "other"
            cleaned.append(item)

        cleaned.sort(key=lambda x: str(x.get("announcement_date", "")), reverse=True)
        return cleaned

    def _clean_governance(self, governance: dict) -> dict:
        """清洗治理数据： 标记可用字段、计算完整性"""
        cleaned = dict(governance)
        fields_present = []
        if cleaned.get("actual_controller"):
            fields_present.append("actual_controller")
        if cleaned.get("equity_pledge_ratio") is not None:
            fields_present.append("equity_pledge")
        if cleaned.get("guarantee_info"):
            fields_present.append("guarantee")
        if cleaned.get("lawsuit_info"):
            fields_present.append("lawsuit")
        if cleaned.get("management_changes"):
            fields_present.append("management_changes")
        cleaned["_fields_available"] = fields_present
        cleaned["_completeness"] = len(fields_present) / 5.0
        return cleaned

    def _clean_research_reports(self, reports: list[dict]) -> list[dict]:
        """清洗研报： 去重、 标准化评级、 按日期排序"""
        if not reports:
            return []

        seen: set[str] = set()
        cleaned: list[dict] = []
        for rpt in reports:
            title = rpt.get("title", "")
            if not title or title in seen:
                continue
            seen.add(title)
            item = dict(rpt)
            rating = str(item.get("rating", "")).upper()
            if "买入" in rating or "BUY" in rating:
                item["rating_normalized"] = "buy"
            elif "增持" in rating or "OVERWEIGHT" in rating:
                item["rating_normalized"] = "overweight"
            elif "中性" in rating or "HOLD" in rating or "NEUTRAL" in rating:
                item["rating_normalized"] = "neutral"
            elif "减持" in rating or "UNDERWEIGHT" in rating:
                item["rating_normalized"] = "underweight"
            elif "卖出" in rating or "SELL" in rating:
                item["rating_normalized"] = "sell"
            else:
                item["rating_normalized"] = rating.lower() if rating else "unknown"
            cleaned.append(item)

        cleaned.sort(key=lambda x: str(x.get("publish_date", "")), reverse=True)
        return cleaned

    def _clean_shareholders(self, shareholders: dict) -> dict:
        """清洗股东数据： 计算集中度指标"""
        cleaned = dict(shareholders)
        top = cleaned.get("top_shareholders", [])
        if top:
            total_ratio = sum(
                self._safe_float(s.get("ratio")) or 0
                for s in top
                if isinstance(s, dict)
            )
            cleaned["top10_total_ratio"] = round(total_ratio, 2)
            if top and isinstance(top[0], dict):
                largest = self._safe_float(top[0].get("ratio"))
                if largest and largest > 30:
                    cleaned["concentration_warning"] = (
                        f"第一大股东持股 {largest}%，集中度较高"
                    )
        fields_present = []
        if cleaned.get("top_shareholders"):
            fields_present.append("top_shareholders")
        if cleaned.get("fund_holders"):
            fields_present.append("fund_holders")
        if cleaned.get("shareholder_count") is not None:
            fields_present.append("shareholder_count")
        cleaned["_fields_available"] = fields_present
        return cleaned

    def _clean_industry_enhanced(self, industry: dict) -> dict:
        """清洗行业增强数据： 验证数值字段"""
        cleaned = dict(industry)
        for field in ["industry_index_close", "industry_change_pct", "industry_pe", "industry_pb"]:
            if field in cleaned:
                cleaned[field] = self._safe_float(cleaned[field])
        return cleaned

    def _clean_valuation_percentile_data(self, vp: dict) -> dict:
        """清洗估值分位数据： 重新校验估值水平"""
        cleaned = dict(vp)
        pe_pct = self._safe_float(cleaned.get("pe_ttm_percentile"))
        if pe_pct is not None:
            if pe_pct <= 20:
                cleaned["valuation_level"] = "低估"
            elif pe_pct <= 50:
                cleaned["valuation_level"] = "合理"
            elif pe_pct <= 80:
                cleaned["valuation_level"] = "偏高"
            else:
                cleaned["valuation_level"] = "极高估"
        return cleaned

    def _clean_news(self, news: list[dict]) -> list[dict]:
        """清洗新闻数据： 去重、截断内容、限制条数"""
        if not news:
            return []

        seen_titles: set[str] = set()
        cleaned: list[dict] = []
        for item in news:
            title = item.get("title", "")
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            cleaned_item = dict(item)
            content = cleaned_item.get("content", "")
            if content and len(content) > 300:
                cleaned_item["content"] = content[:300] + "..."
            cleaned.append(cleaned_item)
        return cleaned[:30]

    def _clean_sentiment(self, sentiment: dict) -> dict:
        """清洗舆情数据： 重算情绪得分"""
        cleaned = dict(sentiment)
        pos = cleaned.get("positive_count", 0)
        neg = cleaned.get("negative_count", 0)
        neu = cleaned.get("neutral_count", 0)
        total = pos + neg + neu
        if total > 0:
            cleaned["sentiment_score"] = round((pos - neg) / total, 2)
        return cleaned