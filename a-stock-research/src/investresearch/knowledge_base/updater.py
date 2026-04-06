"""Incremental updater agent."""

from __future__ import annotations

import time
from datetime import date, datetime
from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import AgentInput, AgentOutput, AgentStatus, CollectorOutput

from .chroma_store import ChromaKnowledgeStore

logger = get_logger("agent.incremental_updater")


class IncrementalUpdaterAgent(AgentBase[AgentInput, AgentOutput]):
    """Fetch data created after the last saved research result."""

    agent_name: str = "incremental_updater"

    def __init__(self, knowledge_store: ChromaKnowledgeStore | None = None) -> None:
        super().__init__()
        self._store = knowledge_store

    @property
    def store(self) -> ChromaKnowledgeStore:
        if self._store is None:
            self._store = ChromaKnowledgeStore()
        return self._store

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """Run an incremental refresh."""
        stock_code = input_data.stock_code
        start_time = time.time()

        self.logger.info(f"开始增量更新 | {stock_code}")

        last_collected = self.store.get_last_collected_at(stock_code)
        if last_collected is None:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=[f"未找到 {stock_code} 的历史研究记录，请先执行全量研究"],
                summary="需要先执行全量研究",
            )

        since_date = last_collected.date() if isinstance(last_collected, datetime) else last_collected
        today = date.today()

        if since_date >= today:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.SUCCESS,
                data={
                    "update_type": "incremental",
                    "since": since_date.isoformat(),
                    "changes": {},
                    "duration_seconds": 0.0,
                    "message": "数据已是最新",
                },
                summary="数据已是最新，无需更新",
            )

        self.logger.info(f"增量时间范围: {since_date} -> {today}")

        changes: dict[str, Any] = {}
        errors: list[str] = []

        try:
            new_prices = self._fetch_incremental_prices(stock_code, since_date)
            changes["new_prices"] = new_prices
            self.logger.info(f"新增行情数据: {new_prices} 条")
        except Exception as exc:
            errors.append(f"行情增量失败: {exc}")
            self.logger.warning(f"行情增量失败: {exc}")

        try:
            new_financials = self._fetch_incremental_financials(stock_code)
            changes["new_financials"] = new_financials
            self.logger.info(f"新增财报数据: {new_financials} 期")
        except Exception as exc:
            errors.append(f"财报增量失败: {exc}")
            self.logger.warning(f"财报增量失败: {exc}")

        try:
            new_valuation = self._fetch_incremental_valuation(stock_code, since_date)
            changes["new_valuation"] = new_valuation
            self.logger.info(f"新增估值数据: {new_valuation} 条")
        except Exception as exc:
            errors.append(f"估值增量失败: {exc}")
            self.logger.warning(f"估值增量失败: {exc}")

        extra_fetchers = {
            "new_announcements": lambda: self._fetch_incremental_announcements(stock_code, since_date),
            "new_policy_documents": lambda: self._fetch_incremental_policy_documents(stock_code, since_date),
            "new_news": lambda: self._fetch_incremental_news(stock_code, since_date),
            "new_research_reports": lambda: self._fetch_incremental_research_reports(stock_code, since_date),
            "new_governance_events": lambda: self._fetch_incremental_governance(stock_code),
            "new_shareholder_changes": lambda: self._fetch_incremental_shareholders(stock_code),
            "new_industry_signals": lambda: self._fetch_incremental_industry_signals(stock_code),
            "new_compliance_events": lambda: self._fetch_incremental_compliance_events(stock_code, since_date),
            "new_patents": lambda: self._fetch_incremental_patents(stock_code, since_date),
        }
        for change_key, fetcher in extra_fetchers.items():
            try:
                value = fetcher()
                changes[change_key] = value
                self.logger.info(f"{change_key}: {value}")
            except Exception as exc:
                errors.append(f"{change_key}失败: {exc}")
                self.logger.warning(f"{change_key}失败: {exc}")

        duration = time.time() - start_time
        self.logger.info(f"增量更新完成 | 耗时: {duration:.1f}s")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={
                "update_type": "incremental",
                "since": since_date.isoformat(),
                "changes": changes,
                "duration_seconds": duration,
            },
            errors=errors,
            summary=self._build_update_summary(changes, duration),
        )

    def validate_output(self, output: AgentOutput) -> None:
        """Validate updater output."""
        if output.status == AgentStatus.FAILED:
            return

        data = output.data
        if "update_type" not in data:
            from investresearch.core.exceptions import AgentValidationError

            raise AgentValidationError(self.agent_name, ["缺少 update_type 字段"])

    def _fetch_incremental_prices(self, stock_code: str, since: date) -> int:
        """Fetch incremental daily prices."""
        try:
            import time as sleep_time

            import akshare as ak

            from investresearch.data_layer.collector import MIN_REQUEST_INTERVAL

            end_date = date.today().strftime("%Y%m%d")
            start_date = since.strftime("%Y%m%d")

            sleep_time.sleep(MIN_REQUEST_INTERVAL)
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            return len(df) if df is not None and not df.empty else 0
        except Exception as exc:
            self.logger.warning(f"行情增量采集失败: {exc}")
            return 0

    def _fetch_incremental_financials(self, stock_code: str) -> int:
        """Check whether new financial statements are available."""
        try:
            import time as sleep_time

            import akshare as ak

            from investresearch.data_layer.collector import MIN_REQUEST_INTERVAL

            sleep_time.sleep(MIN_REQUEST_INTERVAL)
            df = ak.stock_financial_abstract_ths(symbol=stock_code)
            if df is None or df.empty:
                return 0

            return min(len(df), 2)
        except Exception as exc:
            self.logger.warning(f"财报增量检查失败: {exc}")
            return 0

    def _fetch_incremental_valuation(self, stock_code: str, since: date) -> int:
        """Fetch incremental valuation data from BaoStock."""
        try:
            import baostock as bs

            from investresearch.data_layer.collector import DataCollectorAgent

            bs_code = DataCollectorAgent._to_baostock_code(stock_code)
            if not bs_code:
                return 0

            lg = bs.login()
            if lg.error_code != "0":
                return 0

            try:
                end_date = date.today().strftime("%Y-%m-%d")
                start_date = since.strftime("%Y-%m-%d")

                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,peTTM,pbMRQ",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="m",
                )

                count = 0
                while rs.error_code == "0" and rs.next():
                    count += 1
                return count
            finally:
                bs.logout()
        except Exception as exc:
            self.logger.warning(f"估值增量采集失败: {exc}")
            return 0

    def _build_update_summary(self, changes: dict[str, Any], duration: float) -> str:
        """Build a short human-readable update summary."""
        parts = []
        label_map = {
            "new_prices": "行情",
            "new_financials": "财报",
            "new_valuation": "估值",
            "new_announcements": "公告",
            "new_policy_documents": "政策",
            "new_news": "新闻",
            "new_research_reports": "研报",
            "new_governance_events": "治理事件",
            "new_shareholder_changes": "股东变化",
            "new_industry_signals": "行业信号",
        }

        label_map["new_compliance_events"] = "瀹樻柟鍚堣浜嬩欢"
        label_map["new_patents"] = "瀹樻柟涓撳埄"

        for key, value in changes.items():
            if isinstance(value, int) and value > 0:
                parts.append(f"新增{label_map.get(key, key)}{value}条")

        if not parts:
            return f"数据已是最新，无需更新 | 耗时: {duration:.1f}s"
        return f"{', '.join(parts)} | 耗时: {duration:.1f}s"

    def _make_collector(self) -> Any:
        from investresearch.data_layer.collector import DataCollectorAgent
        return DataCollectorAgent()

    def _make_seeded_result(self, stock_code: str) -> tuple[Any, CollectorOutput]:
        collector = self._make_collector()
        result = CollectorOutput()
        collector._get_stock_info(stock_code, result)
        return collector, result

    def _count_since(self, items: list[Any], since: date, date_fields: list[str]) -> int:
        count = 0
        for item in items:
            payload = item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            if not isinstance(payload, dict):
                continue
            value = None
            for field in date_fields:
                if payload.get(field):
                    value = str(payload.get(field))
                    break
            if not value:
                continue
            try:
                item_date = datetime.fromisoformat(value[:10]).date()
            except ValueError:
                continue
            if item_date > since:
                count += 1
        return count

    def _fetch_incremental_announcements(self, stock_code: str, since: date) -> int:
        collector = self._make_collector()
        result = CollectorOutput()
        collector._get_announcements(stock_code, result)
        return self._count_since(result.announcements, since, ["announcement_date"])

    def _fetch_incremental_policy_documents(self, stock_code: str, since: date) -> int:
        collector = self._make_collector()
        result = CollectorOutput()
        collector._get_policy_documents(stock_code, result)
        return self._count_since(result.policy_documents, since, ["policy_date"])

    def _fetch_incremental_news(self, stock_code: str, since: date) -> int:
        collector = self._make_collector()
        result = CollectorOutput()
        collector._get_news(stock_code, result)
        return self._count_since(result.news, since, ["publish_time"])

    def _fetch_incremental_research_reports(self, stock_code: str, since: date) -> int:
        collector = self._make_collector()
        result = CollectorOutput()
        collector._get_research_reports(stock_code, result)
        return self._count_since(result.research_reports, since, ["publish_date"])

    def _fetch_incremental_governance(self, stock_code: str) -> int:
        collector, result = self._make_seeded_result(stock_code)
        collector._get_compliance_events(stock_code, result)
        collector._get_governance_data(stock_code, result)
        if not result.governance:
            return 0
        data = result.governance.model_dump(mode="json")
        return sum(
            1
            for key in [
                "equity_pledge_ratio",
                "related_transaction",
                "guarantee_info",
                "lawsuit_info",
                "management_changes",
                "dividend_history",
                "buyback_history",
                "refinancing_history",
            ]
            if data.get(key)
        )

    def _fetch_incremental_compliance_events(self, stock_code: str, since: date) -> int:
        collector, result = self._make_seeded_result(stock_code)
        collector._get_compliance_events(stock_code, result)
        return self._count_since(result.compliance_events, since, ["publish_date"])

    def _fetch_incremental_shareholders(self, stock_code: str) -> int:
        collector = self._make_collector()
        result = CollectorOutput()
        collector._get_shareholder_data(stock_code, result)
        if not result.shareholders:
            return 0
        data = result.shareholders.model_dump(mode="json")
        return len(data.get("top_shareholders", [])) + (1 if data.get("shareholder_count") is not None else 0)

    def _fetch_incremental_industry_signals(self, stock_code: str) -> int:
        collector = self._make_collector()
        result = CollectorOutput()
        collector._get_industry_enhanced(stock_code, result)
        if not result.industry_enhanced:
            return 0
        data = result.industry_enhanced.model_dump(mode="json")
        return len(data.get("data_points", [])) + (1 if data.get("industry_pe") is not None else 0)

    def _fetch_incremental_patents(self, stock_code: str, since: date) -> int:
        collector, result = self._make_seeded_result(stock_code)
        collector._get_patents(stock_code, result)
        return self._count_since(result.patents, since, ["publish_date"])
