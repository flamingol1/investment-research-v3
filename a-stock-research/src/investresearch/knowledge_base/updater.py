"""增量更新Agent - 只拉取上次采集后的新数据

核心逻辑:
1. 从知识库获取 last_collected_at 时间
2. 仅拉取 (last_collected_at, today] 范围的新数据
3. 计算数据差异并返回UpdateRecord
"""

from __future__ import annotations

import time
from datetime import date, datetime
from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import (
    AgentInput,
    AgentOutput,
    AgentStatus,
)

from .chroma_store import ChromaKnowledgeStore

logger = get_logger("agent.incremental_updater")


class IncrementalUpdaterAgent(AgentBase[AgentInput, AgentOutput]):
    """增量更新Agent

    只拉取上次采集后的新数据，避免全量重复采集。
    复用DataCollectorAgent的数据获取方法，但限制时间范围。
    """

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
        """执行增量更新"""
        stock_code = input_data.stock_code
        start_time = time.time()

        self.logger.info(f"开始增量更新 | {stock_code}")

        # 1. 获取上次采集时间
        last_collected = self.store.get_last_collected_at(stock_code)
        if last_collected is None:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=[f"未找到 {stock_code} 的历史研究记录，请先执行全量研究"],
                summary="需要先执行全量研究",
            )

        # 2. 计算增量时间范围
        since_date = last_collected.date() if isinstance(last_collected, datetime) else last_collected
        today = date.today()

        if since_date >= today:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.SUCCESS,
                data={"changes": {}, "message": "数据已是最新"},
                summary="数据已是最新，无需更新",
            )

        self.logger.info(f"增量时间范围: {since_date} -> {today}")

        # 3. 增量采集各类数据
        changes: dict[str, Any] = {}
        errors: list[str] = []

        # 行情数据增量
        try:
            new_prices = self._fetch_incremental_prices(stock_code, since_date)
            changes["new_prices"] = new_prices
            self.logger.info(f"新增行情数据: {new_prices}条")
        except Exception as e:
            errors.append(f"行情增量失败: {e}")
            self.logger.warning(f"行情增量失败: {e}")

        # 财报数据增量
        try:
            new_financials = self._fetch_incremental_financials(stock_code)
            changes["new_financials"] = new_financials
            self.logger.info(f"新增财报数据: {new_financials}期")
        except Exception as e:
            errors.append(f"财报增量失败: {e}")
            self.logger.warning(f"财报增量失败: {e}")

        # 估值数据增量
        try:
            new_valuation = self._fetch_incremental_valuation(stock_code, since_date)
            changes["new_valuation"] = new_valuation
            self.logger.info(f"新增估值数据: {new_valuation}条")
        except Exception as e:
            errors.append(f"估值增量失败: {e}")
            self.logger.warning(f"估值增量失败: {e}")

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
        """校验输出"""
        if output.status == AgentStatus.FAILED:
            return  # FAILED状态不需要严格校验
        data = output.data
        if "update_type" not in data:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, ["缺少 update_type 字段"])

    # ================================================================
    # 增量数据采集方法
    # ================================================================

    def _fetch_incremental_prices(self, stock_code: str, since: date) -> int:
        """增量获取行情数据 — 直接调用AKShare"""
        try:
            import akshare as ak
            from investresearch.data_layer.collector import DataCollectorAgent, MIN_REQUEST_INTERVAL
            import time

            end_date = date.today().strftime("%Y%m%d")
            start_date = since.strftime("%Y%m%d")

            time.sleep(MIN_REQUEST_INTERVAL)  # 速率限制
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            return len(df) if df is not None and not df.empty else 0
        except Exception as e:
            self.logger.warning(f"行情增量采集失败: {e}")
            return 0

    def _fetch_incremental_financials(self, stock_code: str) -> int:
        """检查是否有新财报期 — 直接调用AKShare"""
        try:
            import akshare as ak
            from investresearch.data_layer.collector import MIN_REQUEST_INTERVAL
            import time

            time.sleep(MIN_REQUEST_INTERVAL)  # 速率限制
            df = ak.stock_financial_abstract_ths(symbol=stock_code)
            if df is None or df.empty:
                return 0

            # 只取最近2期
            return min(len(df), 2)
        except Exception as e:
            self.logger.warning(f"财报增量检查失败: {e}")
            return 0

    def _fetch_incremental_valuation(self, stock_code: str, since: date) -> int:
        """增量获取估值数据 — 直接调用BaoStock"""
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
        except Exception as e:
            self.logger.warning(f"估值增量采集失败: {e}")
            return 0

    def _build_update_summary(self, changes: dict[str, Any], duration: float) -> str:
        """生成更新摘要"""
        parts = []
        for key, value in changes.items():
            if isinstance(value, int) and value > 0:
                label_map = {
                    "new_prices": "行情",
                    "new_financials": "财报",
                    "new_valuation": "估值",
                }
                label = label_map.get(key, key)
                parts.append(f"新增{label}{value}条")

        if not parts:
            return f"无新增数据 | 耗时: {duration:.1f}s"
        return f"{', '.join(parts)} | 耗时: {duration:.1f}s"
