"""研究总协调Agent - 编排全流程: 数据采集→清洗→初筛→分析→报告→结论

职责:
1. 初始化ResearchState
2. 串联数据层(采集+清洗)
3. 运行初筛Agent判断是否继续
4. 并行运行6个分析Agent
5. 运行报告生成Agent和投资结论Agent
6. 返回完整ResearchReport
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from pydantic import ValidationError as PydanticValidationError

from investresearch.core.config import Config
from investresearch.core.logging import get_logger
from investresearch.core.models import (
    AgentInput,
    AgentOutput,
    AgentStatus,
    ResearchReport,
    InvestmentConclusion,
)
from investresearch.core.llm import LLMRouter, llm_router
from investresearch.data_layer.cache import FileCache
from investresearch.data_layer.collector import DataCollectorAgent
from investresearch.data_layer.cleaner import DataCleanerAgent
from investresearch.analysis_layer.screener import ScreenerAgent
from investresearch.analysis_layer.financial import FinancialAgent
from investresearch.analysis_layer.business_model import BusinessModelAgent
from investresearch.analysis_layer.industry import IndustryAgent
from investresearch.analysis_layer.governance import GovernanceAgent
from investresearch.analysis_layer.valuation import ValuationAgent
from investresearch.analysis_layer.risk import RiskAgent

from .report import ReportAgent
from .conclusion import ConclusionAgent

# 知识库为可选依赖，导入失败时优雅降级
try:
    from investresearch.knowledge_base.chroma_store import ChromaKnowledgeStore
    from investresearch.knowledge_base.watch_list import WatchListManager
    _KB_AVAILABLE = True
except ImportError:
    _KB_AVAILABLE = False

# 情报中心为可选依赖，导入失败时使用传统数据采集
try:
    from investresearch.intel_hub.service import IntelligenceHub
    _INTEL_HUB_AVAILABLE = True
except ImportError:
    _INTEL_HUB_AVAILABLE = False

logger = get_logger("coordinator")

# 深度模式对应的Agent集
DEPTH_AGENTS: dict[str, list[str]] = {
    "quick": ["financial", "valuation"],
    "standard": ["financial", "business_model", "industry", "governance", "valuation", "risk"],
    "deep": ["financial", "business_model", "industry", "governance", "valuation", "risk"],
}


class ResearchCoordinator:
    """研究总协调器

    编排数据采集→清洗→初筛→多Agent分析→报告→结论的完整流程。
    单个Agent失败不阻断整体，记录错误继续执行。
    """

    def __init__(
        self,
        progress_callback: Callable[[str, str], None] | None = None,
        use_intel_hub: bool | None = None,
    ) -> None:
        self.config = Config()
        self.llm: LLMRouter = llm_router
        self.progress_callback = progress_callback
        self.logger = get_logger("coordinator")

        # 决定数据采集方式: 情报中心 or 传统 DataCollectorAgent
        if use_intel_hub is None:
            use_intel_hub = self.config.get("intel_hub.enabled", False)
        self._use_intel_hub = use_intel_hub and _INTEL_HUB_AVAILABLE

        if self._use_intel_hub:
            self.logger.info("数据采集模式: 情报中心 (IntelligenceHub)")
        else:
            self.logger.info("数据采集模式: 传统 DataCollectorAgent")

    # ================================================================
    # 主入口
    # ================================================================

    async def run_research(
        self,
        stock_code: str,
        depth: str = "standard",
    ) -> ResearchReport:
        """执行完整研究流程

        Args:
            stock_code: 股票代码，如 300358
            depth: 研究深度 quick/standard/deep

        Returns:
            ResearchReport: 完整研究报告
        """
        self.logger.info(f"开始研究流程 | {stock_code} | depth={depth}")
        self._progress("init", f"初始化投研系统... | 标的: {stock_code}")

        # 检查知识库是否已有近期研究
        existing = self._check_knowledge_base(stock_code)
        if existing:
            self._progress("init", f"[知识库] 已有研究记录: {existing}")

        errors: list[str] = []
        agents_completed: list[str] = []
        agents_skipped: list[str] = []
        context: dict[str, Any] = {}
        stock_name = ""

        # ========================================
        # Phase 1: 数据采集
        # ========================================
        self._progress("data_collector", f"[数据采集] 采集 {stock_code} 数据...")

        if self._use_intel_hub:
            collector_output = await self._collect_via_intel_hub(stock_code)
        else:
            collector = DataCollectorAgent(cache=FileCache())
            collector_input = AgentInput(stock_code=stock_code, depth=depth)
            collector_output = await self._safe_run_agent(collector, collector_input)

        if collector_output.status == AgentStatus.SUCCESS:
            context["raw_data"] = collector_output.data
            stock_name = (
                collector_output.data.get("stock_info", {}).get("name", "")
                if isinstance(collector_output.data.get("stock_info"), dict)
                else ""
            )
            agents_completed.append("data_collector")
            coverage = collector_output.data.get("coverage_ratio", 0)
            self._progress("data_collector", f"[数据采集] 完成 | 覆盖率={coverage:.0%}")
        else:
            errors.append(f"数据采集失败: {collector_output.errors}")
            self._progress("data_collector", f"[数据采集] 失败 | {collector_output.errors}")
            return ResearchReport(
                stock_code=stock_code,
                stock_name=stock_name,
                depth=depth,
                errors=errors,
                agents_skipped=["全流程"],
            )

        # ========================================
        # Phase 2: 数据清洗
        # ========================================
        self._progress("data_cleaner", "[数据清洗] 清洗数据...")
        cleaner = DataCleanerAgent()
        cleaner_input = AgentInput(
            stock_code=stock_code,
            stock_name=stock_name,
            context={"raw_data": collector_output.data},
            depth=depth,
        )
        cleaner_output = await self._safe_run_agent(cleaner, cleaner_input)

        if cleaner_output.status == AgentStatus.SUCCESS:
            cleaned_data = cleaner_output.data.get("cleaned", {})
            # 附加采集状态信息，供分析Agent了解数据可用性
            cleaned_data["collection_status"] = collector_output.data.get("collection_status", {})
            cleaned_data["collection_errors"] = collector_output.data.get("errors", [])
            context["cleaned_data"] = cleaned_data
            agents_completed.append("data_cleaner")
            coverage = cleaned_data.get("coverage_ratio", 0)
            self._progress("data_cleaner", f"[数据清洗] 完成 | 覆盖率={coverage:.0%}")
        else:
            errors.append(f"数据清洗失败: {cleaner_output.errors}")
            self._progress("data_cleaner", f"[数据清洗] 失败 | {cleaner_output.errors}")
            return ResearchReport(
                stock_code=stock_code,
                stock_name=stock_name,
                depth=depth,
                errors=errors,
                agents_completed=agents_completed,
                agents_skipped=["全流程"],
            )

        # ========================================
        # Phase 3: 初筛
        # ========================================
        self._progress("screener", "[初筛] 执行快速排雷...")
        screener = ScreenerAgent()
        screener_input = AgentInput(
            stock_code=stock_code,
            stock_name=stock_name,
            context={"cleaned_data": cleaned_data},
            depth=depth,
        )
        screener_output = await self._safe_run_agent(screener, screener_input)

        if screener_output.status == AgentStatus.SUCCESS:
            screening_result = screener_output.data.get("screening", {})
            context["screening"] = screening_result
            agents_completed.append("screener")
            verdict = screening_result.get("verdict", "")
            self._progress("screener", f"[初筛] 结论: {verdict}")

            # 刚性剔除 → 提前终止
            if verdict == "刚性剔除":
                self.logger.info(f"初筛刚性剔除，终止研究 | {stock_code}")
                return ResearchReport(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    depth=depth,
                    markdown=self._build_early_stop_report(
                        stock_code, stock_name, screening_result
                    ),
                    agents_completed=agents_completed,
                    agents_skipped=["分析层", "报告", "结论"],
                    errors=errors,
                )
        else:
            errors.append(f"初筛失败: {screener_output.errors}")
            agents_skipped.append("screener")
            self._progress("screener", f"[初筛] 失败，跳过")

        # ========================================
        # Phase 4: 分析层 (并行)
        # ========================================
        agent_map: dict[str, type] = {
            "financial": FinancialAgent,
            "business_model": BusinessModelAgent,
            "industry": IndustryAgent,
            "governance": GovernanceAgent,
            "valuation": ValuationAgent,
            "risk": RiskAgent,
        }

        active_agents = DEPTH_AGENTS.get(depth, DEPTH_AGENTS["standard"])
        self._progress(
            "analysis", f"[分析层] {len(active_agents)}个Agent并行执行..."
        )

        # 构建各Agent的只读context（并发中不修改）
        analysis_readonly_context = dict(context)

        async def run_analysis_agent(
            name: str, agent_cls: type
        ) -> tuple[str, AgentOutput]:
            agent = agent_cls()
            agent_input = AgentInput(
                stock_code=stock_code,
                stock_name=stock_name,
                context=analysis_readonly_context,
                depth=depth,
            )
            output = await self._safe_run_agent(agent, agent_input)
            return name, output

        tasks = [
            run_analysis_agent(name, agent_map[name])
            for name in active_agents
            if name in agent_map
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # gather完成后，顺序更新context（避免并发写入问题）
        analysis_context = dict(context)
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"分析Agent异常: {result}", exc_info=True)
                errors.append(f"分析Agent异常: {result}")
                continue

            name, output = result
            if output.status == AgentStatus.SUCCESS:
                context_key = f"{name}_analysis"
                result_data = output.data.get(name, output.data)
                context[context_key] = result_data
                analysis_context[context_key] = result_data
                agents_completed.append(name)

                summary = output.summary or "完成"
                self._progress(name, f"[{name}] {summary}")
            else:
                agents_skipped.append(name)
                errors.append(f"{name}失败: {output.errors}")
                self._progress(name, f"[{name}] 失败 | {output.errors}")

        # ========================================
        # Phase 5: 报告生成
        # ========================================
        self._progress("report", "[报告生成] 生成深度研究报告...")
        report_agent = ReportAgent()
        report_input = AgentInput(
            stock_code=stock_code,
            stock_name=stock_name,
            context=analysis_context,
            depth=depth,
        )
        report_output = await self._safe_run_agent(report_agent, report_input)

        markdown = ""
        if report_output.status == AgentStatus.SUCCESS:
            markdown = report_output.data.get("markdown", report_output.summary or "")
            agents_completed.append("report")
            self._progress("report", "[报告生成] 深度报告生成完成")
        else:
            errors.append(f"报告生成失败: {report_output.errors}")
            agents_skipped.append("report")
            self._progress("report", f"[报告生成] 失败 | {report_output.errors}")

        # ========================================
        # Phase 6: 投资结论
        # ========================================
        self._progress("conclusion", "[投资结论] 生成投资结论...")
        conclusion_agent = ConclusionAgent()
        conclusion_input = AgentInput(
            stock_code=stock_code,
            stock_name=stock_name,
            context=analysis_context,
            depth=depth,
        )
        conclusion_output = await self._safe_run_agent(conclusion_agent, conclusion_input)

        conclusion: InvestmentConclusion | None = None
        if conclusion_output.status == AgentStatus.SUCCESS:
            try:
                conclusion_data = conclusion_output.data.get("conclusion", {})
                conclusion = InvestmentConclusion(**conclusion_data)
                agents_completed.append("conclusion")
                rec = conclusion.recommendation
                target = ""
                if conclusion.target_price_low and conclusion.target_price_high:
                    target = f" | 目标区间: {conclusion.target_price_low}-{conclusion.target_price_high}"
                self._progress("conclusion", f"[投资结论] 结论: {rec}{target}")
            except PydanticValidationError as e:
                self.logger.error(f"结论数据格式异常（LLM返回不符合预期）: {e}")
                errors.append(f"结论数据格式异常: {e}")
                agents_skipped.append("conclusion")
            except Exception as e:
                self.logger.error(f"结论解析失败: {e}")
                errors.append(f"结论解析失败: {e}")
                agents_skipped.append("conclusion")
        else:
            errors.append(f"投资结论失败: {conclusion_output.errors}")
            agents_skipped.append("conclusion")
            self._progress("conclusion", f"[投资结论] 失败 | {conclusion_output.errors}")

        # ========================================
        # 返回
        # ========================================

        report = ResearchReport(
            stock_code=stock_code,
            stock_name=stock_name,
            depth=depth,
            markdown=markdown,
            conclusion=conclusion,
            agents_completed=agents_completed,
            agents_skipped=agents_skipped,
            errors=errors,
        )

        # 研究结果存入知识库（失败不阻塞主流程）
        self._save_to_knowledge_base(report, context)

        self._progress("done", "✓ 研究完成，报告已保存")
        return report

    # ================================================================
    # 内部方法
    # ================================================================

    async def _safe_run_agent(
        self, agent: Any, input_data: AgentInput
    ) -> AgentOutput:
        """安全运行单个Agent，捕获所有异常"""
        try:
            return await agent.safe_run(input_data)
        except Exception as e:
            self.logger.error(f"Agent[{getattr(agent, 'agent_name', '?')}] 异常: {e}")
            return AgentOutput(
                agent_name=getattr(agent, "agent_name", "unknown"),
                status=AgentStatus.FAILED,
                errors=[str(e)],
            )

    async def _collect_via_intel_hub(self, stock_code: str) -> AgentOutput:
        """通过情报中心采集数据，转换为 AgentOutput 格式

        同步的 IntelligenceHub 调用在线程池中执行，避免阻塞事件循环。
        使用 try/finally 确保 hub 资源始终释放。
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self._sync_collect_via_intel_hub,
            stock_code,
        )

    def _sync_collect_via_intel_hub(self, stock_code: str) -> AgentOutput:
        """同步执行情报中心采集（在线程池中调用）"""
        hub = IntelligenceHub()
        try:
            hub.initialize()
            results = hub.collect_stock(stock_code)

            # 将采集结果转换为 CollectorOutput 兼容格式
            collected_data: dict[str, Any] = {}
            collection_status: dict[str, str] = {}
            collection_errors: list[str] = []
            success_count = 0

            for r in results:
                if r.status in ("success", "partial"):
                    collected_data[r.data_type] = r.data
                    collection_status[r.data_type] = r.status
                    success_count += 1
                else:
                    collection_status[r.data_type] = "failed"
                    if r.error:
                        collection_errors.append(f"{r.data_type}: {r.error}")

            total = len(results)
            coverage_ratio = success_count / total if total > 0 else 0.0

            return AgentOutput(
                agent_name="intel_hub_collector",
                status=AgentStatus.SUCCESS if success_count > 0 else AgentStatus.FAILED,
                data={
                    **collected_data,
                    "collection_status": collection_status,
                    "coverage_ratio": coverage_ratio,
                    "errors": collection_errors,
                },
                data_sources=list({r.source_name for r in results}),
                confidence=coverage_ratio,
                summary=f"情报中心采集{total}类数据，成功{success_count}，覆盖率{coverage_ratio:.0%}",
            )
        except Exception as e:
            self.logger.error(f"情报中心采集异常: {e}")
            return AgentOutput(
                agent_name="intel_hub_collector",
                status=AgentStatus.FAILED,
                errors=[str(e)],
            )
        finally:
            hub.close()

    def _progress(self, step: str, message: str) -> None:
        """报告进度"""
        self.logger.info(message)
        if self.progress_callback:
            self.progress_callback(step, message)

    # ================================================================
    # 知识库集成
    # ================================================================

    def _check_knowledge_base(self, stock_code: str) -> str | None:
        """检查知识库是否已有该标的的近期研究"""
        if not _KB_AVAILABLE:
            return None
        try:
            store = ChromaKnowledgeStore()
            latest = store.get_latest_research(stock_code)
            if latest:
                return f"深度={latest.depth} 建议={latest.recommendation or 'N/A'} 日期={latest.research_date.strftime('%Y-%m-%d')}"
        except Exception as e:
            self.logger.warning(f"知识库检查失败（不影响主流程）: {e}")
        return None

    def _save_to_knowledge_base(
        self, report: ResearchReport, context: dict[str, Any]
    ) -> None:
        """将研究结果存入知识库（可选，失败不影响主流程）"""
        if not _KB_AVAILABLE:
            self.logger.debug("知识库模块不可用，跳过存储")
            return

        auto_save = self.config.get("knowledge_base.auto_save", True)
        if not auto_save:
            self.logger.debug("知识库自动保存已禁用")
            return

        try:
            store = ChromaKnowledgeStore()
            store.save_research(
                stock_code=report.stock_code,
                stock_name=report.stock_name,
                report=report,
                conclusion=report.conclusion,
            )
            self._progress("knowledge_base", f"[知识库] 已存入知识库 | {report.stock_code}")
        except Exception as e:
            self.logger.warning(f"知识库存储失败（不影响主流程）: {e}")

    @staticmethod
    def _build_early_stop_report(
        stock_code: str, stock_name: str, screening: dict
    ) -> str:
        """初筛不通过时的简要报告"""
        verdict = screening.get("verdict", "未知")
        checks = screening.get("checks", [])
        recommendation = screening.get("recommendation", "")

        lines = [
            f"# {stock_code} {stock_name} 研究报告（初筛未通过）\n",
            f"## 初筛结论: {verdict}\n",
            "### 检查结果\n",
        ]
        for check in checks:
            if isinstance(check, dict):
                status = check.get("status", "?")
                item = check.get("item", "")
                detail = check.get("detail", "")
                lines.append(f"- **[{status}]** {item}: {detail}")

        lines.append(f"\n### 建议\n{recommendation}")
        return "\n".join(lines)
