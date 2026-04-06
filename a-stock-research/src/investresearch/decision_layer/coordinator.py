"""Research coordinator."""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from pydantic import ValidationError as PydanticValidationError

from investresearch.analysis_layer.business_model import BusinessModelAgent
from investresearch.analysis_layer.financial import FinancialAgent
from investresearch.analysis_layer.governance import GovernanceAgent
from investresearch.analysis_layer.industry import IndustryAgent
from investresearch.analysis_layer.risk import RiskAgent
from investresearch.analysis_layer.screener import ScreenerAgent
from investresearch.analysis_layer.valuation import ValuationAgent
from investresearch.core.config import Config
from investresearch.core.llm import LLMRouter, llm_router
from investresearch.core.logging import get_logger
from investresearch.core.models import (
    AgentInput,
    AgentOutput,
    AgentStatus,
    InvestmentConclusion,
    ResearchReport,
)
from investresearch.data_layer.cache import FileCache
from investresearch.data_layer.cleaner import DataCleanerAgent
from investresearch.data_layer.collector import DataCollectorAgent

from .conclusion import ConclusionAgent
from .report import ReportAgent

try:
    from investresearch.knowledge_base.chroma_store import ChromaKnowledgeStore

    _KB_AVAILABLE = True
except ImportError:
    _KB_AVAILABLE = False

try:
    from investresearch.intel_hub.service import IntelligenceHub

    _INTEL_HUB_AVAILABLE = True
except ImportError:
    _INTEL_HUB_AVAILABLE = False

logger = get_logger("coordinator")

DEPTH_AGENTS: dict[str, list[str]] = {
    "quick": ["financial", "valuation"],
    "standard": ["financial", "business_model", "industry", "governance", "valuation", "risk"],
    "deep": ["financial", "business_model", "industry", "governance", "valuation", "risk"],
}

AGENT_LABELS: dict[str, str] = {
    "init": "初始化",
    "data_collector": "数据采集",
    "data_cleaner": "数据清洗",
    "screener": "初筛检查",
    "analysis": "并行分析",
    "financial": "财务分析",
    "business_model": "商业模式",
    "industry": "行业分析",
    "governance": "治理分析",
    "valuation": "估值分析",
    "risk": "风险分析",
    "report": "报告生成",
    "conclusion": "投资结论",
    "knowledge_base": "知识库归档",
    "done": "完成",
    "error": "异常",
}


class ResearchCoordinator:
    """Coordinate the full research pipeline."""

    def __init__(
        self,
        progress_callback: Callable[..., None] | None = None,
        use_intel_hub: bool | None = None,
    ) -> None:
        self.config = Config()
        self.llm: LLMRouter = llm_router
        self.progress_callback = progress_callback
        self.logger = get_logger("coordinator")

        if use_intel_hub is None:
            use_intel_hub = self.config.get("intel_hub.enabled", False)
        self._use_intel_hub = bool(use_intel_hub and _INTEL_HUB_AVAILABLE)

        if self._use_intel_hub:
            self.logger.info("数据采集模式: 情报中心 (IntelligenceHub)")
        else:
            self.logger.info("数据采集模式: 传统 DataCollectorAgent")

    @staticmethod
    def _metric(
        key: str,
        label: str,
        value: Any,
        tone: str = "default",
    ) -> dict[str, str]:
        return {
            "key": key,
            "label": label,
            "value": str(value),
            "tone": tone,
        }

    @staticmethod
    def _agent_label(name: str) -> str:
        return AGENT_LABELS.get(name, name.replace("_", " "))

    @staticmethod
    def _format_count(items: Any, suffix: str = "条") -> str:
        if isinstance(items, (list, tuple, set, dict)):
            return f"{len(items)}{suffix}"
        if items is None:
            return f"0{suffix}"
        return f"{items}{suffix}"

    @staticmethod
    def _format_percent(value: Any) -> str:
        try:
            return f"{float(value):.0%}"
        except (TypeError, ValueError):
            return "-"

    @staticmethod
    def _format_number(value: Any, digits: int = 2) -> str:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "--"
        if numeric.is_integer():
            return str(int(numeric))
        return f"{numeric:.{digits}f}"

    def _build_collection_data_summary(
        self,
        collected: dict[str, Any],
        stock_name: str,
    ) -> list[dict[str, str]]:
        realtime = collected.get("realtime") or {}
        latest_price = realtime.get("close") if isinstance(realtime, dict) else None
        return [
            self._metric("stock_name", "股票名称", stock_name or "--", "info"),
            self._metric("prices", "行情K线", self._format_count(collected.get("prices"), "条"), "info"),
            self._metric("financials", "财报期数", self._format_count(collected.get("financials"), "期"), "info"),
            self._metric("announcements", "公告", self._format_count(collected.get("announcements"), "条"), "success"),
            self._metric("policy_documents", "政策", self._format_count(collected.get("policy_documents"), "条"), "success"),
            self._metric("news", "新闻", self._format_count(collected.get("news"), "条"), "success"),
            self._metric("research_reports", "研报", self._format_count(collected.get("research_reports"), "篇"), "warning"),
            self._metric("latest_price", "最新价", self._format_number(latest_price), "info"),
            self._metric("coverage", "采集覆盖率", self._format_percent(collected.get("coverage_ratio")), "success"),
        ]

    def _build_collection_detail(
        self,
        collected: dict[str, Any],
        stock_name: str,
        agents_completed: list[str],
    ) -> dict[str, Any]:
        return {
            "headline": f"已完成 {self._agent_label('data_collector')}",
            "note": f"{stock_name or '目标标的'} 的核心原始数据已经拉取完成，正在进入下一阶段。",
            "metrics": [
                self._metric("coverage", "覆盖率", self._format_percent(collected.get("coverage_ratio")), "success"),
                self._metric("prices", "K线", self._format_count(collected.get("prices"), "条"), "info"),
                self._metric("financials", "财报", self._format_count(collected.get("financials"), "期"), "info"),
                self._metric("policies", "政策", self._format_count(collected.get("policy_documents"), "条"), "success"),
                self._metric("news", "新闻", self._format_count(collected.get("news"), "条"), "success"),
            ],
            "bullets": [str(item) for item in (collected.get("errors") or [])[:3]],
            "data_summary": self._build_collection_data_summary(collected, stock_name),
            "completed_agents": agents_completed,
            "current_agent": "data_collector",
        }

    def _build_cleaner_detail(
        self,
        cleaned_data: dict[str, Any],
        warnings: list[str],
        agents_completed: list[str],
    ) -> dict[str, Any]:
        return {
            "headline": f"已完成 {self._agent_label('data_cleaner')}",
            "note": "清洗后的结构化数据已经准备好，可供后续初筛与分析 Agent 直接使用。",
            "metrics": [
                self._metric("coverage", "清洗覆盖率", self._format_percent(cleaned_data.get("coverage_ratio")), "success"),
                self._metric("warnings", "提示数", len(warnings), "warning" if warnings else "success"),
                self._metric("prices", "有效行情", self._format_count(cleaned_data.get("prices"), "条"), "info"),
                self._metric("financials", "有效财报", self._format_count(cleaned_data.get("financials"), "期"), "info"),
                self._metric("policies", "政策原文", self._format_count(cleaned_data.get("policy_documents"), "条"), "success"),
            ],
            "bullets": [str(item) for item in warnings[:4]],
            "completed_agents": agents_completed,
            "current_agent": "data_cleaner",
        }

    def _build_analysis_start_detail(
        self,
        active_agents: list[str],
        agents_completed: list[str],
    ) -> dict[str, Any]:
        return {
            "headline": f"正在并行执行 {len(active_agents)} 个分析 Agent",
            "note": "后台会并发完成财务、商业模式、行业、治理、估值和风险分析。",
            "metrics": [
                self._metric("parallel_agents", "并行数", len(active_agents), "info"),
                self._metric("modules", "分析模块", " / ".join(self._agent_label(name) for name in active_agents), "info"),
            ],
            "bullets": [self._agent_label(name) for name in active_agents],
            "active_agents": active_agents,
            "completed_agents": agents_completed,
            "current_agent": "analysis",
        }

    def _build_analysis_result_detail(
        self,
        agent_name: str,
        summary: str,
        active_agents: list[str],
        agents_completed: list[str],
        *,
        failed: bool = False,
    ) -> dict[str, Any]:
        return {
            "headline": f"{self._agent_label(agent_name)}{'失败' if failed else '已完成'}",
            "note": summary,
            "metrics": [
                self._metric("completed", "已完成模块", len(agents_completed), "success"),
                self._metric("remaining", "剩余并行模块", len(active_agents), "warning" if active_agents else "success"),
            ],
            "bullets": [self._agent_label(name) for name in active_agents[:4]],
            "active_agents": active_agents,
            "completed_agents": agents_completed,
            "current_agent": "analysis",
        }

    def _build_conclusion_detail(
        self,
        conclusion: InvestmentConclusion,
        agents_completed: list[str],
    ) -> dict[str, Any]:
        target_range = "--"
        if conclusion.target_price_low and conclusion.target_price_high:
            target_range = f"{conclusion.target_price_low}-{conclusion.target_price_high}"
        return {
            "headline": "投资结论已生成",
            "note": conclusion.conclusion_summary,
            "metrics": [
                self._metric("recommendation", "建议", conclusion.recommendation, "success"),
                self._metric("risk_level", "风险", conclusion.risk_level, "warning"),
                self._metric("target_range", "目标区间", target_range, "info"),
            ],
            "bullets": conclusion.monitoring_points[:3],
            "completed_agents": agents_completed,
            "current_agent": "conclusion",
        }

    async def run_research(
        self,
        stock_code: str,
        depth: str = "standard",
    ) -> ResearchReport:
        self.logger.info(f"开始研究流程 | {stock_code} | depth={depth}")
        self._progress(
            "init",
            f"初始化投研系统 | 标的: {stock_code}",
            detail={
                "headline": "研究任务已创建",
                "note": f"准备拉取 {stock_code} 的基础数据并启动完整研究链路。",
                "metrics": [
                    self._metric("stock_code", "股票代码", stock_code, "info"),
                    self._metric("depth", "研究深度", depth, "info"),
                ],
                "current_agent": "init",
            },
        )

        existing = self._check_knowledge_base(stock_code)
        if existing:
            self._progress(
                "init",
                f"[知识库] 已有研究记录: {existing}",
                detail={
                    "headline": "发现历史研究记录",
                    "note": "后台会继续执行新一轮研究，并在完成后写回知识库。",
                    "bullets": [existing],
                    "current_agent": "init",
                },
            )

        errors: list[str] = []
        agents_completed: list[str] = []
        agents_skipped: list[str] = []
        context: dict[str, Any] = {}
        stock_name = ""

        self._progress(
            "data_collector",
            f"[数据采集] 正在采集 {stock_code} 数据...",
            detail={
                "headline": "正在采集基础数据",
                "note": "会拉取行情、财报、公告原文、政策原文、新闻、研报等多源信息。",
                "metrics": [
                    self._metric("target", "标的", stock_code, "info"),
                    self._metric("depth", "深度", depth, "info"),
                ],
                "current_agent": "data_collector",
            },
        )
        if self._use_intel_hub:
            collector_output = await self._collect_via_intel_hub(stock_code)
        else:
            collector = DataCollectorAgent(cache=FileCache())
            collector_output = await self._safe_run_agent(
                collector,
                AgentInput(stock_code=stock_code, depth=depth),
            )

        if collector_output.status == AgentStatus.SUCCESS:
            context["raw_data"] = collector_output.data
            stock_name = (
                collector_output.data.get("stock_info", {}).get("name", "")
                if isinstance(collector_output.data.get("stock_info"), dict)
                else ""
            )
            agents_completed.append("data_collector")
            coverage = collector_output.data.get("coverage_ratio", 0)
            collector_errors = collector_output.errors or collector_output.data.get("errors", [])
            self._extend_unique(errors, [f"数据采集缺口: {err}" for err in collector_errors])
            self._progress(
                "data_collector",
                f"[数据采集] 完成 | 覆盖率={coverage:.0%}",
                detail=self._build_collection_detail(collector_output.data, stock_name, agents_completed),
                stage_status="completed",
            )
        else:
            errors.append(f"数据采集失败: {collector_output.errors}")
            self._progress(
                "data_collector",
                f"[数据采集] 失败 | {collector_output.errors}",
                detail={
                    "headline": "数据采集失败",
                    "note": "关键原始数据未能成功获取，研究流程无法继续。",
                    "bullets": [str(item) for item in collector_output.errors[:4]],
                    "current_agent": "data_collector",
                },
                stage_status="failed",
            )
            return ResearchReport(
                stock_code=stock_code,
                stock_name=stock_name,
                depth=depth,
                errors=errors,
                agents_skipped=["全流程"],
            )

        self._progress(
            "data_cleaner",
            "[数据清洗] 正在清洗和标准化数据...",
            detail={
                "headline": "正在清洗数据",
                "note": "会做去重、缺失标注、字段标准化，并产出统一分析上下文。",
                "metrics": [
                    self._metric("raw_coverage", "原始覆盖率", self._format_percent(collector_output.data.get("coverage_ratio")), "success"),
                ],
                "completed_agents": agents_completed,
                "current_agent": "data_cleaner",
            },
        )
        cleaner_output = await self._safe_run_agent(
            DataCleanerAgent(),
            AgentInput(
                stock_code=stock_code,
                stock_name=stock_name,
                context={"raw_data": collector_output.data},
                depth=depth,
            ),
        )

        if cleaner_output.status == AgentStatus.SUCCESS:
            cleaned_data = cleaner_output.data.get("cleaned", {})
            cleaned_data["collection_status"] = collector_output.data.get("collection_status", {})
            cleaned_data["collection_errors"] = collector_output.data.get("errors", [])
            context["cleaned_data"] = cleaned_data
            agents_completed.append("data_cleaner")
            coverage = cleaned_data.get("coverage_ratio", 0)
            cleaner_warnings = cleaner_output.data.get("warnings", [])
            self._extend_unique(errors, [f"数据清洗提示: {warning}" for warning in cleaner_warnings])
            self._progress(
                "data_cleaner",
                f"[数据清洗] 完成 | 覆盖率={coverage:.0%}",
                detail=self._build_cleaner_detail(cleaned_data, cleaner_warnings, agents_completed),
                stage_status="completed",
            )
        else:
            errors.append(f"数据清洗失败: {cleaner_output.errors}")
            self._progress(
                "data_cleaner",
                f"[数据清洗] 失败 | {cleaner_output.errors}",
                detail={
                    "headline": "数据清洗失败",
                    "note": "原始数据存在结构问题，当前无法进入后续分析阶段。",
                    "bullets": [str(item) for item in cleaner_output.errors[:4]],
                    "completed_agents": agents_completed,
                    "current_agent": "data_cleaner",
                },
                stage_status="failed",
            )
            return ResearchReport(
                stock_code=stock_code,
                stock_name=stock_name,
                depth=depth,
                errors=errors,
                agents_completed=agents_completed,
                agents_skipped=["全流程"],
            )

        self._progress(
            "screener",
            "[初筛] 正在执行快速排雷...",
            detail={
                "headline": "开始初筛检查",
                "note": "先判断是否存在硬伤，避免无效深入分析。",
                "completed_agents": agents_completed,
                "current_agent": "screener",
            },
        )
        screener_output = await self._safe_run_agent(
            ScreenerAgent(),
            AgentInput(
                stock_code=stock_code,
                stock_name=stock_name,
                context={"cleaned_data": cleaned_data},
                depth=depth,
            ),
        )

        if screener_output.status == AgentStatus.SUCCESS:
            screening_result = screener_output.data.get("screening", {})
            context["screening"] = screening_result
            agents_completed.append("screener")
            verdict = screening_result.get("verdict", "")
            recommendation = screening_result.get("recommendation", "")
            key_risks = screening_result.get("key_risks", [])
            self._progress(
                "screener",
                f"[初筛] 结论: {verdict}",
                detail={
                    "headline": "初筛已完成",
                    "note": recommendation or "已完成是否继续深入研究的快速判断。",
                    "metrics": [
                        self._metric("verdict", "初筛结论", verdict or "--", "success"),
                        self._metric("risks", "重点风险", len(key_risks), "warning" if key_risks else "success"),
                    ],
                    "bullets": [str(item) for item in key_risks[:3]],
                    "completed_agents": agents_completed,
                    "current_agent": "screener",
                },
                stage_status="completed",
            )

            if verdict in {"刚性剔除", "鍒氭€у墧闄?"}:
                self.logger.info(f"初筛刚性剔除，终止研究 | {stock_code}")
                return ResearchReport(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    depth=depth,
                    markdown=self._build_early_stop_report(stock_code, stock_name, screening_result),
                    agents_completed=agents_completed,
                    agents_skipped=["分析层", "报告", "结论"],
                    errors=errors,
                )
        else:
            errors.append(f"初筛失败: {screener_output.errors}")
            agents_skipped.append("screener")
            self._progress(
                "screener",
                f"[初筛] 失败 | {screener_output.errors}",
                detail={
                    "headline": "初筛阶段失败，后续继续执行",
                    "note": "系统会保留错误并继续进入分析阶段。",
                    "bullets": [str(item) for item in screener_output.errors[:4]],
                    "completed_agents": agents_completed,
                    "current_agent": "screener",
                },
                stage_status="failed",
            )

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
            "analysis",
            f"[分析层] {len(active_agents)} 个 Agent 并行执行中...",
            detail=self._build_analysis_start_detail(active_agents, agents_completed),
        )

        analysis_readonly_context = dict(context)

        async def run_analysis_agent(name: str, agent_cls: type) -> tuple[str, AgentOutput]:
            output = await self._safe_run_agent(
                agent_cls(),
                AgentInput(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    context=analysis_readonly_context,
                    depth=depth,
                ),
            )
            return name, output

        tasks = [
            run_analysis_agent(name, agent_map[name])
            for name in active_agents
            if name in agent_map
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        analysis_context = dict(context)
        remaining_active = list(active_agents)
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"分析Agent异常: {result}", exc_info=True)
                errors.append(f"分析Agent异常: {result}")
                continue

            name, output = result
            remaining_active = [agent for agent in remaining_active if agent != name]
            if output.status == AgentStatus.SUCCESS:
                context_key = f"{name}_analysis"
                result_data = output.data.get(name, output.data)
                context[context_key] = result_data
                analysis_context[context_key] = result_data
                agents_completed.append(name)

                summary = output.summary or "完成"
                self._progress(
                    name,
                    f"[{self._agent_label(name)}] {summary}",
                    detail=self._build_analysis_result_detail(name, summary, remaining_active, agents_completed),
                    stage_status="completed",
                )
            else:
                agents_skipped.append(name)
                errors.append(f"{name}失败: {output.errors}")
                failure_summary = "；".join(str(item) for item in output.errors[:3]) or "执行失败"
                self._progress(
                    name,
                    f"[{self._agent_label(name)}] 失败 | {output.errors}",
                    detail=self._build_analysis_result_detail(
                        name,
                        failure_summary,
                        remaining_active,
                        agents_completed,
                        failed=True,
                    ),
                    stage_status="failed",
                )

        self._progress(
            "report",
            "[报告生成] 正在生成深度研究报告...",
            detail={
                "headline": "开始生成研究报告",
                "note": "后台会汇总所有分析结果，拼装成完整 Markdown 报告。",
                "completed_agents": agents_completed,
                "active_agents": [],
                "current_agent": "report",
            },
        )
        report_output = await self._safe_run_agent(
            ReportAgent(),
            AgentInput(
                stock_code=stock_code,
                stock_name=stock_name,
                context=analysis_context,
                depth=depth,
            ),
        )

        markdown = ""
        chart_pack: list[dict[str, Any]] = []
        evidence_pack: list[dict[str, Any]] = []
        if report_output.status == AgentStatus.SUCCESS:
            markdown = report_output.data.get("markdown", report_output.summary or "")
            chart_pack = list(report_output.data.get("chart_pack", []) or [])
            evidence_pack = list(report_output.data.get("evidence_pack", []) or [])
            report_warnings = report_output.errors or []
            self._extend_unique(errors, [f"报告生成提示: {warning}" for warning in report_warnings])
            agents_completed.append("report")
            self._progress(
                "report",
                "[报告生成] 深度报告生成完成",
                detail={
                    "headline": "研究报告已生成",
                    "note": report_output.summary or "已完成报告草稿拼装。",
                    "metrics": [
                        self._metric("length", "报告长度", f"{len(markdown)} 字符", "info"),
                        self._metric("charts", "图表包", len(chart_pack), "success"),
                        self._metric("evidence", "证据包", len(evidence_pack), "success"),
                        self._metric("warnings", "提示数", len(report_warnings), "warning" if report_warnings else "success"),
                    ],
                    "bullets": [str(item) for item in report_warnings[:3]],
                    "completed_agents": agents_completed,
                    "current_agent": "report",
                },
                stage_status="completed",
            )
        else:
            errors.append(f"报告生成失败: {report_output.errors}")
            agents_skipped.append("report")
            self._progress(
                "report",
                f"[报告生成] 失败 | {report_output.errors}",
                detail={
                    "headline": "报告生成失败",
                    "note": "系统仍会尝试继续生成投资结论。",
                    "bullets": [str(item) for item in report_output.errors[:4]],
                    "completed_agents": agents_completed,
                    "current_agent": "report",
                },
                stage_status="failed",
            )

        self._progress(
            "conclusion",
            "[投资结论] 正在生成投资结论...",
            detail={
                "headline": "开始生成投资结论",
                "note": "会综合估值、风险和核心判断，形成最终建议。",
                "completed_agents": agents_completed,
                "current_agent": "conclusion",
            },
        )
        conclusion_output = await self._safe_run_agent(
            ConclusionAgent(),
            AgentInput(
                stock_code=stock_code,
                stock_name=stock_name,
                context=analysis_context,
                depth=depth,
            ),
        )

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
                self._progress(
                    "conclusion",
                    f"[投资结论] 结论: {rec}{target}",
                    detail=self._build_conclusion_detail(conclusion, agents_completed),
                    stage_status="completed",
                )
            except PydanticValidationError as exc:
                self.logger.error(f"结论数据格式异常: {exc}")
                errors.append(f"结论数据格式异常: {exc}")
                agents_skipped.append("conclusion")
                self._progress(
                    "conclusion",
                    f"[投资结论] 失败 | {exc}",
                    detail={
                        "headline": "投资结论解析失败",
                        "note": "LLM 返回了不可用的结论结构。",
                        "bullets": [str(exc)],
                        "completed_agents": agents_completed,
                        "current_agent": "conclusion",
                    },
                    stage_status="failed",
                )
            except Exception as exc:
                self.logger.error(f"结论解析失败: {exc}")
                errors.append(f"结论解析失败: {exc}")
                agents_skipped.append("conclusion")
                self._progress(
                    "conclusion",
                    f"[投资结论] 失败 | {exc}",
                    detail={
                        "headline": "投资结论解析失败",
                        "note": "系统未能从结论 Agent 输出中提取结构化结果。",
                        "bullets": [str(exc)],
                        "completed_agents": agents_completed,
                        "current_agent": "conclusion",
                    },
                    stage_status="failed",
                )
        else:
            errors.append(f"投资结论失败: {conclusion_output.errors}")
            agents_skipped.append("conclusion")
            self._progress(
                "conclusion",
                f"[投资结论] 失败 | {conclusion_output.errors}",
                detail={
                    "headline": "投资结论生成失败",
                    "note": "研究报告可能已经生成，但最终建议不可用。",
                    "bullets": [str(item) for item in conclusion_output.errors[:4]],
                    "completed_agents": agents_completed,
                    "current_agent": "conclusion",
                },
                stage_status="failed",
            )

        report = ResearchReport(
            stock_code=stock_code,
            stock_name=stock_name,
            depth=depth,
            markdown=markdown,
            conclusion=conclusion,
            chart_pack=chart_pack,
            evidence_pack=evidence_pack,
            agents_completed=agents_completed,
            agents_skipped=agents_skipped,
            errors=errors,
        )

        self._save_to_knowledge_base(report, context)

        self._progress(
            "done",
            "研究完成，报告已保存",
            detail={
                "headline": "研究流程已完成",
                "note": "报告和投资结论已经准备好，可进入详情页查看。",
                "metrics": [
                    self._metric("completed_agents", "完成模块", len(agents_completed), "success"),
                    self._metric("skipped_agents", "跳过模块", len(agents_skipped), "warning" if agents_skipped else "success"),
                    self._metric("issues", "提示/缺口", len(errors), "warning" if errors else "success"),
                ],
                "bullets": [str(item) for item in errors[:4]],
                "completed_agents": agents_completed,
                "active_agents": [],
                "current_agent": "done",
            },
            stage_status="completed",
        )
        return report

    async def _safe_run_agent(self, agent: Any, input_data: AgentInput) -> AgentOutput:
        """Safely run an agent and convert exceptions into AgentOutput."""
        try:
            return await agent.safe_run(input_data)
        except Exception as exc:
            self.logger.error(f"Agent[{getattr(agent, 'agent_name', '?')}] 异常: {exc}")
            return AgentOutput(
                agent_name=getattr(agent, "agent_name", "unknown"),
                status=AgentStatus.FAILED,
                errors=[str(exc)],
            )

    async def _collect_via_intel_hub(self, stock_code: str) -> AgentOutput:
        """Run the optional intel-hub collector in a worker thread."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self._sync_collect_via_intel_hub,
            stock_code,
        )

    def _sync_collect_via_intel_hub(self, stock_code: str) -> AgentOutput:
        """Synchronously collect via IntelligenceHub and normalize to AgentOutput."""
        hub = IntelligenceHub()
        try:
            hub.initialize()
            results = hub.collect_stock(stock_code)

            collected_data: dict[str, Any] = {}
            collection_status: dict[str, str] = {}
            collection_errors: list[str] = []
            success_count = 0

            for result in results:
                if result.status in ("success", "partial"):
                    collected_data[result.data_type] = result.data
                    collection_status[result.data_type] = result.status
                    success_count += 1
                else:
                    collection_status[result.data_type] = "failed"
                    if result.error:
                        collection_errors.append(f"{result.data_type}: {result.error}")

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
                data_sources=list({result.source_name for result in results}),
                confidence=coverage_ratio,
                summary=f"情报中心采集{total}类数据，成功{success_count}，覆盖率{coverage_ratio:.0%}",
            )
        except Exception as exc:
            self.logger.error(f"情报中心采集异常: {exc}")
            return AgentOutput(
                agent_name="intel_hub_collector",
                status=AgentStatus.FAILED,
                errors=[str(exc)],
            )
        finally:
            hub.close()

    def _progress(
        self,
        step: str,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
        stage_status: str = "running",
    ) -> None:
        """Emit progress to logs and optional callback."""
        self.logger.info(message)
        if self.progress_callback:
            try:
                self.progress_callback(step, message, detail, stage_status)
            except TypeError:
                self.progress_callback(step, message)

    @staticmethod
    def _extend_unique(target: list[str], issues: list[str]) -> None:
        for issue in issues:
            if issue not in target:
                target.append(issue)

    def _check_knowledge_base(self, stock_code: str) -> str | None:
        """Check whether the knowledge base already has recent research."""
        if not _KB_AVAILABLE:
            return None
        try:
            store = ChromaKnowledgeStore()
            latest = store.get_latest_research(stock_code)
            if latest:
                return (
                    f"深度={latest.depth} "
                    f"建议={latest.recommendation or 'N/A'} "
                    f"日期={latest.research_date.strftime('%Y-%m-%d')}"
                )
        except Exception as exc:
            self.logger.warning(f"知识库检查失败（不影响主流程）: {exc}")
        return None

    def _save_to_knowledge_base(
        self,
        report: ResearchReport,
        context: dict[str, Any],
    ) -> None:
        """Persist research result into the optional knowledge base."""
        del context
        if not _KB_AVAILABLE:
            self.logger.debug("知识库模块不可用，跳过保存")
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
            self._progress(
                "knowledge_base",
                f"[知识库] 已存入知识库 | {report.stock_code}",
                detail={
                    "headline": "研究结果已写入知识库",
                    "note": "后续搜索、历史记录和增量更新都可以复用本次结果。",
                    "completed_agents": report.agents_completed,
                    "current_agent": "knowledge_base",
                },
                stage_status="completed",
            )
        except Exception as exc:
            self.logger.warning(f"知识库存储失败（不影响主流程）: {exc}")

    @staticmethod
    def _build_early_stop_report(
        stock_code: str,
        stock_name: str,
        screening: dict[str, Any],
    ) -> str:
        """Build a lightweight report for early stop scenarios."""
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
