"""Deterministic business-model analysis agent."""

from __future__ import annotations

from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import AgentInput, AgentOutput, AgentStatus
from investresearch.core.trust import get_module_profile, merge_evidence_refs

logger = get_logger("agent.business_model")

SYSTEM_PROMPT = "Business model analysis is implemented deterministically in this build."


class BusinessModelAgent(AgentBase[AgentInput, AgentOutput]):
    """Analyze business model, moats, and negative cases conservatively."""

    agent_name: str = "business_model"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        cleaned = input_data.context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行商业模式分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始商业模式分析 | {stock_code} {stock_name}")

        result = self._build_result(input_data.context)
        score = result.get("model_score")
        moat = result.get("moat_overall", "未知")
        score_text = "待验证" if score is None else f"{score}/10"
        summary = f"商业模式评分: {score_text}, 护城河: {moat}"

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"business_model": result},
            data_sources=["stock_info", "financials", "announcements", "research_reports", "patents"],
            confidence=0.78 if result.get("evidence_status") == "ok" else 0.46,
            summary=summary,
        )

    def validate_output(self, output: AgentOutput) -> None:
        if output.status != AgentStatus.SUCCESS:
            return

        bm = output.data.get("business_model", {})
        errors: list[str] = []

        score = bm.get("model_score")
        if score is not None and not (0 <= score <= 10):
            errors.append(f"model_score 超出范围: {score}")

        if bm.get("moat_overall") not in {"宽", "窄", "无"}:
            errors.append(f"moat_overall 无效: {bm.get('moat_overall')}")

        if not bm.get("profit_driver"):
            errors.append("缺少 profit_driver")
        if not bm.get("negative_view"):
            errors.append("缺少 negative_view")
        if not bm.get("conclusion"):
            errors.append("缺少 conclusion")

        if errors:
            from investresearch.core.exceptions import AgentValidationError

            raise AgentValidationError(self.agent_name, errors)

    def _get_model(self) -> str:
        return self.config.get_layer_model("analysis_layer", task="business_model")

    def _build_result(self, context: dict[str, Any]) -> dict[str, Any]:
        cleaned = context.get("cleaned_data", {})
        info = cleaned.get("stock_info", {})
        financials = [item for item in cleaned.get("financials", []) if isinstance(item, dict)]
        announcements = [item for item in cleaned.get("announcements", []) if isinstance(item, dict)]
        research_reports = [item for item in cleaned.get("research_reports", []) if isinstance(item, dict)]
        patents = [item for item in cleaned.get("patents", []) if isinstance(item, dict)]

        stock_profile = get_module_profile(cleaned, "stock_info")
        announcement_profile = get_module_profile(cleaned, "announcements")
        report_profile = get_module_profile(cleaned, "research_reports")
        patent_profile = get_module_profile(cleaned, "patents")

        latest_financial = financials[0] if financials else {}
        main_business = str(info.get("main_business") or "主营业务待验证")
        asset_model = str(info.get("asset_model") or self._infer_asset_model(latest_financial))
        client_type = str(info.get("client_type") or "待验证")

        evidence_refs = merge_evidence_refs(
            stock_profile.evidence_refs,
            announcement_profile.evidence_refs,
            report_profile.evidence_refs,
            patent_profile.evidence_refs,
        )

        moats = self._infer_moats(main_business, announcements, research_reports, patents)
        evidence_status = "ok" if (
            stock_profile.completeness >= 0.6
            or announcement_profile.completeness >= 0.5
            or (stock_profile.completeness >= 0.4 and patent_profile.completeness >= 0.4)
        ) else "partial"

        moat_bonus = sum(
            1
            for moat in moats
            if moat.get("strength") in {"strong", "medium"} and moat.get("moat_type") != "无"
        )
        model_score = None
        if evidence_status == "ok":
            model_score = round(min(8.8, 4.8 + moat_bonus * 0.9 + (0.5 if asset_model == "轻" else 0.2)), 1)

        missing_fields = sorted(
            set(stock_profile.missing_fields + announcement_profile.missing_fields + report_profile.missing_fields)
        )
        if patent_profile.missing_fields:
            missing_fields.extend([f"patents.{item}" for item in patent_profile.missing_fields[:2]])

        return {
            "model_score": model_score,
            "revenue_structure": self._build_revenue_structure(main_business, latest_financial),
            "profit_driver": self._infer_profit_driver(latest_financial, main_business, patents),
            "asset_model": asset_model,
            "client_concentration": client_type,
            "moats": moats,
            "moat_overall": self._infer_moat_overall(moats),
            "negative_view": self._negative_view(main_business, patents),
            "conclusion": self._build_conclusion(evidence_status, patents, moats),
            "evidence_status": evidence_status,
            "missing_fields": missing_fields[:10],
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }

    @staticmethod
    def _infer_asset_model(latest_financial: dict[str, Any]) -> str:
        total_assets = latest_financial.get("total_assets")
        revenue = latest_financial.get("revenue")
        if total_assets in (None, "") or revenue in (None, "", 0):
            return "混合"
        try:
            ratio = float(total_assets) / float(revenue)
        except (ValueError, TypeError, ZeroDivisionError):
            return "混合"
        if ratio < 1.0:
            return "轻"
        if ratio > 2.0:
            return "重"
        return "混合"

    @staticmethod
    def _build_revenue_structure(main_business: str, latest_financial: dict[str, Any]) -> list[dict[str, Any]]:
        revenue = latest_financial.get("revenue")
        if not main_business or main_business == "主营业务待验证":
            return []
        return [
            {
                "segment_name": main_business[:32],
                "revenue": revenue,
                "ratio": 100.0 if revenue is not None else None,
                "growth": latest_financial.get("revenue_yoy"),
                "gross_margin": latest_financial.get("gross_margin"),
            }
        ]

    @staticmethod
    def _infer_profit_driver(
        latest_financial: dict[str, Any],
        main_business: str,
        patents: list[dict[str, Any]],
    ) -> str:
        revenue_yoy = latest_financial.get("revenue_yoy")
        gross_margin = latest_financial.get("gross_margin")
        operating_cashflow = latest_financial.get("operating_cashflow")
        patent_hint = patents[0].get("title", "") if patents else ""
        parts = [
            f"利润驱动仍围绕主营业务[{main_business[:24]}]",
            f"收入增速={revenue_yoy if revenue_yoy is not None else '待验证'}",
            f"毛利率={gross_margin if gross_margin is not None else '待验证'}",
            f"经营现金流={operating_cashflow if operating_cashflow is not None else '待验证'}",
        ]
        if patent_hint:
            parts.append(f"技术侧有官方专利线索[{patent_hint[:24]}]")
        return "；".join(parts)

    @staticmethod
    def _infer_moats(
        main_business: str,
        announcements: list[dict[str, Any]],
        research_reports: list[dict[str, Any]],
        patents: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        text = " ".join(
            [main_business]
            + [str(item.get("excerpt") or item.get("summary") or "") for item in announcements[:3]]
            + [str(item.get("excerpt") or item.get("summary") or "") for item in research_reports[:3]]
            + [str(item.get("title") or item.get("summary") or "") for item in patents[:3]]
        )

        moat_specs = [
            ("品牌", ["品牌", "渠道", "高端"]),
            ("成本优势", ["规模", "成本", "产能", "工艺"]),
            ("专利", ["专利", "技术", "研发", "牌照"]),
            ("转换成本", ["粘性", "复购", "客户关系", "替换成本"]),
        ]

        moats: list[dict[str, Any]] = []
        for moat_type, keywords in moat_specs:
            matched = [keyword for keyword in keywords if keyword in text]
            if not matched:
                continue
            moats.append(
                {
                    "moat_type": moat_type,
                    "strength": "strong" if len(matched) >= 3 else "medium" if len(matched) >= 2 else "emerging",
                    "evidence": f"命中关键词: {', '.join(matched[:3])}",
                    "sustainability": "仍需结合年报、公告和后续跟踪继续验证。",
                }
            )

        if patents and not any(moat.get("moat_type") == "专利" for moat in moats):
            latest_patent = patents[0]
            moats.append(
                {
                    "moat_type": "专利",
                    "strength": "medium",
                    "evidence": f"官方专利资料: {latest_patent.get('title', 'N/A')}",
                    "sustainability": "需继续核验专利数量、法律状态与商业化落地情况。",
                }
            )

        if not moats:
            return [
                {
                    "moat_type": "无",
                    "strength": "none",
                    "evidence": "当前公开资料不足以支持明确护城河判断",
                    "sustainability": "待验证",
                }
            ]
        return moats

    @staticmethod
    def _infer_moat_overall(moats: list[dict[str, Any]]) -> str:
        if not moats or moats[0].get("moat_type") == "无":
            return "无"
        strong_count = sum(1 for moat in moats if moat.get("strength") in {"strong", "medium"})
        return "宽" if strong_count >= 2 else "窄"

    @staticmethod
    def _negative_view(main_business: str, patents: list[dict[str, Any]]) -> str:
        patent_clause = "专利落地效果不及预期" if patents else "技术升级或替代路线冲击现有优势"
        return (
            f"该商业模式可能失败的路径包括：主营业务[{main_business[:20]}]景气下行、"
            f"竞争加剧压缩毛利，以及{patent_clause}。"
        )

    @staticmethod
    def _build_conclusion(evidence_status: str, patents: list[dict[str, Any]], moats: list[dict[str, Any]]) -> str:
        if evidence_status != "ok":
            if patents:
                return "商业模式证据仍偏薄，但已补充官方专利资料，可作为技术壁垒的跟踪线索。"
            return "商业模式证据仍偏薄，当前仅保留主营业务、资产模式和护城河方向性判断。"
        if patents:
            return "商业模式已有基础证据支撑，且官方专利资料为技术壁垒判断提供了增量验证。"
        if any(item.get("moat_type") != "无" for item in moats):
            return "商业模式已有一定原始资料支撑，但仍需年报拆分与后续公告继续验证。"
        return "商业模式基础框架已形成，但护城河判断仍需更多一手资料补强。"
