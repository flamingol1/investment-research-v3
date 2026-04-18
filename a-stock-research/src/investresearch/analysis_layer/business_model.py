"""Hybrid business-model analysis agent."""

from __future__ import annotations

from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import AgentInput, AgentOutput, AgentStatus
from investresearch.core.trust import get_field_quality_trace, get_module_profile, merge_evidence_refs

logger = get_logger("agent.business_model")

SYSTEM_PROMPT = """你是一位A股商业模式研究员，负责在已有原始资料基础上做商业模式与护城河判断。

请只基于输入中已经提供的资料输出 JSON：
{
  "model_score": 7.5,
  "revenue_structure": [{"segment_name": "业务A", "revenue": 1.0, "ratio": 60.0, "growth": 12.0, "gross_margin": 35.0}],
  "profit_driver": "一句话说明核心盈利驱动",
  "asset_model": "轻/重/混合",
  "client_concentration": "客户结构判断",
  "moats": [
    {
      "moat_type": "品牌/成本优势/专利/转换成本/无",
      "strength": "strong/medium/emerging/none",
      "evidence": "证据说明",
      "sustainability": "可持续性判断"
    }
  ],
  "moat_overall": "宽/窄/无",
  "negative_view": "最可能失败的路径",
  "conclusion": "商业模式综合结论"
}

要求：
- 不要编造新业务、新客户、新专利
- 若证据不足，保留“待验证”表述
- moat_overall 只能是 宽/窄/无
- asset_model 只能是 轻/重/混合/unknown
"""


class BusinessModelAgent(AgentBase[AgentInput, AgentOutput]):
    """Analyze business model, moats, and negative cases conservatively."""

    agent_name: str = "business_model"
    execution_mode: str = "hybrid"

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

        baseline = self._build_result(input_data.context)
        result = dict(baseline)
        allow_live_llm = bool(input_data.context.get("_allow_live_llm"))
        llm_invoked = False
        model_used: str | None = None
        runtime_mode = "deterministic"

        if allow_live_llm:
            model = self._get_model()
            model_used = model
            llm_invoked = True
            try:
                llm_result = await self.llm.call_json(
                    prompt=self._build_prompt(stock_code, stock_name, input_data.context),
                    system_prompt=SYSTEM_PROMPT,
                    model=model,
                )
                result = self._merge_llm_result(baseline, llm_result)
                runtime_mode = "llm"
            except Exception as exc:
                self.logger.warning(f"商业模式LLM不可用，退回规则兜底 | {exc}")
                runtime_mode = "hybrid"

        result = self._normalize_result(result, baseline)
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
            execution_mode=runtime_mode,
            llm_invoked=llm_invoked,
            model_used=model_used if runtime_mode != "deterministic" else None,
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

    def _build_prompt(self, stock_code: str, stock_name: str, context: dict[str, Any]) -> str:
        cleaned = context.get("cleaned_data", {})
        info = cleaned.get("stock_info", {})
        financials = [item for item in cleaned.get("financials", []) if isinstance(item, dict)]
        announcements = [item for item in cleaned.get("announcements", []) if isinstance(item, dict)]
        research_reports = [item for item in cleaned.get("research_reports", []) if isinstance(item, dict)]
        patents = [item for item in cleaned.get("patents", []) if isinstance(item, dict)]

        parts = [f"## 标的\n- 股票代码: {stock_code}\n- 股票名称: {stock_name or 'N/A'}"]
        trace_sections = [
            ("主营业务证据", get_field_quality_trace(cleaned, "stock_info.main_business")),
            ("商业模式标签证据", get_field_quality_trace(cleaned, "stock_info.business_model")),
            ("资产模式证据", get_field_quality_trace(cleaned, "stock_info.asset_model")),
            ("客户类型证据", get_field_quality_trace(cleaned, "stock_info.client_type")),
        ]
        trace_lines: list[str] = []
        for label, trace in trace_sections:
            if trace is None:
                continue
            evidence_state = getattr(trace.evidence_state, "value", trace.evidence_state)
            trace_lines.append(
                f"- {label}: 值状态={trace.value_state.value} / 证据状态={evidence_state} / 置信度={trace.confidence_score:.0%}"
            )
        if trace_lines:
            parts.extend(["## 字段证据状态", *trace_lines, ""])
        if info:
            parts.extend(
                [
                    "## 基础信息",
                    f"- 主营业务: {info.get('main_business', 'N/A')}",
                    f"- 现有盈利模式: {info.get('business_model', 'N/A')}",
                    f"- 资产模式: {info.get('asset_model', 'N/A')}",
                    f"- 客户结构: {info.get('client_type', 'N/A')}",
                    "",
                ]
            )

        if financials:
            latest = financials[0]
            parts.extend(
                [
                    "## 最新财务快照",
                    f"- 营收: {latest.get('revenue', 'N/A')}",
                    f"- 营收增速: {latest.get('revenue_yoy', 'N/A')}",
                    f"- 毛利率: {latest.get('gross_margin', 'N/A')}",
                    f"- 经营现金流: {latest.get('operating_cashflow', 'N/A')}",
                    "",
                ]
            )

        if announcements:
            parts.append("## 公告原文摘录")
            for item in announcements[:4]:
                excerpt = item.get("excerpt") or item.get("summary") or ""
                parts.append(f"- {item.get('announcement_date', 'N/A')} {item.get('title', 'N/A')}: {str(excerpt)[:180]}")
            parts.append("")

        if research_reports:
            parts.append("## 卖方资料")
            for item in research_reports[:3]:
                excerpt = item.get("excerpt") or item.get("summary") or ""
                parts.append(f"- {item.get('publish_date', 'N/A')} {item.get('institution', 'N/A')}: {str(excerpt)[:180]}")
            parts.append("")

        if patents:
            parts.append("## 官方专利资料")
            for item in patents[:4]:
                excerpt = item.get("excerpt") or item.get("summary") or ""
                parts.append(f"- {item.get('publish_date', 'N/A')} {item.get('title', 'N/A')}: {str(excerpt)[:160]}")
            parts.append("")

        parts.append("请基于以上资料判断商业模式、护城河和反方观点，不要补充输入中没有出现的新事实。")
        return "\n".join(parts)

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
        main_business_trace = get_field_quality_trace(cleaned, "stock_info.main_business")
        business_model_trace = get_field_quality_trace(cleaned, "stock_info.business_model")
        asset_model_trace = get_field_quality_trace(cleaned, "stock_info.asset_model")
        client_type_trace = get_field_quality_trace(cleaned, "stock_info.client_type")

        main_business_ok = self._trace_is_usable(main_business_trace, min_score=0.68)
        business_model_ok = self._trace_is_usable(business_model_trace, min_score=0.62)
        asset_model_ok = self._trace_is_usable(asset_model_trace, min_score=0.6)
        client_type_ok = self._trace_is_usable(client_type_trace, min_score=0.56)

        main_business = str(info.get("main_business") or "unknown") if main_business_ok else "unknown"
        asset_model = (
            str(info.get("asset_model") or self._infer_asset_model(latest_financial))
            if asset_model_ok
            else "unknown"
        )
        client_type = str(info.get("client_type") or "unknown") if client_type_ok else "unknown"
        business_profile_hint = str(info.get("business_model") or "unknown") if business_model_ok else "unknown"

        evidence_refs = merge_evidence_refs(
            stock_profile.evidence_refs,
            announcement_profile.evidence_refs,
            report_profile.evidence_refs,
            patent_profile.evidence_refs,
        )

        evidence_constraints = self._collect_evidence_constraints(
            main_business_trace,
            business_model_trace,
            asset_model_trace,
            client_type_trace,
        )
        evidence_status = "insufficient"
        if main_business_ok and (
            business_model_ok
            or announcement_profile.completeness >= 0.5
            or report_profile.completeness >= 0.4
            or patent_profile.completeness >= 0.4
        ):
            evidence_status = "ok"
        elif main_business_ok or business_model_ok:
            evidence_status = "partial"

        if evidence_status == "ok":
            moats = self._infer_moats(main_business, announcements, research_reports, patents)
        else:
            moats = [
                {
                    "moat_type": "无",
                    "strength": "none",
                    "evidence": "主营业务/商业模式证据不足，暂不建立明确护城河画像",
                    "sustainability": "待验证",
                }
            ]

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
            "profit_driver": self._infer_profit_driver(latest_financial, main_business, patents, business_profile_hint),
            "asset_model": asset_model,
            "client_concentration": client_type,
            "moats": moats,
            "moat_overall": self._infer_moat_overall(moats),
            "negative_view": self._negative_view(main_business, patents),
            "conclusion": self._build_conclusion(evidence_status, patents, moats),
            "evidence_status": evidence_status,
            "evidence_constraints": evidence_constraints,
            "missing_fields": missing_fields[:10],
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }

    def _merge_llm_result(self, baseline: dict[str, Any], llm_result: dict[str, Any]) -> dict[str, Any]:
        merged = dict(baseline)
        for key in ("profit_driver", "client_concentration", "negative_view", "conclusion"):
            value = llm_result.get(key)
            if value not in (None, "", [], {}):
                merged[key] = value

        score = self._safe_float(llm_result.get("model_score"))
        if score is not None:
            merged["model_score"] = round(max(0.0, min(score, 10.0)), 1)

        asset_model = self._normalize_asset_model(llm_result.get("asset_model"))
        if asset_model:
            merged["asset_model"] = asset_model

        llm_moats = self._normalize_moats(llm_result.get("moats"))
        if llm_moats:
            merged["moats"] = llm_moats

        moat_overall = self._normalize_moat_overall(llm_result.get("moat_overall"))
        if moat_overall:
            merged["moat_overall"] = moat_overall
        elif llm_moats:
            merged["moat_overall"] = self._infer_moat_overall(llm_moats)

        revenue_structure = llm_result.get("revenue_structure")
        if isinstance(revenue_structure, list) and revenue_structure:
            merged["revenue_structure"] = revenue_structure[:3]

        return merged

    def _normalize_result(self, result: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(result)
        normalized["model_score"] = self._safe_float(normalized.get("model_score"))
        if normalized["model_score"] is not None:
            normalized["model_score"] = round(max(0.0, min(float(normalized["model_score"]), 10.0)), 1)
        normalized["asset_model"] = (
            self._normalize_asset_model(normalized.get("asset_model"))
            or baseline.get("asset_model")
            or "unknown"
        )
        normalized["moats"] = self._normalize_moats(normalized.get("moats")) or baseline.get("moats", [])
        normalized["moat_overall"] = (
            self._normalize_moat_overall(normalized.get("moat_overall"))
            or self._infer_moat_overall(normalized["moats"])
            or baseline.get("moat_overall", "无")
        )
        for key in ("profit_driver", "negative_view", "conclusion"):
            normalized[key] = str(normalized.get(key) or baseline.get(key) or "").strip()
        normalized["client_concentration"] = str(
            normalized.get("client_concentration") or baseline.get("client_concentration") or "unknown"
        ).strip()
        normalized["evidence_status"] = baseline.get("evidence_status", "partial")
        normalized["evidence_constraints"] = list(baseline.get("evidence_constraints", []) or [])
        normalized["missing_fields"] = list(baseline.get("missing_fields", []) or [])
        normalized["evidence_refs"] = list(baseline.get("evidence_refs", []) or [])
        if baseline.get("evidence_status") != "ok":
            normalized["model_score"] = None
            normalized["moats"] = list(baseline.get("moats", []) or [])
            normalized["moat_overall"] = baseline.get("moat_overall", "无")
            normalized["asset_model"] = baseline.get("asset_model", "unknown")
            normalized["client_concentration"] = baseline.get("client_concentration", "unknown")
        return normalized

    @staticmethod
    def _normalize_asset_model(value: Any) -> str:
        text = str(value or "").strip()
        aliases = {"轻资产": "轻", "重资产": "重", "混合型": "混合", "待验证": "unknown", "未知": "unknown"}
        text = aliases.get(text, text)
        return text if text in {"轻", "重", "混合", "unknown"} else ""

    @staticmethod
    def _normalize_moat_overall(value: Any) -> str:
        text = str(value or "").strip()
        aliases = {"强": "宽", "中等": "窄", "弱": "无", "待验证": "无"}
        text = aliases.get(text, text)
        return text if text in {"宽", "窄", "无"} else ""

    @staticmethod
    def _normalize_moats(value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in value[:4]:
            if not isinstance(item, dict):
                continue
            moat_type = str(item.get("moat_type") or "").strip()
            if not moat_type:
                continue
            strength = str(item.get("strength") or "").strip().lower()
            if strength not in {"strong", "medium", "emerging", "none"}:
                strength = "emerging" if moat_type != "无" else "none"
            normalized.append(
                {
                    "moat_type": moat_type,
                    "strength": strength,
                    "evidence": str(item.get("evidence") or "待验证").strip(),
                    "sustainability": str(item.get("sustainability") or "待验证").strip(),
                }
            )
        return normalized

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value in (None, "", "-"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

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
        if not main_business or main_business in {"主营业务待验证", "unknown"}:
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
        business_profile_hint: str,
    ) -> str:
        if main_business in {"", "unknown"}:
            return "主营业务与商业模式证据不足，利润驱动暂不做确定性归因，需等待年报结构化字段或定向摘录补齐。"
        revenue_yoy = latest_financial.get("revenue_yoy")
        gross_margin = latest_financial.get("gross_margin")
        operating_cashflow = latest_financial.get("operating_cashflow")
        patent_hint = patents[0].get("title", "") if patents else ""
        parts = [
            f"利润驱动仍围绕主营业务[{main_business[:24]}]",
            f"业务模式标签={business_profile_hint[:24] if business_profile_hint not in {'', 'unknown'} else '待验证'}",
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
        if main_business in {"", "unknown"}:
            return "当前主营业务画像证据不足，最大风险是先验业务标签出错并污染后续判断。"
        patent_clause = "专利落地效果不及预期" if patents else "技术升级或替代路线冲击现有优势"
        return (
            f"该商业模式可能失败的路径包括：主营业务[{main_business[:20]}]景气下行、"
            f"竞争加剧压缩毛利，以及{patent_clause}。"
        )

    @staticmethod
    def _build_conclusion(evidence_status: str, patents: list[dict[str, Any]], moats: list[dict[str, Any]]) -> str:
        if evidence_status != "ok":
            if patents:
                return "商业模式证据仍偏薄，相关业务画像已主动降级为 unknown/待验证，官方专利仅作为旁证跟踪。"
            return "商业模式证据仍偏薄，当前已主动降级为 unknown/待验证，避免错误业务画像污染后续结论。"
        if patents:
            return "商业模式已有基础证据支撑，且官方专利资料为技术壁垒判断提供了增量验证。"
        if any(item.get("moat_type") != "无" for item in moats):
            return "商业模式已有一定原始资料支撑，但仍需年报拆分与后续公告继续验证。"
        return "商业模式基础框架已形成，但护城河判断仍需更多一手资料补强。"

    @staticmethod
    def _trace_is_usable(trace: Any, *, min_score: float) -> bool:
        if trace is None:
            return False
        value_state = str(getattr(getattr(trace, "value_state", ""), "value", getattr(trace, "value_state", "")))
        if value_state != "present":
            return False
        evidence_state = str(getattr(getattr(trace, "evidence_state", ""), "value", getattr(trace, "evidence_state", "")))
        if evidence_state == "consistent":
            return True
        return evidence_state == "single_source" and float(getattr(trace, "confidence_score", 0.0) or 0.0) >= min_score

    @classmethod
    def _collect_evidence_constraints(cls, *traces: Any) -> list[str]:
        notes: list[str] = []
        for trace in traces:
            if trace is None or cls._trace_is_usable(trace, min_score=0.6):
                continue
            evidence_state = str(getattr(getattr(trace, "evidence_state", ""), "value", getattr(trace, "evidence_state", "")))
            value_state = str(getattr(getattr(trace, "value_state", ""), "value", getattr(trace, "value_state", "")))
            notes.append(
                f"{getattr(trace, 'label', getattr(trace, 'field', 'field'))}证据偏弱: 值状态={value_state}，证据状态={evidence_state}"
            )
        return notes[:4]
