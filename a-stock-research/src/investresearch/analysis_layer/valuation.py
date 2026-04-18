"""Hybrid valuation analysis agent."""

from __future__ import annotations

from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import AgentInput, AgentOutput, AgentStatus
from investresearch.core.trust import get_module_profile, merge_evidence_refs

logger = get_logger("agent.valuation")

SYSTEM_PROMPT = """你是一位专业的A股估值分析专家。

## 你的任务
使用多种估值方法评估股票的合理价值，给出估值结论和合理价格区间。

## 估值方法（至少使用2种，按适用性选择）
1. **PE相对估值**: 当前PE对比历史PE范围(最低/中位/最高)，判断高估/低估
2. **PB相对估值**: 当前PB对比历史PB范围，适用于银行、地产等重资产行业
3. **PEG估值**: PE/G比率，适用于增长确定性较高的成长股，PEG<1偏低估
4. **PS估值**: 市销率对比，适用于高增长但尚未稳定盈利的公司
5. **DCF验证**: 不做精确DCF计算，而是反向验证当前股价隐含的增长预期是否合理

## 分析要求
- 必须至少使用2种估值方法
- 必须说明每种方法的核心假设和局限性
- 必须给出结论，但不要编造不存在的数据
- 若数据不足，可保留部分方法的 intrinsic_value 为 null

## 输出格式（严格JSON）
```json
{
  "methods": [
    {
      "method": "PE",
      "intrinsic_value": 25.5,
      "upside_pct": 15.3,
      "assumptions": ["假设1", "假设2"],
      "limitations": ["局限1"]
    }
  ],
  "reasonable_range_low": 22.0,
  "reasonable_range_high": 30.0,
  "valuation_level": "低估|合理|高估|严重高估",
  "conclusion": "估值综合结论"
}
```
"""


class ValuationAgent(AgentBase[AgentInput, AgentOutput]):
    """Estimate valuation with a deterministic numeric spine and LLM narratives."""

    agent_name: str = "valuation"
    execution_mode: str = "hybrid"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        context = input_data.context
        cleaned = context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行估值分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始估值分析 | {stock_code} {stock_name}")

        baseline = self._build_result(context)
        result = dict(baseline)
        allow_live_llm = bool(context.get("_allow_live_llm"))
        llm_invoked = False
        model_used: str | None = None
        runtime_mode = "deterministic"

        if allow_live_llm:
            model = self._get_model()
            model_used = model
            llm_invoked = True
            try:
                llm_result = await self.llm.call_json(
                    prompt=self._build_prompt(stock_code, stock_name, cleaned, context),
                    system_prompt=SYSTEM_PROMPT,
                    model=model,
                )
                result = self._merge_llm_result(baseline, llm_result)
                runtime_mode = "llm"
            except Exception as exc:
                self.logger.warning(f"估值分析LLM不可用，退回规则结果 | {exc}")
                runtime_mode = "hybrid"

        result = self._normalize_result(result, baseline)
        level = result.get("valuation_level", "未知")
        price = result.get("current_price")
        low = result.get("reasonable_range_low")
        high = result.get("reasonable_range_high")

        summary_parts = [f"估值水平: {level}"]
        if price is not None and low is not None and high is not None:
            summary_parts.append(f"合理区间: {low}-{high}, 当前: {price}")
        summary = " | ".join(summary_parts)

        self.logger.info(f"估值分析完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"valuation": result},
            data_sources=["realtime", "valuation", "valuation_percentile", "financials"],
            confidence=0.8 if result.get("evidence_status") == "ok" else 0.45,
            summary=summary,
            execution_mode=runtime_mode,
            llm_invoked=llm_invoked,
            model_used=model_used if runtime_mode != "deterministic" else None,
        )

    def validate_output(self, output: AgentOutput) -> None:
        if output.status != AgentStatus.SUCCESS:
            return

        valuation = output.data.get("valuation", {})
        errors = []

        methods = valuation.get("methods", [])
        if valuation.get("evidence_status") == "ok" and (not isinstance(methods, list) or len(methods) < 1):
            errors.append(f"估值方法不足: 需要>=2种，实际{len(methods) if isinstance(methods, list) else '非列表'}")

        level = valuation.get("valuation_level")
        if level not in ("低估", "合理", "高估", "严重高估", "待验证"):
            errors.append(f"valuation_level无效: {level}")

        if not valuation.get("conclusion"):
            errors.append("缺少conclusion")

        low = valuation.get("reasonable_range_low")
        high = valuation.get("reasonable_range_high")
        if low is not None and high is not None and low >= high:
            errors.append(f"合理区间异常: low={low} >= high={high}")

        if errors:
            from investresearch.core.exceptions import AgentValidationError

            raise AgentValidationError(self.agent_name, errors)

    def _get_model(self) -> str:
        return self.config.get_layer_model("analysis_layer", task="valuation")

    def _build_result(self, context: dict[str, Any]) -> dict[str, Any]:
        cleaned = context.get("cleaned_data", {})
        realtime = cleaned.get("realtime", {})
        valuation = cleaned.get("valuation", [])
        valuation_percentile = cleaned.get("valuation_percentile", {})
        financials = cleaned.get("financials", [])

        valuation_profile = get_module_profile(cleaned, "valuation")
        percentile_profile = get_module_profile(cleaned, "valuation_percentile")
        financial_profile = get_module_profile(cleaned, "financials")

        current_price = self._safe_float(realtime.get("close"))
        current_pe = self._safe_float(realtime.get("pe_ttm"))
        current_pb = self._safe_float(realtime.get("pb_mrq"))

        methods: list[dict[str, Any]] = []
        intrinsic_values: list[float] = []

        pe_values = [self._safe_float(item.get("pe_ttm")) for item in valuation if isinstance(item, dict)]
        pe_values = [value for value in pe_values if value is not None and value > 0]
        if current_price is not None and current_pe and pe_values:
            median_pe = self._median(pe_values)
            intrinsic = round(current_price * median_pe / current_pe, 2)
            intrinsic_values.append(intrinsic)
            methods.append(
                {
                    "method": "PE",
                    "intrinsic_value": intrinsic,
                    "upside_pct": round((intrinsic / current_price - 1) * 100, 2),
                    "assumptions": [f"历史PE中位数约 {median_pe:.2f}", "利润质量没有发生结构性恶化"],
                    "limitations": ["仅适用于盈利口径稳定时", "未纳入一致预期分歧"],
                }
            )

        pb_values = [self._safe_float(item.get("pb_mrq")) for item in valuation if isinstance(item, dict)]
        pb_values = [value for value in pb_values if value is not None and value > 0]
        if current_price is not None and current_pb and pb_values:
            median_pb = self._median(pb_values)
            intrinsic = round(current_price * median_pb / current_pb, 2)
            intrinsic_values.append(intrinsic)
            methods.append(
                {
                    "method": "PB",
                    "intrinsic_value": intrinsic,
                    "upside_pct": round((intrinsic / current_price - 1) * 100, 2),
                    "assumptions": [f"历史PB中位数约 {median_pb:.2f}", "净资产质量和盈利能力维持稳定"],
                    "limitations": ["更适合重资产或资产质量稳定公司", "未纳入行业轮动影响"],
                }
            )

        latest_financial = financials[0] if financials and isinstance(financials[0], dict) else {}
        if current_price is not None:
            methods.append(
                {
                    "method": "DCF审计",
                    "intrinsic_value": None,
                    "upside_pct": None,
                    "assumptions": [
                        f"最新营收增速={latest_financial.get('revenue_yoy', 'N/A')}",
                        f"经营现金流={latest_financial.get('operating_cashflow', 'N/A')}",
                    ],
                    "limitations": ["当前仅作为隐含假设审计工具", "缺少一致预期数据，不输出精确DCF数值"],
                }
            )

        low = round(min(intrinsic_values), 2) if intrinsic_values else None
        high = round(max(intrinsic_values), 2) if intrinsic_values else None
        midpoint = round((low + high) / 2, 2) if low is not None and high is not None else None
        margin_of_safety = (
            round((midpoint / current_price - 1) * 100, 2)
            if midpoint is not None and current_price not in (None, 0)
            else None
        )

        pe_percentile = self._safe_float(valuation_percentile.get("pe_ttm_percentile"))
        pb_percentile = self._safe_float(valuation_percentile.get("pb_mrq_percentile"))
        valuation_level = self._infer_valuation_level(pe_percentile, pb_percentile, low, high, current_price)
        evidence_refs = merge_evidence_refs(
            valuation_profile.evidence_refs,
            percentile_profile.evidence_refs,
            financial_profile.evidence_refs,
        )
        evidence_status = "ok" if intrinsic_values or pe_percentile is not None or pb_percentile is not None else "insufficient"
        conclusion = self._build_conclusion(valuation_level, low, high, current_price, evidence_status)

        return {
            "methods": methods,
            "pe_percentile": pe_percentile,
            "pb_percentile": pb_percentile,
            "reasonable_range_low": low,
            "reasonable_range_high": high,
            "current_price": current_price,
            "margin_of_safety": margin_of_safety,
            "valuation_level": valuation_level,
            "conclusion": conclusion,
            "evidence_status": evidence_status,
            "missing_fields": sorted(set(valuation_profile.missing_fields + percentile_profile.missing_fields)),
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }

    def _merge_llm_result(self, baseline: dict[str, Any], llm_result: dict[str, Any]) -> dict[str, Any]:
        merged = dict(baseline)
        methods = self._normalize_methods(llm_result.get("methods"), baseline)
        if methods:
            merged["methods"] = methods
            low, high = self._derive_range_from_methods(methods)
            if low is not None and high is not None:
                merged["reasonable_range_low"], merged["reasonable_range_high"] = self._guard_range(low, high, baseline)

        explicit_low = self._safe_float(llm_result.get("reasonable_range_low"))
        explicit_high = self._safe_float(llm_result.get("reasonable_range_high"))
        if explicit_low is not None and explicit_high is not None:
            guarded = self._guard_range(explicit_low, explicit_high, baseline)
            if guarded != (None, None):
                merged["reasonable_range_low"], merged["reasonable_range_high"] = guarded

        conclusion = self._clean_text(llm_result.get("conclusion"))
        if conclusion:
            merged["conclusion"] = conclusion

        return merged

    def _normalize_result(self, result: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(baseline)
        normalized.update(result)

        normalized["current_price"] = baseline.get("current_price")
        normalized["pe_percentile"] = baseline.get("pe_percentile")
        normalized["pb_percentile"] = baseline.get("pb_percentile")
        normalized["evidence_status"] = baseline.get("evidence_status", "insufficient")
        normalized["missing_fields"] = baseline.get("missing_fields", [])
        normalized["evidence_refs"] = baseline.get("evidence_refs", [])

        methods = self._normalize_methods(normalized.get("methods"), baseline) or baseline.get("methods", [])
        normalized["methods"] = methods

        low, high = self._derive_range_from_methods(methods)
        if low is None or high is None:
            low = self._safe_float(normalized.get("reasonable_range_low"))
            high = self._safe_float(normalized.get("reasonable_range_high"))
        guarded_low, guarded_high = self._guard_range(low, high, baseline)
        normalized["reasonable_range_low"] = guarded_low
        normalized["reasonable_range_high"] = guarded_high

        current_price = baseline.get("current_price")
        midpoint = (
            round((guarded_low + guarded_high) / 2, 2)
            if guarded_low is not None and guarded_high is not None
            else None
        )
        normalized["margin_of_safety"] = (
            round((midpoint / current_price - 1) * 100, 2)
            if midpoint is not None and current_price not in (None, 0)
            else None
        )
        normalized["valuation_level"] = self._infer_valuation_level(
            baseline.get("pe_percentile"),
            baseline.get("pb_percentile"),
            guarded_low,
            guarded_high,
            current_price,
        )

        conclusion = self._clean_text(normalized.get("conclusion"))
        normalized["conclusion"] = conclusion or self._build_conclusion(
            normalized["valuation_level"],
            guarded_low,
            guarded_high,
            current_price,
            baseline.get("evidence_status", "insufficient"),
        )
        return normalized

    @staticmethod
    def _median(values: list[float]) -> float:
        ordered = sorted(values)
        mid = len(ordered) // 2
        if len(ordered) % 2 == 1:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2

    @staticmethod
    def _infer_valuation_level(
        pe_percentile: float | None,
        pb_percentile: float | None,
        low: float | None,
        high: float | None,
        current_price: float | None,
    ) -> str:
        percentiles = [value for value in [pe_percentile, pb_percentile] if value is not None]
        if percentiles:
            avg_pct = sum(percentiles) / len(percentiles)
            if avg_pct <= 25:
                return "低估"
            if avg_pct <= 60:
                return "合理"
            if avg_pct <= 85:
                return "高估"
            return "严重高估"
        if low is not None and high is not None and current_price is not None:
            if current_price < low:
                return "低估"
            if current_price > high:
                return "高估"
            return "合理"
        return "待验证"

    @staticmethod
    def _build_conclusion(
        valuation_level: str,
        low: float | None,
        high: float | None,
        current_price: float | None,
        evidence_status: str,
    ) -> str:
        if evidence_status != "ok":
            return "估值证据不足，当前只保留历史分位和相对估值草图，目标区间待验证。"
        if low is not None and high is not None and current_price is not None:
            return f"基于历史估值中位数法，合理区间约 {low}-{high}，当前价格 {current_price}，估值判断为{valuation_level}。"
        return f"当前估值判断为{valuation_level}，但缺少完整价格区间。"

    def _build_prompt(self, stock_code: str, stock_name: str, cleaned: dict, context: dict) -> str:
        parts = [f"## 标的: {stock_code} {stock_name}\n"]

        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append("### 当前估值指标")
            parts.append(f"- 最新价: {realtime.get('close', 'N/A')}")
            parts.append(f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}")
            parts.append(f"- PB(MRQ): {realtime.get('pb_mrq', 'N/A')}")
            parts.append(f"- 总市值: {self._fmt_cap(realtime.get('market_cap'))}")
            parts.append("")

        valuation = cleaned.get("valuation", [])
        if valuation:
            pe_values = [v.get("pe_ttm") for v in valuation if isinstance(v, dict) and v.get("pe_ttm") is not None]
            pb_values = [v.get("pb_mrq") for v in valuation if isinstance(v, dict) and v.get("pb_mrq") is not None]

            parts.append("### 历史估值范围（月度数据）")
            if pe_values:
                pe_float = [float(v) for v in pe_values if self._safe_float(v) is not None]
                if pe_float:
                    parts.append(
                        f"- PE(TTM): 最低={min(pe_float):.1f} | 中位={sorted(pe_float)[len(pe_float)//2]:.1f} | "
                        f"最高={max(pe_float):.1f} | 数据点={len(pe_float)}"
                    )
            if pb_values:
                pb_float = [float(v) for v in pb_values if self._safe_float(v) is not None]
                if pb_float:
                    parts.append(
                        f"- PB(MRQ): 最低={min(pb_float):.2f} | 中位={sorted(pb_float)[len(pb_float)//2]:.2f} | "
                        f"最高={max(pb_float):.2f} | 数据点={len(pb_float)}"
                    )

            parts.append("\n### 近期月度估值")
            parts.append("| 日期 | PE(TTM) | PB(MRQ) |")
            parts.append("|---|---|---|")
            for item in valuation[-6:]:
                if isinstance(item, dict):
                    parts.append(
                        f"| {item.get('date', 'N/A')} | {item.get('pe_ttm', 'N/A')} | {item.get('pb_mrq', 'N/A')} |"
                    )
            parts.append("")

        financials = cleaned.get("financials", [])
        if financials:
            latest = financials[0] if isinstance(financials[0], dict) else {}
            parts.append("### DCF关键参数（最新期）")
            parts.append(f"- 报告期: {latest.get('report_date', 'N/A')}")
            parts.append(f"- 营业收入: {self._fmt_num(latest.get('revenue'))}")
            parts.append(f"- 净利润: {self._fmt_num(latest.get('net_profit'))}")
            parts.append(f"- 经营现金流: {self._fmt_num(latest.get('operating_cashflow'))}")
            parts.append(f"- 总资产: {self._fmt_num(latest.get('total_assets'))}")
            parts.append(f"- 净资产: {self._fmt_num(latest.get('equity'))}")
            parts.append(f"- ROE: {latest.get('roe', 'N/A')}")
            parts.append(f"- 营收增速: {latest.get('revenue_yoy', 'N/A')}")
            parts.append("")

        info = cleaned.get("stock_info", {})
        if info:
            parts.append(f"行业: {info.get('industry_sw', 'N/A')}")
            parts.append("")

        financial_analysis = context.get("financial_analysis")
        if financial_analysis:
            parts.append("### 财务分析参考")
            parts.append(f"- 综合评分: {financial_analysis.get('overall_score', 'N/A')}/10")
            conclusion = financial_analysis.get("conclusion", "")
            if conclusion:
                parts.append(f"- 财务结论: {conclusion[:200]}")
            parts.append("")

        parts.append("请根据以上数据对该标的进行多方法估值分析，按指定JSON格式输出。至少使用2种估值方法。")
        return "\n".join(parts)

    def _normalize_methods(self, value: Any, baseline: dict[str, Any]) -> list[dict[str, Any]]:
        baseline_methods = [
            item for item in baseline.get("methods", []) if isinstance(item, dict) and item.get("method")
        ]
        baseline_by_name = {str(item.get("method")): item for item in baseline_methods}

        if not isinstance(value, list):
            return baseline_methods

        llm_by_name: dict[str, dict[str, Any]] = {}
        llm_order: list[str] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            method_name = self._normalize_method_name(item.get("method"))
            if method_name is None:
                continue
            if method_name not in llm_order:
                llm_order.append(method_name)
            llm_by_name[method_name] = item

        merged: list[dict[str, Any]] = []
        for method_name in llm_order + [name for name in baseline_by_name if name not in llm_order]:
            base = baseline_by_name.get(method_name, {})
            candidate = llm_by_name.get(method_name, {})
            intrinsic = self._safe_float(candidate.get("intrinsic_value"))
            if intrinsic is None:
                intrinsic = self._safe_float(base.get("intrinsic_value"))
            if intrinsic is not None and intrinsic <= 0:
                intrinsic = None

            upside = self._safe_float(candidate.get("upside_pct"))
            if upside is None:
                upside = self._safe_float(base.get("upside_pct"))

            assumptions = self._normalize_text_list(candidate.get("assumptions"), limit=4) or self._normalize_text_list(
                base.get("assumptions"), limit=4
            )
            limitations = self._normalize_text_list(
                candidate.get("limitations"), limit=4
            ) or self._normalize_text_list(base.get("limitations"), limit=4)

            if assumptions == []:
                assumptions = ["关键假设待验证"]
            if limitations == []:
                limitations = ["方法局限待验证"]

            current_price = self._safe_float(baseline.get("current_price"))
            if upside is None and intrinsic is not None and current_price not in (None, 0):
                upside = round((intrinsic / current_price - 1) * 100, 2)

            if method_name != "DCF审计" and intrinsic is None:
                continue

            merged.append(
                {
                    "method": method_name,
                    "intrinsic_value": round(intrinsic, 2) if intrinsic is not None else None,
                    "upside_pct": round(upside, 2) if upside is not None else None,
                    "assumptions": assumptions,
                    "limitations": limitations,
                }
            )

        return merged or baseline_methods

    @staticmethod
    def _derive_range_from_methods(methods: list[dict[str, Any]]) -> tuple[float | None, float | None]:
        intrinsic_values = [
            float(item["intrinsic_value"])
            for item in methods
            if isinstance(item, dict) and isinstance(item.get("intrinsic_value"), (int, float))
        ]
        if len(intrinsic_values) < 2:
            return None, None
        return round(min(intrinsic_values), 2), round(max(intrinsic_values), 2)

    def _guard_range(
        self,
        low: float | None,
        high: float | None,
        baseline: dict[str, Any],
    ) -> tuple[float | None, float | None]:
        if low is None or high is None or low <= 0 or high <= 0 or low >= high:
            return baseline.get("reasonable_range_low"), baseline.get("reasonable_range_high")

        base_low = self._safe_float(baseline.get("reasonable_range_low"))
        base_high = self._safe_float(baseline.get("reasonable_range_high"))
        current_price = self._safe_float(baseline.get("current_price"))

        candidate_low = float(low)
        candidate_high = float(high)
        if base_low is not None and base_high is not None:
            candidate_low = max(candidate_low, round(base_low * 0.65, 2))
            candidate_high = min(candidate_high, round(base_high * 1.35, 2))
            if candidate_low >= candidate_high:
                return base_low, base_high
        elif current_price is not None:
            if candidate_low < current_price * 0.35 or candidate_high > current_price * 3.0:
                return baseline.get("reasonable_range_low"), baseline.get("reasonable_range_high")

        return round(candidate_low, 2), round(candidate_high, 2)

    @staticmethod
    def _normalize_method_name(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip().upper()
        if "PEG" in text:
            return "PEG"
        if "PS" in text:
            return "PS"
        if "PB" in text:
            return "PB"
        if "PE" in text:
            return "PE"
        if "DCF" in text:
            return "DCF审计"
        return None

    @staticmethod
    def _normalize_text_list(value: Any, limit: int = 4) -> list[str]:
        if isinstance(value, list):
            items = [str(item).strip() for item in value if str(item).strip()]
            return items[:limit]
        if value in (None, "", [], {}):
            return []
        text = str(value).strip()
        return [text[:120]] if text else []

    @staticmethod
    def _clean_text(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()[:260]

    @staticmethod
    def _fmt_cap(v: Any) -> str:
        if v is None:
            return "N/A"
        try:
            n = float(v)
            if n >= 1e12:
                return f"{n/1e12:.1f}万亿"
            if n >= 1e8:
                return f"{n/1e8:.1f}亿"
            return f"{n/1e4:.1f}万"
        except (ValueError, TypeError):
            return str(v)

    @staticmethod
    def _fmt_num(v: Any) -> str:
        if v is None:
            return "N/A"
        try:
            n = float(v)
            if abs(n) >= 1e8:
                return f"{n/1e8:.1f}亿"
            if abs(n) >= 1e4:
                return f"{n/1e4:.1f}万"
            return f"{n:.2f}"
        except (ValueError, TypeError):
            return str(v)

    @staticmethod
    def _safe_float(v: Any) -> float | None:
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
