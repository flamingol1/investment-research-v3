"""行业分析Agent - 生命周期判断、竞争格局、景气度指标

分析维度:
1. 行业生命周期: 初创期/成长期/成熟期/衰退期
2. 市场空间: 市场规模、增速、天花板
3. 竞争格局: 集中度(CR5)、竞争强度、竞争对手
4. 景气度: 当前景气方向、核心景气指标
5. 政策环境: 监管态度、产业政策
"""

from __future__ import annotations

from typing import Any

from investresearch.core.agent_base import AgentBase
from investresearch.core.logging import get_logger
from investresearch.core.models import (
    AgentInput,
    AgentOutput,
    AgentStatus,
)
from investresearch.core.trust import get_module_profile, merge_evidence_refs

logger = get_logger("agent.industry")

SYSTEM_PROMPT = """你是一位专业的A股行业研究分析师，擅长行业赛道判断和竞争格局分析。

## 你的任务
对给定股票所属行业进行β分析，判断行业赛道质量和公司行业地位。

## 分析框架
### 1. 行业生命周期判断
- **初创期**: 市场小但增速极快，技术路线未定型，玩家少
- **成长期**: 市场快速扩大，渗透率快速提升，参与者增多，竞争加剧
- **成熟期**: 增速放缓，渗透率高，格局稳定，龙头份额集中
- **衰退期**: 市场萎缩，产能过剩，企业退出
- 判断依据：市场规模增速、渗透率、参与者数量变化、产能周期

### 2. 市场空间分析
- 当前市场规模（估算）
- 5年复合增速
- 天花板在哪里（渗透率上限/替代品威胁）
- 增量市场 vs 存量博弈

### 3. 竞争格局
- 行业集中度(CR5/CR10)
- 竞争模式: 价格战/差异化/寡头协调
- 主要竞争对手（至少3家）及其优势
- 新进入者威胁
- 替代品威胁

### 4. 景气度判断
- 当前处于景气上行/平稳/下行
- 列出3-5个核心景气度跟踪指标
- 产业链传导信号（上游/中游/下游）

### 5. 政策环境
- 监管态度: 鼓励/中性/限制
- 产业政策影响
- 潜在政策风险

### 6. 公司行业地位
- 市场份额排名
- 相对竞争对手的优势/劣势
- 行业β中的个股α来源

## 输出格式（严格JSON）
```json
{
  "lifecycle": "初创期/成长期/成熟期/衰退期",
  "lifecycle_evidence": "生命周期判断的具体依据",
  "market_size": 5000,
  "market_growth": 15.0,
  "competition_pattern": "寡头垄断/寡头竞争/垄断竞争/完全竞争",
  "cr5": 45.0,
  "top_competitors": [
    {
      "name": "竞争对手名称",
      "market_share": 20.0,
      "advantage": "竞争优势",
      "threat_level": "高/中/低"
    }
  ],
  "prosperity_indicators": ["指标1", "指标2", "指标3"],
  "prosperity_direction": "上行/平稳/下行",
  "policy_stance": "政策态度描述",
  "company_position": "公司在行业中的地位描述",
  "conclusion": "行业综合结论"
}
```

## 重要约束
- lifecycle只能取"初创期"/"成长期"/"成熟期"/"衰退期"
- top_competitors至少列出3个竞争对手
- prosperity_indicators至少3个指标
- prosperity_direction只能取"上行"/"平稳"/"下行"
- competition_pattern只能取4个标准值之一
- 所有判断必须有依据，不得凭空臆断
"""


class IndustryAgent(AgentBase[AgentInput, AgentOutput]):
    """行业分析Agent - 生命周期判断、竞争格局、景气度指标"""

    agent_name: str = "industry"
    execution_mode: str = "hybrid"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行行业分析"""
        context = input_data.context
        cleaned = context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行行业分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始行业分析 | {stock_code} {stock_name}")

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
                    prompt=self._build_prompt(stock_code, stock_name, context),
                    system_prompt=SYSTEM_PROMPT,
                    model=model,
                )
                result = self._merge_llm_result(baseline, llm_result, context)
                runtime_mode = "llm"
            except Exception as exc:
                self.logger.warning(f"行业分析LLM不可用，退回规则兜底 | {exc}")
                runtime_mode = "hybrid"

        result = self._normalize_result(result, baseline, context)

        lifecycle = result.get("lifecycle", "未知")
        direction = result.get("prosperity_direction", "未知")
        summary = f"行业生命周期: {lifecycle}, 景气方向: {direction}"
        self.logger.info(f"行业分析完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"industry": result},
            data_sources=["industry_enhanced", "policy_documents", "research_reports"],
            confidence=0.75 if result.get("evidence_status") == "ok" else 0.45,
            summary=summary,
            execution_mode=runtime_mode,
            llm_invoked=llm_invoked,
            model_used=model_used if runtime_mode != "deterministic" else None,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验行业分析输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        industry = output.data.get("industry", {})
        errors = []

        lifecycle = industry.get("lifecycle")
        if lifecycle not in ("初创期", "成长期", "成熟期", "衰退期", "待验证"):
            errors.append(f"lifecycle无效: {lifecycle}")

        direction = industry.get("prosperity_direction")
        if direction not in ("上行", "平稳", "下行", "待验证"):
            errors.append(f"prosperity_direction无效: {direction}")

        competitors = industry.get("top_competitors", [])
        if (
            industry.get("evidence_status") == "ok"
            and (not isinstance(competitors, list) or len(competitors) < 2)
        ):
            errors.append(f"top_competitors不足: 需要>=3个，至少2个，实际{len(competitors) if isinstance(competitors, list) else '非列表'}")

        indicators = industry.get("prosperity_indicators", [])
        if (
            industry.get("evidence_status") == "ok"
            and (not isinstance(indicators, list) or len(indicators) < 2)
        ):
            errors.append(f"prosperity_indicators不足: 需要>=3个，至少2个")

        if not industry.get("lifecycle_evidence"):
            errors.append("缺少lifecycle_evidence")

        if not industry.get("conclusion"):
            errors.append("缺少conclusion")

        if errors:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取分析层模型"""
        return self.config.get_layer_model("analysis_layer", task="industry")

    def _build_result(self, context: dict[str, Any]) -> dict[str, Any]:
        cleaned = context.get("cleaned_data", {})
        raw = context.get("raw_data", {})
        raw_industry = raw.get("industry", {}) if isinstance(raw.get("industry", {}), dict) else {}
        enhanced = cleaned.get("industry_enhanced", {})
        stock_info = cleaned.get("stock_info", {})
        policy_documents = cleaned.get("policy_documents", [])
        research_reports = cleaned.get("research_reports", [])

        profile = get_module_profile(cleaned, "industry_enhanced")

        # 交叉验证数据（同业年报多源交叉比对）
        cross_verification = cleaned.get("cross_verification", {}) or context.get("cross_verification", {})
        cv_metrics = self._extract_cv_metrics(cross_verification)

        # 优先使用交叉验证推荐值，回退到原始数据
        raw_market_size = cv_metrics.get("market_size") or raw_industry.get("market_size")
        raw_growth = cv_metrics.get("cagr") or raw_industry.get("cagr_5y")
        raw_cr5 = cv_metrics.get("cr5") or raw_industry.get("cr5")

        lifecycle = raw_industry.get("lifecycle") or "待验证"
        if hasattr(lifecycle, "value"):
            lifecycle = lifecycle.value

        competition_pattern = self._infer_competition_pattern(raw_cr5, enhanced)
        prosperity_direction = self._infer_prosperity_direction(enhanced)
        company_position = self._infer_company_position(enhanced, stock_info)
        policy_stance = self._infer_policy_stance(policy_documents)
        competitors = self._baseline_competitors(context)

        evidence_refs = merge_evidence_refs(
            profile.evidence_refs,
            get_module_profile(cleaned, "policy_documents").evidence_refs,
            get_module_profile(cleaned, "research_reports").evidence_refs,
        )
        missing_fields = list(profile.missing_fields)
        if raw_market_size in (None, ""):
            missing_fields.append("market_size")
        if raw_growth in (None, ""):
            missing_fields.append("market_growth")
        if raw_cr5 in (None, ""):
            missing_fields.append("cr5")

        competitor_count = len([item for item in competitors if isinstance(item, dict) and str(item.get("name") or "").strip()])
        indicator_count = len(list(enhanced.get("data_points", []) or []))
        has_core_metrics = any(value not in (None, "") for value in (raw_market_size, raw_growth, raw_cr5))

        if has_core_metrics and competitor_count >= 2 and indicator_count >= 2:
            evidence_status = "ok"
        elif has_core_metrics or competitor_count >= 2 or indicator_count >= 2:
            evidence_status = "partial"
        else:
            evidence_status = "insufficient"
        conclusion = (
            f"行业当前{prosperity_direction}，公司处于{company_position}。"
            if evidence_status == "ok"
            else "行业核心规模、增速或集中度数据不足，当前仅能给出方向性判断，需继续补充行业数据库。"
        )

        return {
            "lifecycle": lifecycle if lifecycle in ("初创期", "成长期", "成熟期", "衰退期") else "待验证",
            "lifecycle_evidence": raw_industry.get("policy_stance") or self._first_evidence_excerpt(evidence_refs) or "缺少稳定行业生命周期证据，待验证",
            "market_size": raw_market_size,
            "market_growth": raw_growth,
            "competition_pattern": competition_pattern,
            "cr5": raw_cr5,
            "top_competitors": competitors,
            "prosperity_indicators": list(enhanced.get("data_points", []) or [])[:5],
            "prosperity_direction": prosperity_direction,
            "policy_stance": policy_stance,
            "company_position": company_position,
            "conclusion": conclusion,
            "evidence_status": evidence_status,
            "missing_fields": sorted(set(missing_fields)),
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }

    @staticmethod
    def _extract_cv_metrics(cross_verification: dict[str, Any]) -> dict[str, float | None]:
        """从交叉验证结果中提取推荐值."""
        result: dict[str, float | None] = {}
        for metric in cross_verification.get("verified_metrics", []):
            name = metric.get("metric_name", "")
            recommended = metric.get("recommended_value")
            confidence = metric.get("confidence_score", 0)
            if recommended is not None and confidence >= 0.4:
                result[name] = recommended
        return result

    @staticmethod
    def _infer_competition_pattern(cr5: Any, enhanced: dict[str, Any]) -> str:
        cr5_value = None
        try:
            cr5_value = float(cr5)
        except (TypeError, ValueError):
            cr5_value = None
        if cr5_value is not None:
            if cr5_value >= 70:
                return "寡头垄断"
            if cr5_value >= 45:
                return "寡头竞争"
            return "垄断竞争"
        leaders = list(enhanced.get("industry_leaders", []) or [])
        if len(leaders) >= 3:
            return "垄断竞争"
        return "待验证"

    @staticmethod
    def _infer_prosperity_direction(enhanced: dict[str, Any]) -> str:
        value = enhanced.get("industry_change_pct")
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "待验证"
        if numeric >= 1:
            return "上行"
        if numeric <= -1:
            return "下行"
        return "平稳"

    @staticmethod
    def _infer_company_position(enhanced: dict[str, Any], stock_info: dict[str, Any]) -> str:
        rank = enhanced.get("stock_rank_in_industry")
        total = enhanced.get("total_in_industry")
        if rank and total:
            return f"行业排名第 {rank}/{total}"
        if stock_info.get("industry_sw"):
            return f"已归属于 {stock_info.get('industry_sw')}，但行业地位仍待验证"
        return "公司行业地位待验证"

    @staticmethod
    def _infer_policy_stance(policy_documents: list[dict[str, Any]]) -> str:
        if not policy_documents:
            return "政策信息不足，待验证"
        latest = policy_documents[0]
        title = str(latest.get("title") or "")
        excerpt = str(latest.get("excerpt") or latest.get("summary") or "")
        return f"存在政策原文支撑：{title}。{excerpt[:80]}"

    @staticmethod
    def _first_evidence_excerpt(evidence_refs: list[Any]) -> str:
        for item in evidence_refs:
            excerpt = item.excerpt if hasattr(item, "excerpt") else item.get("excerpt", "")
            if excerpt:
                return str(excerpt)[:120]
        return ""

    def _build_prompt(self, stock_code: str, stock_name: str, context: dict[str, Any]) -> str:
        """构建行业分析提示词"""
        cleaned = context.get("cleaned_data", {})
        parts = [f"## 标的: {stock_code} {stock_name}\n"]

        # 公司所属行业
        info = cleaned.get("stock_info", {})
        if info:
            parts.append("### 公司基本信息")
            parts.append(f"- 行业: {info.get('industry_sw', 'N/A')}")
            mb = info.get('main_business') or 'N/A'
            parts.append(f"- 主营业务: {mb[:300]}")
            parts.append("")

        # 行业已有数据
        industry_data = cleaned.get("industry", {})
        if industry_data:
            parts.append("### 已知行业数据")
            if industry_data.get("market_size"):
                parts.append(f"- 市场规模: {industry_data['market_size']}亿元")
            if industry_data.get("cagr_5y"):
                parts.append(f"- 5年复合增速: {industry_data['cagr_5y']}%")
            if industry_data.get("cr5"):
                parts.append(f"- CR5集中度: {industry_data['cr5']}%")
            parts.append("")

        # 交叉验证数据（同业年报多源交叉比对）
        cross_verification = context.get("cross_verification", {})
        if cross_verification and cross_verification.get("verified_metrics"):
            cv_metrics = cross_verification["verified_metrics"]
            parts.append("### 同业交叉验证数据（多源交叉比对，可信度更高）")
            parts.append(
                f"- 同业公司数: {cross_verification.get('peer_count', 'N/A')}家"
            )
            parts.append(
                f"- 整体置信度: {cross_verification.get('overall_confidence', 0):.0%}"
            )
            for metric in cv_metrics[:6]:
                name = metric.get("metric_name", "")
                recommended = metric.get("recommended_value")
                confidence = metric.get("confidence_score", 0)
                consistency = metric.get("consistency_flag", "")
                sources = metric.get("sources", [])
                consulting = metric.get("consulting_sources", [])
                unit = "%" if name in ("cagr", "cr5", "market_share") else "亿元"
                label = {
                    "market_size": "市场规模",
                    "cagr": "复合增速",
                    "cr5": "CR5集中度",
                    "market_share": "市场份额",
                }.get(name, name)
                if recommended is not None:
                    parts.append(
                        f"- {label}: {recommended}{unit} "
                        f"(置信度={confidence:.0%}, 一致性={consistency}, "
                        f"来源={len(sources)}家)"
                    )
                if consulting:
                    parts.append(f"  - 引用咨询机构: {', '.join(consulting)}")
                if sources:
                    parts.append(f"  - 来源公司: {', '.join(sources[:5])}")
            parts.append("")

        # 财务数据（行业地位判断用）
        financials = cleaned.get("financials", [])
        if financials:
            parts.append("### 公司核心指标（判断行业地位）")
            parts.append("| 报告期 | 营收 | 净利润 | 营收增速 | 毛利率 | ROE |")
            parts.append("|---|---|---|---|---|---|")
            for f in financials[:5]:
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('revenue'))} "
                    f"| {self._fmt(f.get('net_profit'))} "
                    f"| {self._fmt_pct(f.get('revenue_yoy'))} "
                    f"| {self._fmt_pct(f.get('gross_margin'))} "
                    f"| {self._fmt_pct(f.get('roe'))} |"
                )
            parts.append("")

        industry_enhanced = cleaned.get("industry_enhanced", {})
        if industry_enhanced:
            parts.append("### 行业高频数据")
            for item in industry_enhanced.get("data_points", [])[:6]:
                parts.append(f"- {item}")
            if industry_enhanced.get("industry_pe") is not None:
                parts.append(f"- 行业PE: {industry_enhanced.get('industry_pe')}")
            if industry_enhanced.get("industry_pb") is not None:
                parts.append(f"- 行业PB: {industry_enhanced.get('industry_pb')}")
            parts.append("")

        # 同业公司信息
        cross_verification = context.get("cross_verification", {})
        if cross_verification and cross_verification.get("peers"):
            peers = cross_verification["peers"]
            parts.append(f"### 同业公司（共{len(peers)}家，数据已交叉验证）")
            for peer in peers[:6]:
                name = peer.get("stock_name") or peer.get("stock_code", "")
                code = peer.get("stock_code", "")
                cap = peer.get("market_cap")
                cap_str = self._fmt_cap(cap) if cap else "N/A"
                rank = peer.get("rank_in_industry", "")
                parts.append(f"- {name}({code}) 市值={cap_str} 排名={rank}")
            parts.append("")

        # 市值数据
        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append(f"### 当前总市值: {self._fmt_cap(realtime.get('market_cap'))}")
            parts.append("")

        # 上游分析参考
        financial_analysis = context.get("financial_analysis", {})
        if financial_analysis:
            parts.append("### 财务分析参考")
            parts.append(f"- 综合评分: {financial_analysis.get('overall_score', 'N/A')}/10")
            parts.append("")

        research_reports = cleaned.get("research_reports", [])
        if research_reports:
            parts.append("### 行业/公司卖方资料")
            for item in research_reports[:3]:
                excerpt = item.get("excerpt") or item.get("summary") or ""
                parts.append(
                    f"- {item.get('publish_date', 'N/A')} {item.get('institution', 'N/A')}《{item.get('title', 'N/A')}》: {str(excerpt)[:180]}"
                )
            parts.append("")

        policy_documents = cleaned.get("policy_documents", [])
        if policy_documents:
            parts.append("### 政策原文资料")
            for item in policy_documents[:4]:
                excerpt = item.get("excerpt") or item.get("summary") or ""
                parts.append(
                    f"- {item.get('policy_date', 'N/A')} {item.get('issuing_body', item.get('source', 'gov.cn'))}《{item.get('title', 'N/A')}》: {str(excerpt)[:220]}"
                )
            parts.append("")

        parts.append("请根据以上数据对该标的所属行业进行全面分析，按指定JSON格式输出。重点关注行业生命周期、竞争格局和景气度。")
        return "\n".join(parts)

    def _merge_llm_result(
        self,
        baseline: dict[str, Any],
        llm_result: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(baseline)
        for key in (
            "lifecycle",
            "lifecycle_evidence",
            "competition_pattern",
            "prosperity_direction",
            "policy_stance",
            "company_position",
            "conclusion",
        ):
            value = llm_result.get(key)
            if value not in (None, "", [], {}):
                merged[key] = value

        competitors = self._normalize_competitors(llm_result.get("top_competitors"), context)
        if competitors:
            merged["top_competitors"] = competitors

        indicators = self._normalize_text_list(llm_result.get("prosperity_indicators"), limit=5)
        if indicators:
            merged["prosperity_indicators"] = indicators

        return merged

    def _normalize_result(
        self,
        result: dict[str, Any],
        baseline: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = dict(result)
        normalized["lifecycle"] = self._normalize_lifecycle(normalized.get("lifecycle")) or baseline.get("lifecycle", "待验证")
        normalized["competition_pattern"] = (
            self._normalize_competition_pattern(normalized.get("competition_pattern"))
            or baseline.get("competition_pattern", "待验证")
        )
        normalized["prosperity_direction"] = (
            self._normalize_direction(normalized.get("prosperity_direction"))
            or baseline.get("prosperity_direction", "待验证")
        )
        normalized["lifecycle_evidence"] = str(
            normalized.get("lifecycle_evidence") or baseline.get("lifecycle_evidence") or ""
        ).strip()
        normalized["policy_stance"] = str(normalized.get("policy_stance") or baseline.get("policy_stance") or "").strip()
        normalized["company_position"] = str(
            normalized.get("company_position") or baseline.get("company_position") or ""
        ).strip()
        normalized["conclusion"] = str(normalized.get("conclusion") or baseline.get("conclusion") or "").strip()
        normalized["top_competitors"] = self._normalize_competitors(normalized.get("top_competitors"), context) or baseline.get("top_competitors", [])
        normalized["prosperity_indicators"] = (
            self._normalize_text_list(normalized.get("prosperity_indicators"), limit=5)
            or baseline.get("prosperity_indicators", [])
        )
        if baseline.get("evidence_status") == "ok" and len(normalized["top_competitors"]) < 2:
            normalized["top_competitors"] = baseline.get("top_competitors", [])
        if baseline.get("evidence_status") == "ok" and len(normalized["prosperity_indicators"]) < 2:
            normalized["prosperity_indicators"] = baseline.get("prosperity_indicators", [])
        normalized["market_size"] = baseline.get("market_size")
        normalized["market_growth"] = baseline.get("market_growth")
        normalized["cr5"] = baseline.get("cr5")
        normalized["evidence_status"] = baseline.get("evidence_status", "partial")
        normalized["missing_fields"] = list(baseline.get("missing_fields", []) or [])
        normalized["evidence_refs"] = list(baseline.get("evidence_refs", []) or [])
        return normalized

    @staticmethod
    def _normalize_lifecycle(value: Any) -> str:
        text = str(value or "").strip()
        aliases = {"成长": "成长期", "成熟": "成熟期", "衰退": "衰退期", "初创": "初创期"}
        text = aliases.get(text, text)
        return text if text in {"初创期", "成长期", "成熟期", "衰退期", "待验证"} else ""

    @staticmethod
    def _normalize_direction(value: Any) -> str:
        text = str(value or "").strip()
        aliases = {"向上": "上行", "向下": "下行", "稳定": "平稳"}
        text = aliases.get(text, text)
        return text if text in {"上行", "平稳", "下行", "待验证"} else ""

    @staticmethod
    def _normalize_competition_pattern(value: Any) -> str:
        text = str(value or "").strip()
        aliases = {
            "寡头": "寡头竞争",
            "垄断": "寡头垄断",
            "充分竞争": "完全竞争",
        }
        text = aliases.get(text, text)
        return text if text in {"寡头垄断", "寡头竞争", "垄断竞争", "完全竞争", "待验证"} else ""

    def _normalize_competitors(self, value: Any, context: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        cleaned = context.get("cleaned_data", {})
        enhanced = cleaned.get("industry_enhanced", {})
        cross_verification = context.get("cross_verification", {})
        allowed_names = {
            str(name).strip()
            for name in list(enhanced.get("industry_leaders", []) or [])
            if str(name).strip()
        }
        for item in cross_verification.get("peers", []) or []:
            name = str(item.get("stock_name") or item.get("stock_code") or "").strip()
            if name:
                allowed_names.add(name)

        normalized: list[dict[str, Any]] = []
        for item in value[:5]:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            if allowed_names and name not in allowed_names:
                continue
            threat = str(item.get("threat_level") or "中").strip()
            threat = {"高风险": "高", "中风险": "中", "低风险": "低"}.get(threat, threat)
            if threat not in {"高", "中", "低"}:
                threat = "中"
            normalized.append(
                {
                    "name": name,
                    "market_share": item.get("market_share"),
                    "advantage": str(item.get("advantage") or "公开资料提及").strip(),
                    "threat_level": threat,
                }
            )
        return normalized

    def _baseline_competitors(self, context: dict[str, Any]) -> list[dict[str, Any]]:
        cleaned = context.get("cleaned_data", {})
        enhanced = cleaned.get("industry_enhanced", {})
        cross_verification = cleaned.get("cross_verification", {}) or context.get("cross_verification", {})

        candidates: list[str] = []
        for name in list(enhanced.get("industry_leaders", []) or []):
            text = str(name).strip()
            if text and text not in candidates:
                candidates.append(text)

        for peer in cross_verification.get("peers", []) or []:
            if not isinstance(peer, dict):
                continue
            text = str(peer.get("stock_name") or peer.get("stock_code") or "").strip()
            if text and text not in candidates:
                candidates.append(text)

        return [
            {
                "name": name,
                "market_share": None,
                "advantage": "同业样本/公开资料提及",
                "threat_level": "中",
            }
            for name in candidates[:5]
        ]

    @staticmethod
    def _normalize_text_list(value: Any, limit: int = 5) -> list[str]:
        if not isinstance(value, list):
            return []
        result: list[str] = []
        for item in value:
            text = str(item).strip()
            if not text or text in result:
                continue
            result.append(text)
            if len(result) >= limit:
                break
        return result

    @staticmethod
    def _fmt(v: Any) -> str:
        """格式化数值"""
        if v is None or v == "":
            return "N/A"
        try:
            n = float(v)
            if abs(n) >= 1e8:
                return f"{n/1e8:.1f}亿"
            elif abs(n) >= 1e4:
                return f"{n/1e4:.1f}万"
            else:
                return f"{n:.2f}"
        except (ValueError, TypeError):
            return str(v)

    @staticmethod
    def _fmt_pct(v: Any) -> str:
        """格式化百分比"""
        if v is None or v == "":
            return "N/A"
        try:
            return f"{float(v):.1f}%"
        except (ValueError, TypeError):
            return str(v)

    @staticmethod
    def _fmt_cap(v: Any) -> str:
        """格式化市值"""
        if v is None:
            return "N/A"
        try:
            n = float(v)
            if n >= 1e12:
                return f"{n/1e12:.1f}万亿"
            elif n >= 1e8:
                return f"{n/1e8:.1f}亿"
            else:
                return f"{n/1e4:.1f}万"
        except (ValueError, TypeError):
            return str(v)
