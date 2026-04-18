"""风险分析Agent - 全维度风险清单、三情景测算

分析维度:
1. 行业风险: 技术变革、政策变化、周期波动
2. 经营风险: 客户集中、供应商依赖、产能风险
3. 财务风险: 杠杆率、流动性、商誉减值
4. 治理风险: 实控人、关联交易、信息不对称
5. 市场风险: 估值、流动性、机构持仓
6. 三情景测算: 乐观/中性/悲观
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

logger = get_logger("agent.risk")

SYSTEM_PROMPT = """你是一位专业的A股风险评估分析师，擅长识别投资风险和情景分析。

## 你的任务
对给定股票进行全维度风险分析和三情景测算。

## 风险维度（6大类，每类至少1个风险项）
### 1. 行业风险
- 技术颠覆风险
- 政策/监管变化风险
- 行业周期波动风险
- 替代品威胁

### 2. 经营风险
- 客户集中度风险（大客户依赖）
- 供应商依赖风险
- 产能扩张/收缩风险
- 产品价格波动风险
- 新项目/新业务失败风险

### 3. 财务风险
- 杠杆率过高风险
- 流动性风险
- 商誉减值风险
- 汇率/利率风险
- 应收账款坏账风险

### 4. 治理风险
- 实控人风险（质押、变更、违规）
- 关联交易利益输送风险
- 管理层道德风险
- 信息披露风险

### 5. 市场风险
- 估值过高风险
- 流动性不足风险
- 机构集中抛售风险
- 大小非解禁风险

### 6. 政策风险
- 产业政策变化
- 环保/安全监管趋严
- 国际贸易摩擦
- 数据安全/反垄断

## 三情景测算
### 乐观情景（概率约25%）
- 核心假设
- 目标价
- 上行空间

### 中性情景（概率约50%）
- 核心假设
- 目标价
- 上行空间

### 悲观情景（概率约25%）
- 核心假设
- 目标价
- 下行空间

## 输出格式（严格JSON）
```json
{
  "overall_risk_level": "低/中/高/极高",
  "risk_score": 5.5,
  "risks": [
    {
      "category": "行业/经营/财务/治理/市场/政策",
      "risk_name": "风险名称",
      "severity": "高/中/低",
      "probability": "高/中/低",
      "impact": "影响说明",
      "mitigation": "缓解措施"
    }
  ],
  "scenarios": [
    {
      "scenario": "乐观",
      "target_price": 30.0,
      "upside_pct": 35.0,
      "assumptions": ["假设1", "假设2"],
      "probability": 25.0
    },
    {
      "scenario": "中性",
      "target_price": 22.0,
      "upside_pct": 5.0,
      "assumptions": ["假设1", "假设2"],
      "probability": 50.0
    },
    {
      "scenario": "悲观",
      "target_price": 15.0,
      "upside_pct": -30.0,
      "assumptions": ["假设1", "假设2"],
      "probability": 25.0
    }
  ],
  "fatal_risks": ["致命风险1", "致命风险2"],
  "monitoring_points": ["跟踪指标1", "跟踪指标2"],
  "conclusion": "风险综合结论"
}
```

## 重要约束
- risks至少包含6个风险项（覆盖全部6大类）
- scenarios必须包含乐观/中性/悲观三种情景
- fatal_risks列出可能否定投资逻辑的致命风险
- monitoring_points至少列出3个需持续跟踪的指标
- overall_risk_level只能取"低"/"中"/"高"/"极高"
- risk_score范围0-10（越高越危险）
- 所有风险评估必须有数据支撑
"""


class RiskAgent(AgentBase[AgentInput, AgentOutput]):
    """风险分析Agent - 全维度风险清单、三情景测算"""

    agent_name: str = "risk"
    execution_mode: str = "hybrid"

    async def run(self, input_data: AgentInput) -> AgentOutput:
        """执行风险分析"""
        context = input_data.context
        cleaned = context.get("cleaned_data", {})
        if not cleaned:
            return AgentOutput(
                agent_name=self.agent_name,
                status=AgentStatus.FAILED,
                errors=["无清洗后的数据，无法执行风险分析"],
            )

        stock_code = input_data.stock_code
        stock_name = input_data.stock_name or cleaned.get("stock_info", {}).get("name", "")
        self.logger.info(f"开始风险分析 | {stock_code} {stock_name}")

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
                result = self._merge_llm_result(baseline, llm_result)
                runtime_mode = "llm"
            except Exception as exc:
                self.logger.warning(f"风险分析 LLM 不可用，回退规则结果 | {exc}")
                runtime_mode = "hybrid"

        result = self._normalize_result(result, baseline)

        level = result.get("overall_risk_level", "未知")
        score = result.get("risk_score", 0)
        fatal_count = len(result.get("fatal_risks", []))
        summary = f"风险等级: {level}({score}/10), 致命风险: {fatal_count}个"
        self.logger.info(f"风险分析完成 | {summary}")

        return AgentOutput(
            agent_name=self.agent_name,
            status=AgentStatus.SUCCESS,
            data={"risk": result},
            data_sources=["financials", "governance", "announcements", "policy_documents", "realtime"],
            confidence=0.75 if result.get("evidence_status") == "ok" else 0.45,
            summary=summary,
            execution_mode=runtime_mode,
            llm_invoked=llm_invoked,
            model_used=model_used if runtime_mode != "deterministic" else None,
        )

    def validate_output(self, output: AgentOutput) -> None:
        """校验风险分析输出"""
        if output.status != AgentStatus.SUCCESS:
            return

        risk = output.data.get("risk", {})
        errors = []

        level = risk.get("overall_risk_level")
        if level not in ("低", "中", "高", "极高"):
            errors.append(f"overall_risk_level无效: {level}")

        score = risk.get("risk_score")
        if score is None or not isinstance(score, (int, float)):
            errors.append("缺少有效的risk_score")
        elif not (0 <= score <= 10):
            errors.append(f"risk_score超出范围: {score}")

        risks = risk.get("risks", [])
        if risk.get("evidence_status") == "ok" and (not isinstance(risks, list) or len(risks) < 4):
            errors.append(f"risks不足: 需要>=6项(覆盖6大类)，至少4项，实际{len(risks) if isinstance(risks, list) else '非列表'}")

        # 检查风险类别覆盖
        if isinstance(risks, list):
            categories = set(r.get("category", "") for r in risks if isinstance(r, dict))
            required = {"行业", "经营", "财务", "治理", "市场", "政策"}
            missing = required - categories
            if len(missing) > 2:
                errors.append(f"风险类别覆盖不足，缺少: {missing}")

        scenarios = risk.get("scenarios", [])
        if not isinstance(scenarios, list) or len(scenarios) < 3:
            errors.append(f"scenarios不足: 需要乐观/中性/悲观3种，实际{len(scenarios) if isinstance(scenarios, list) else '非列表'}")

        monitors = risk.get("monitoring_points", [])
        if not isinstance(monitors, list) or len(monitors) < 2:
            errors.append(f"monitoring_points不足: 需要>=3个，至少2个")

        if not risk.get("conclusion"):
            errors.append("缺少conclusion")

        if errors:
            from investresearch.core.exceptions import AgentValidationError
            raise AgentValidationError(self.agent_name, errors)

    # ================================================================
    # 内部方法
    # ================================================================

    def _get_model(self) -> str:
        """获取分析层模型"""
        return self.config.get_layer_model("analysis_layer", task="risk")

    def _build_result(self, context: dict[str, Any]) -> dict[str, Any]:
        cleaned = context.get("cleaned_data", {})
        screening = context.get("screening", {})
        financials = cleaned.get("financials", [])
        realtime = cleaned.get("realtime", {})
        stock_info = cleaned.get("stock_info", {})
        announcements = cleaned.get("announcements", [])
        policy_documents = cleaned.get("policy_documents", [])
        cross_verification = cleaned.get("cross_verification", {})

        latest_financial = financials[0] if financials and isinstance(financials[0], dict) else {}
        governance_profile = get_module_profile(cleaned, "governance")
        policy_profile = get_module_profile(cleaned, "policy_documents")
        financial_profile = get_module_profile(cleaned, "financials")
        industry_profile = get_module_profile(cleaned, "industry_enhanced")
        valuation_profile = get_module_profile(cleaned, "valuation_percentile")
        cross_profile = get_module_profile(cleaned, "cross_verification")

        risks = [
            self._industry_risk(industry_profile, cleaned.get("industry_enhanced", {})),
            self._operating_risk(stock_info, latest_financial, announcements),
            self._financial_risk(latest_financial, financial_profile.completeness, cross_verification),
            self._governance_risk(governance_profile, cleaned.get("governance", {})),
            self._market_risk(realtime, cleaned.get("valuation_percentile", {}), valuation_profile.completeness),
            self._policy_risk(policy_documents, policy_profile.completeness),
        ]
        risks = [risk for risk in risks if risk]

        risk_score = self._calculate_risk_score(risks, screening, cross_verification)
        overall_risk_level = self._risk_level_from_score(risk_score)
        current_price = self._safe_float(realtime.get("close"))
        scenarios = self._build_scenarios(current_price, overall_risk_level)

        monitoring_points = self._build_monitoring_points(risks, stock_info, latest_financial, cross_verification)
        fatal_risks = [
            risk["risk_name"]
            for risk in risks
            if risk.get("severity") == "高" and risk.get("probability") in {"中", "高"}
        ][:3]
        evidence_refs = merge_evidence_refs(
            financial_profile.evidence_refs,
            governance_profile.evidence_refs,
            policy_profile.evidence_refs,
            cross_profile.evidence_refs,
            get_module_profile(cleaned, "announcements").evidence_refs,
        )
        divergent_metrics = list(cross_verification.get("divergent_metrics", []) or [])
        evidence_status = "ok" if financial_profile.completeness >= 0.4 and not divergent_metrics else "partial"

        conclusion = (
            f"当前风险等级为{overall_risk_level}，需重点跟踪 {monitoring_points[0]}。"
            if monitoring_points
            else f"当前风险等级为{overall_risk_level}。"
        )
        if divergent_metrics:
            conclusion += f" 另需优先复核多源分歧指标: {', '.join(divergent_metrics[:4])}。"

        return {
            "overall_risk_level": overall_risk_level,
            "risk_score": risk_score,
            "risks": risks,
            "scenarios": scenarios,
            "fatal_risks": fatal_risks or ["关键资料缺失导致投资逻辑暂无充分验证"],
            "monitoring_points": monitoring_points,
            "conclusion": conclusion,
            "evidence_status": evidence_status,
            "missing_fields": sorted(
                set(
                    governance_profile.missing_fields
                    + financial_profile.missing_fields
                    + industry_profile.missing_fields
                    + valuation_profile.missing_fields
                    + cross_profile.missing_fields
                )
            ),
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }

    @staticmethod
    def _industry_risk(profile: Any, industry_enhanced: dict[str, Any]) -> dict[str, Any]:
        direction = str(industry_enhanced.get("industry_change_pct") or "")
        if profile.completeness < 0.4:
            return {
                "category": "行业",
                "risk_name": "行业景气度证据不足",
                "severity": "中",
                "probability": "中",
                "impact": "缺少稳定行业规模、增速和集中度数据库，行业判断需持续补证。",
                "mitigation": "补齐行业数据库并复核行业景气指标。",
            }
        return {
            "category": "行业",
            "risk_name": "行业景气波动",
            "severity": "中" if "-" in direction else "低",
            "probability": "中",
            "impact": f"行业高频涨跌幅={direction or '待验证'}，景气方向可能变化。",
            "mitigation": "持续跟踪行业涨跌幅、政策和龙头表现。",
        }

    @staticmethod
    def _operating_risk(stock_info: dict[str, Any], latest_financial: dict[str, Any], announcements: list[dict[str, Any]]) -> dict[str, Any]:
        revenue_yoy = latest_financial.get("revenue_yoy")
        if revenue_yoy is not None and float(revenue_yoy) < 0:
            return {
                "category": "经营",
                "risk_name": "收入增长承压",
                "severity": "中",
                "probability": "中",
                "impact": f"最新营收增速为 {revenue_yoy}，经营兑现存在压力。",
                "mitigation": "跟踪订单、产品价格和季度收入改善情况。",
            }
        if not stock_info.get("main_business") or not announcements:
            return {
                "category": "经营",
                "risk_name": "经营结构待验证",
                "severity": "中",
                "probability": "高",
                "impact": "主营业务或经营公告抽取不足，难以完全确认盈利驱动。",
                "mitigation": "补齐定期报告结构化抽取并复核主业描述。",
            }
        return {
            "category": "经营",
            "risk_name": "经营执行风险",
            "severity": "低",
            "probability": "中",
            "impact": "当前经营结构已有基础信息，但仍需持续跟踪执行与订单兑现。",
            "mitigation": "按季度复核财报和经营公告。",
        }

    @staticmethod
    def _financial_risk(
        latest_financial: dict[str, Any],
        completeness: float,
        cross_verification: dict[str, Any],
    ) -> dict[str, Any]:
        debt_ratio = RiskAgent._safe_float(latest_financial.get("debt_ratio"))
        operating_cashflow = RiskAgent._safe_float(latest_financial.get("operating_cashflow"))
        suspect_fields = list(latest_financial.get("_cashflow_suspect_fields", []) or [])
        divergent_metrics = set(cross_verification.get("divergent_metrics", []) or [])
        key_divergent_metrics = [
            metric
            for metric in ("revenue", "net_profit", "operating_cashflow", "market_cap", "pe_ttm", "pb_mrq")
            if metric in divergent_metrics
        ]
        if completeness < 0.4:
            return {
                "category": "财务",
                "risk_name": "财务证据不足",
                "severity": "中",
                "probability": "中",
                "impact": "现金流、商誉、负债等关键字段未充分覆盖。",
                "mitigation": "补齐财务深水区字段并重新评估。",
            }
        if suspect_fields:
            return {
                "category": "财务",
                "risk_name": "现金流口径待验证",
                "severity": "中",
                "probability": "高",
                "impact": f"字段 {', '.join(suspect_fields)} 疑似存在量纲异常，当前现金流质量判断不可靠。",
                "mitigation": "优先以审计口径或原始财报附注复核现金流单位与净现比。",
            }
        if key_divergent_metrics:
            return {
                "category": "财务",
                "risk_name": "关键指标多源分歧",
                "severity": "高",
                "probability": "高",
                "impact": f"多源交叉验证发现 {', '.join(key_divergent_metrics)} 存在分歧，当前财务与估值判断可信度明显下降。",
                "mitigation": "回溯官方财报、行情源和衍生计算口径，确认最新可用的一致值后再给出判断。",
            }
        if debt_ratio is not None and debt_ratio >= 65:
            severity = "高"
        else:
            severity = "中" if operating_cashflow is not None and operating_cashflow < 0 else "低"
        return {
            "category": "财务",
            "risk_name": "杠杆与现金流风险",
            "severity": severity,
            "probability": "中",
            "impact": f"资产负债率={debt_ratio if debt_ratio is not None else '待验证'}，经营现金流={operating_cashflow if operating_cashflow is not None else '待验证'}。",
            "mitigation": "重点跟踪净现比、自由现金流与债务结构。",
        }

    @staticmethod
    def _governance_risk(profile: Any, governance: dict[str, Any]) -> dict[str, Any]:
        if profile.completeness < 0.4:
            return {
                "category": "治理",
                "risk_name": "治理信息不足",
                "severity": "中",
                "probability": "高",
                "impact": "当前仅有部分实控人或股东资料，无法完成治理质量审计。",
                "mitigation": "补齐质押、诉讼、关联交易与资本配置记录。",
            }
        if governance.get("lawsuit_info") or governance.get("guarantee_info"):
            severity = "高"
        else:
            severity = "中" if governance.get("equity_pledge_ratio") else "低"
        return {
            "category": "治理",
            "risk_name": "控制权与治理事件风险",
            "severity": severity,
            "probability": "中",
            "impact": "需持续关注质押、担保、诉讼和管理层变动。",
            "mitigation": "监控治理公告和股东结构变化。",
        }

    @staticmethod
    def _market_risk(realtime: dict[str, Any], valuation_percentile: dict[str, Any], completeness: float) -> dict[str, Any]:
        pe_pct = valuation_percentile.get("pe_ttm_percentile")
        pb_pct = valuation_percentile.get("pb_mrq_percentile")
        if completeness < 0.4:
            return {
                "category": "市场",
                "risk_name": "估值锚不足",
                "severity": "中",
                "probability": "中",
                "impact": "估值分位与历史估值锚点覆盖不足，市场风险需谨慎处理。",
                "mitigation": "补齐历史估值与可比口径。",
            }
        avg_pct = sum(value for value in [pe_pct, pb_pct] if isinstance(value, (int, float))) / max(
            len([value for value in [pe_pct, pb_pct] if isinstance(value, (int, float))]),
            1,
        )
        severity = "高" if avg_pct >= 80 else "中" if avg_pct >= 60 else "低"
        return {
            "category": "市场",
            "risk_name": "估值波动风险",
            "severity": severity,
            "probability": "中",
            "impact": f"当前PE/PB分位约为 {pe_pct}/{pb_pct}，市场情绪波动可能放大股价弹性。",
            "mitigation": "结合估值分位和价格波动设定仓位边界。",
        }

    @staticmethod
    def _policy_risk(policy_documents: list[dict[str, Any]], completeness: float) -> dict[str, Any]:
        if completeness < 0.4:
            return {
                "category": "政策",
                "risk_name": "政策覆盖不足",
                "severity": "中",
                "probability": "中",
                "impact": "缺少稳定政策原文库，政策扰动难以及时量化。",
                "mitigation": "补齐 gov.cn 与部委政策资料并建立增量跟踪。",
            }
        latest = policy_documents[0]
        return {
            "category": "政策",
            "risk_name": "政策节奏变化",
            "severity": "中",
            "probability": "中",
            "impact": f"最近政策资料《{latest.get('title', 'N/A')}》可能改变行业预期。",
            "mitigation": "跟踪政策发布时间窗和关键词变化。",
        }

    @staticmethod
    def _calculate_risk_score(
        risks: list[dict[str, Any]],
        screening: dict[str, Any],
        cross_verification: dict[str, Any],
    ) -> float:
        severity_map = {"低": 1.0, "中": 1.8, "高": 2.7}
        probability_map = {"低": 0.7, "中": 1.0, "高": 1.3}
        score = 0.0
        for risk in risks:
            score += severity_map.get(risk.get("severity", "中"), 1.8) * probability_map.get(risk.get("probability", "中"), 1.0)
        if screening.get("key_risks"):
            score += 0.8
        score += min(1.5, len(list(cross_verification.get("divergent_metrics", []) or [])) * 0.35)
        return round(min(score, 10.0), 2)

    @staticmethod
    def _risk_level_from_score(score: float) -> str:
        if score >= 8:
            return "极高"
        if score >= 6:
            return "高"
        if score >= 3.5:
            return "中"
        return "低"

    @staticmethod
    def _build_scenarios(current_price: float | None, overall_risk_level: str) -> list[dict[str, Any]]:
        severity_factor = {"低": (1.18, 1.05, 0.9), "中": (1.12, 1.0, 0.85), "高": (1.08, 0.96, 0.8), "极高": (1.03, 0.92, 0.75)}
        optimistic_factor, base_factor, pessimistic_factor = severity_factor.get(overall_risk_level, (1.1, 1.0, 0.85))
        def _scenario(name: str, factor: float, probability: float, note: str) -> dict[str, Any]:
            target_price = round(current_price * factor, 2) if current_price is not None else None
            upside_pct = round((factor - 1) * 100, 2) if current_price is not None else None
            return {
                "scenario": name,
                "target_price": target_price,
                "upside_pct": upside_pct,
                "assumptions": [note, "该区间基于当前价格弹性带而非独立估值模型"],
                "probability": probability,
            }
        return [
            _scenario("乐观", optimistic_factor, 25.0, "经营兑现与风险缓解同步发生"),
            _scenario("中性", base_factor, 50.0, "经营按当前趋势延续"),
            _scenario("悲观", pessimistic_factor, 25.0, "核心风险触发或证据缺口未被补齐"),
        ]

    @staticmethod
    def _build_monitoring_points(
        risks: list[dict[str, Any]],
        stock_info: dict[str, Any],
        latest_financial: dict[str, Any],
        cross_verification: dict[str, Any],
    ) -> list[str]:
        points = [
            "行业景气度与政策关键词变化",
            "季度营收/净利润/经营现金流是否兑现",
            "治理事件、质押、担保、诉讼公告",
        ]
        divergent_metrics = list(cross_verification.get("divergent_metrics", []) or [])
        if divergent_metrics:
            points.insert(0, f"多源分歧指标复核: {', '.join(divergent_metrics[:4])}")
        if stock_info.get("main_business"):
            points.append(f"主营业务[{stock_info.get('main_business')[:18]}]的经营兑现")
        if latest_financial.get("debt_ratio") is not None:
            points.append(f"资产负债率是否继续偏离当前水平({latest_financial.get('debt_ratio')})")
        deduped: list[str] = []
        for item in points:
            if item not in deduped:
                deduped.append(item)
        return deduped[:5]

    def _build_prompt(self, stock_code: str, stock_name: str, context: dict[str, Any]) -> str:
        """构建风险分析提示词"""
        cleaned = context.get("cleaned_data", {})
        parts = [f"## 标的: {stock_code} {stock_name}\n"]

        # 公司基本信息
        info = cleaned.get("stock_info", {})
        if info:
            parts.append("### 公司基本信息")
            parts.append(f"- 行业: {info.get('industry_sw', 'N/A')}")
            parts.append(f"- 实控人: {info.get('actual_controller', 'N/A')}")
            parts.append(f"- 上市日期: {info.get('listing_date', 'N/A')}")
            parts.append("")

        # 财务数据（财务风险评估用）
        financials = cleaned.get("financials", [])
        if financials:
            parts.append("### 财务数据（风险识别）")
            parts.append("| 报告期 | 营收 | 净利润 | 营收增速 | 净利增速 | 毛利率 | ROE | 资产负债率 | 商誉/净资产 |")
            parts.append("|---|---|---|---|---|---|---|---|---|")
            for f in financials[:5]:
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('revenue'))} "
                    f"| {self._fmt(f.get('net_profit'))} "
                    f"| {self._fmt_pct(f.get('revenue_yoy'))} "
                    f"| {self._fmt_pct(f.get('net_profit_yoy'))} "
                    f"| {self._fmt_pct(f.get('gross_margin'))} "
                    f"| {self._fmt_pct(f.get('roe'))} "
                    f"| {self._fmt_pct(f.get('debt_ratio'))} "
                    f"| {self._fmt_pct(f.get('goodwill_ratio'))} |"
                )
            parts.append("")

            # 现金流风险
            parts.append("### 现金流分析")
            parts.append("| 报告期 | 经营现金流 | 投资现金流 | 筹资现金流 | 自由现金流 | 净现比 |")
            parts.append("|---|---|---|---|---|---|")
            for f in financials[:5]:
                if not isinstance(f, dict):
                    continue
                parts.append(
                    f"| {f.get('report_date', 'N/A')} "
                    f"| {self._fmt(f.get('operating_cashflow'))} "
                    f"| {self._fmt(f.get('investing_cashflow'))} "
                    f"| {self._fmt(f.get('financing_cashflow'))} "
                    f"| {self._fmt(f.get('free_cashflow'))} "
                    f"| {f.get('cash_to_profit', 'N/A')} |"
                )
            parts.append("")

        cross_verification = cleaned.get("cross_verification", {})
        if cross_verification and cross_verification.get("verified_metrics"):
            parts.append("### 多来源交叉验证")
            parts.append(f"- 整体置信度: {cross_verification.get('overall_confidence', 0):.0%}")
            if cross_verification.get("summary"):
                parts.append(f"- 总结: {cross_verification.get('summary')}")
            for metric in cross_verification.get("verified_metrics", [])[:5]:
                if not isinstance(metric, dict):
                    continue
                parts.append(
                    f"- {metric.get('metric_name', 'N/A')}: "
                    f"consistency={metric.get('consistency_flag', 'N/A')} | "
                    f"recommended={metric.get('recommended_value', 'N/A')} | "
                    f"sources={', '.join(metric.get('sources', [])[:4]) or 'N/A'}"
                )
            if cross_verification.get("divergent_metrics"):
                parts.append(f"- 需重点复核: {', '.join(cross_verification.get('divergent_metrics', [])[:5])}")
            parts.append("")

        # 当前估值（市场风险评估用）
        realtime = cleaned.get("realtime", {})
        if realtime:
            parts.append("### 当前估值与市值")
            parts.append(f"- 最新价: {realtime.get('close', 'N/A')}")
            parts.append(f"- PE(TTM): {realtime.get('pe_ttm', 'N/A')}")
            parts.append(f"- PB(MRQ): {realtime.get('pb_mrq', 'N/A')}")
            parts.append(f"- 总市值: {self._fmt_cap(realtime.get('market_cap'))}")
            parts.append("")

        # 上游分析结论
        screening = context.get("screening", {})
        if screening:
            parts.append(f"### 初筛结论: {screening.get('verdict', 'N/A')}")
            key_risks = screening.get("key_risks", [])
            if key_risks:
                parts.append(f"- 初筛风险: {', '.join(key_risks[:5])}")
            parts.append("")

        financial_analysis = context.get("financial_analysis", {})
        if financial_analysis:
            anomaly_flags = financial_analysis.get("anomaly_flags", [])
            if anomaly_flags:
                parts.append(f"### 财务异常标记: {', '.join(anomaly_flags[:5])}")
            parts.append("")

        valuation_analysis = context.get("valuation_analysis", {})
        if valuation_analysis:
            parts.append(f"### 估值结论: {valuation_analysis.get('valuation_level', 'N/A')}")
            parts.append(f"- 合理区间: {valuation_analysis.get('reasonable_range_low', 'N/A')}-{valuation_analysis.get('reasonable_range_high', 'N/A')}")
            parts.append("")

        announcements = cleaned.get("announcements", [])
        if announcements:
            parts.append("### 公告原文风险线索")
            for item in announcements[:4]:
                excerpt = item.get("excerpt") or "；".join(item.get("highlights", [])[:2])
                parts.append(
                    f"- {item.get('announcement_date', 'N/A')} {item.get('title', 'N/A')}: {str(excerpt)[:180]}"
                )
            parts.append("")

        policy_documents = cleaned.get("policy_documents", [])
        if policy_documents:
            parts.append("### 政策原文线索")
            for item in policy_documents[:4]:
                excerpt = item.get("excerpt") or item.get("summary") or ""
                parts.append(
                    f"- {item.get('policy_date', 'N/A')} {item.get('issuing_body', item.get('source', 'gov.cn'))}: {item.get('title', 'N/A')} - {str(excerpt)[:180]}"
                )
            parts.append("")

        parts.append("请根据以上数据对该标的进行全维度风险分析和三情景测算，按指定JSON格式输出。必须覆盖行业/经营/财务/治理/市场/政策6大类风险。")
        return "\n".join(parts)

    def _merge_llm_result(self, baseline: dict[str, Any], llm_result: dict[str, Any]) -> dict[str, Any]:
        merged = dict(baseline)

        level = self._normalize_level(llm_result.get("overall_risk_level"))
        if level:
            merged["overall_risk_level"] = level

        score = self._safe_float(llm_result.get("risk_score"))
        if score is not None:
            merged["risk_score"] = round(max(0.0, min(score, 10.0)), 2)

        risks = self._normalize_risks(llm_result.get("risks"))
        if risks:
            merged["risks"] = risks

        scenarios = self._normalize_scenarios(llm_result.get("scenarios"))
        if scenarios:
            merged["scenarios"] = scenarios

        fatal_risks = self._normalize_text_list(llm_result.get("fatal_risks"), limit=4)
        if fatal_risks:
            merged["fatal_risks"] = fatal_risks

        monitoring_points = self._normalize_text_list(llm_result.get("monitoring_points"), limit=5)
        if monitoring_points:
            merged["monitoring_points"] = monitoring_points

        conclusion = str(llm_result.get("conclusion") or "").strip()
        if conclusion:
            merged["conclusion"] = conclusion

        return merged

    def _normalize_result(self, result: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(result)
        normalized["risks"] = self._normalize_risks(normalized.get("risks")) or baseline.get("risks", [])
        normalized["risk_score"] = self._safe_float(normalized.get("risk_score"))
        if normalized["risk_score"] is None:
            normalized["risk_score"] = baseline.get("risk_score", 5.0)
        normalized["risk_score"] = round(max(0.0, min(float(normalized["risk_score"]), 10.0)), 2)
        normalized["overall_risk_level"] = (
            self._normalize_level(normalized.get("overall_risk_level"))
            or self._risk_level_from_score(normalized["risk_score"])
        )
        normalized["scenarios"] = self._normalize_scenarios(normalized.get("scenarios")) or baseline.get("scenarios", [])
        normalized["fatal_risks"] = (
            self._normalize_text_list(normalized.get("fatal_risks"), limit=4) or baseline.get("fatal_risks", [])
        )
        normalized["monitoring_points"] = (
            self._normalize_text_list(normalized.get("monitoring_points"), limit=5)
            or baseline.get("monitoring_points", [])
        )
        if baseline.get("evidence_status") == "ok" and len(normalized["risks"]) < 4:
            normalized["risks"] = baseline.get("risks", [])
        if self._missing_risk_categories(normalized["risks"]) > 2:
            normalized["risks"] = baseline.get("risks", [])
        if len(normalized["scenarios"]) < 3:
            normalized["scenarios"] = baseline.get("scenarios", [])
        if len(normalized["monitoring_points"]) < 2:
            normalized["monitoring_points"] = baseline.get("monitoring_points", [])
        normalized["conclusion"] = str(normalized.get("conclusion") or baseline.get("conclusion") or "").strip()
        normalized["evidence_status"] = baseline.get("evidence_status", "partial")
        normalized["missing_fields"] = list(baseline.get("missing_fields", []) or [])
        normalized["evidence_refs"] = list(baseline.get("evidence_refs", []) or [])
        return normalized

    @staticmethod
    def _normalize_level(value: Any) -> str:
        text = str(value or "").strip()
        aliases = {"低风险": "低", "中风险": "中", "高风险": "高", "极高风险": "极高"}
        text = aliases.get(text, text)
        return text if text in {"低", "中", "高", "极高"} else ""

    @staticmethod
    def _normalize_risks(value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        normalized: list[dict[str, Any]] = []
        category_aliases = {
            "行业风险": "行业",
            "经营风险": "经营",
            "财务风险": "财务",
            "治理风险": "治理",
            "市场风险": "市场",
            "政策风险": "政策",
        }
        for item in value[:8]:
            if not isinstance(item, dict):
                continue
            category = category_aliases.get(str(item.get("category") or "").strip(), str(item.get("category") or "").strip())
            if category not in {"行业", "经营", "财务", "治理", "市场", "政策"}:
                continue
            severity = str(item.get("severity") or "中").strip()
            probability = str(item.get("probability") or "中").strip()
            if severity not in {"低", "中", "高"}:
                severity = "中"
            if probability not in {"低", "中", "高"}:
                probability = "中"
            risk_name = str(item.get("risk_name") or "").strip()
            if not risk_name:
                continue
            normalized.append(
                {
                    "category": category,
                    "risk_name": risk_name,
                    "severity": severity,
                    "probability": probability,
                    "impact": str(item.get("impact") or "待验证").strip(),
                    "mitigation": str(item.get("mitigation") or "持续跟踪并补充证据").strip(),
                }
            )
        return normalized

    def _normalize_scenarios(self, value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        normalized: list[dict[str, Any]] = []
        aliases = {"乐观情景": "乐观", "中性情景": "中性", "悲观情景": "悲观"}
        for item in value:
            if not isinstance(item, dict):
                continue
            scenario = aliases.get(str(item.get("scenario") or "").strip(), str(item.get("scenario") or "").strip())
            if scenario not in {"乐观", "中性", "悲观"}:
                continue
            target_price = self._safe_float(item.get("target_price"))
            upside_pct = self._safe_float(item.get("upside_pct"))
            probability = self._safe_float(item.get("probability"))
            assumptions = self._normalize_text_list(item.get("assumptions"), limit=4) or ["关键假设待验证"]
            normalized.append(
                {
                    "scenario": scenario,
                    "target_price": target_price,
                    "upside_pct": upside_pct,
                    "assumptions": assumptions,
                    "probability": probability if probability is not None else {"乐观": 25.0, "中性": 50.0, "悲观": 25.0}[scenario],
                }
            )
        order = {"乐观": 0, "中性": 1, "悲观": 2}
        normalized.sort(key=lambda item: order.get(item["scenario"], 99))
        return normalized

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
    def _missing_risk_categories(risks: list[dict[str, Any]]) -> int:
        categories = {str(item.get("category") or "") for item in risks if isinstance(item, dict)}
        required = {"行业", "经营", "财务", "治理", "市场", "政策"}
        return len(required - categories)

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

    @staticmethod
    def _safe_float(v: Any) -> float | None:
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
