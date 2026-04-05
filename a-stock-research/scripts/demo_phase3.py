"""Phase 3 验证脚本 - 数据采集+清洗+分析 完整链路

Usage:
    python scripts/demo_phase3.py 300358
    python scripts/demo_phase3.py 600519
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from investresearch.core.config import Config
from investresearch.core.logging import setup_logging, get_logger
from investresearch.core.models import AgentInput, AgentStatus


async def main():
    parser = argparse.ArgumentParser(description="A股投研系统 Phase 3 分析层验证")
    parser.add_argument("stock", help="股票代码，如 300358 或 600519")
    args = parser.parse_args()

    setup_logging(log_level="INFO")
    config = Config()
    logger = get_logger("demo_phase3")

    stock_code = args.stock
    logger.info("=" * 60)
    logger.info(f"Phase 3 验证 | 标的: {stock_code}")
    logger.info("=" * 60)

    # ============================================================
    # Step 1: 数据采集
    # ============================================================
    logger.info("\n[Step 1] 数据采集...")
    from investresearch.data_layer.collector import DataCollectorAgent

    collector = DataCollectorAgent()
    collect_output = await collector.safe_run(AgentInput(stock_code=stock_code))

    logger.info(f"采集覆盖率: {collect_output.data.get('coverage_ratio', 0):.0%}")
    if collect_output.status != AgentStatus.SUCCESS:
        logger.error(f"数据采集失败: {collect_output.summary}")
        return

    # ============================================================
    # Step 2: 数据清洗
    # ============================================================
    logger.info("\n[Step 2] 数据清洗...")
    from investresearch.data_layer.cleaner import DataCleanerAgent

    cleaner = DataCleanerAgent()
    clean_output = await cleaner.safe_run(AgentInput(
        stock_code=stock_code,
        context={"raw_data": collect_output.data},
    ))

    logger.info(f"清洗状态: {clean_output.status}")
    cleaned_data = clean_output.data.get("cleaned", {})

    # ============================================================
    # Step 3: 初筛
    # ============================================================
    logger.info("\n[Step 3] 初筛排雷...")
    from investresearch.analysis_layer.screener import ScreenerAgent

    screener = ScreenerAgent()
    screen_output = await screener.safe_run(AgentInput(
        stock_code=stock_code,
        stock_name=collect_output.data.get("stock_info", {}).get("name"),
        context={"cleaned_data": cleaned_data},
    ))

    screening = screen_output.data.get("screening", {})
    verdict = screening.get("verdict", "未知")
    logger.info(f"初筛结论: {verdict}")

    if verdict == "刚性剔除":
        logger.warning(f"标的被剔除: {screening.get('recommendation', '')}")
        logger.info("跳过后续深度分析。")
        print(json.dumps({
            "stock_code": stock_code,
            "screening": screening,
            "status": "rejected",
        }, ensure_ascii=False, indent=2))
        return

    # ============================================================
    # Step 4: 财务分析
    # ============================================================
    logger.info("\n[Step 4] 财务分析...")
    from investresearch.analysis_layer.financial import FinancialAgent

    financial_agent = FinancialAgent()
    financial_output = await financial_agent.safe_run(AgentInput(
        stock_code=stock_code,
        stock_name=collect_output.data.get("stock_info", {}).get("name"),
        context={"cleaned_data": cleaned_data},
    ))

    financial_result = financial_output.data.get("financial", {})
    overall_score = financial_result.get("overall_score", "N/A")
    logger.info(f"财务综合评分: {overall_score}/10")

    # ============================================================
    # Step 5: 估值分析
    # ============================================================
    logger.info("\n[Step 5] 估值分析...")
    from investresearch.analysis_layer.valuation import ValuationAgent

    valuation_agent = ValuationAgent()
    valuation_output = await valuation_agent.safe_run(AgentInput(
        stock_code=stock_code,
        stock_name=collect_output.data.get("stock_info", {}).get("name"),
        context={
            "cleaned_data": cleaned_data,
            "financial_analysis": financial_result,
        },
    ))

    valuation_result = valuation_output.data.get("valuation", {})
    valuation_level = valuation_result.get("valuation_level", "N/A")
    price = valuation_result.get("current_price")
    low = valuation_result.get("reasonable_range_low")
    high = valuation_result.get("reasonable_range_high")
    logger.info(f"估值水平: {valuation_level}")
    if price and low and high:
        logger.info(f"合理区间: {low} - {high}, 当前: {price}")

    # ============================================================
    # 输出结果摘要
    # ============================================================
    logger.info("\n" + "=" * 60)
    logger.info("Phase 3 验证结果汇总")
    logger.info("=" * 60)
    logger.info(f"  初筛: {verdict}")
    logger.info(f"  财务评分: {overall_score}/10")
    logger.info(f"  估值水平: {valuation_level}")

    print(json.dumps({
        "stock_code": stock_code,
        "screening": {
            "verdict": verdict,
            "key_risks": screening.get("key_risks", []),
        },
        "financial": {
            "overall_score": overall_score,
            "anomaly_flags": financial_result.get("anomaly_flags", []),
            "conclusion": financial_result.get("conclusion", "")[:200],
        },
        "valuation": {
            "valuation_level": valuation_level,
            "current_price": price,
            "reasonable_range": [low, high],
            "methods_count": len(valuation_result.get("methods", [])),
        },
    }, ensure_ascii=False, indent=2))

    # 关闭LLM连接
    from investresearch.core.llm import llm_router
    await llm_router.close()


if __name__ == "__main__":
    asyncio.run(main())
