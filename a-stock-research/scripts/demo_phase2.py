"""Phase 2 验证脚本 - 数据采集 + 清洗完整链路

Usage:
    python scripts/demo_phase2.py 300358
    python scripts/demo_phase2.py 600519
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from investresearch.core.config import Config
from investresearch.core.logging import setup_logging, get_logger
from investresearch.core.models import AgentInput


async def main():
    parser = argparse.ArgumentParser(description="A股投研系统 Phase 2 数据层验证")
    parser.add_argument("stock", help="股票代码，如 300358 或 600519")
    args = parser.parse_args()

    setup_logging(log_level="INFO")
    config = Config()
    logger = get_logger("demo_phase2")

    stock_code = args.stock
    logger.info("=" * 60)
    logger.info(f"Phase 2 验证 | 标的: {stock_code}")
    logger.info("=" * 60)

    # ============================================================
    # Step 1: 数据采集
    # ============================================================
    logger.info("\n[Step 1] 数据采集...")
    from investresearch.data_layer.collector import DataCollectorAgent

    collector = DataCollectorAgent()
    agent_input = AgentInput(stock_code=stock_code)

    collect_output = await collector.safe_run(agent_input)

    logger.info(f"采集状态: {collect_output.data.get('collection_status', {})}")
    logger.info(f"覆盖率: {collect_output.data.get('coverage_ratio', 0):.0%}")
    logger.info(f"摘要: {collect_output.summary}")

    # 打印各数据类型采集结果
    raw = collect_output.data
    prices = raw.get("prices", [])
    financials = raw.get("financials", [])
    valuation = raw.get("valuation", [])

    logger.info(f"  行情数据: {len(prices)} 条")
    logger.info(f"  财务数据: {len(financials)} 期")
    logger.info(f"  估值数据: {len(valuation)} 条")

    stock_info = raw.get("stock_info")
    if stock_info:
        logger.info(f"  股票名称: {stock_info.get('name', 'N/A')}")

    # ============================================================
    # Step 2: 数据清洗
    # ============================================================
    logger.info("\n[Step 2] 数据清洗...")
    from investresearch.data_layer.cleaner import DataCleanerAgent

    cleaner = DataCleanerAgent()
    clean_input = AgentInput(
        stock_code=stock_code,
        context={"raw_data": raw},
    )

    clean_output = await cleaner.safe_run(clean_input)

    logger.info(f"清洗状态: {clean_output.status}")
    logger.info(f"清洗摘要: {clean_output.summary}")

    cleaned = clean_output.data.get("cleaned", {})
    warnings = clean_output.data.get("warnings", [])

    if warnings:
        logger.warning(f"清洗警告: {warnings}")

    logger.info(f"  清洗后覆盖率: {cleaned.get('coverage_ratio', 0):.0%}")

    # ============================================================
    # 输出结果摘要
    # ============================================================
    logger.info("\n" + "=" * 60)
    logger.info("验证结果汇总")
    logger.info("=" * 60)

    coverage = raw.get("coverage_ratio", 0)
    passed = coverage >= 0.6  # 至少3/5数据类型成功

    if passed:
        logger.info(f"✓ Phase 2 验证通过 | 覆盖率={coverage:.0%}")
    else:
        logger.warning(f"⚠ Phase 2 覆盖率不足 | 覆盖率={coverage:.0%} | 需要检查数据源")

    # 输出JSON
    print(json.dumps({
        "stock_code": stock_code,
        "collection_status": raw.get("collection_status", {}),
        "coverage_ratio": coverage,
        "data_counts": {
            "prices": len(prices),
            "financials": len(financials),
            "valuation": len(valuation),
            "has_stock_info": stock_info is not None,
        },
        "clean_warnings": warnings,
        "cleaned_coverage": cleaned.get("coverage_ratio", 0),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
