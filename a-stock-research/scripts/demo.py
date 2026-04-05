"""端到端验证脚本

Usage:
    python scripts/demo.py 300358
    python scripts/demo.py 300358 --dry-run
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

# 将项目src加入path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from investresearch.core.config import Config
from investresearch.core.logging import setup_logging, get_logger
from investresearch.core.llm import llm_router
from investresearch.core.exceptions import LLMError


async def main():
    parser = argparse.ArgumentParser(description="A股投研系统 Demo验证")
    parser.add_argument("stock", help="股票代码或名称，如 300358 或 湖南裕能")
    parser.add_argument("--dry-run", action="store_true", help="跳过LLM调用，仅验证框架")
    args = parser.parse_args()

    setup_logging(log_level="INFO")
    logger = get_logger("demo")

    logger.info("=" * 60)
    logger.info("A股投研系统 Phase 1 验证")
    logger.info("=" * 60)

    # Step 1: 验证配置加载
    logger.info("\n[Step 1] 验证配置加载...")
    config = Config()
    logger.info(f"  系统: {config.get('system.name')}")
    logger.info(f"  版本: {config.get('system.version')}")
    logger.info("  ✓ 配置加载成功")

    # Step 2: 验证模型别名
    logger.info("\n[Step 2] 验证模型别名...")
    from investresearch.core.llm import MODEL_ALIASES, LAYER_DEFAULTS
    logger.info(f"  可用模型: {list(MODEL_ALIASES.keys())}")
    logger.info(f"  层级默认: {LAYER_DEFAULTS}")
    logger.info("  ✓ 模型别名解析正常")

    # Step 3: 验证异常体系
    logger.info("\n[Step 3] 验证异常体系...")
    from investresearch.core.exceptions import (
        InvestResearchError, ConfigurationError, LLMError,
        AgentError, AgentValidationError, DataCollectionError,
    )
    try:
        raise AgentValidationError("demo", ["测试异常"])
    except AgentValidationError as e:
        logger.info(f"  ✓ 异常捕获正常: {e}")
    logger.info("  ✓ 异常层次正确")

    # Step 4: 验证Pydantic模型
    logger.info("\n[Step 4] 验证Pydantic模型...")
    from investresearch.core.models import (
        AgentInput, AgentOutput, AgentStatus, ResearchRequest, ResearchState,
    )
    agent_out = AgentOutput(
        agent_name="test",
        status=AgentStatus.SUCCESS,
        data={"stock_name": "湖南裕能"},
        data_sources=["akshare"],
        confidence=0.8,
    )
    dumped = agent_out.model_dump()
    logger.info(f"  ✓ Pydantic序列化正常: {dumped['agent_name']}")

    # Step 5: 验证Agent基类
    logger.info("\n[Step 5] 验证Agent基类...")
    from investresearch.data_layer.agents.demo import DemoAgent
    demo_agent = DemoAgent()
    logger.info(f"  Agent: {demo_agent.agent_name}")
    logger.info("  ✓ Agent基类实例化成功")

    # Step 6: 调用LLM (如果非dry-run)
    if not args.dry_run:
        logger.info("\n[Step 6] 调用LLM...")
        try:
            result = await llm_router.call_json(
                prompt=f"请简要介绍股票 {args.stock} 的基本信息，包括公司全称、主营业务、所属行业。",
                system_prompt=(
                    "你是一个专业的A股投研助手。请用JSON格式回答。\n"
                    '输出格式: {"stock_name": "公司全称", "main_business": "主营业务", "industry": "行业"}'
                ),
                model=config.get_layer_model("data_layer"),
            )
            logger.info("  ✓ LLM调用成功")
            print(json.dumps(result, ensure_ascii=False, indent=2))
        except LLMError as e:
            logger.warning(f"  ⚠ LLM调用失败(可能API Key未配置): {e}")
            logger.info("  请设置 .env 文件中的 API Key 后重试")
    else:
        logger.info("\n[Step 6] 跳过LLM调用 (dry-run模式)")

    dry_run_flag = args.dry_run
    logger.info(f"\n验证完成! 标的: {args.stock} (dry-run={dry_run_flag})")

    # 清理
    await llm_router.close()
if __name__ == "__main__":
    asyncio.run(main())
