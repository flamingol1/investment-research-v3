"""CLI入口点 - 命令行交互界面

支持子命令:
  <stock>              默认研究模式（保持向后兼容）
  serve                启动Web API服务器
  update <stock>       增量更新
  update --track       批量动态跟踪
  watch add <stock>    添加到跟踪列表
  watch remove <stock> 从跟踪列表移除
  watch list           显示跟踪列表
  history <stock>      查看研究历史
  search <query>       语义搜索知识库
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

from .core.logging import setup_logging, get_logger


def main() -> None:
    """CLI入口"""
    parser = argparse.ArgumentParser(
        description="A股投研多Agent系统",
        prog="investresearch",
    )

    # 全局参数
    parser.add_argument("--debug", action="store_true", help="启用调试模式")

    # 子命令
    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # --- 研究命令（默认） ---
    research_parser = subparsers.add_parser("research", help="深度研究标的")
    research_parser.add_argument("stock", help="股票代码，如 300358")
    research_parser.add_argument(
        "--depth",
        choices=["quick", "standard", "deep"],
        default="standard",
        help="研究深度 (默认: standard)",
    )
    research_parser.add_argument(
        "--output-dir",
        default="output/reports",
        help="报告输出目录",
    )

    # --- 增量更新命令 ---
    update_parser = subparsers.add_parser("update", help="增量更新数据")
    update_parser.add_argument("stock", nargs="?", help="股票代码（不指定则跟踪全部）")
    update_parser.add_argument("--track", action="store_true", help="执行批量动态跟踪")

    # --- 跟踪列表命令 ---
    watch_parser = subparsers.add_parser("watch", help="管理跟踪列表")
    watch_sub = watch_parser.add_subparsers(dest="watch_action", help="跟踪列表操作")
    watch_sub.add_parser("list", help="显示跟踪列表")

    watch_add = watch_sub.add_parser("add", help="添加到跟踪列表")
    watch_add.add_argument("stock", help="股票代码")

    watch_remove = watch_sub.add_parser("remove", help="从跟踪列表移除")
    watch_remove.add_argument("stock", help="股票代码")

    # --- 历史查询命令 ---
    history_parser = subparsers.add_parser("history", help="查看研究历史")
    history_parser.add_argument("stock", help="股票代码")

    # --- 搜索命令 ---
    search_parser = subparsers.add_parser("search", help="语义搜索知识库")
    search_parser.add_argument("query", help="搜索关键词")
    search_parser.add_argument("--category", help="限制搜索分类")
    search_parser.add_argument("-n", "--num-results", type=int, default=5, help="返回结果数")

    # --- Web服务命令 ---
    serve_parser = subparsers.add_parser("serve", help="启动Web API服务器")
    serve_parser.add_argument("--host", default="0.0.0.0", help="监听地址")
    serve_parser.add_argument("--port", type=int, default=8000, help="监听端口")
    serve_parser.add_argument("--reload", action="store_true", help="开发模式（自动重载）")

    # 兼容旧模式：无子命令 + 位置参数
    parser.add_argument("stock_legacy", nargs="?", help=argparse.SUPPRESS)
    parser.add_argument("--depth", choices=["quick", "standard", "deep"], default="standard", help=argparse.SUPPRESS)
    parser.add_argument("--output-dir", default="output/reports", help=argparse.SUPPRESS)
    parser.add_argument("--demo", action="store_true", help="运行快速Demo模式（旧版）")

    args = parser.parse_args()

    # 初始化日志
    setup_logging(log_level="DEBUG" if args.debug else "INFO")
    logger = get_logger("cli")

    try:
        command = getattr(args, "command", None)

        # 兼容旧模式：无子命令但有位置参数 -> 默认研究模式
        if command is None and args.stock_legacy:
            args.stock = args.stock_legacy
            command = "research"

        if command == "research":
            asyncio.run(_run_research(args.stock, args.depth, args.output_dir))
        elif command == "update":
            asyncio.run(_run_update(args))
        elif command == "watch":
            _run_watch(args)
        elif command == "history":
            _run_history(args.stock)
        elif command == "search":
            _run_search(args)
        elif command == "serve":
            _run_serve(args)
        elif args.demo and args.stock_legacy:
            result = asyncio.run(_run_demo(args.stock_legacy, args.depth))
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            parser.print_help()

    except KeyboardInterrupt:
        logger.info("用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"执行失败: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def _print_progress(step: str, message: str) -> None:
    """CLI进度回调"""
    print(f"  [INFO] {message}")


# ================================================================
# 子命令实现
# ================================================================


async def _run_research(stock: str, depth: str, output_dir: str) -> None:
    """完整研究流程"""
    from .decision_layer.coordinator import ResearchCoordinator

    coordinator = ResearchCoordinator(progress_callback=_print_progress)
    report = await coordinator.run_research(stock, depth=depth)

    # 保存输出
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")

    if report.markdown:
        report_file = out_path / f"{stock}_{date_str}.md"
        report_file.write_text(report.markdown, encoding="utf-8")
        print(f"\n  报告: {report_file}")

    if report.conclusion:
        conclusion_file = out_path / f"{stock}_conclusion.json"
        conclusion_data = report.conclusion.model_dump(mode="json")
        conclusion_file.write_text(
            json.dumps(conclusion_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"  结论卡片: {conclusion_file}")

    _print_conclusion(report)
    _print_execution_summary(report)


async def _run_update(args: argparse.Namespace) -> None:
    """增量更新"""
    from .knowledge_base.chroma_store import ChromaKnowledgeStore
    from .knowledge_base.updater import IncrementalUpdaterAgent
    from .knowledge_base.tracker import DynamicTrackerAgent
    from .knowledge_base.watch_list import WatchListManager
    from .core.models import AgentInput

    if args.track:
        # 批量动态跟踪
        print("  [INFO] [动态跟踪] 开始批量跟踪检查...")
        tracker = DynamicTrackerAgent()
        output = await tracker.safe_run(AgentInput(stock_code="TRACK_ALL"))
        _print_tracking_results(output)
        return

    if not args.stock:
        print("  [ERROR] 请指定股票代码，或使用 --track 批量跟踪")
        return

    stock = args.stock
    print(f"  [INFO] [增量更新] 检查 {stock} 数据更新...")

    updater = IncrementalUpdaterAgent()
    output = await updater.safe_run(AgentInput(stock_code=stock))

    if output.status.value == "failed":
        print(f"  [ERROR] {output.summary}")
        for err in output.errors:
            print(f"    - {err}")
        return

    data = output.data
    changes = data.get("changes", {})
    duration = data.get("duration_seconds", 0)

    for key, count in changes.items():
        if isinstance(count, int) and count > 0:
            label = {"new_prices": "行情数据", "new_financials": "财报数据", "new_valuation": "估值数据"}.get(key, key)
            print(f"  [INFO] [增量更新] 新增{label}: {count}条")

    print(f"  [INFO] [增量更新] 更新完成 | 耗时: {duration:.1f}s")


def _run_watch(args: argparse.Namespace) -> None:
    """跟踪列表管理"""
    from .knowledge_base.watch_list import WatchListManager

    mgr = WatchListManager()

    action = getattr(args, "watch_action", None)
    if action == "add":
        if mgr.add(args.stock):
            print(f"  [INFO] 已添加 {args.stock} 到跟踪列表")
        else:
            print(f"  [INFO] {args.stock} 已在跟踪列表中")
        mgr.save()
    elif action == "remove":
        if mgr.remove(args.stock):
            print(f"  [INFO] 已从跟踪列表移除 {args.stock}")
        else:
            print(f"  [INFO] {args.stock} 不在跟踪列表中")
    elif action == "list":
        _print_watch_list(mgr)
    else:
        print("  用法: watch [add|remove|list]")


def _run_history(stock: str) -> None:
    """查看研究历史"""
    from .knowledge_base.chroma_store import ChromaKnowledgeStore

    store = ChromaKnowledgeStore()
    history = store.get_research_history(stock)

    if not history:
        print(f"  [INFO] {stock} 无研究历史记录")
        return

    print(f"  [INFO] {stock} 研究历史:")
    print(f"  {'日期':<12} {'深度':<10} {'建议':<16} {'风险':<6} {'价格':<8}")
    print(f"  {'-'*12} {'-'*10} {'-'*16} {'-'*6} {'-'*8}")

    for entry in history:
        date_str = entry.research_date.strftime("%Y-%m-%d") if hasattr(entry, "research_date") else str(entry.get("research_date", ""))
        rec = entry.recommendation or "N/A"
        risk = entry.risk_level or "N/A"
        price = f"{entry.current_price:.1f}" if entry.current_price else "N/A"
        depth = entry.depth if hasattr(entry, "depth") else "standard"
        print(f"  {date_str:<12} {depth:<10} {rec:<16} {risk:<6} {price:<8}")


def _run_search(args: argparse.Namespace) -> None:
    """语义搜索知识库"""
    from .knowledge_base.chroma_store import ChromaKnowledgeStore

    store = ChromaKnowledgeStore()
    results = store.search_similar(
        query=args.query,
        category=args.category,
        n=args.num_results,
    )

    if not results:
        print(f"  [INFO] 未找到与 '{args.query}' 相关的结果")
        return

    print(f"  [INFO] 搜索 '{args.query}' 结果 ({len(results)}条):")
    for i, r in enumerate(results, 1):
        meta = r.get("metadata", {})
        doc = r.get("document", "")
        distance = r.get("distance", 0)
        print(f"\n  {i}. [{meta.get('stock_code', '?')}] {meta.get('stock_name', '')}")
        print(f"     分类: {meta.get('category', '')} | 日期: {meta.get('date', '')}")
        print(f"     相似度: {1-distance:.2f}")
        print(f"     {doc[:200]}...")


# ================================================================
# 输出格式化
# ================================================================


def _print_conclusion(report: "ResearchReport") -> None:
    """打印投资结论"""
    if not report.conclusion:
        return
    c = report.conclusion
    print(f"\n  ─── 投资结论 ───")
    print(f"  建议: {c.recommendation}")
    print(f"  置信度: {c.confidence_level}")
    print(f"  风险等级: {c.risk_level}")
    if c.target_price_low and c.target_price_high:
        print(f"  目标价区间: {c.target_price_low} - {c.target_price_high}")
    if c.current_price:
        print(f"  当前价: {c.current_price}")
    if c.upside_pct is not None:
        print(f"  上行空间: {c.upside_pct:.1f}%")
    print(f"\n  {c.conclusion_summary}")


def _print_execution_summary(report: "ResearchReport") -> None:
    """打印执行摘要"""
    print(f"\n  ─── 执行摘要 ───")
    print(f"  已完成Agent: {', '.join(report.agents_completed)}")
    if report.agents_skipped:
        print(f"  跳过Agent: {', '.join(report.agents_skipped)}")
    if report.errors:
        print(f"  错误: {len(report.errors)}个")
        for err in report.errors[:3]:
            print(f"    - {err}")


def _print_watch_list(mgr: "WatchListManager") -> None:
    """打印跟踪列表"""
    items = mgr.get_all().items
    if not items:
        print("  [INFO] 跟踪列表为空")
        return

    print(f"  {'代码':<8} {'名称':<10} {'建议':<14} {'上次更新':<12} {'状态':<8}")
    print(f"  {'-'*8} {'-'*10} {'-'*14} {'-'*12} {'-'*8}")

    for item in items:
        updated = item.last_updated_at.strftime("%Y-%m-%d") if item.last_updated_at else "N/A"
        status_icon = {"normal": "正常", "warning": "预警", "critical": "严重"}.get(item.status, item.status)
        print(f"  {item.stock_code:<8} {item.stock_name:<10} {item.recommendation:<14} {updated:<12} {status_icon:<8}")


def _print_tracking_results(output: "AgentOutput") -> None:
    """打印动态跟踪结果"""
    alerts = output.data.get("alerts", [])
    checked = output.data.get("checked_count", 0)

    print(f"  [INFO] [动态跟踪] 检查 {checked} 个跟踪标的...")

    if not alerts:
        print(f"  [INFO] [动态跟踪] 全部正常")
        return

    for alert in alerts:
        severity = alert.get("severity", "info")
        icon = {"info": "INFO", "warning": "WARNING", "critical": "CRITICAL"}.get(severity, "INFO")
        code = alert.get("stock_code", "")
        name = alert.get("stock_name", "")
        msg = alert.get("message", "")
        print(f"  [{icon}] [{code}] {name} | {msg}")


async def _run_demo(stock: str, depth: str) -> dict:
    """Phase 1 最小Demo"""
    from .core.llm import llm_router

    logger = get_logger("cli")
    logger.info(f"开始分析 | stock={stock} | depth={depth}")

    system_prompt = (
        "你是一个专业的A股投研助手。请用JSON格式回答。\n"
        '输出格式: {"stock_name": "公司全称", "main_business": "主营业务描述", '
        '"industry": "所属行业", "listing_date": "上市日期", '
        '"market_cap": "当前市值（如知道）"}'
    )
    user_prompt = f"请简要介绍股票 {stock} 的基本信息。"

    result = await llm_router.call_json(
        prompt=user_prompt,
        system_prompt=system_prompt,
        model="qwen3-coder",
    )

    logger.info("分析完成")
    return result


def _run_serve(args: argparse.Namespace) -> None:
    """启动Web API服务器"""
    import uvicorn

    host = args.host
    port = args.port
    reload = args.reload

    print(f"  [INFO] 启动Web API服务器: http://{host}:{port}")
    print(f"  [INFO] API文档: http://{host}:{port}/docs")
    if reload:
        print(f"  [INFO] 开发模式（自动重载）")

    uvicorn.run(
        "investresearch.api.app:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )


if __name__ == "__main__":
    main()
