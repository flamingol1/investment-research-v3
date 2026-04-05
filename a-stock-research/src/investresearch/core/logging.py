"""结构化日志系统 - Rich console handler"""

import logging
import sys
from pathlib import Path

from rich.logging import RichHandler


def setup_logging(log_level: str = "INFO", log_dir: str | None = None) -> None:
    """初始化日志系统

    Args:
        log_level: 日志级别
        log_dir: 日志文件目录，None则仅输出到控制台
    """
    root_logger = logging.getLogger("investresearch")
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # 清除已有handler（防止重复添加）
    root_logger.handlers.clear()

    # Rich控制台输出
    console_handler = RichHandler(
        show_time=True,
        show_path=False,
        markup=True,
        rich_tracebacks=True,
    )
    console_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)

    # 文件输出
    if log_dir:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(
            log_path / "investresearch.log", encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # 降低第三方库日志级别
    for noisy in ["httpx", "httpcore", "urllib3", "asyncio", "chromadb"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """获取模块日志器

    Args:
        name: 模块名，如 "config", "llm", "collector"

    Returns:
        Logger实例
    """
    return logging.getLogger(f"investresearch.{name}")
