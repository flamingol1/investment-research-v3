"""采集任务类型定义"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TaskTypeDefinition:
    """采集任务类型定义"""
    name: str           # 类型标识
    display_name: str   # 显示名称
    description: str    # 描述
    category: str       # 大类: basic/market/fundamental/alternative


TASK_TYPE_DEFINITIONS: dict[str, TaskTypeDefinition] = {
    "stock_info": TaskTypeDefinition(
        name="stock_info",
        display_name="股票基础信息",
        description="股票名称、交易所、行业分类、上市日期等",
        category="basic",
    ),
    "daily_prices": TaskTypeDefinition(
        name="daily_prices",
        display_name="历史日线行情",
        description="前复权日线OHLCV数据，默认3年",
        category="market",
    ),
    "realtime_quote": TaskTypeDefinition(
        name="realtime_quote",
        display_name="实时行情",
        description="实时价格、涨跌幅、PE/PB等",
        category="market",
    ),
    "financials": TaskTypeDefinition(
        name="financials",
        display_name="财务报表",
        description="利润表/资产负债表/现金流量表关键指标",
        category="fundamental",
    ),
    "valuation": TaskTypeDefinition(
        name="valuation",
        display_name="估值数据",
        description="PE(TTM)/PB(MRQ)历史数据",
        category="fundamental",
    ),
    "announcements": TaskTypeDefinition(
        name="announcements",
        display_name="公告披露",
        description="公司公告、问询函等",
        category="fundamental",
    ),
    "governance": TaskTypeDefinition(
        name="governance",
        display_name="公司治理",
        description="股权质押、担保、诉讼、高管变动等",
        category="fundamental",
    ),
    "research_reports": TaskTypeDefinition(
        name="research_reports",
        display_name="研报摘要",
        description="券商研报评级和核心观点",
        category="alternative",
    ),
    "shareholders": TaskTypeDefinition(
        name="shareholders",
        display_name="股东数据",
        description="十大股东、基金持仓、股东户数等",
        category="fundamental",
    ),
    "industry": TaskTypeDefinition(
        name="industry",
        display_name="行业数据",
        description="申万行业分类、行业板块行情等",
        category="alternative",
    ),
    "valuation_pct": TaskTypeDefinition(
        name="valuation_pct",
        display_name="估值分位",
        description="PE/PB历史分位数据",
        category="fundamental",
    ),
    "news": TaskTypeDefinition(
        name="news",
        display_name="新闻资讯",
        description="个股相关新闻和要闻",
        category="alternative",
    ),
}


# "all" 类型展开为所有具体类型
ALL_DATA_TYPES = list(TASK_TYPE_DEFINITIONS.keys())
