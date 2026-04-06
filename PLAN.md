# 重构计划：情报中心（Intelligence Hub）

## 一、需求重述

将现有项目中**数据采集层**（`data_layer/`）和**知识库**（`knowledge_base/`）独立出来，构建一个独立的"情报中心"子系统，具备以下核心能力：

1. **数据源管理** — 配置、启用/禁用、优先级排序、健康检测
2. **采集任务调度** — 定时采集、手动触发、增量更新、采集监控
3. **资料归档与知识库** — 将采集到的原始数据清洗后归档，构建可检索的知识库
4. **对外服务** — 通过 API 为分析层/决策层提供统一的数据查询接口

---

## 二、架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────┐
│                    情报中心 (intel-hub)               │
│                                                      │
│  ┌──────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ 数据源    │  │ 采集调度引擎  │  │  知识库引擎    │  │
│  │ 管理器    │  │ (Scheduler)  │  │ (Knowledge)   │  │
│  └────┬─────┘  └──────┬───────┘  └───────┬───────┘  │
│       │               │                  │           │
│  ┌────┴───────────────┴──────────────────┴───────┐  │
│  │              统一数据访问层 (Repository)         │  │
│  └───────────────────┬───────────────────────────┘  │
│                      │                               │
│  ┌───────────────────┴───────────────────────────┐  │
│  │           SQLite + ChromaDB 存储层              │  │
│  └───────────────────────────────────────────────┘  │
│                                                      │
│  ┌───────────────────────────────────────────────┐  │
│  │           FastAPI 对外服务接口                   │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
  ┌──────────────┐             ┌──────────────┐
  │ 分析层/决策层  │             │  前端管理界面  │
  │ (现有Agent)   │             │  (新建)       │
  └──────────────┘             └──────────────┘
```

### 2.2 关键设计决策

| 决策项 | 方案 | 理由 |
|--------|------|------|
| 数据库 | **SQLite** (通过 SQLAlchemy) | 单机部署、零运维、Python原生支持、足够处理投研数据量 |
| 任务调度 | **APScheduler** + 自定义调度器 | 轻量、支持 cron/interval/date 三种触发方式 |
| 知识库 | 保留 **ChromaDB** | 已有实现、向量检索满足需求 |
| 数据源适配 | **适配器模式(Adapter Pattern)** | 统一接口、易于扩展新数据源 |
| 对外接口 | **FastAPI** REST API | 已有基础设施、与现有系统一致 |
| 配置管理 | YAML + 数据库双存储 | 静态配置用 YAML，动态状态存 DB |

---

## 三、目录结构

在 `a-stock-research/src/investresearch/` 下新增 `intel_hub/` 包：

```
intel_hub/
├── __init__.py
├── config.py                    # 情报中心配置
├── models/                      # 数据模型
│   ├── __init__.py
│   ├── db_models.py             # SQLAlchemy ORM 模型
│   └── schemas.py               # Pydantic 请求/响应模型
├── sources/                     # 数据源适配器
│   ├── __init__.py
│   ├── base.py                  # 抽象基类 DataSourceAdapter
│   ├── akshare_adapter.py       # AKShare 适配器
│   ├── baostock_adapter.py      # BaoStock 适配器
│   ├── tushare_adapter.py       # Tushare 适配器
│   ├── sina_adapter.py          # Sina 财经适配器
│   └── registry.py              # 数据源注册中心
├── collectors/                  # 采集引擎
│   ├── __init__.py
│   ├── engine.py                # 采集调度引擎 (核心)
│   ├── tasks.py                 # 采集任务定义
│   └── monitor.py               # 采集监控 & 健康检查
├── knowledge/                   # 知识库引擎
│   ├── __init__.py
│   ├── store.py                 # 知识库存储 (整合 ChromaDB)
│   ├── archive.py               # 资料归档管理
│   ├── search.py                # 知识检索服务
│   └── indexer.py               # 知识索引构建
├── repository/                  # 数据访问层
│   ├── __init__.py
│   ├── database.py              # 数据库连接 & 初始化
│   ├── source_repo.py           # 数据源 CRUD
│   ├── collection_repo.py       # 采集记录 CRUD
│   └── archive_repo.py          # 归档资料 CRUD
├── api/                         # 对外 API
│   ├── __init__.py
│   ├── router.py                # 路由注册
│   ├── sources_api.py           # 数据源管理 API
│   ├── collection_api.py        # 采集任务 API
│   ├── knowledge_api.py         # 知识库查询 API
│   └── archive_api.py           # 归档资料 API
└── service.py                   # 情报中心门面 (Facade)
```

---

## 四、数据库设计

### 4.1 数据源表 (`intel_sources`)

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| name | TEXT UNIQUE | 数据源标识 (akshare/baostock/tushare) |
| display_name | TEXT | 显示名称 |
| description | TEXT | 描述 |
| enabled | BOOLEAN | 是否启用 |
| priority | INTEGER | 优先级 (1=最高) |
| config_json | TEXT | JSON 格式的数据源特有配置 |
| health_status | TEXT | healthy/degraded/down/unknown |
| last_health_check | DATETIME | 上次健康检查时间 |
| last_error | TEXT | 最近一次错误信息 |
| created_at | DATETIME | 创建时间 |
| updated_at | DATETIME | 更新时间 |

### 4.2 采集任务表 (`intel_collection_tasks`)

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| name | TEXT | 任务名称 |
| task_type | TEXT | stock_info/price/financial/valuation/news/... |
| target | TEXT | 采集目标 (股票代码/行业代码) |
| schedule_type | TEXT | cron/interval/manual/once |
| schedule_expr | TEXT | cron 表达式或间隔秒数 |
| enabled | BOOLEAN | 是否启用 |
| source_id | INTEGER FK | 指定数据源 (null=自动选择) |
| status | TEXT | idle/running/success/failed |
| last_run_at | DATETIME | 上次执行时间 |
| next_run_at | DATETIME | 下次执行时间 |
| success_count | INTEGER | 成功次数 |
| fail_count | INTEGER | 失败次数 |
| created_at | DATETIME | 创建时间 |

### 4.3 采集记录表 (`intel_collection_logs`)

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| task_id | INTEGER FK | 关联任务 |
| source_name | TEXT | 实际使用的数据源 |
| status | TEXT | success/partial/failed |
| records_fetched | INTEGER | 获取记录数 |
| records_stored | INTEGER | 存储记录数 |
| error_message | TEXT | 错误信息 |
| duration_ms | INTEGER | 耗时 |
| raw_data_path | TEXT | 原始数据文件路径 |
| started_at | DATETIME | 开始时间 |
| completed_at | DATETIME | 完成时间 |

### 4.4 归档资料表 (`intel_archives`)

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| stock_code | TEXT | 股票代码 |
| stock_name | TEXT | 股票名称 |
| category | TEXT | 资料类别 (basic/price/financial/valuation/news/report/...) |
| source_name | TEXT | 数据来源 |
| data_date | DATE | 数据日期 |
| title | TEXT | 资料标题 |
| summary | TEXT | 摘要 (用于向量检索) |
| content_json | TEXT | 结构化内容 (JSON) |
| file_path | TEXT | 关联文件路径 (如有) |
| tags | TEXT | 标签 (逗号分隔) |
| collection_log_id | INTEGER FK | 关联采集记录 |
| indexed | BOOLEAN | 是否已入向量库 |
| created_at | DATETIME | 归档时间 |

### 4.5 知识库向量索引 (ChromaDB 现有 + 扩展)

保留现有 6 个 collection，新增 2 个：
- `raw_data_archive` — 原始数据归档索引
- `collection_summary` — 采集摘要索引

---

## 五、API 设计

### 5.1 数据源管理

```
GET    /api/intel/sources              # 列出所有数据源
POST   /api/intel/sources              # 添加数据源
GET    /api/intel/sources/{name}       # 获取数据源详情
PUT    /api/intel/sources/{name}       # 更新数据源配置
DELETE /api/intel/sources/{name}       # 删除数据源
POST   /api/intel/sources/{name}/test  # 测试数据源连通性
POST   /api/intel/sources/{name}/health # 触发健康检查
```

### 5.2 采集任务

```
GET    /api/intel/tasks                    # 列出所有任务
POST   /api/intel/tasks                    # 创建采集任务
GET    /api/intel/tasks/{id}               # 获取任务详情
PUT    /api/intel/tasks/{id}               # 更新任务
DELETE /api/intel/tasks/{id}               # 删除任务
POST   /api/intel/tasks/{id}/run           # 手动触发执行
POST   /api/intel/tasks/{id}/stop          # 停止运行中的任务
GET    /api/intel/tasks/{id}/logs          # 获取任务执行日志
```

### 5.3 采集执行

```
POST   /api/intel/collect/stock/{code}     # 一键采集指定股票全量数据
POST   /api/intel/collect/batch            # 批量采集多只股票
GET    /api/intel/collect/status/{job_id}   # 查询采集任务进度
```

### 5.4 知识库 & 归档

```
GET    /api/intel/archives                 # 列出归档资料 (分页+筛选)
GET    /api/intel/archives/{id}            # 获取归档详情
DELETE /api/intel/archives/{id}            # 删除归档
GET    /api/intel/archives/search          # 全文搜索归档
POST   /api/intel/archives/{id}/reindex    # 重新索引到向量库

GET    /api/intel/knowledge/search         # 知识库语义检索
GET    /api/intel/knowledge/stats          # 知识库统计信息
POST   /api/intel/knowledge/rebuild        # 重建知识库索引
```

---

## 六、实现阶段

### Phase 1: 基础框架 (核心)

**目标：** 搭建情报中心骨架，数据库 + 数据源适配器 + 基本采集能力

1. **创建 `intel_hub/` 包结构**
2. **实现数据库层**
   - `repository/database.py` — SQLite 连接、表创建、会话管理
   - `models/db_models.py` — ORM 模型定义
   - `models/schemas.py` — Pydantic 模型
3. **实现数据源适配器**
   - `sources/base.py` — 抽象基类 `DataSourceAdapter`，定义统一接口：
     ```python
     class DataSourceAdapter(Protocol):
         name: str
         async def health_check(self) -> SourceHealth: ...
         async def collect(self, task: CollectionTask) -> CollectionResult: ...
         async def get_supported_types(self) -> list[str]: ...
     ```
   - `sources/akshare_adapter.py` — 从现有 `collector.py` 提取 AKShare 逻辑
   - `sources/baostock_adapter.py` — 提取 BaoStock 逻辑
   - `sources/registry.py` — 注册中心，按优先级选择适配器
4. **实现采集引擎核心**
   - `collectors/engine.py` — 接收任务，选择数据源，执行采集，存储结果
   - `collectors/tasks.py` — 任务定义
5. **实现 Repository 层**
   - `repository/source_repo.py` — 数据源 CRUD
   - `repository/collection_repo.py` — 采集记录 CRUD
6. **编写配置加载**
   - `config.py` — 从 `data_sources.yaml` 初始化数据源到数据库

**验证：** 可以通过 Python 脚本手动采集单只股票数据并存入 SQLite

### Phase 2: API 服务层

**目标：** 对外暴露 REST API，供分析和决策层调用

1. **实现 FastAPI 路由**
   - `api/sources_api.py` — 数据源管理 CRUD
   - `api/collection_api.py` — 采集任务管理 + 手动触发
   - `api/router.py` — 统一路由注册
2. **实现门面服务**
   - `service.py` — `IntelligenceHub` 类，统一入口
3. **改造现有系统对接**
   - 修改 `data_layer/collector.py` → 调用情报中心 API 获取数据（或直接调用 service）
   - 保持向后兼容：`DataCollectorAgent` 可选择直接使用情报中心或原有逻辑

**验证：** 启动 FastAPI 服务，可通过 API 管理数据源和触发采集

### Phase 3: 知识库引擎

**目标：** 归档采集数据，构建可检索的知识库

1. **实现归档管理**
   - `knowledge/archive.py` — 采集结果 → 归档资料的转换和存储
   - `repository/archive_repo.py` — 归档资料 CRUD
2. **整合向量知识库**
   - `knowledge/store.py` — 重构现有 `chroma_store.py`，适配新模型
   - `knowledge/indexer.py` — 自动将归档资料索引到 ChromaDB
3. **实现知识检索**
   - `knowledge/search.py` — 统一检索入口（结构化查询 + 语义检索）
4. **实现 API**
   - `api/archive_api.py` — 归档资料 API
   - `api/knowledge_api.py` — 知识库检索 API

**验证：** 采集数据后自动归档，可通过 API 检索历史数据

### Phase 4: 调度与监控

**目标：** 定时采集、健康检测、采集监控

1. **实现调度引擎**
   - `collectors/engine.py` — 集成 APScheduler
   - 支持 cron / interval / manual 三种调度方式
   - 从数据库加载启用的任务，注册调度
2. **实现监控**
   - `collectors/monitor.py` — 数据源健康检查、采集成功率统计、异常告警
3. **采集日志完善**
   - 详细的采集日志记录
   - 采集进度 WebSocket 推送

**验证：** 配置定时任务后自动采集，监控面板显示采集状态

### Phase 5: 前端管理界面

**目标：** 情报中心管理 UI

1. **在现有 React 前端中新增模块**
   - 数据源管理页 — 列表、启用/禁用、优先级调整、健康状态
   - 采集任务页 — 任务列表、创建/编辑任务、执行日志
   - 知识库页 — 归档资料浏览、搜索、统计
2. **利用现有 Ant Design 组件库**

**验证：** 通过前端完整操作情报中心所有功能

### Phase 6: 集成与清理

**目标：** 完成与现有系统的集成，清理旧代码

1. **改造 `ResearchCoordinator`**
   - 数据层调用改为使用 `IntelligenceHub` service
   - 保持分析层/决策层不变
2. **迁移现有数据**
   - 将 `data/cache/` 中的缓存数据导入归档库
   - 将 `data/chroma/` 中的知识库数据迁移到新结构
3. **清理旧代码**
   - 移除或标记废弃 `data_layer/cache.py` (被 DB 替代)
   - 更新配置文件
4. **更新文档和测试**

---

## 七、与现有系统的集成方式

### 方案：Service 层直接调用（推荐）

```
ResearchCoordinator
    │
    ├── 不再直接使用 DataCollectorAgent
    │
    └── 通过 IntelligenceHub 获取数据:
        │
        IntelligenceHub.collect_stock("300358")
        IntelligenceHub.get_archive("300358", category="financial")
        IntelligenceHub.search_knowledge("估值分析")
```

**好处：**
- 不需要额外的 HTTP 服务开销
- 共享同一进程空间
- 可以逐步迁移，非一次性重写

**向后兼容：**
- `DataCollectorAgent` 保留但内部委托给 `IntelligenceHub`
- 现有 CLI/API 命令继续工作
- 新功能通过情报中心 API 暴露

---

## 八、风险评估

| 风险 | 级别 | 应对策略 |
|------|------|----------|
| AKShare/BaoStock API 频率限制 | 中 | 适配器内置速率控制，全局请求队列 |
| 数据模型迁移不一致 | 中 | 使用 Alembic 管理 DB schema 版本 |
| ChromaDB 数据迁移丢失 | 高 | 先备份，迁移后校验记录数 |
| 定时任务与手动任务冲突 | 低 | 任务锁机制，同一目标同时只允许一个采集 |
| 前端工作量较大 | 中 | Phase 5 可拆分，优先实现数据源+采集管理 |

---

## 九、实施建议

1. **从 Phase 1 开始**，每个 Phase 完成后验证再进入下一个
2. **保持现有系统可用**，新代码在 `intel_hub/` 中独立开发，不修改现有代码直到 Phase 6
3. **优先做数据源适配器**，这是核心价值 — 统一的采集接口让扩展新数据源变得简单
4. **Phase 3 知识库可提前**，如果用户更关注知识检索能力

---

**等待确认：** 是否按此计划执行？可以针对某个 Phase 进行调整，或者修改架构方案。

---

## 十、实施进度

### Phase 1-4: 已完成 (2026-04-05)

- intel_hub 包结构、数据库层、数据源适配器
- API 服务层 (sources/collection/archive/knowledge)
- 知识库引擎 (archive/indexer/search)
- 调度与监控 (scheduler/monitor)

### Phase 5: 已完成 (前端管理界面)

- **数据源管理页** (`IntelSources.tsx`): 数据源列表、健康检测、启用/禁用切换
- **采集任务页** (`IntelCollect.tsx`): 任务 CRUD、一键采集、执行日志抽屉
- **归档与知识库页** (`IntelArchives.tsx`): 归档资料分页浏览、关键词搜索、知识库语义检索
- API 客户端扩展 (`api.ts`): 新增 20+ 情报中心 API 调用函数
- 侧边栏新增"情报中心"分组导航 (3个子页面)
- 路由注册 (`/intel/sources`, `/intel/collect`, `/intel/archives`)

### Phase 6: 已完成 (集成与清理)

- `ResearchCoordinator` 支持双模式数据采集:
  - 传统模式: `DataCollectorAgent` (默认)
  - 情报中心模式: `IntelligenceHub` (配置 `intel_hub.enabled=True` 启用)
- 情报中心采集结果自动转换为 `AgentOutput` 格式，对下游 Agent 透明
- 情报中心导入失败时自动降级到传统模式
