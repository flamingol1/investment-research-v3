"""ChromaDB向量知识库存储 - 6大类分类存储+语义检索

存储策略:
- 轻量文本(结论摘要、风险清单) -> 直接存入ChromaDB document
- 重量数据(完整报告markdown、原始财务数据) -> 存入本地JSON文件, ChromaDB只存引用路径
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from investresearch.core.config import Config
from investresearch.core.exceptions import KnowledgeBaseConnectionError, KnowledgeBaseQueryError
from investresearch.core.logging import get_logger
from investresearch.core.models import (
    InvestmentConclusion,
    KnowledgeCategory,
    ResearchHistoryEntry,
    ResearchReport,
)

logger = get_logger("knowledge_base.chroma_store")

# Collection名称映射
COLLECTION_NAMES: dict[KnowledgeCategory, str] = {
    KnowledgeCategory.STOCK: "stock_research",
    KnowledgeCategory.INDUSTRY: "industry_analysis",
    KnowledgeCategory.MACRO: "macro_environment",
    KnowledgeCategory.REPORT: "research_report",
    KnowledgeCategory.RISK: "risk_analysis",
    KnowledgeCategory.DECISION: "investment_decision",
}


class ChromaKnowledgeStore:
    """ChromaDB向量知识库

    用法:
        store = ChromaKnowledgeStore()
        store.save_research("300358", "湖南裕能", report, conclusion)
        history = store.get_research_history("300358")
        results = store.search_similar("估值分析", category="stock")
    """

    def __init__(self, persist_dir: str | None = None) -> None:
        self._config = Config()
        chroma_cfg = self._config.get("storage.chroma", {})
        env_var = chroma_cfg.get("persist_dir_env", "CHROMA_PERSIST_DIR")

        import os
        self._persist_dir = persist_dir or os.environ.get(env_var, chroma_cfg.get("default_dir", "./data/chroma"))
        self._persist_path = Path(self._persist_dir)
        self._persist_path.mkdir(parents=True, exist_ok=True)

        # 重型数据文件存储目录
        self._files_dir = self._persist_path / "files"
        self._files_dir.mkdir(parents=True, exist_ok=True)

        self._client = None
        self._collections: dict[str, Any] = {}

        self._defaults = chroma_cfg.get("defaults", {})
        self._n_results = self._defaults.get("n_results", 5)
        self._similarity_threshold = self._defaults.get("similarity_threshold", 0.7)

    def _get_client(self) -> Any:
        """懒加载ChromaDB客户端"""
        if self._client is None:
            try:
                import chromadb
                self._client = chromadb.PersistentClient(path=str(self._persist_path))
                logger.info(f"ChromaDB客户端初始化 | path={self._persist_path}")
            except ImportError:
                raise KnowledgeBaseConnectionError(
                    "chromadb未安装，请运行: pip install chromadb"
                )
            except Exception as e:
                raise KnowledgeBaseConnectionError(f"ChromaDB连接失败: {e}")
        return self._client

    def _ensure_collections(self) -> None:
        """确保所有6个collection存在"""
        client = self._get_client()
        for category, name in COLLECTION_NAMES.items():
            if name not in self._collections:
                try:
                    self._collections[name] = client.get_or_create_collection(
                        name=name,
                        metadata={"description": f"{category.value} research data"},
                    )
                except Exception as e:
                    raise KnowledgeBaseConnectionError(
                        f"创建集合失败: {name}", collection=name
                    ) from e

    def _get_collection(self, category: KnowledgeCategory) -> Any:
        """获取指定分类的collection"""
        name = COLLECTION_NAMES[category]
        if name not in self._collections:
            self._ensure_collections()
        return self._collections[name]

    # ================================================================
    # 存储方法
    # ================================================================

    def save_research(
        self,
        stock_code: str,
        stock_name: str,
        report: ResearchReport,
        conclusion: InvestmentConclusion | None = None,
    ) -> None:
        """存储完整研究结果到知识库

        按分类存储到6个collection:
        - decision: 投资结论摘要
        - report: 报告摘要(前500字)
        - risk: 风险分析要点
        - stock: 个股研究关键指标
        - (industry/macro: 如有相关数据)
        """
        self._ensure_collections()
        timestamp = datetime.now().isoformat()
        doc_id = f"{stock_code}_{timestamp}"

        # 1. 存储投资结论到 decision collection
        if conclusion:
            # 保存结论重型数据（供 get_conclusion 加载）
            self._save_heavy_data(stock_code, timestamp, "conclusion", conclusion.model_dump(mode="json"))

            conclusion_text = (
                f"{stock_code} {stock_name} 投资结论: {conclusion.recommendation} | "
                f"置信度: {conclusion.confidence_level} | 风险: {conclusion.risk_level} | "
                f"目标价: {conclusion.target_price_low}-{conclusion.target_price_high} | "
                f"当前价: {conclusion.current_price} | "
                f"理由: {'; '.join(conclusion.key_reasons_buy[:3])} | "
                f"跟踪指标: {'; '.join(conclusion.monitoring_points[:5])}"
            )
            self._add_document(
                KnowledgeCategory.DECISION,
                doc_id=f"{doc_id}_decision",
                document=conclusion_text,
                metadata={
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "date": timestamp,
                    "recommendation": conclusion.recommendation,
                    "risk_level": conclusion.risk_level,
                    "current_price": str(conclusion.current_price or ""),
                    "category": "decision",
                },
            )

        # 2. 存储报告摘要到 report collection
        report_summary = report.markdown[:2000] if report.markdown else ""
        report_file_path = self._save_heavy_data(stock_code, timestamp, "report", {
            "markdown": report.markdown,
            "chart_pack": report.chart_pack,
            "evidence_pack": report.evidence_pack,
            "agents_completed": report.agents_completed,
            "agents_skipped": report.agents_skipped,
            "errors": report.errors,
        })
        chart_pack_path = None
        evidence_pack_path = None
        if report.chart_pack:
            chart_pack_path = self._save_heavy_data(stock_code, timestamp, "chart_pack", report.chart_pack)
        if report.evidence_pack:
            evidence_pack_path = self._save_heavy_data(stock_code, timestamp, "evidence_pack", report.evidence_pack)
        if report_summary:
            self._add_document(
                KnowledgeCategory.REPORT,
                doc_id=f"{doc_id}_report",
                document=report_summary,
                metadata={
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "date": timestamp,
                    "depth": report.depth,
                    "file_path": str(report_file_path),
                    "category": "report",
                },
            )

        if report.chart_pack:
            chart_items = [
                item.model_dump(mode="json") if hasattr(item, "model_dump") else dict(item)
                for item in report.chart_pack[:6]
            ]
            self._add_document(
                KnowledgeCategory.REPORT,
                doc_id=f"{doc_id}_chart_pack",
                document=" | ".join(f"{item.get('title', 'chart')}: {item.get('summary', '')}" for item in chart_items),
                metadata={
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "date": timestamp,
                    "file_path": str(chart_pack_path or ""),
                    "category": "chart_pack",
                },
            )

        if report.evidence_pack:
            self._index_evidence_pack(stock_code, stock_name, timestamp, report.evidence_pack)

        # 3. 存储研究历史记录
        history_entry = ResearchHistoryEntry(
            stock_code=stock_code,
            stock_name=stock_name,
            research_date=datetime.now(),
            depth=report.depth,
            recommendation=conclusion.recommendation if conclusion else None,
            risk_level=conclusion.risk_level if conclusion else None,
            target_price_low=conclusion.target_price_low if conclusion else None,
            target_price_high=conclusion.target_price_high if conclusion else None,
            current_price=conclusion.current_price if conclusion else None,
            report_path=str(report_file_path),
            chart_pack_path=str(chart_pack_path) if chart_pack_path else None,
            evidence_pack_path=str(evidence_pack_path) if evidence_pack_path else None,
            agents_completed=report.agents_completed,
            errors=report.errors,
        )
        self._save_heavy_data(stock_code, timestamp, "history", history_entry.model_dump(mode="json"))

        # 4. 存储到 stock collection (综合摘要)
        stock_text = (
            f"{stock_code} {stock_name} 研究概要 | "
            f"深度: {report.depth} | "
            f"完成Agent: {', '.join(report.agents_completed)} | "
            f"结论: {conclusion.recommendation if conclusion else 'N/A'}"
        )
        self._add_document(
            KnowledgeCategory.STOCK,
            doc_id=f"{doc_id}_stock",
            document=stock_text,
            metadata={
                "stock_code": stock_code,
                "stock_name": stock_name,
                "date": timestamp,
                "depth": report.depth,
                "category": "stock",
            },
        )

        logger.info(f"研究已存入知识库 | {stock_code} | id={doc_id}")

    def _index_evidence_pack(
        self,
        stock_code: str,
        stock_name: str,
        timestamp: str,
        evidence_pack: list[dict[str, Any]],
    ) -> None:
        """Index evidence pack summaries into the logical 6 collections."""
        category_map = {
            "policy": KnowledgeCategory.MACRO,
            "industry": KnowledgeCategory.INDUSTRY,
            "risk": KnowledgeCategory.RISK,
            "governance": KnowledgeCategory.STOCK,
            "valuation": KnowledgeCategory.DECISION,
            "compliance": KnowledgeCategory.RISK,
            "patent": KnowledgeCategory.STOCK,
        }
        normalized_items = [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else dict(item)
            for item in evidence_pack[:12]
        ]
        for index, item in enumerate(normalized_items):
            category_hint = str(item.get("category", "") or "").lower()
            target_category = next(
                (value for key, value in category_map.items() if key in category_hint),
                KnowledgeCategory.STOCK,
            )
            document = (
                f"{stock_code} {stock_name} | {item.get('title', 'evidence')} | "
                f"{item.get('source', '')} | {str(item.get('excerpt', ''))[:280]}"
            )
            self._add_document(
                target_category,
                doc_id=f"{stock_code}_{timestamp}_evidence_{index}",
                document=document,
                metadata={
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "date": timestamp,
                    "category": target_category.value,
                    "source": item.get("source", ""),
                    "title": item.get("title", ""),
                    "reference_date": item.get("reference_date", ""),
                },
            )

    def save_analysis_result(
        self,
        stock_code: str,
        agent_name: str,
        data: dict[str, Any],
        summary: str = "",
    ) -> None:
        """存储单个Agent分析结果"""
        self._ensure_collections()
        timestamp = datetime.now().isoformat()

        # 映射agent到知识库分类
        category_map = {
            "financial": KnowledgeCategory.STOCK,
            "business_model": KnowledgeCategory.STOCK,
            "industry": KnowledgeCategory.INDUSTRY,
            "governance": KnowledgeCategory.STOCK,
            "valuation": KnowledgeCategory.STOCK,
            "risk": KnowledgeCategory.RISK,
        }
        category = category_map.get(agent_name, KnowledgeCategory.STOCK)

        # 保存重型数据到JSON文件
        self._save_heavy_data(stock_code, timestamp, agent_name, data)

        # 保存摘要到ChromaDB
        doc_text = summary or f"{stock_code} {agent_name} 分析结果"
        if len(doc_text) > 2000:
            doc_text = doc_text[:2000]

        self._add_document(
            category,
            doc_id=f"{stock_code}_{timestamp}_{agent_name}",
            document=doc_text,
            metadata={
                "stock_code": stock_code,
                "date": timestamp,
                "agent_name": agent_name,
                "category": category.value,
            },
        )

    # ================================================================
    # 查询方法
    # ================================================================

    def get_latest_research(self, stock_code: str) -> ResearchHistoryEntry | None:
        """获取最近一次研究记录"""
        history_files = sorted(
            self._files_dir.glob(f"{stock_code}_*_history.json"),
            reverse=True,
        )
        if not history_files:
            return None

        try:
            data = json.loads(history_files[0].read_text(encoding="utf-8"))
            return ResearchHistoryEntry(**data)
        except Exception as e:
            logger.warning(f"读取研究历史失败: {e}")
            return None

    def get_research_history(self, stock_code: str) -> list[ResearchHistoryEntry]:
        """获取所有历史记录"""
        entries = []
        for f in sorted(self._files_dir.glob(f"{stock_code}_*_history.json"), reverse=True):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                entries.append(ResearchHistoryEntry(**data))
            except Exception:
                continue
        return entries

    def get_last_collected_at(self, stock_code: str) -> datetime | None:
        """获取上次数据采集时间"""
        latest = self.get_latest_research(stock_code)
        if latest:
            return latest.research_date
        return None

    def get_conclusion(self, stock_code: str) -> InvestmentConclusion | None:
        """获取最新投资结论"""
        # 从最新历史文件的同期结论重型数据中加载
        history_files = sorted(
            self._files_dir.glob(f"{stock_code}_*_conclusion.json"),
            reverse=True,
        )
        if history_files:
            try:
                data = json.loads(history_files[0].read_text(encoding="utf-8"))
                return InvestmentConclusion(**data)
            except Exception as e:
                logger.warning(f"加载结论数据失败: {e}")

        return None

    def get_latest_evidence_pack(self, stock_code: str) -> list[dict[str, Any]]:
        """Load the latest saved evidence pack for a stock."""
        candidate_paths: list[Path] = []

        latest = self.get_latest_research(stock_code)
        if latest and latest.evidence_pack_path:
            candidate_paths.append(Path(latest.evidence_pack_path))

        candidate_paths.extend(
            sorted(
                self._files_dir.glob(f"{stock_code}_*_evidence_pack.json"),
                reverse=True,
            )
        )

        seen: set[Path] = set()
        for path in candidate_paths:
            resolved = Path(path)
            if resolved in seen or not resolved.exists():
                continue
            seen.add(resolved)

            try:
                payload = json.loads(resolved.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning(f"加载证据包失败: {e}")
                continue

            if isinstance(payload, list):
                return [item for item in payload if isinstance(item, dict)]

        return []

    def get_monitoring_points(self, stock_code: str) -> list[str]:
        """获取跟踪指标"""
        conclusion = self.get_conclusion(stock_code)
        if conclusion:
            points = list(conclusion.monitoring_points or [])
            if points:
                return points
        return []

    def search_similar(
        self,
        query: str,
        category: str | None = None,
        n: int | None = None,
    ) -> list[dict[str, Any]]:
        """语义搜索知识库"""
        self._ensure_collections()
        n = n or self._n_results
        results_list = []

        collections_to_search = (
            [self._get_collection(KnowledgeCategory(category))]
            if category
            else list(self._collections.values())
        )

        for collection in collections_to_search:
            try:
                results = collection.query(
                    query_texts=[query],
                    n_results=n,
                )
                if results["documents"] and results["documents"][0]:
                    for i, doc in enumerate(results["documents"][0]):
                        meta = results["metadatas"][0][i] if results["metadatas"] else {}
                        distance = results["distances"][0][i] if results.get("distances") else 0
                        results_list.append({
                            "document": doc,
                            "metadata": meta,
                            "distance": distance,
                            "collection": collection.name,
                        })
            except Exception as e:
                logger.warning(f"搜索失败 [{collection.name}]: {e}")

        # 按距离排序(越小越相似)
        results_list.sort(key=lambda x: x.get("distance", 1))
        return results_list[:n]

    # ================================================================
    # 删除方法
    # ================================================================

    def delete_research(self, stock_code: str) -> int:
        """删除该标的所有研究数据, 返回删除的文件数"""
        count = 0

        # 删除ChromaDB中的记录
        self._ensure_collections()
        for collection in self._collections.values():
            try:
                results = collection.get(where={"stock_code": stock_code})
                if results["ids"]:
                    collection.delete(ids=results["ids"])
                    count += len(results["ids"])
            except Exception as e:
                logger.warning(f"删除ChromaDB记录失败: {e}")

        # 删除重型数据文件
        for f in self._files_dir.glob(f"{stock_code}_*.json"):
            f.unlink(missing_ok=True)
            count += 1

        logger.info(f"已删除 {stock_code} 的 {count} 条研究数据")
        return count

    # ================================================================
    # 内部方法
    # ================================================================

    def _add_document(
        self,
        category: KnowledgeCategory,
        doc_id: str,
        document: str,
        metadata: dict[str, Any],
    ) -> None:
        """安全添加文档到collection"""
        collection = self._get_collection(category)
        try:
            # 确保metadata值都是ChromaDB支持的类型
            safe_meta = {}
            for k, v in metadata.items():
                if isinstance(v, (str, int, float, bool)):
                    safe_meta[k] = v
                elif v is None:
                    safe_meta[k] = ""
                else:
                    safe_meta[k] = str(v)

            collection.upsert(
                ids=[doc_id],
                documents=[document],
                metadatas=[safe_meta],
            )
        except Exception as e:
            raise KnowledgeBaseQueryError(
                f"存储文档失败: {e}", collection=COLLECTION_NAMES[category]
            ) from e

    def _save_heavy_data(
        self, stock_code: str, timestamp: str, data_type: str, data: Any
    ) -> Path:
        """保存重型数据到本地JSON文件"""
        # 清理timestamp中的特殊字符
        safe_ts = timestamp.replace(":", "-").replace(".", "-")
        filename = f"{stock_code}_{safe_ts}_{data_type}.json"
        filepath = self._files_dir / filename

        try:
            if isinstance(data, dict):
                content = data
            elif isinstance(data, list):
                content = [
                    item.model_dump(mode="json") if hasattr(item, "model_dump") else item
                    for item in data
                ]
            elif hasattr(data, "model_dump"):
                content = data.model_dump(mode="json")
            else:
                content = {"data": data}

            filepath.write_text(
                json.dumps(content, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"重型数据保存失败: {e}")

        return filepath

    def _load_heavy_data(
        self, stock_code: str, timestamp: str, data_type: str
    ) -> dict[str, Any] | None:
        """从本地JSON文件加载重型数据"""
        safe_ts = timestamp.replace(":", "-").replace(".", "-")
        filename = f"{stock_code}_{safe_ts}_{data_type}.json"
        filepath = self._files_dir / filename

        if not filepath.exists():
            return None

        try:
            return json.loads(filepath.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"重型数据加载失败: {e}")
            return None
