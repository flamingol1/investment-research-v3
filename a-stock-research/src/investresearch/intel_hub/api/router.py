"""情报中心 API 路由注册"""

from __future__ import annotations

from fastapi import APIRouter

from .sources_api import router as sources_router
from .collection_api import router as collection_router
from .archive_api import router as archive_router
from .knowledge_api import router as knowledge_router
from .collection_stream_api import router as stream_router

intel_router = APIRouter(prefix="/api/intel", tags=["情报中心"])

intel_router.include_router(sources_router)
intel_router.include_router(collection_router)
intel_router.include_router(stream_router)
intel_router.include_router(archive_router)
intel_router.include_router(knowledge_router)


@intel_router.get("/stats")
async def intel_stats() -> dict:
    """情报中心统计概览"""
    from ..service import IntelligenceHub
    hub = IntelligenceHub()
    try:
        hub.initialize()
        archive_stats = hub.get_archive_stats()
        sources = hub.list_sources()
        data_types = hub.get_supported_data_types()
        return {
            "sources": {
                "total": len(sources),
                "healthy": sum(1 for s in sources if s.health_status == "healthy"),
            },
            "archives": archive_stats,
            "data_types": len(data_types),
        }
    finally:
        hub.close()


@intel_router.get("/data-types")
async def list_data_types() -> dict[str, dict[str, str]]:
    """列出所有支持的采集数据类型"""
    from ..service import IntelligenceHub
    hub = IntelligenceHub()
    try:
        hub.initialize()
        return hub.get_supported_data_types()
    finally:
        hub.close()
