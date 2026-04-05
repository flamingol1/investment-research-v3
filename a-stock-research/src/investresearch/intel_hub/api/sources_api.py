"""数据源管理 API"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..models.schemas import SourceCreate, SourceUpdate, SourceRead

router = APIRouter(prefix="/sources", tags=["数据源管理"])


def _get_hub():
    from ..service import IntelligenceHub
    return IntelligenceHub()


@router.get("", response_model=list[SourceRead])
async def list_sources():
    """列出所有数据源"""
    hub = _get_hub()
    try:
        hub.initialize()
        return hub.list_sources()
    finally:
        hub.close()


@router.get("/{name}", response_model=SourceRead)
async def get_source(name: str):
    """获取数据源详情"""
    hub = _get_hub()
    try:
        hub.initialize()
        source = hub.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"数据源不存在: {name}")
        return source
    finally:
        hub.close()


@router.put("/{name}", response_model=SourceRead)
async def update_source(name: str, data: SourceUpdate):
    """更新数据源配置"""
    hub = _get_hub()
    try:
        hub.initialize()
        source = hub.update_source(name, data)
        if source is None:
            raise HTTPException(status_code=404, detail=f"数据源不存在: {name}")
        return source
    finally:
        hub.close()


@router.delete("/{name}")
async def delete_source(name: str):
    """删除数据源"""
    from ..repository import SourceRepository, get_session
    session = get_session()
    try:
        repo = SourceRepository(session)
        if not repo.delete(name):
            raise HTTPException(status_code=404, detail=f"数据源不存在: {name}")
        session.commit()
        return {"message": f"数据源 {name} 已删除"}
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()


@router.post("/{name}/health")
async def check_health(name: str):
    """触发数据源健康检查"""
    hub = _get_hub()
    try:
        hub.initialize()
        return hub.check_source_health(name)
    finally:
        hub.close()


@router.post("/check-all")
async def check_all_health():
    """检查所有数据源健康状态"""
    hub = _get_hub()
    try:
        hub.initialize()
        return hub.check_all_health()
    finally:
        hub.close()
