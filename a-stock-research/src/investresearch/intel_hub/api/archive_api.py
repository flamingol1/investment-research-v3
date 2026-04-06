"""归档资料 API"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from ..models.schemas import ArchiveRead

router = APIRouter(prefix="/archives", tags=["归档管理"])


def _get_hub():
    from ..service import IntelligenceHub
    return IntelligenceHub()


@router.get("")
async def list_archives(
    stock_code: str | None = Query(default=None),
    category: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    source_name: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
):
    """列出归档资料 (分页+筛选)"""
    hub = _get_hub()
    try:
        hub.initialize()
        results, total = hub.search_archives(
            stock_code=stock_code,
            category=category,
            keyword=keyword,
            source_name=source_name,
            page=page,
            page_size=page_size,
        )
        return {
            "items": [r.model_dump() for r in results],
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    finally:
        hub.close()


@router.get("/stats")
async def archive_stats():
    """归档统计信息"""
    hub = _get_hub()
    try:
        hub.initialize()
        return hub.get_archive_stats()
    finally:
        hub.close()


@router.get("/search")
async def search_archives(
    keyword: str = Query(..., description="搜索关键词"),
    stock_code: str | None = Query(default=None),
    category: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
):
    """全文搜索归档"""
    hub = _get_hub()
    try:
        hub.initialize()
        results, total = hub.search_archives(
            keyword=keyword,
            stock_code=stock_code,
            category=category,
            page=page,
            page_size=page_size,
        )
        return {
            "items": [r.model_dump() for r in results],
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    finally:
        hub.close()


@router.get("/{archive_id}")
async def get_archive(archive_id: int):
    """获取归档详情(含完整内容)"""
    hub = _get_hub()
    try:
        hub.initialize()
        content = hub.get_archive_content(archive_id)
        if content is None:
            raise HTTPException(status_code=404, detail=f"归档不存在: {archive_id}")
        return {"id": archive_id, "content": content}
    finally:
        hub.close()


@router.delete("/{archive_id}")
async def delete_archive(archive_id: int):
    """删除归档"""
    from ..repository import ArchiveRepository, get_session
    session = get_session()
    try:
        repo = ArchiveRepository(session)
        if not repo.delete(archive_id):
            raise HTTPException(status_code=404, detail=f"归档不存在: {archive_id}")
        session.commit()
        return {"message": f"归档 {archive_id} 已删除"}
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()


@router.post("/{archive_id}/reindex")
async def reindex_archive(archive_id: int):
    """重新索引归档到向量库"""
    from ..repository import ArchiveRepository, get_session
    session = get_session()
    try:
        repo = ArchiveRepository(session)
        archive = repo.get_by_id(archive_id)
        if archive is None:
            raise HTTPException(status_code=404, detail=f"归档不存在: {archive_id}")
        repo.mark_indexed(archive_id)
        session.commit()
        return {"message": f"归档 {archive_id} 已重新索引"}
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()
