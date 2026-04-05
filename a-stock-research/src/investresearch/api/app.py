"""FastAPI应用入口"""

from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from investresearch.core.logging import get_logger, setup_logging

logger = get_logger("api.app")


def create_app() -> FastAPI:
    """创建FastAPI应用实例"""
    setup_logging()

    app = FastAPI(
        title="A股投研多Agent系统",
        description="A股自动化深度研究决策平台 - REST API + WebSocket",
        version="0.1.0",
    )

    # CORS - 允许前端开发服务器
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://localhost:3000",
            "http://127.0.0.1:5173",
            "http://127.0.0.1:3000",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 注册路由
    from investresearch.api.routes.research import router as research_router
    from investresearch.api.routes.watch import router as watch_router
    from investresearch.api.routes.history import router as history_router
    from investresearch.api.routes.search import router as search_router

    app.include_router(research_router)
    app.include_router(watch_router)
    app.include_router(history_router)
    app.include_router(search_router)

    @app.get("/api/health")
    async def health_check() -> dict:
        return {"status": "ok", "version": "0.1.0"}

    # 前端静态文件（生产环境）
    frontend_dist = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "frontend",
        "dist",
    )
    if os.path.isdir(frontend_dist):
        from fastapi.staticfiles import StaticFiles
        app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="frontend")

    logger.info("FastAPI应用已创建")
    return app


app = create_app()
