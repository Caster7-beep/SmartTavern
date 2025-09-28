import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from api.endpoints import router as api_router, initialize as initialize_api

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan hook that prepares the flow engine on startup."""
    logger.info("🚀 正在启动应用程序...")
    try:
        initialize_api()
        logger.info("✅ Flow API 初始化完成")
    except Exception as exc:  # noqa: BLE001
        logger.error("❌ Flow API 初始化失败: %s", exc, exc_info=True)
        raise

    try:
        yield
    finally:
        logger.info("🔌 应用关闭。")


app = FastAPI(
    title="LLM 文字冒险工作流后端",
    description="n8n 粒度原子节点 + IR 组合语义的可扩展后端（无前端）。",
    version="0.2.0",
    lifespan=lifespan,
)

app.include_router(api_router, prefix="/api")


@app.get("/")
def read_root() -> dict[str, str]:
    """根路径，提供一个简单的欢迎信息。"""
    return {"message": "后端正在运行。访问 /docs 查看API文档；可使用 /api/* 路由。"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8192)), reload=True)
