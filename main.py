import logging
import os
from contextlib import asynccontextmanager

import uvicorn
import mimetypes
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import RedirectResponse

class PatchedStaticFiles(StaticFiles):
    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        # Force proper MIME types for problematic extensions on Windows
        try:
            if path.endswith(".js"):
                response.media_type = "application/javascript"
            elif path.endswith(".mjs"):
                response.media_type = "application/javascript"
            elif path.endswith(".css"):
                response.media_type = "text/css"
            elif path.endswith(".svg"):
                response.media_type = "image/svg+xml"
            elif path.endswith(".map"):
                response.media_type = "application/json"
        except Exception:
            # Best-effort; if anything goes wrong, return original response
            pass
        return response

from api.endpoints import router as api_router, initialize as initialize_api
from api.chat_endpoints import router as chat_router, initialize as initialize_chat
from api.debug_endpoints import router as debug_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

# Fix incorrect MIME types on Windows (avoid serving .js as text/plain)
# Ensure both legacy and modern JS MIME types are registered (last one wins).
mimetypes.add_type("text/javascript", ".js")
mimetypes.add_type("text/javascript", ".mjs")
mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("application/javascript", ".mjs")
mimetypes.add_type("text/css", ".css")
mimetypes.add_type("image/svg+xml", ".svg")

# Fix incorrect MIME types on Windows (avoid serving .js as text/plain)
mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("application/javascript", ".mjs")
mimetypes.add_type("text/css", ".css")
mimetypes.add_type("image/svg+xml", ".svg")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan hook that prepares the flow engine on startup."""
    logger.info("🚀 正在启动应用程序...")
    try:
        initialize_api()
        logger.info("✅ Flow API 初始化完成")
        # 初始化 Chat 子系统
        initialize_chat()
        logger.info("✅ Chat API 初始化完成")
    except Exception as exc:  # noqa: BLE001
        logger.error("❌ Flow API/Chat 初始化失败: %s", exc, exc_info=True)
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
app.include_router(chat_router, prefix="/api/chat")
app.include_router(debug_router, prefix="/api/debug")

# Mount Debug Console static assets (built frontend)
app.mount(
    "/debugconsole",
    PatchedStaticFiles(directory="frontend/dist", html=True),
    name="debugconsole",
)

# Convenience redirect to ensure trailing slash (for SPA assets resolution)
@app.get("/debugconsole")
def debugconsole_index() -> RedirectResponse:
    return RedirectResponse(url="/debugconsole/")


@app.get("/")
def read_root() -> dict[str, str]:
    """根路径，提供一个简单的欢迎信息。"""
    return {"message": "后端正在运行。访问 /docs 查看API文档；可使用 /api/* 路由。"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8192)), reload=True)
