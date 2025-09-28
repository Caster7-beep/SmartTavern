import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from flow.registry import NodeRegistry
from flow.ir import IRLoader
from flow.executor import FlowExecutor
from flow.state_manager import StateManager
from flow.node_base import NodeContext
from services.llm_adapter import llm_adapter

logger = logging.getLogger(__name__)

router = APIRouter()

# Globals initialized at app startup (wired by main.py lifespan)
_registry: Optional[NodeRegistry] = None
_loader: Optional[IRLoader] = None
_executor: Optional[FlowExecutor] = None

# Default IR directories
IR_DIRS = [
    Path("config") / "workflows",
    Path("config") / "workflows" / "subflows",
]


def initialize(schema_path: Path | str = Path("schemas") / "ir.schema.json") -> None:
    """Initialize NodeRegistry, IRLoader and FlowExecutor; discover nodes and load IR docs."""
    global _registry, _loader, _executor
    _registry = NodeRegistry()
    # Discover all atomic nodes under flow.nodes.*
    _registry.discover()

    _loader = IRLoader(schema_path=str(schema_path))
    # Load default IR directories
    _loader.load_dirs(IR_DIRS)

    _executor = FlowExecutor(registry=_registry, loader=_loader)
    logger.info("Flow API initialized; flows=%s; node_types=%s", _loader.list_flows(), _registry.known_types())


def _require_initialized() -> None:
    if _registry is None or _loader is None or _executor is None:
        raise HTTPException(status_code=503, detail="Flow engine not initialized")


def _load_initial_state() -> Dict[str, Any]:
    cfg_path = Path("config") / "world_config.yaml"
    if not cfg_path.exists():
        return {"location": "酒馆", "turn_count": 0, "protagonist_mood": "中性"}
    with cfg_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return (data.get("initial_state") or {}) if isinstance(data, dict) else {}


class RunFlowRequest(BaseModel):
    ref: str = Field(..., description="Flow ref id@version, e.g. 'main@1'")
    items: List[Dict[str, Any]] = Field(default_factory=list)
    session_id: Optional[str] = Field(default=None)
    use_world_state: bool = Field(default=True, description="Use config/world_config.yaml if initial_state not provided")
    initial_state: Optional[Dict[str, Any]] = Field(default=None)
    resources: Optional[Dict[str, Any]] = Field(default=None, description="Optional resource overrides, e.g. code_funcs")


class RunFlowResponse(BaseModel):
    items: List[Dict[str, Any]]
    logs: List[str]
    metrics: Dict[str, Any]
    state_snapshot: Dict[str, Any]


class ReloadRequest(BaseModel):
    dirs: Optional[List[str]] = Field(default=None, description="Override IR directories to load")


class ReloadResponse(BaseModel):
    flows: List[str]
    node_types: List[str]


class ValidateDocRequest(BaseModel):
    doc: Dict[str, Any]


@router.post("/flow/run", response_model=RunFlowResponse, summary="执行指定工作流并返回结果")
async def run_flow(request: RunFlowRequest) -> RunFlowResponse:
    _require_initialized()
    assert _executor is not None and _loader is not None

    # Prepare state
    initial_state = request.initial_state if request.initial_state is not None else (_load_initial_state() if request.use_world_state else {})
    state = StateManager(initial_state)

    # Build NodeContext
    resources: Dict[str, Any] = {
        "llm": llm_adapter,
    }
    if request.resources and isinstance(request.resources, dict):
        # Only allow whitelisted keys to be overridden
        for k in ("llm", "code", "code_funcs"):
            if k in request.resources:
                resources[k] = request.resources[k]

    ctx = NodeContext(
        session_id=request.session_id or "session",
        state=state,
        resources=resources,
    )

    try:
        result = _executor.execute_ref(request.ref, request.items or [], ctx)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Flow run failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Flow execution failed") from exc

    return RunFlowResponse(
        items=result.items,
        logs=result.logs,
        metrics=result.metrics,
        state_snapshot=state.get_working_state(),
    )


@router.post("/flow/reload", response_model=ReloadResponse, summary="重载 IR 目录并刷新节点注册")
async def reload_flows(request: ReloadRequest) -> ReloadResponse:
    _require_initialized()
    assert _registry is not None and _loader is not None

    # Reload node registry (optional clear + discover)
    _registry.clear()
    _registry.discover()

    # Reload IR documents
    dirs = [Path(d) for d in (request.dirs or [str(p) for p in IR_DIRS])]
    _loader.load_dirs(dirs)

    return ReloadResponse(flows=_loader.list_flows(), node_types=_registry.known_types())


@router.post("/flow/validate", summary="校验 IR 文档结构（JSON Schema）")
async def validate_doc(request: ValidateDocRequest) -> Dict[str, Any]:
    _require_initialized()
    assert _loader is not None
    try:
        _loader.validate(request.doc)
        return {"valid": True}
    except Exception as exc:
        return {"valid": False, "error": str(exc)}
