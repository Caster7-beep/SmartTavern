import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

# 复用已有 Flow 引擎初始化与对象
from api import endpoints as flow_api

from flow.node_base import NodeContext
from flow.state_manager import StateManager
from services.llm_adapter import llm_adapter
from services.session_store import get_default_store, SessionStore
from services.job_queue_interface import get_default_job_queue, JobQueue, JobType
from services.job_queue_rq import get_rq_job_queue
from services.job_worker import process_job
from services.outbox_poller import start_outbox_poller
from services.code_funcs import select_context

logger = logging.getLogger(__name__)

router = APIRouter()

# Module globals
_store: Optional[SessionStore] = None
_job_queue: Optional[JobQueue] = None


def initialize() -> None:
    """
    初始化 /chat 子系统：会话存储与队列。
    - 优先尝试 RQ（Redis Queue），不可用则回退 NullJobQueue。
    """
    global _store, _job_queue
    _store = get_default_store()
    _job_queue = get_rq_job_queue() or get_default_job_queue()
    logger.info("Chat API initialized; queue=%s", _job_queue.worker_hint())
    # 启动 Outbox 轮询器（开发/生产通用；在 null 队列下会同步执行 pending 作业）
    try:
        start_outbox_poller(_store, _job_queue, interval_sec=3.0)
        logger.info("Outbox poller started")
    except Exception as exc:
        logger.warning("Failed to start outbox poller: %s", exc)


# --------- Models ----------

class ChatStartRequest(BaseModel):
    initial_state: Optional[Dict[str, Any]] = Field(default=None)
    use_world_state: bool = Field(default=True)


class ChatStartResponse(BaseModel):
    session_id: str
    branch_id: str
    state_snapshot: Dict[str, Any]


class ChatSendRequest(BaseModel):
    session_id: str
    branch_id: Optional[str] = Field(default=None)
    user_input: str
    ref: str = Field(default="main@1", description="工作流引用 id@version")
    extras: Optional[Dict[str, Any]] = Field(default=None, description="附加到 item 的键值对")
    resources: Optional[Dict[str, Any]] = Field(default=None, description="资源覆盖，如 {'llm': ..., 'code_funcs': {...}}")


class ChatSendResponse(BaseModel):
    round_no: int
    snapshot_id: str
    llm_reply: str
    items: List[Dict[str, Any]]
    logs: List[str]
    metrics: Dict[str, Any]
    state_snapshot: Dict[str, Any]
    round_status: str


# --------- Helpers ----------

def _require_ready() -> None:
    if _store is None or _job_queue is None:
        raise HTTPException(status_code=503, detail="Chat subsystem not initialized")
    flow_api._require_initialized()  # Ensure Flow API ready


def _get_registry_loader_executor():
    return flow_api._registry, flow_api._loader, flow_api._executor  # type: ignore


def _load_initial_state() -> Dict[str, Any]:
    # 复用 flow API 的世界初始态加载逻辑
    try:
        return flow_api._load_initial_state()  # type: ignore
    except Exception:
        return {"location": "酒馆", "turn_count": 0, "protagonist_mood": "中性"}


# --------- Endpoints ----------

@router.post("/session/start", response_model=ChatStartResponse, summary="创建会话与默认分支，生成初始快照锚")
async def chat_session_start(request: ChatStartRequest) -> ChatStartResponse:
    _require_ready()
    assert _store is not None

    initial_state = request.initial_state if request.initial_state is not None else (_load_initial_state() if request.use_world_state else {})

    # 创建会话 + 默认分支
    session_id = _store.create_session(initial_state)
    sess = _store.load_session(session_id)
    branch_id = str(sess.get("active_branch_id") or "")

    if not branch_id:
        raise HTTPException(status_code=500, detail="Failed to create default branch")

    return ChatStartResponse(session_id=session_id, branch_id=branch_id, state_snapshot=initial_state)


@router.post("/send", response_model=ChatSendResponse, summary="玩家发送消息（锚点：快照创建），立即返回 LLM-1 故事回复，并调度异步作业")
async def chat_send(request: ChatSendRequest) -> ChatSendResponse:
    _require_ready()
    assert _store is not None

    reg, loader, executor = _get_registry_loader_executor()
    if executor is None:
        raise HTTPException(status_code=503, detail="Flow engine not initialized")

    # 解析会话与分支
    sess = _store.load_session(request.session_id)
    branch_id = request.branch_id or str(sess.get("active_branch_id") or "")
    if not branch_id:
        raise HTTPException(status_code=404, detail="Active branch not found")

    lss_state: Dict[str, Any] = dict(sess.get("lss_state_json") or {})
    turn_count: int = int(sess.get("turn_count", 0))

    # 以“玩家发送”为锚点，创建回合与快照（区间 0..turn_count）
    round_no, snapshot_id = _store.begin_round(
        session_id=request.session_id,
        branch_id=branch_id,
        user_input=request.user_input,
        lss_state=lss_state,
        convo_range_start=0,
        convo_range_end=turn_count,
    )

    # 构建 NodeContext
    resources: Dict[str, Any] = {"llm": llm_adapter, "code_funcs": {"select_context": select_context}}
    if request.resources and isinstance(request.resources, dict):
        # 白名单覆盖
        for k in ("llm", "code", "code_funcs"):
            if k in request.resources:
                resources[k] = request.resources[k]

    state = StateManager(lss_state)
    ctx = NodeContext(session_id=request.session_id, state=state, resources=resources)

    # 组装 items（单条）
    base_item: Dict[str, Any] = {"user_input": request.user_input}
    if request.extras and isinstance(request.extras, dict):
        for k, v in request.extras.items():
            base_item[k] = v
    items: List[Dict[str, Any]] = [base_item]

    # 执行主通路
    try:
        result = executor.execute_ref(request.ref, items, ctx)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("chat_send flow failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Flow execution failed: {type(exc).__name__}: {exc}") from exc

    final_items = result.items or [{}]
    first = final_items[0] if final_items else {}
    llm_reply: str = str(first.get("llm_response") or first.get("narrative") or "")

    # 回合中记录 LLM-1 故事回复（立即可见）
    _store.save_round_llm_reply(request.session_id, branch_id, round_no, llm_reply)
    # 记录本回合使用的 messages（完整上下文持久化）
    try:
        msgs = first.get("messages") if isinstance(first.get("messages"), list) else []
        _store.save_round_messages(request.session_id, branch_id, round_no, msgs)  # will sanitize
    except Exception as exc:
        logger.warning("chat_send: save messages failed: %s", exc)

    # 调度 gating 作业（状态更新）→ Outbox 记录并入队
    job_payload: Dict[str, Any] = {
        "id": None,  # 由 store 分配
        "session_id": request.session_id,
        "branch_id": branch_id,
        "anchor_round": round_no,
        "snapshot_id": snapshot_id,
        "type": JobType.STATUS_UPDATE,
        "base_range_end": turn_count,  # 使用 0..turn_count 的区间作为依赖范围
        "gating": True,
        "payload": {"text": llm_reply, "user_input": request.user_input},
    }
    job_id = _store.record_job(
        session_id=request.session_id,
        job_type=JobType.STATUS_UPDATE,
        branch_id=branch_id,
        anchor_round=round_no,
        base_range_end=turn_count,
        gating=True,
        payload={"text": llm_reply},
        snapshot_id=snapshot_id,
    )
    job_payload["id"] = job_id

    # 标记当前回合阻滞（直到状态更新完成）
    _store.set_round_blockers(request.session_id, branch_id, round_no, keys=["gating"])
 
    # 入队（如果队列可用），并标记 Outbox 已投递（阻滞式：状态更新）
    try:
        assert _job_queue is not None
        hint = _job_queue.worker_hint()
        if hint == "null":
            # 开发态队列不可用：同步执行作业以避免阻滞
            result_obj = process_job(job_payload)
            _store.update_job_status(request.session_id, job_id, status="completed", result=result_obj)
            logger.info("chat_send: executed gating job synchronously (null queue)")
        else:
            queue_job_id = _job_queue.enqueue(job_payload)
            _store.mark_job_enqueued(request.session_id, job_id)
            logger.info("chat_send: gating job enqueued=%s", queue_job_id)
    except Exception as exc:
        logger.warning("chat_send: enqueue/execute gating job failed: %s", exc)
 
    # 非阻滞作业：生成幕后指导（LLM-3），完成后写入 LSS.gudance，供下次提示合入
    try:
        assert _job_queue is not None
        guidance_job_id = _store.record_job(
            session_id=request.session_id,
            job_type=JobType.GUIDANCE,
            branch_id=branch_id,
            anchor_round=round_no,
            base_range_end=turn_count,
            gating=False,
            payload={"text": llm_reply},
            snapshot_id=snapshot_id,
        )
        guidance_payload = {
            "id": guidance_job_id,
            "session_id": request.session_id,
            "branch_id": branch_id,
            "anchor_round": round_no,
            "snapshot_id": snapshot_id,
            "type": JobType.GUIDANCE,
            "base_range_end": turn_count,
            "gating": False,
            "payload": {"text": llm_reply},
        }
        hint2 = _job_queue.worker_hint()
        if hint2 == "null":
            result_obj2 = process_job(guidance_payload)
            _store.update_job_status(request.session_id, guidance_job_id, status="completed", result=result_obj2)
            logger.info("chat_send: executed guidance job synchronously (null queue)")
        else:
            queue_job_id2 = _job_queue.enqueue(guidance_payload)
            _store.mark_job_enqueued(request.session_id, guidance_job_id)
            logger.info("chat_send: guidance job enqueued=%s", queue_job_id2)
    except Exception as exc:
        logger.warning("chat_send: enqueue/execute guidance job failed: %s", exc)

    # 更新会话 LSS 与 turn_count（由工作流中 IncrementCounter 写入到 state）
    state_snapshot = state.get_working_state()
    try:
        # 使用存储层的私有辅助进行原子更新（MVP 简化）
        sess_path = _store._session_dir(request.session_id) / "session.json"  # type: ignore
        data = _store._read_json(sess_path)  # type: ignore
        data["lss_state_json"] = state_snapshot
        data["turn_count"] = int(state_snapshot.get("turn_count", round_no))
        _store._write_json_atomic(sess_path, data)  # type: ignore
    except Exception as exc:
        logger.warning("chat_send: update session LSS failed: %s", exc)

    # 返回前读取当前回合状态（若同步执行作业，则可能已解锁）
    round_status_str = "pending_blocked"
    try:
        rd = _store.get_round(request.session_id, branch_id, round_no)
        round_status_str = str(rd.get("status") or round_status_str)
    except Exception:
        pass

    return ChatSendResponse(
        round_no=round_no,
        snapshot_id=snapshot_id,
        llm_reply=llm_reply,
        items=final_items,
        logs=result.logs,
        metrics=result.metrics,
        state_snapshot=state_snapshot,
        round_status=round_status_str,
    )
# --- Additional models and endpoints for status / reroll / branch ---

class RoundStatusResponse(BaseModel):
    round_no: int
    status: str
    blockers: List[str]
    state_snapshot: Dict[str, Any]


class RerollRequest(BaseModel):
    session_id: str
    branch_id: str
    round_no: int
    ref: str = Field(default="main@1")
    extras: Optional[Dict[str, Any]] = Field(default=None)
    resources: Optional[Dict[str, Any]] = Field(default=None)


class RerollResponse(BaseModel):
    round_no: int
    llm_reply: str
    items: List[Dict[str, Any]]
    logs: List[str]
    metrics: Dict[str, Any]
    state_snapshot: Dict[str, Any]
    round_status: str


class BranchRequest(BaseModel):
    session_id: str
    from_round: Optional[int] = Field(default=None, description="从哪个回合分叉；可选")
    parent_branch_id: Optional[str] = Field(default=None, description="父分支ID；缺省为当前活动分支")
    set_active: bool = Field(default=True, description="是否将新分支设为活动分支")


class BranchResponse(BaseModel):
    branch_id: str


@router.get("/round/{session_id}/{branch_id}/{round_no}/status", response_model=RoundStatusResponse, summary="查询回合阻滞与状态（附当前会话 LSS 快照）")
async def chat_round_status(session_id: str, branch_id: str, round_no: int) -> RoundStatusResponse:
    _require_ready()
    assert _store is not None
    try:
        data = _store.get_round(session_id, branch_id, int(round_no))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="round not found")
    # 读取当前会话 LSS（用于前端在阻滞解除后及时看到状态更新）
    sess = _store.load_session(session_id)
    lss_snapshot = dict(sess.get("lss_state_json") or {})
    return RoundStatusResponse(
        round_no=int(data.get("round_no", round_no)),
        status=str(data.get("status") or "open"),
        blockers=list(data.get("blockers") or []),
        state_snapshot=lss_snapshot,
    )


@router.post("/round/reroll", response_model=RerollResponse, summary="对指定回合进行重roll，仅重算故事回复，不触发异步作业，也不递增回合计数")
async def chat_round_reroll(request: RerollRequest) -> RerollResponse:
    _require_ready()
    assert _store is not None
    reg, loader, executor = _get_registry_loader_executor()
    if executor is None:
        raise HTTPException(status_code=503, detail="Flow engine not initialized")

    # 读取回合与锚点快照
    try:
        round_data = _store.get_round(request.session_id, request.branch_id, int(request.round_no))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="round not found")

    snapshot_id = str(round_data.get("snapshot_id") or "")
    if not snapshot_id:
        raise HTTPException(status_code=400, detail="round missing snapshot_id")
    snap = _store.get_snapshot(request.session_id, snapshot_id)
    lss_state = dict(snap.get("lss_state_json") or {})

    # 构建上下文（基于当时的 LSS 快照）
    resources: Dict[str, Any] = {"llm": llm_adapter}
    if request.resources and isinstance(request.resources, dict):
        for k in ("llm", "code", "code_funcs"):
            if k in request.resources:
                resources[k] = request.resources[k]

    state = StateManager(lss_state)
    ctx = NodeContext(session_id=request.session_id, state=state, resources=resources)

    # 使用回合原始用户输入
    user_input = str(round_data.get("user_input") or "")
    item: Dict[str, Any] = {"user_input": user_input}
    if request.extras and isinstance(request.extras, dict):
        for k, v in request.extras.items():
            item[k] = v

    # 执行主通路（仅用于重算故事回复）
    try:
        result = executor.execute_ref(request.ref, [item], ctx)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("chat_round_reroll flow failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Flow execution failed: {type(exc).__name__}: {exc}") from exc

    final_items = result.items or [{}]
    first = final_items[0] if final_items else {}
    llm_reply = str(first.get("llm_response") or first.get("narrative") or "")

    # 仅覆盖该回合可见的故事回复，不触发新的 gating/非阻滞作业
    _store.save_round_llm_reply(request.session_id, request.branch_id, int(request.round_no), llm_reply)

    # 回合状态维持原状
    round_after = _store.get_round(request.session_id, request.branch_id, int(request.round_no))
    return RerollResponse(
        round_no=int(round_after.get("round_no", request.round_no)),
        llm_reply=llm_reply,
        items=final_items,
        logs=result.logs,
        metrics=result.metrics,
        state_snapshot=state.get_working_state(),
        round_status=str(round_after.get("status") or "open"),
    )


@router.post("/branch", response_model=BranchResponse, summary="从指定回合创建新分支")
async def chat_create_branch(req: BranchRequest) -> BranchResponse:
    _require_ready()
    assert _store is not None

    sess = _store.load_session(req.session_id)
    parent = req.parent_branch_id or str(sess.get("active_branch_id") or "")
    if not parent:
        raise HTTPException(status_code=404, detail="parent branch not found")

    branch_id = _store.create_branch(req.session_id, parent_branch_id=parent, fork_from_round=req.from_round)
    if req.set_active:
        _store.set_active_branch(req.session_id, branch_id)

    return BranchResponse(branch_id=branch_id)