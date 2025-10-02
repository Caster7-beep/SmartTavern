import logging
from pathlib import Path
from typing import Any, Dict, List

from services.session_store import get_default_store, SessionStore
from services.llm_adapter import llm_adapter
from services.job_queue_interface import JobType
from flow.registry import NodeRegistry
from flow.ir import IRLoader
from flow.executor import FlowExecutor
from flow.node_base import NodeContext
from flow.state_manager import StateManager
from services.code_funcs import build_analyzer_messages, build_guidance_messages, derive_status_hp

logger = logging.getLogger(__name__)


def _update_session_lss(session_id: str, updates: Dict[str, Any]) -> None:
    """
    直接更新 session.json 内的 lss_state_json（MVP 简化）。
    """
    store = get_default_store()
    sess_path = store._session_dir(session_id) / "session.json"  # type: ignore
    data = store._read_json(sess_path)  # type: ignore
    lss = dict(data.get("lss_state_json") or {})
    lss.update(updates or {})
    data["lss_state_json"] = lss
    store._write_json_atomic(sess_path, data)  # type: ignore


def _status_update_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    基于子流 status_update@1 执行状态更新（gating 作业）。
    - 输入：job.payload.text
    - 输出：result.updated 包含更新的键值（如 protagonist_mood）
    - 流程：加载 IR/节点 → 执行子流 → 写回会话 LSS → 解除阻滞并完成回合
    """
    store: SessionStore = get_default_store()

    session_id = str(job.get("session_id"))
    branch_id = str(job.get("branch_id"))
    round_no = int(job.get("anchor_round") or 0)
    snapshot_id = str(job.get("snapshot_id") or "")
    payload = dict(job.get("payload") or {})
    reply_text = str(payload.get("text") or "")

    # 1) 基于会话 LSS 构造上下文
    sess_path = store._session_dir(session_id) / "session.json"  # type: ignore
    sess_data = store._read_json(sess_path)  # type: ignore
    lss_state = dict(sess_data.get("lss_state_json") or {})
    state = StateManager(lss_state)
    ctx = NodeContext(
        session_id=session_id,
        state=state,
        resources={"llm": llm_adapter, "code_funcs": {"build_analyzer_messages": build_analyzer_messages, "derive_status_hp": derive_status_hp}},
    )

    # 2) 准备执行器与加载子流
    registry = NodeRegistry()
    registry.discover()
    loader = IRLoader(schema_path=str(Path("schemas") / "ir.schema.json"))
    loader.load_dirs([Path("config") / "workflows" / "subflows"])
    executor = FlowExecutor(registry=registry, loader=loader)

    # 3) 执行 status_update 子流
    try:
        result = executor.execute_ref("status_update_hp@1", [{"text": reply_text}], ctx)
        items = result.items or [{}]
        first = items[0] if items else {}
        hp_text = str(first.get("status_hp") or "")
        if not hp_text:
            # 回退：基于叙事文本推导 HP（演示规则）
            try:
                hp_text = derive_status_hp({"text": reply_text}, ctx).get("status_hp", "")  # type: ignore
            except Exception:
                hp_text = "90/100"
    except Exception as exc:
        logger.warning("StatusUpdate(HP) subflow failed, fallback to derive: %s", exc)
        try:
            hp_text = derive_status_hp({"text": reply_text}, ctx).get("status_hp", "")  # type: ignore
        except Exception:
            hp_text = "90/100"

    # 4) 写回 LSS
    _update_session_lss(session_id, {"status_hp": hp_text})

    # 5) 解除阻滞并完成回合
    try:
        store.resolve_round_blockers(session_id, branch_id, round_no, keys=["gating"])
        store.complete_round(session_id, branch_id, round_no)
    except Exception as exc:
        logger.warning("StatusUpdate: resolve blockers failed: %s", exc)

    return {
        "updated": {"status_hp": hp_text},
        "anchor_round": round_no,
        "snapshot_id": snapshot_id,
    }


def process_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    RQ Worker 入口函数。
    - 读取作业类型并执行相应逻辑
    - 更新作业状态（completed/failed）应由上层或外部管理；此处仅返回结果
    """
    try:
        job_type = str(job.get("type") or "")
        if job_type == JobType.STATUS_UPDATE:
            result = _status_update_job(job)
            logger.info("Job(StatusUpdate) done session=%s round=%s", job.get("session_id"), job.get("anchor_round"))
            return {"ok": True, "type": job_type, "result": result}
        elif job_type in (JobType.GUIDANCE, JobType.SUMMARIZE):
            # 使用 guidance 子流生成非阻滞指导文本（示例）
            try:
                store = get_default_store()
                session_id = str(job.get("session_id"))
                sess_path = store._session_dir(session_id) / "session.json"  # type: ignore
                sess_data = store._read_json(sess_path)  # type: ignore
                lss_state = dict(sess_data.get("lss_state_json") or {})
                state = StateManager(lss_state)
                ctx = NodeContext(
                    session_id=session_id,
                    state=state,
                    resources={"llm": llm_adapter, "code_funcs": {"build_guidance_messages": build_guidance_messages}},
                )
                registry = NodeRegistry()
                registry.discover()
                loader = IRLoader(schema_path=str(Path("schemas") / "ir.schema.json"))
                loader.load_dirs([Path("config") / "workflows" / "subflows"])
                executor = FlowExecutor(registry=registry, loader=loader)

                # narrative/text 作为输入
                payload = dict(job.get("payload") or {})
                text = str(payload.get("text") or payload.get("narrative") or "")

                result = executor.execute_ref("guidance@1", [{"narrative": text, "text": text}], ctx)
                items = result.items or [{}]
                first = items[0] if items else {}
                guidance_text = str(first.get("guidance") or first.get("guidance_text") or "")
                
                # 将指导文本写回会话 LSS，供下次 Code 节点合入提示
                _update_session_lss(session_id, {"guidance": guidance_text})
                return {"ok": True, "type": job_type, "result": {"guidance": guidance_text}}
            except Exception as exc:
                logger.info("Job(%s) placeholder fallback due to error: %s", job_type, exc)
                return {"ok": True, "type": job_type, "result": {"placeholder": True}}
        else:
            raise ValueError(f"Unknown job type: {job_type}")
    except Exception as exc:
        logger.error("process_job failed: %s", exc, exc_info=True)
        return {"ok": False, "error": str(exc), "type": str(job.get("type") or "")}