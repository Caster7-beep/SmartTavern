import json
import os
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime


class SessionStore:
    """
    基于 JSON 文件的会话/分支/回合/快照/作业持久化存储（MVP）。
    - 目录布局（默认 base_dir="storage/sessions"）：
      storage/sessions/{session_id}/session.json
      storage/sessions/{session_id}/branches/{branch_id}/branch.json
      storage/sessions/{session_id}/branches/{branch_id}/rounds/{round_no}.json
      storage/sessions/{session_id}/snapshots/{snapshot_id}.json
      storage/sessions/{session_id}/jobs/{job_id}.json

    一致性策略（开发态优先简化）：
    - 原子写：写入到 .tmp 临时文件后用 os.replace() 覆盖目标。
    - 本地进程锁：对每个路径使用内存锁，避免同进程并发写入互相干扰。
      注意：不同进程间不保证锁；如需要跨进程锁，请引入文件锁库（例如 portalocker）。
    - Outbox 模式（简化版）：Job 记录包含 "enqueued": false，后台轮询将其入队后标记为 true；避免事件丢失。
    """

    def __init__(self, base_dir: str = "storage/sessions") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        # 进程内路径级锁
        self._locks: Dict[Path, threading.Lock] = {}
        self._locks_lock = threading.Lock()

    # -------------- 公共 API --------------

    def create_session(self, initial_state: Dict[str, Any]) -> str:
        """
        创建会话：
        - 生成 session_id
        - 写入 session.json（包含 turn_count=0, lss_state_json=initial_state）
        - 创建默认分支（branch_id=主分支 UUID）
        Returns: session_id
        """
        session_id = self._new_id("sess_")
        now = self._now()
        session_dir = self._session_dir(session_id)
        session_dir.mkdir(parents=True, exist_ok=True)
        default_branch_id = self._new_id("br_")

        data = {
            "id": session_id,
            "created_at": now,
            "turn_count": 0,
            "active_branch_id": default_branch_id,
            "lss_state_json": initial_state or {},
        }
        self._write_json_atomic(session_dir / "session.json", data)

        # 创建默认分支
        self._ensure_branch(session_id, default_branch_id, parent_branch_id=None, fork_from_round=None)

        return session_id

    def load_session(self, session_id: str) -> Dict[str, Any]:
        """读取 session.json 内容。"""
        return self._read_json(self._session_dir(session_id) / "session.json")

    def create_branch(self, session_id: str, parent_branch_id: Optional[str], fork_from_round: Optional[int]) -> str:
        """
        创建新分支：
        - 生成 branch_id
        - 将其记录到 branches/{branch_id}/branch.json
        - 若指定 parent_branch_id + fork_from_round，则记录来源信息
        Returns: branch_id
        """
        branch_id = self._new_id("br_")
        self._ensure_branch(session_id, branch_id, parent_branch_id=parent_branch_id, fork_from_round=fork_from_round)
        # 更新 active_branch_id 为新分支（可选，当前逻辑不自动切换）
        return branch_id

    def set_active_branch(self, session_id: str, branch_id: str) -> None:
        """设置当前会话的活动分支。"""
        sess_path = self._session_dir(session_id) / "session.json"
        sess = self._read_json(sess_path)
        sess["active_branch_id"] = branch_id
        self._write_json_atomic(sess_path, sess)

    def begin_round(
        self,
        session_id: str,
        branch_id: str,
        user_input: str,
        lss_state: Dict[str, Any],
        convo_range_start: int,
        convo_range_end: int,
    ) -> Tuple[int, str]:
        """
        开始一个新回合（以“玩家发送”为锚点）：
        - 计算 round_no = turn_count + 1（会话层计数）
        - 写入回合文件 rounds/{round_no}.json（状态 open）
        - 创建快照（锚定 LSS 与区间），返回 snapshot_id
        - 不在此处递增状态管理器；主通路 inc_turn 节点负责状态计数
        Returns: (round_no, snapshot_id)
        """
        sess_path = self._session_dir(session_id) / "session.json"
        sess = self._read_json(sess_path)
        round_no = int(sess.get("turn_count", 0)) + 1

        # 回合文件
        round_path = self._round_path(session_id, branch_id, round_no)
        round_path.parent.mkdir(parents=True, exist_ok=True)
        round_data = {
            "id": f"{branch_id}:{round_no}",
            "session_id": session_id,
            "branch_id": branch_id,
            "round_no": round_no,
            "user_input": user_input,
            "llm_reply": None,
            "created_at": self._now(),
            "status": "open",  # open -> pending_blocked（如有 gating） -> completed
            "blockers": [],
            "snapshot_id": None,
        }
        self._write_json_atomic(round_path, round_data)

        # 快照（锚）
        snapshot_id = self.create_snapshot(
            session_id=session_id,
            branch_id=branch_id,
            anchor_round=round_no,
            lss_state_json=lss_state or {},
            convo_range_start=convo_range_start,
            convo_range_end=convo_range_end,
            tags=["anchor"],
        )

        # 写回快照 id 到回合
        round_data["snapshot_id"] = snapshot_id
        self._write_json_atomic(round_path, round_data)

        # 注意：不修改 session.turn_count；由主通路中的 IncrementCounter 控制状态层计数
        return round_no, snapshot_id

    def save_round_llm_reply(self, session_id: str, branch_id: str, round_no: int, llm_reply: str) -> None:
        """在回合中记录 LLM-1 的故事回复（玩家应立即可见）。"""
        path = self._round_path(session_id, branch_id, round_no)
        data = self._read_json(path)
        data["llm_reply"] = llm_reply
        self._write_json_atomic(path, data)
    def save_round_messages(self, session_id: str, branch_id: str, round_no: int, messages: List[Dict[str, str]]) -> None:
        """在回合中记录 OpenAI 风格的 messages（用于完整持久化对话上下文）。"""
        path = self._round_path(session_id, branch_id, round_no)
        data = self._read_json(path)
        # 仅保留 role/content 两字段，避免写入多余不兼容结构
        sanitized: List[Dict[str, str]] = []
        for m in (messages or []):
            if isinstance(m, dict):
                role = str(m.get("role") or "")
                content = str(m.get("content") or "")
                sanitized.append({"role": role, "content": content})
        data["messages"] = sanitized
        self._write_json_atomic(path, data)

    def set_round_blockers(self, session_id: str, branch_id: str, round_no: int, keys: List[str]) -> None:
        """为当前回合设置阻滞键集合，并将状态标记为 pending_blocked。"""
        path = self._round_path(session_id, branch_id, round_no)
        data = self._read_json(path)
        uniq = sorted(set((keys or [])))
        data["blockers"] = uniq
        data["status"] = "pending_blocked" if uniq else "open"
        self._write_json_atomic(path, data)

    def resolve_round_blockers(self, session_id: str, branch_id: str, round_no: int, keys: List[str]) -> None:
        """从阻滞集合中移除指定键；若清空则状态回到 open。"""
        path = self._round_path(session_id, branch_id, round_no)
        data = self._read_json(path)
        remain = [k for k in (data.get("blockers") or []) if k not in set(keys or [])]
        data["blockers"] = remain
        data["status"] = "open" if not remain else "pending_blocked"
        self._write_json_atomic(path, data)

    def complete_round(self, session_id: str, branch_id: str, round_no: int) -> None:
        """将回合状态设置为 completed（用于业务显式完成时）。"""
        path = self._round_path(session_id, branch_id, round_no)
        data = self._read_json(path)
        data["status"] = "completed"
        self._write_json_atomic(path, data)

    def record_job(
        self,
        session_id: str,
        job_type: str,
        branch_id: str,
        anchor_round: int,
        base_range_end: int,
        gating: bool,
        payload: Dict[str, Any],
        snapshot_id: str,
    ) -> str:
        """
        记录一个作业条目（Outbox：默认 enqueued=false，由后台轮询处理）：
        - type: StatusUpdate | Guidance | Summarize 等
        - gating: True 表示此作业影响阻滞键（如状态字段），需要在用户下一次发送前完成
        """
        job_id = self._new_id("job_")
        job_dir = self._session_dir(session_id) / "jobs"
        job_dir.mkdir(parents=True, exist_ok=True)
        job_path = job_dir / f"{job_id}.json"
        data = {
            "id": job_id,
            "session_id": session_id,
            "branch_id": branch_id,
            "anchor_round": anchor_round,
            "snapshot_id": snapshot_id,
            "type": job_type,
            "base_range_end": base_range_end,
            "gating": bool(gating),
            "status": "pending",  # pending -> running -> completed/canceled
            "enqueued": False,    # Outbox 标记
            "created_at": self._now(),
            "payload": payload or {},
            "result": None,
        }
        self._write_json_atomic(job_path, data)
        return job_id

    def mark_job_enqueued(self, session_id: str, job_id: str) -> None:
        """标记作业已投递到队列（Outbox→Queue）。"""
        path = self._session_dir(session_id) / "jobs" / f"{job_id}.json"
        data = self._read_json(path)
        data["enqueued"] = True
        self._write_json_atomic(path, data)

    def update_job_status(
        self,
        session_id: str,
        job_id: str,
        status: str,
        result: Optional[Dict[str, Any]] = None,
    ) -> None:
        """更新作业状态及结果。"""
        path = self._session_dir(session_id) / "jobs" / f"{job_id}.json"
        data = self._read_json(path)
        data["status"] = status
        if result is not None:
            data["result"] = result
        self._write_json_atomic(path, data)

    def list_pending_jobs(self, session_id: str) -> List[Dict[str, Any]]:
        """列出当前会话下所有未入队的 pending 作业（Outbox 用途）。"""
        job_dir = self._session_dir(session_id) / "jobs"
        if not job_dir.exists():
            return []
        out: List[Dict[str, Any]] = []
        for p in sorted(job_dir.glob("*.json")):
            try:
                data = self._read_json(p)
                if not data.get("enqueued") and data.get("status") == "pending":
                    out.append(data)
            except Exception:
                continue
        return out

    def create_snapshot(
        self,
        session_id: str,
        branch_id: str,
        anchor_round: int,
        lss_state_json: Dict[str, Any],
        convo_range_start: int,
        convo_range_end: int,
        tags: Optional[List[str]] = None,
    ) -> str:
        """创建并写入一个快照条目，返回 snapshot_id。"""
        snapshot_id = self._new_id("snap_")
        snap_dir = self._session_dir(session_id) / "snapshots"
        snap_dir.mkdir(parents=True, exist_ok=True)
        path = snap_dir / f"{snapshot_id}.json"
        data = {
            "id": snapshot_id,
            "session_id": session_id,
            "branch_id": branch_id,
            "anchor_round": anchor_round,
            "created_at": self._now(),
            "lss_state_json": lss_state_json or {},
            "convo_range_start": int(convo_range_start),
            "convo_range_end": int(convo_range_end),
            "tags": list(tags or []),
        }
        self._write_json_atomic(path, data)
        return snapshot_id

    def get_round(self, session_id: str, branch_id: str, round_no: int) -> Dict[str, Any]:
        """读取指定回合文件。"""
        return self._read_json(self._round_path(session_id, branch_id, round_no))

    def get_snapshot(self, session_id: str, snapshot_id: str) -> Dict[str, Any]:
        """读取指定快照文件。"""
        return self._read_json(self._session_dir(session_id) / "snapshots" / f"{snapshot_id}.json")

    # -------------- 私有/辅助 --------------

    def _ensure_branch(
        self,
        session_id: str,
        branch_id: str,
        parent_branch_id: Optional[str],
        fork_from_round: Optional[int],
    ) -> None:
        """
        确认并创建分支目录与 branch.json。
        """
        bdir = self._branch_dir(session_id, branch_id)
        bdir.mkdir(parents=True, exist_ok=True)
        path = bdir / "branch.json"
        data = {
            "id": branch_id,
            "session_id": session_id,
            "created_at": self._now(),
            "parent_branch_id": parent_branch_id,
            "fork_from_round": fork_from_round,
        }
        self._write_json_atomic(path, data)

    def _session_dir(self, session_id: str) -> Path:
        return self.base_dir / session_id

    def _branch_dir(self, session_id: str, branch_id: str) -> Path:
        return self._session_dir(session_id) / "branches" / branch_id

    def _round_path(self, session_id: str, branch_id: str, round_no: int) -> Path:
        return self._branch_dir(session_id, branch_id) / "rounds" / f"{int(round_no)}.json"

    @staticmethod
    def _new_id(prefix: str) -> str:
        return f"{prefix}{uuid.uuid4().hex}"

    @staticmethod
    def _now() -> str:
        return datetime.utcnow().isoformat(timespec="seconds") + "Z"

    def _get_lock(self, path: Path) -> threading.Lock:
        with self._locks_lock:
            lock = self._locks.get(path)
            if lock is None:
                lock = threading.Lock()
                self._locks[path] = lock
            return lock

    def _write_json_atomic(self, path: Path, data: Dict[str, Any]) -> None:
        """
        原子写 JSON：写入到 .tmp，然后 os.replace() 覆盖目标。
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        lock = self._get_lock(path)
        with lock:
            txt = json.dumps(data, ensure_ascii=False, indent=2)
            with tmp_path.open("w", encoding="utf-8") as fh:
                fh.write(txt)
            # Windows 下 os.replace 覆盖目标
            os.replace(str(tmp_path), str(path))

    @staticmethod
    def _read_json(path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(str(path))
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    def get_latest_round_meta(self, session_id: str, branch_id: str) -> Optional[Dict[str, Any]]:
        """
        获取指定分支的最新回合元数据（基于 rounds 目录中最大 round_no）。
        若目录不存在或没有有效文件，返回 None。
        """
        rounds_dir = self._branch_dir(session_id, branch_id) / "rounds"
        if not rounds_dir.exists():
            return None
        latest_no: Optional[int] = None
        latest_data: Optional[Dict[str, Any]] = None
        for p in sorted(rounds_dir.glob("*.json")):
            try:
                data = self._read_json(p)
                # 优先读取文件内容中的 round_no；否则用文件名 stem 兜底
                rn = int(data.get("round_no") or int(p.stem))
                if latest_no is None or rn > latest_no:
                    latest_no = rn
                    latest_data = data
            except Exception:
                continue
        return latest_data


# ------------- 便捷工厂 -------------

def get_default_store() -> SessionStore:
    """
    获取默认 SessionStore 实例（base_dir="storage/sessions"）。
    """
    return SessionStore()