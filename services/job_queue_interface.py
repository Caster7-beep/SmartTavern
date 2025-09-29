import abc
import hashlib
import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class JobType:
    """
    作业类型常量。
    """
    STATUS_UPDATE = "StatusUpdate"  # 基于 LLM-1 产物更新状态（gating）
    GUIDANCE = "Guidance"           # 生成下一步故事指导（非阻滞）
    SUMMARIZE = "Summarize"         # 段落/区间总结（非阻滞）


def compute_idempotency_key(job: Dict[str, Any]) -> str:
    """
    计算幂等键：
    由 (type, session_id, branch_id, anchor_round, base_range_end, payload_hash) 组成。
    用于外部队列的去重或幂等控制（可选）。
    """
    parts = [
        str(job.get("type", "")),
        str(job.get("session_id", "")),
        str(job.get("branch_id", "")),
        str(job.get("anchor_round", "")),
        str(job.get("base_range_end", "")),
    ]
    payload = dict(job.get("payload") or {})
    payload_str = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(("|".join(parts) + "|" + payload_str).encode("utf-8")).hexdigest()
    return digest


class JobQueue(abc.ABC):
    """
    队列抽象接口。不同实现（RQ/ARQ/Null）需满足以下契约：

    - enqueue(job) 返回队列侧可追踪的 job_id（可与存储层 id 相同或独立）。
    - cancel(job_id) 取消队列中的作业（若不支持则 no-op）。
    - status(job_id) 返回队列侧的状态信息，用于 API 查询。
    - worker_hint() 返回实现类型提示（如 "rq" / "arq" / "null"），便于诊断。
    """

    @abc.abstractmethod
    def enqueue(self, job: Dict[str, Any]) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def cancel(self, job_id: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def status(self, job_id: str) -> Dict[str, Any]:
        raise NotImplementedError

    def worker_hint(self) -> str:
        return "internal"


class NullJobQueue(JobQueue):
    """
    开发态后备实现：不实际执行作业，仅返回幂等键或 job.id。
    - enqueue: 接受作业并返回 job.id 或幂等键
    - cancel: 记录日志
    - status: 始终返回 pending/null
    """

    def enqueue(self, job: Dict[str, Any]) -> str:
        job_id = str(job.get("id") or compute_idempotency_key(job))
        logger.warning("NullJobQueue: accepted job(type=%s) id=%s (no execution)", job.get("type"), job_id)
        return job_id

    def cancel(self, job_id: str) -> None:
        logger.info("NullJobQueue: cancel ignored id=%s", job_id)

    def status(self, job_id: str) -> Dict[str, Any]:
        return {"id": job_id, "status": "pending", "queue": "null", "note": "No worker configured"}

    def worker_hint(self) -> str:
        return "null"


def get_default_job_queue() -> JobQueue:
    """
    获取默认队列实现（开发态为 NullJobQueue）。
    生产态或准备好 Redis 后，可切换到 RQ/ARQ 实现。
    """
    return NullJobQueue()