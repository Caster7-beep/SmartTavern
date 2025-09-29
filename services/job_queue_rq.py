import logging
import importlib
from typing import Any, Dict, Optional

from services.job_queue_interface import JobQueue, compute_idempotency_key, NullJobQueue

logger = logging.getLogger(__name__)


class RQJobQueue(JobQueue):
    """
    RQ 队列实现（Redis Queue）。
    - 需要可用的 Redis 与 rq 包；未安装或连接失败时请改用 NullJobQueue。
    - 任务函数通过可导入路径（如 "services.job_worker.process_job"）加载，参数为 job(dict)。

    注意：生产部署需运行 rq worker 进程并配置相同的 Python 模块路径。
    """

    def __init__(self, redis_url: Optional[str] = None, queue_name: str = "smarttavern", job_func_path: str = "services.job_worker.process_job") -> None:
        self._redis_url = redis_url or "redis://localhost:6379/0"
        self._queue_name = queue_name
        self._job_func_path = job_func_path
        # 惰性初始化
        self._rq = None
        self._redis = None
        self._conn = None
        self._queue = None
        self._ensure_queue()

    def _ensure_queue(self) -> None:
        if self._queue is not None:
            return
        try:
            self._rq = importlib.import_module("rq")
            self._redis = importlib.import_module("redis")
        except Exception as exc:
            logger.warning("RQJobQueue: missing dependencies (rq/redis): %s", exc)
            raise

        try:
            self._conn = self._redis.from_url(self._redis_url)
            self._queue = self._rq.Queue(self._queue_name, connection=self._conn)
            logger.info("RQJobQueue: connected queue='%s' url='%s'", self._queue_name, self._redis_url)
        except Exception as exc:
            logger.warning("RQJobQueue: failed to connect redis '%s': %s", self._redis_url, exc)
            raise

    @staticmethod
    def _resolve_func(func_path: str):
        """
        解析可导入的函数路径，例如 "services.job_worker.process_job"。
        """
        try:
            mod_name, func_name = func_path.rsplit(".", 1)
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, func_name)
            if not callable(fn):
                raise TypeError(f"{func_path} is not callable")
            return fn
        except Exception as exc:
            raise ImportError(f"Cannot resolve job function '{func_path}': {exc}") from exc

    def enqueue(self, job: Dict[str, Any]) -> str:
        self._ensure_queue()
        fn = self._resolve_func(self._job_func_path)
        idem = compute_idempotency_key(job)

        # 可选：去重逻辑由调用者控制；此处仅附加 meta。
        try:
            rq_job = self._queue.enqueue(
                fn,
                job,
                job_timeout=job.get("timeout", 300),
                result_ttl=job.get("result_ttl", 3600),
                description=f"{job.get('type')}@{job.get('session_id')}:{job.get('branch_id')}#{job.get('anchor_round')}",
            )
            rq_job.meta = rq_job.meta or {}
            rq_job.meta["idempotency_key"] = idem
            rq_job.save_meta()
            logger.info("RQJobQueue: enqueued rq_id=%s type=%s", rq_job.id, job.get("type"))
            return rq_job.id
        except Exception as exc:
            logger.error("RQJobQueue: enqueue failed: %s", exc, exc_info=True)
            # 上层可选择回退到 NullJobQueue 或重试
            raise

    def cancel(self, job_id: str) -> None:
        self._ensure_queue()
        try:
            Job = getattr(self._rq, "job").Job  # rq.job.Job
            rq_job = Job.fetch(job_id, connection=self._conn)
            rq_job.cancel()
            rq_job.delete()
            logger.info("RQJobQueue: canceled job id=%s", job_id)
        except Exception as exc:
            logger.warning("RQJobQueue: cancel failed id=%s: %s", job_id, exc)

    def status(self, job_id: str) -> Dict[str, Any]:
        self._ensure_queue()
        try:
            Job = getattr(self._rq, "job").Job
            rq_job = Job.fetch(job_id, connection=self._conn)
            state = rq_job.get_status()
            return {
                "id": job_id,
                "status": state,
                "queue": self._queue_name,
                "enqueued_at": str(getattr(rq_job, "enqueued_at", None)),
                "started_at": str(getattr(rq_job, "started_at", None)),
                "ended_at": str(getattr(rq_job, "ended_at", None)),
                "meta": dict(getattr(rq_job, "meta", {}) or {}),
            }
        except Exception as exc:
            logger.warning("RQJobQueue: status failed id=%s: %s", job_id, exc)
            return {"id": job_id, "status": "unknown", "queue": self._queue_name}

    def worker_hint(self) -> str:
        return "rq"


def get_rq_job_queue(redis_url: Optional[str] = None, queue_name: str = "smarttavern", job_func_path: str = "services.job_worker.process_job") -> JobQueue:
    """
    工厂：优先返回 RQ 队列，若依赖缺失或连接失败则回退到 NullJobQueue。
    """
    try:
        return RQJobQueue(redis_url=redis_url, queue_name=queue_name, job_func_path=job_func_path)
    except Exception as exc:
        logger.warning("get_rq_job_queue: fallback to NullJobQueue due to: %s", exc)
        return NullJobQueue()