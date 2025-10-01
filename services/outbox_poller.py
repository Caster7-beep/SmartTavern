import logging
import threading
import time
from typing import Any, Dict, List, Optional

from services.session_store import SessionStore, get_default_store
from services.job_queue_interface import JobQueue
from services.job_worker import process_job

logger = logging.getLogger(__name__)

# 全局轮询器资源（简化管理）
_poller_thread: Optional[threading.Thread] = None
_stop_event: Optional[threading.Event] = None
_store_ref: Optional[SessionStore] = None
_queue_ref: Optional[JobQueue] = None


def _list_sessions(store: SessionStore) -> List[str]:
    """
    列出所有已存在的会话ID（基于存储根目录扫描）。
    """
    base = store.base_dir
    out: List[str] = []
    try:
        for p in base.iterdir():
            if p.is_dir():
                out.append(p.name)
    except Exception as exc:
        logger.warning("OutboxPoller: list sessions failed: %s", exc)
    return sorted(out)


def _poll_loop(interval_sec: float) -> None:
    """
    轮询循环：扫描所有会话下未入队的 pending 作业，投递或在 null 队列下同步执行。
    """
    assert _store_ref is not None and _queue_ref is not None
    store = _store_ref
    queue = _queue_ref
    hint = queue.worker_hint()

    logger.info("OutboxPoller: started (interval=%.1fs, queue=%s)", interval_sec, hint)

    while _stop_event is not None and not _stop_event.is_set():
        try:
            sessions = _list_sessions(store)
            for sid in sessions:
                jobs = store.list_pending_jobs(sid)
                if not jobs:
                    continue
                for job in jobs:
                    try:
                        if hint == "null":
                            # 开发态：直接在服务器进程内执行，避免阻滞
                            result_obj = process_job(job)
                            store.update_job_status(sid, job["id"], status="completed" if result_obj.get("ok") else "failed", result=result_obj)
                            logger.info("OutboxPoller: executed pending job synchronously id=%s", job["id"])
                        else:
                            # 投递到外部队列
                            queue_id = queue.enqueue(job)
                            store.mark_job_enqueued(sid, job["id"])
                            logger.info("OutboxPoller: enqueued pending job id=%s -> %s", job["id"], queue_id)
                    except Exception as exc:
                        logger.warning("OutboxPoller: job dispatch failed id=%s: %s", job.get("id"), exc)
            # 小睡一会儿
            time.sleep(interval_sec)
        except Exception as exc:
            logger.error("OutboxPoller: loop error: %s", exc, exc_info=True)
            time.sleep(interval_sec)


def start_outbox_poller(store: SessionStore, queue: JobQueue, interval_sec: float = 3.0) -> None:
    """
    启动 Outbox 轮询器（后台线程）。
    - store: 会话存储
    - queue: 作业队列（可为 RQ 或 Null）
    - interval_sec: 轮询间隔秒
    """
    global _poller_thread, _stop_event, _store_ref, _queue_ref
    if _poller_thread is not None and _poller_thread.is_alive():
        logger.info("OutboxPoller: already running")
        return
    _store_ref = store
    _queue_ref = queue
    _stop_event = threading.Event()
    _poller_thread = threading.Thread(target=_poll_loop, args=(interval_sec,), name="OutboxPoller", daemon=True)
    _poller_thread.start()


def stop_outbox_poller() -> None:
    """
    停止 Outbox 轮询器。
    """
    global _poller_thread, _stop_event
    try:
        if _stop_event is not None:
            _stop_event.set()
        if _poller_thread is not None:
            _poller_thread.join(timeout=2.0)
    finally:
        _poller_thread = None
        _stop_event = None