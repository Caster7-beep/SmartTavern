import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _redact_url(url: str) -> str:
    try:
        parts = urlparse(url)
        q = parse_qsl(parts.query, keep_blank_values=True)
        redacted_keys = {"key", "api_key", "apikey", "token", "access_token"}
        q2 = []
        for k, v in q:
            if k.lower() in redacted_keys:
                q2.append((k, "***"))
            else:
                q2.append((k, v))
        new_query = urlencode(q2)
        return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))
    except Exception:
        return url


def _redact_headers(headers: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (headers or {}).items():
        kl = str(k).lower()
        if ("authorization" in kl) or ("api-key" in kl) or ("x-goog-api-key" in kl) or ("bearer" in kl):
            out[k] = "***"
        else:
            out[k] = v
    return out


def _to_excerpt(value: Any, limit: int = 2048) -> str:
    try:
        if isinstance(value, (dict, list)):
            s = json.dumps(value, ensure_ascii=False)
        else:
            s = str(value)
        if len(s) > limit:
            return s[:limit] + "...(truncated)"
        return s
    except Exception:
        try:
            s2 = str(value)
            return s2[:limit] + ("...(truncated)" if len(s2) > limit else "")
        except Exception:
            return "<unprintable>"


class TrafficLog:
    """
    线程安全的内存环形缓冲，用于记录对外HTTP调用（开发/调试用）。
    事件格式（示例）：
      {
        "id": "uuid",
        "ts": "2025-10-01T00:00:00.000Z",
        "type": "request" | "response" | "error",
        "service": "llm",
        "method": "POST",
        "url": "https://.../v1beta/models/xxx:generateContent?key=***",
        "req_headers": {...},
        "req_body": "...",
        "status": 200,
        "elapsed_ms": 123,
        "resp_headers": {...},
        "resp_body": "...(excerpt)",
        "pair_id": "same as id of request"
      }
    """
    def __init__(self, max_events: int = 200) -> None:
        self._max = max(50, int(max_events))
        self._buf: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def clear(self) -> None:
        with self._lock:
            self._buf.clear()

    def _append(self, ev: Dict[str, Any]) -> None:
        with self._lock:
            self._buf.append(ev)
            if len(self._buf) > self._max:
                # 环形裁剪
                overflow = len(self._buf) - self._max
                if overflow > 0:
                    del self._buf[0:overflow]

    def log_request(self, *, service: str, method: str, url: str, headers: Optional[Dict[str, Any]] = None, body: Any = None) -> str:
        ev_id = uuid.uuid4().hex
        event = {
            "id": ev_id,
            "ts": _iso_now(),
            "type": "request",
            "service": service,
            "method": str(method or "").upper(),
            "url": _redact_url(url),
            "req_headers": _redact_headers(headers or {}),
            "req_body": _to_excerpt(body),
        }
        self._append(event)
        return ev_id

    def log_response(self, *, pair_id: str, status: int, headers: Optional[Dict[str, Any]] = None, body: Any = None, elapsed_ms: Optional[int] = None) -> str:
        ev_id = uuid.uuid4().hex
        event = {
            "id": ev_id,
            "ts": _iso_now(),
            "type": "response",
            "service": "llm",
            "status": int(status),
            "elapsed_ms": int(elapsed_ms or 0),
            "resp_headers": _redact_headers(headers or {}),
            "resp_body": _to_excerpt(body),
            "pair_id": pair_id,
        }
        self._append(event)
        return ev_id

    def log_error(self, *, pair_id: Optional[str], error: str, status: Optional[int] = None) -> str:
        ev_id = uuid.uuid4().hex
        event = {
            "id": ev_id,
            "ts": _iso_now(),
            "type": "error",
            "service": "llm",
            "status": int(status) if status is not None else None,
            "error": _to_excerpt(error, limit=1024),
            "pair_id": pair_id,
        }
        self._append(event)
        return ev_id

    def events(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            if limit <= 0 or limit >= len(self._buf):
                return list(self._buf)
            return list(self._buf[-limit:])


# 全局单例
traffic_log = TrafficLog(max_events=300)