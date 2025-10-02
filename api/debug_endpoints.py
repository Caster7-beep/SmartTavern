import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from services.http_traffic import traffic_log

logger = logging.getLogger(__name__)
router = APIRouter()


class TrafficEvent(BaseModel):
    id: str
    ts: str
    type: str
    service: str
    method: Optional[str] = None
    url: Optional[str] = None
    req_headers: Optional[Dict[str, Any]] = None
    req_body: Optional[str] = None
    status: Optional[int] = None
    elapsed_ms: Optional[int] = None
    resp_headers: Optional[Dict[str, Any]] = None
    resp_body: Optional[str] = None
    error: Optional[str] = None
    pair_id: Optional[str] = None


class TrafficListResponse(BaseModel):
    count: int
    events: List[TrafficEvent]


@router.get("/traffic", response_model=TrafficListResponse, summary="获取最近对外HTTP流量（LLM 调用）")
async def list_traffic(limit: int = Query(200, ge=1, le=500)) -> TrafficListResponse:
    raw = traffic_log.events(limit=limit)
    # Pydantic 会自动验证/转换
    return TrafficListResponse(count=len(raw), events=raw)  # type: ignore[arg-type]


@router.post("/traffic/clear", summary="清空流量缓冲（仅内存）")
async def clear_traffic() -> Dict[str, Any]:
    traffic_log.clear()
    return {"ok": True, "message": "cleared"}