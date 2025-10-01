import json
import time
from typing import Any, Dict

from fastapi.testclient import TestClient

from main import app


def run_chat_flow_once(user_input: str = "我走进酒馆，环顾四周。") -> Dict[str, Any]:
    """
    端到端测试：
    1) /api/chat/session/start
    2) /api/chat/send
    3) /api/chat/round/{id}/status 轮询一次（非必须）
    返回 send 的响应字典。
    """
    with TestClient(app) as client:
        # 1) 启动会话
        resp = client.post("/api/chat/session/start", json={"use_world_state": True})
        assert resp.status_code == 200, f"start failed: {resp.text}"
        start = resp.json()
        session_id = start["session_id"]
        branch_id = start["branch_id"]

        # 2) 发送一轮
        payload = {
            "session_id": session_id,
            "branch_id": branch_id,
            "user_input": user_input,
            "ref": "main@1",
        }
        resp2 = client.post("/api/chat/send", json=payload)
        assert resp2.status_code == 200, f"send failed: {resp2.text}"
        send = resp2.json()

        # 3) 可选：查询状态（队列为 Null 时通常已同步完成）
        round_no = send["round_no"]
        status_url = f"/api/chat/round/{session_id}/{branch_id}/{round_no}/status"
        resp3 = client.get(status_url)
        if resp3.status_code == 200:
            status = resp3.json()
            # 简单断言：status 存在且包含必需字段
            assert "status" in status and "blockers" in status

        return send


def main() -> None:
    result = run_chat_flow_once()
    # 基本断言
    assert isinstance(result, dict)
    assert "llm_reply" in result and isinstance(result["llm_reply"], str)
    assert "round_no" in result and result["round_no"] >= 1
    print("chat_flow_test OK")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()