import logging
from typing import Any, Dict, List

from flow.node_base import NodeContext

logger = logging.getLogger(__name__)


def _state_to_system_prompt(state_for_prompt: Dict[str, Any]) -> str:
    parts = [f"{k}={state_for_prompt.get(k)}" for k in sorted(state_for_prompt.keys())]
    return "[world_state]\n" + ("\n".join(parts) if parts else "(empty)")


def build_analyzer_messages(item: Dict[str, Any], ctx: NodeContext) -> Dict[str, Any]:
    """
    根据快照状态与传入文本 item['text'] 构建用于分析/状态更新的消息列表。
    输出:
      - messages: List[{"role": "...", "content": "..."}]
    """
    state_for_prompt = ctx.state.get_for_prompt() if hasattr(ctx.state, "get_for_prompt") else {}
    system_content = _state_to_system_prompt(state_for_prompt)

    text = str(item.get("text") or item.get("llm_response") or item.get("narrative") or "")
    messages: List[Dict[str, str]] = [{"role": "system", "content": system_content}]
    if text:
        messages.append({"role": "user", "content": text})
    return {"messages": messages}


def build_guidance_messages(item: Dict[str, Any], ctx: NodeContext) -> Dict[str, Any]:
    """
    构建用于生成“下一步故事指导/幕后设定提示”的消息列表（非阻滞）。
    可结合状态中的关键字段（如 location、protagonist_mood 等）。
    输出:
      - messages: List[{"role": "...", "content": "..."}]
    """
    state_for_prompt = ctx.state.get_for_prompt() if hasattr(ctx.state, "get_for_prompt") else {}
    system_lines = ["[guidance_context]"]
    for k in ("location", "protagonist_mood", "turn_count"):
        if k in state_for_prompt:
            system_lines.append(f"{k}={state_for_prompt.get(k)}")
    system_content = "\n".join(system_lines)

    # 允许携带最近叙事文本（如果存在）
    recent = str(item.get("narrative") or item.get("text") or "")
    messages: List[Dict[str, str]] = [{"role": "system", "content": system_content}]
    if recent:
        messages.append({"role": "user", "content": f"基于近期叙事，生成幕后设定/指导：\n{recent}"})
    else:
        messages.append({"role": "user", "content": "生成下一步幕后设定/故事指导建议。"})
    return {"messages": messages}