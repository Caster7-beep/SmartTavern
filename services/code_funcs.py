import logging
import re
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


def derive_status_hp(item: Dict[str, Any], ctx: NodeContext) -> Dict[str, Any]:
    """
    从 LLM-1 的叙事文本中推导 HP（示例规则）：
    - 初始/当前 HP 优先从 state['status']['hp'] 读取（如 '100/100'）；
      兼容旧键 state['status_hp']（若无则假定 100/100）。
    - 规则（演示）：
        * 包含 “毒蛇”/“蛇咬”/“snake” -> -40
        * 包含 “受伤”/“攻击”/“跌落”/“中毒”/“烧伤” -> -20
        * 包含 “治疗”/“休息”/“包扎”/“用药”/“吃饭” -> +20
      最终 HP 在 [0, Max] 范围内截断。
    返回:
      { "status_hp": "60/100" }   # 子流输出字段（由上层写回到 state.status.hp）
    """
    text = str(item.get("text") or item.get("llm_response") or item.get("narrative") or "")
    state_for_prompt = ctx.state.get_for_prompt() if hasattr(ctx.state, "get_for_prompt") else {}
    
    # 解析当前/最大 HP（优先嵌套 status.hp，兼容 status_hp）
    status_dict = state_for_prompt.get("status") or {}
    hp_raw = str((status_dict.get("hp") if isinstance(status_dict, dict) else None) or state_for_prompt.get("status_hp") or "100/100")
    m = re.match(r"^\s*(\d+)\s*/\s*(\d+)\s*$", hp_raw)
    if m:
        cur, mx = int(m.group(1)), int(m.group(2))
    else:
        cur, mx = 100, 100
    
    lower = text.lower()
    delta = 0
    # 伤害优先规则
    if ("毒蛇" in text) or ("蛇咬" in text) or ("snake" in lower):
        delta -= 40
    elif any(k in text for k in ["受伤", "攻击", "跌落", "中毒", "烧伤"]):
        delta -= 20
    # 恢复
    if any(k in text for k in ["治疗", "休息", "包扎", "用药", "吃饭"]):
        delta += 20
    
    new_hp = max(0, min(mx, cur + delta))
    return {"status_hp": f"{new_hp}/{mx}"}


def select_context(item: Dict[str, Any], ctx: NodeContext) -> Dict[str, Any]:
    """
    主通路上下文构建（用于 Code 节点 function='select_context'）
    - 读取 get_for_prompt()（对 pending 键使用 LSS 回退）
    - 如果存在 guidance，将其作为单独的 system 指令注入
    - 合入用户输入
    返回：{"messages": [...], "context_slots": {...}}
    """
    state_for_prompt = ctx.state.get_for_prompt() if hasattr(ctx.state, "get_for_prompt") else {}

    # world_state 段
    lines: List[str] = ["[world_state]"]
    for k in sorted(state_for_prompt.keys()):
        if k == "guidance":
            continue
        lines.append(f"{k}={state_for_prompt.get(k)}")
    messages: List[Dict[str, str]] = [{"role": "system", "content": "\n".join(lines)}]

    # guidance 段（非阻滞作业生成）
    if state_for_prompt.get("guidance"):
        messages.append({"role": "system", "content": "[guidance]\n" + str(state_for_prompt.get("guidance"))})

    # 用户输入
    user_text = str(item.get("user_input") or item.get("text") or "")
    if user_text:
        messages.append({"role": "user", "content": user_text})

    return {"messages": messages, "context_slots": {"state_for_prompt": state_for_prompt}}