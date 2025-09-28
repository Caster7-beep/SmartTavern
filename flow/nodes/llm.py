import logging
from typing import Any, Dict, List, Optional

from flow.node_base import Node, NodeContext, NodeResult
from flow.registry import as_node
from services.llm_adapter import llm_adapter as default_llm

logger = logging.getLogger(__name__)


def _fallback_messages(item: Dict[str, Any], ctx: NodeContext) -> List[Dict[str, str]]:
    """
    一个回退函数，用于在item中缺少'messages'字段时，构造一个最小化的消息列表。
    
    其逻辑与 CodeNode 的默认函数类似，都是基于当前状态和用户输入来构建
    一个 system + user 的消息结构。
    """
    """构造最小消息列表：system(world_state) + user(user_input)。仅在缺少messages时使用。"""
    state_for_prompt = ctx.state.get_for_prompt() if hasattr(ctx.state, "get_for_prompt") else {}
    parts = [f"{k}={state_for_prompt.get(k)}" for k in sorted(state_for_prompt.keys())]
    system_content = "[world_state]\n" + "\n".join(parts) if parts else "[world_state]\n(empty)"
    user_text = str(item.get("user_input", "")).strip()
    msgs: List[Dict[str, str]] = [{"role": "system", "content": system_content}]
    if user_text:
        msgs.append({"role": "user", "content": user_text})
    return msgs


@as_node("LLMChat")
class LLMChatNode(Node):
    """
    LLM（大语言模型）调用节点。
    
    该节点负责与语言模型进行交互。它会从输入的item中提取消息列表，
    调用指定的模型，然后将模型返回的文本结果写回到item的一个新字段中。
    
    参数:
      - model: (str) 要使用的模型名称。这个名称会传递给LLM适配器，由适配器解析为具体的模型实例。
      - messages_from: (str) 从item中读取消息列表的字段名，默认为 'messages'。
      - response_field: (str) 用于存储LLM返回结果的字段名，默认为 'llm_response'。
    """
    """调用LLM并将输出写回 item 指定字段。参数:
    - model: 使用的模型名称（映射由适配器处理）
    - messages_from: 从 item 中读取消息的字段名（缺省 'messages'）
    - response_field: 写回LLM输出的字段名（缺省 'llm_response'）
    """

    type_name = "LLMChat"

    def run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        params = self.params or {}
        model: str = str(params.get("model", "narrative-llm"))
        messages_field: str = str(params.get("messages_from", "messages"))
        response_field: str = str(params.get("response_field", "llm_response"))

        # 从上下文中获取LLM适配器实例，如果不存在则使用默认的全局实例
        llm = ctx.get_resource("llm") or default_llm

        out_items: List[Dict[str, Any]] = []
        logs: List[str] = []
        for it in items:
            try:
                # 获取消息列表，如果不存在或格式不正确，则使用回退函数生成
                msgs = it.get(messages_field)
                if not isinstance(msgs, list):
                    msgs = _fallback_messages(it, ctx)
                
                # 调用LLM
                result_text: str = llm.call_model(msgs, model)
                
                # 将结果写回item
                new_it = dict(it)
                new_it[response_field] = result_text
                out_items.append(new_it)
                logs.append(f"LLMChat: model={model}, field={response_field}")
            except Exception as exc:
                logger.error("LLMChat failed: %s", exc, exc_info=True)
                out_items.append(dict(it)) # 出错时原样返回item
                logs.append(f"LLMChat error: {exc}")

        return NodeResult(items=out_items, logs=logs, metrics={"type": "LLMChat", "items_out": len(out_items)})