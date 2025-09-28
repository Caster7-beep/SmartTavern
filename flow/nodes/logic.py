import logging
from typing import Any, Callable, Dict, List, Optional

from flow.node_base import Node, NodeContext, NodeResult
from flow.registry import as_node

logger = logging.getLogger(__name__)


def _default_context_selector(item: Dict[str, Any], ctx: NodeContext) -> Dict[str, Any]:
    """
    一个默认的回退函数，用于在CodeNode中没有指定有效函数时构造上下文和消息。
    
    它会：
    1. 从状态管理器中获取用于生成提示的状态（get_for_prompt会处理pending回退）。
    2. 将状态扁平化为一个简单的key=value格式的系统消息内容。
    3. 结合item中的'user_input'字段，构建一个包含system和user角色的消息列表。
    """
    """Fallback context/message builder for CodeNode."""
    state_for_prompt = ctx.state.get_for_prompt() if hasattr(ctx.state, "get_for_prompt") else {}
    # 将状态渲染成一个最小化的系统消息内容 (key=value 格式)
    parts = [f"{k}={state_for_prompt.get(k)}" for k in sorted(state_for_prompt.keys())]
    system_content = "[world_state]\n" + "\n".join(parts) if parts else "[world_state]\n(empty)"
    user_text = str(item.get("user_input", "")).strip()
    messages: List[Dict[str, str]] = []
    messages.append({"role": "system", "content": system_content})
    if user_text:
        messages.append({"role": "user", "content": user_text})
    return {
        "messages": messages,
        "context_slots": {"state_for_prompt": state_for_prompt},
    }


@as_node("Code")
class CodeNode(Node):
    """
    代码执行节点。
    
    该节点的核心功能是通过调用一个在资源白名单中预定义的Python函数，来处理
    输入的item并生成新的数据。这通常用于复杂的逻辑处理，如：
    - 根据当前状态和用户输入，动态构建发送给LLM的上下文（prompt）。
    - 对LLM的返回结果进行结构化解析或后处理。
    
    参数:
      - function: (str) 在NodeContext资源中注册的函数名。
      - outputs: (List[str]) 一个列表，指定函数返回的字典中哪些键的值需要被合并回item中。
                 如果省略，则函数返回的所有键值对都会被合并。
    """
    """Select/compose context and prompt pieces from items/state via a whitelisted function."""

    type_name = "Code"

    def run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        fn_name: Optional[str] = None
        outputs: Optional[List[str]] = None
        try:
            # 从节点参数中解析 function 和 outputs
            fn_name = self.params.get("function")
            raw_outputs = self.params.get("outputs")
            if isinstance(raw_outputs, list):
                outputs = [str(x) for x in raw_outputs]
        except Exception:
            fn_name = None
            outputs = None

        # 从NodeContext的资源池中解析出可用的函数白名单
        func_table: Dict[str, Callable[[Dict[str, Any], NodeContext], Dict[str, Any]]] = {}
        res = ctx.get_resource("code_funcs") or ctx.get_resource("code") or {}
        if isinstance(res, dict):
            for k, v in res.items():
                if callable(v):
                    func_table[str(k)] = v

        # 根据函数名从白名单中查找对应的函数，如果找不到则使用默认的回退函数
        fn: Callable[[Dict[str, Any], NodeContext], Dict[str, Any]] = _default_context_selector
        if fn_name and fn_name in func_table:
            fn = func_table[fn_name]
        elif fn_name:
            logger.warning("CodeNode: function '%s' not found; using default", fn_name)

        out_items: List[Dict[str, Any]] = []
        logs: List[str] = []
        for it in items:
            try:
                # 对每个item执行选定的函数
                produced = fn(it, ctx) or {}
                if not isinstance(produced, dict):
                    raise TypeError("Code function must return dict")
                new_it = dict(it)
                # 根据outputs参数决定如何将函数结果合并回item
                if outputs:
                    # 只合并outputs列表中指定的字段
                    for key in outputs:
                        if key in produced:
                            new_it[key] = produced[key]
                else:
                    # 合并所有返回的字段
                    for k, v in produced.items():
                        new_it[k] = v
                out_items.append(new_it)
                logs.append(f"CodeNode: applied {fn.__name__}")
            except Exception as exc:
                logger.error("CodeNode failed on item: %s", exc, exc_info=True)
                out_items.append(dict(it)) # 出错时原样返回item
                logs.append(f"CodeNode error: {exc}")

        return NodeResult(items=out_items, logs=logs, metrics={"type": "Code", "items_out": len(out_items)})