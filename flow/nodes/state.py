import logging
from typing import Any, Dict, List, Optional

from flow.node_base import Node, NodeContext, NodeResult
from flow.registry import as_node

logger = logging.getLogger(__name__)


@as_node("ReadState")
class ReadStateNode(Node):
    """
    读取状态节点。
    
    该节点用于从状态管理器中读取一个或多个状态值，并将其作为一个字典
    写入到每个item的指定字段中。
    
    参数:
      - keys: (List[str], optional) 一个包含要读取的状态键的列表。如果省略，则读取所有状态。
      - into: (str, optional) 用于存储状态值的目标字段名，默认为 'state'。
      - for_prompt: (bool, optional) 是否启用“为提示读取”模式。如果为True，
                    对于正在异步更新（pending）的键，将回退读取其在LSS中的旧值。默认为False。
    """
    """读取状态并写入到item指定字段。
    params:
      - keys: List[str] 要读取的键（可选，省略表示读取全部）
      - into: str 写入item的字段名，默认 'state'
      - for_prompt: bool 是否使用LSS对pending键进行回退，默认 False
    """

    type_name = "ReadState"

    def run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        params = self.params or {}
        keys = params.get("keys")
        into = str(params.get("into", "state"))
        for_prompt = bool(params.get("for_prompt", False))

        out_items: List[Dict[str, Any]] = []
        logs: List[str] = []
        for it in items:
            try:
                state_slice: Dict[str, Any] = {}
                if keys is None:
                    # 读取所有状态
                    state_slice = ctx.state.read(None, for_prompt=for_prompt)
                else:
                    if not isinstance(keys, list):
                        raise TypeError("ReadState.keys must be a list")
                    # 读取指定的键
                    state_slice = ctx.state.read(keys, for_prompt=for_prompt)
                
                new_it = dict(it)
                new_it[into] = state_slice
                out_items.append(new_it)
                logs.append(f"ReadState into={into} keys={keys if keys is not None else '<all>'}")
            except Exception as exc:
                logger.error("ReadState failed: %s", exc, exc_info=True)
                out_items.append(dict(it))
                logs.append(f"ReadState error: {exc}")

        return NodeResult(items=out_items, logs=logs, metrics={"type": "ReadState", "items_out": len(out_items)})


@as_node("WriteState")
class WriteStateNode(Node):
    """
    写入状态节点。
    
    该节点用于将数据同步写入到状态管理器中。它不修改流经的items，仅产生副作用。
    支持两种写入方式：
    1. 直接写入固定的键值对。
    2. 从item的字段中动态提取值并映射到状态的字段中。
    
    参数:
      - updates: (Dict[str, Any], optional) 一个包含直接要写入状态的键值对的字典。
      - from_item_map: (Dict[str, str], optional) 一个映射关系字典，
                       键是item中的源字段名，值是状态中的目标字段名。
                       例如：{'llm_response': 'last_narrative'}
    """
    """将更新写入状态（同步更新）。
    params:
      - updates: Dict[str, Any] 直接写入的键值对
      - from_item_map: Dict[str, str] 从item字段映射到state字段，如 {'llm_response': 'last_response'}
    """

    type_name = "WriteState"

    def run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        params = self.params or {}
        direct_updates: Dict[str, Any] = dict(params.get("updates") or {})
        from_item_map: Dict[str, str] = dict(params.get("from_item_map") or {})

        logs: List[str] = []

        # 从第一个item中提取并映射需要更新的值
        # 注意：当前实现只使用第一个item作为数据源，未来可扩展为更复杂的合并策略
        src_item = items[0] if items else {}
        mapped_updates: Dict[str, Any] = {}
        for src, dst in from_item_map.items():
            if src in src_item:
                mapped_updates[dst] = src_item[src]

        # 合并直接更新和映射更新
        combined_updates = {**direct_updates, **mapped_updates}

        if combined_updates:
            try:
                # 执行同步写入操作
                ctx.state.update_state_sync(combined_updates)
                logs.append(f"WriteState committed: {list(combined_updates.keys())}")
            except Exception as exc:
                logger.error("WriteState failed: %s", exc, exc_info=True)
                logs.append(f"WriteState error: {exc}")
        else:
            logs.append("WriteState no-op: no updates")

        # 写入状态节点不修改items，原样返回
        out_items = [dict(it) for it in items]
        return NodeResult(items=out_items, logs=logs, metrics={"type": "WriteState", "items_out": len(out_items)})


@as_node("IncrementCounter")
class IncrementCounterNode(Node):
    """
    计数器递增节点。
    
    一个专用于将状态中某个数值型字段的值加1的便捷节点。
    这是一个同步写入操作。
    
    参数:
      - field: (str) 需要递增其值的状态字段名（必填）。
    """
    """将指定计数器字段 +1 并写回状态（同步）。
    params:
      - field: str 需要递增的状态字段名（必填）
    """

    type_name = "IncrementCounter"

    def run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        params = self.params or {}
        field = params.get("field")
        if not isinstance(field, str) or not field:
            raise ValueError("IncrementCounter requires 'field' (non-empty string)")

        try:
            # 1. 读取当前值
            state = ctx.state.get_working_state()
            current = int(state.get(field, 0))
            # 2. 写入新值
            ctx.state.update_state_sync({field: current + 1})
            logs = [f"IncrementCounter: {field} -> {current + 1}"]
        except Exception as exc:
            logger.error("IncrementCounter failed: %s", exc, exc_info=True)
            logs = [f"IncrementCounter error: {exc}"]

        # 该节点不修改items，原样返回
        out_items = [dict(it) for it in items]
        return NodeResult(items=out_items, logs=logs, metrics={"type": "IncrementCounter", "items_out": len(out_items)})