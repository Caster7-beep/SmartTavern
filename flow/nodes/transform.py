import logging
from typing import Any, Dict, List, Optional

from flow.node_base import Node, NodeContext, NodeResult
from flow.registry import as_node

logger = logging.getLogger(__name__)

try:
    import jmespath  # type: ignore
except Exception as _exc:  # pragma: no cover
    jmespath = None  # 懒加载，如果实际用到会报错提示

def _search(expr: str, data: Any) -> Any:
    """jmespath.search 的一个简单包装，用于在未安装jmespath时提供更清晰的错误提示。"""
    if jmespath is None:
        raise RuntimeError("jmespath is required for transform nodes; please add 'jmespath' to requirements.txt")
    return jmespath.search(expr, data)


@as_node("Map")
class MapNode(Node):
    """
    映射节点。
    
    使用JMESPath表达式对每个item进行操作，计算新值并将其设置到新的或已有的字段中。
    这对于数据重塑、提取嵌套值或基于现有值计算新值非常有用。
    
    参数:
      - set: (Dict[str, str]) 一个映射字典，键是目标字段名，值是用于计算该字段值的JMESPath表达式。
             表达式的上下文是当前的item。
      - overwrite: (bool, optional) 如果目标字段已存在，是否覆盖它。默认为True。
    """
    """使用 jmespath 将 item 映射/扩展为新字段。
    params:
      - set: Dict[dest_field, jmespath_expr] 映射表
      - overwrite: bool 是否覆盖已有字段，默认 True
    """

    type_name = "Map"

    def run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        params = self.params or {}
        mapping: Dict[str, str] = dict(params.get("set") or {})
        overwrite: bool = bool(params.get("overwrite", True))

        out_items: List[Dict[str, Any]] = []
        logs: List[str] = []

        for it in items:
            new_it = dict(it)
            try:
                for dst, expr in mapping.items():
                    value = _search(expr, it)
                    if dst in new_it and not overwrite:
                        continue
                    new_it[dst] = value
                out_items.append(new_it)
                logs.append(f"Map applied {len(mapping)} rule(s)")
            except Exception as exc:
                logger.error("Map failed: %s", exc, exc_info=True)
                out_items.append(dict(it))
                logs.append(f"Map error: {exc}")

        return NodeResult(items=out_items, logs=logs, metrics={"type": "Map", "items_out": len(out_items)})


@as_node("Filter")
class FilterNode(Node):
    """
    过滤节点。
    
    使用JMESPath表达式对每个item进行评估，只有当表达式结果为真值时，该item才会被保留在输出中。
    
    参数:
      - where: (str) 一个返回布尔值的JMESPath表达式。
    """
    """过滤 items，保留满足条件的项。
    params:
      - where: str jmespath 布尔表达式
    """

    type_name = "Filter"

    def run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        params = self.params or {}
        expr: Optional[str] = params.get("where")
        if not isinstance(expr, str) or not expr.strip():
            raise ValueError("Filter requires 'where' jmespath expression")

        out_items: List[Dict[str, Any]] = []
        logs: List[str] = []

        for it in items:
            try:
                ok = bool(_search(expr, it))
                if ok:
                    out_items.append(dict(it))
                logs.append(f"Filter[{expr}]: {'keep' if ok else 'drop'}")
            except Exception as exc:
                logger.error("Filter failed: %s", exc, exc_info=True)
                out_items.append(dict(it)) # 出错时保留该item
                logs.append(f"Filter error: {exc}")

        return NodeResult(items=out_items, logs=logs, metrics={"type": "Filter", "items_out": len(out_items)})


@as_node("Merge")
class MergeNode(Node):
    """
    合并节点。
    
    将item中某个字段（其值必须是一个字典）的内容合并到item的顶层。
    
    参数:
      - from_field: (str) 源字段名，其值必须是一个字典。
      - overwrite: (bool, optional) 如果合并时遇到同名字段，是否覆盖。默认为True。
      - prefix: (str, optional) 在合并的键名前添加一个前缀。
    """
    """将 item[from_field] 的字典内容合并到根。
    params:
      - from_field: str 源字段名（其值应为 dict）
      - overwrite: bool 是否覆盖已有字段，默认 True
      - prefix: str 可选，目标字段名前缀
    """

    type_name = "Merge"

    def run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        params = self.params or {}
        from_field: str = str(params.get("from_field", ""))
        overwrite: bool = bool(params.get("overwrite", True))
        prefix: str = str(params.get("prefix", ""))

        if not from_field:
            raise ValueError("Merge requires 'from_field'")

        out_items: List[Dict[str, Any]] = []
        logs: List[str] = []

        for it in items:
            new_it = dict(it)
            try:
                payload = it.get(from_field, {})
                if not isinstance(payload, dict):
                    raise TypeError(f"Merge source '{from_field}' must be a dict")
                for k, v in payload.items():
                    dst_key = f"{prefix}{k}" if prefix else k
                    if dst_key in new_it and not overwrite:
                        continue
                    new_it[dst_key] = v
                out_items.append(new_it)
                logs.append(f"Merge from={from_field} keys={len(payload)}")
            except Exception as exc:
                logger.error("Merge failed: %s", exc, exc_info=True)
                out_items.append(dict(it))
                logs.append(f"Merge error: {exc}")

        return NodeResult(items=out_items, logs=logs, metrics={"type": "Merge", "items_out": len(out_items)})


@as_node("Split")
class SplitNode(Node):
    """
    拆分节点。
    
    该节点能将一个item拆分为多个items。它基于源字段的值（列表或可分割的字符串），
    为其中的每个元素生成一个原始item的副本，并将该元素的值赋给副本中的目标字段。
    
    模式:
      1. 列表模式: 如果源字段是一个列表，则为列表中的每个元素生成一个副本。
      2. 字符串模式: 如果源字段是一个字符串，则先用分隔符将其拆分为一个列表，然后按列表模式处理。
      
    参数:
      - from_field: (str) 源字段名。
      - dest_field: (str, optional) 在每个副本中用于存储拆分出的元素的目标字段名。默认为 'element'。
      - delimiter: (str, optional) 在字符串模式下使用的分隔符。默认为 ','。
    """
    """基于 item[from_field] 生成多项。
    两种模式：
      1) 列表模式：from_field 为 List[Any]，为每个元素复制一份 item，并写入到 dest_field
      2) 字符串模式：from_field 为 str，按 delimiter 分割得到列表，按模式1处理
    params:
      - from_field: str 源字段名
      - dest_field: str 目标字段名（默认 'element'）
      - delimiter: str 用于字符串模式，默认 ','
    """

    type_name = "Split"

    def run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        params = self.params or {}
        from_field: str = str(params.get("from_field", ""))
        dest_field: str = str(params.get("dest_field", "element"))
        delimiter: str = str(params.get("delimiter", ","))

        if not from_field:
            raise ValueError("Split requires 'from_field'")

        out_items: List[Dict[str, Any]] = []
        logs: List[str] = []

        for it in items:
            try:
                src = it.get(from_field)
                elements: List[Any]
                if isinstance(src, list):
                    elements = src
                elif isinstance(src, str):
                    elements = [s for s in src.split(delimiter)]
                else:
                    raise TypeError("Split source must be list or str")
                
                for elem in elements:
                    new_it = dict(it)
                    new_it[dest_field] = elem
                    out_items.append(new_it)
                logs.append(f"Split {len(elements)} element(s) from {from_field} into {dest_field}")
            except Exception as exc:
                logger.error("Split failed: %s", exc, exc_info=True)
                out_items.append(dict(it)) # 出错时保留原始item
                logs.append(f"Split error: {exc}")

        return NodeResult(items=out_items, logs=logs, metrics={"type": "Split", "items_out": len(out_items)})