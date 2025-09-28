import logging
from typing import Any, Dict, List

from flow.node_base import Node, NodeContext, NodeResult
from flow.registry import NodeRegistry
from flow.ir import IRValidationError, IRLoader

logger = logging.getLogger(__name__)


class FlowExecutor:
    """
    工作流执行器（解释器）。
    
    负责解释加载后的工作流IR文档，并根据其定义的组合语义（如Sequence, If, Subflow）
    来调度和执行相应的原子节点。
    """

    def __init__(self, registry: NodeRegistry, loader: IRLoader) -> None:
        """
        初始化执行器。
        
        Args:
            registry: 节点注册中心实例，用于查找节点类型对应的类。
            loader: IR加载器实例，用于获取已注册的工作流文档。
        """
        self.registry = registry
        self.loader = loader

    # Public API
    def execute_doc(self, doc: Dict[str, Any], items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        """
        直接执行一个未经注册的IR文档。
        
        Args:
            doc: 工作流IR文档（字典形式）。
            items: 初始输入的数据项列表。
            ctx: 执行上下文。
            
        Returns:
            整个工作流执行完毕后的最终结果。
        """
        node_map = self._build_node_map(doc)
        entry_id = doc.get("entry")
        if not entry_id:
            raise IRValidationError("IR doc missing 'entry'")
        entry_spec = self._get_node_spec(node_map, entry_id)
        return self._run_spec(entry_spec, doc, node_map, items, ctx)

    def execute_ref(self, ref: str, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        """
        根据引用标识（id@version）执行一个已注册的工作流。
        
        Args:
            ref: 工作流的引用标识。
            items: 初始输入的数据项列表。
            ctx: 执行上下文。
            
        Returns:
            整个工作流执行完毕后的最终结果。
        """
        doc = self.loader.get(ref)
        return self.execute_doc(doc, items, ctx)

    # Internal dispatch
    def _run_spec(
        self,
        spec: Dict[str, Any],
        doc: Dict[str, Any],
        node_map: Dict[str, Dict[str, Any]],
        items: List[Dict[str, Any]],
        ctx: NodeContext,
    ) -> NodeResult:
        """
        内部核心调度方法，根据节点的类型（type）来决定如何执行。
        
        该方法递归地处理组合节点（Sequence, If, Subflow），并最终将
        原子节点的执行分派给其对应的Node子类实例。
        """
        t = (spec.get("type") or "").strip()
        if not t:
            raise IRValidationError(f"Node missing 'type': {spec}")

        # --- 组合语义：顺序执行 (Sequence) ---
        if t == "Sequence":
            children: List[str] = list(spec.get("children") or [])
            logs: List[str] = []
            metrics: Dict[str, Any] = {"type": "Sequence"}
            current_items = items
            for cid in children:
                child_spec = self._get_node_spec(node_map, cid)
                result = self._run_spec(child_spec, doc, node_map, current_items, ctx)
                logs.extend(result.logs)
                current_items = result.items # 将当前节点输出作为下一个节点的输入
            metrics["items_out"] = len(current_items)
            return NodeResult(items=current_items, logs=logs, metrics=metrics)

        # --- 组合语义：条件判断 (If) ---
        if t == "If":
            cond = spec.get("if") or {}
            expr = cond.get("condition")
            then_ids: List[str] = list(cond.get("then") or [])
            else_ids: List[str] = list(cond.get("else") or [])
            if expr is None or not isinstance(expr, str) or not expr.strip():
                raise IRValidationError("If node requires string condition")
            truthy = self._evaluate_condition(expr, items)
            selected = then_ids if truthy else else_ids
            logs: List[str] = [f"If[{spec.get('id')}] condition={'then' if truthy else 'else'}"]
            current_items = items
            for cid in selected:
                child_spec = self._get_node_spec(node_map, cid)
                result = self._run_spec(child_spec, doc, node_map, current_items, ctx)
                logs.extend(result.logs)
                current_items = result.items
            return NodeResult(items=current_items, logs=logs, metrics={"type": "If"})

        # --- 组合语义：子工作流 (Subflow) ---
        if t == "Subflow":
            sf = spec.get("subflow") or {}
            ref = sf.get("ref")
            if not isinstance(ref, str) or "@" not in ref:
                raise IRValidationError("Subflow.ref must be 'id@version'")
            input_map: Dict[str, str] = dict(sf.get("input_map") or {})
            output_map: Dict[str, str] = dict(sf.get("output_map") or {})
            share_state: bool = bool(sf.get("share_state", True))

            # 1. 根据 input_map 映射父流程的 items 到子流程的输入 items
            mapped_items = self._apply_input_map(items, input_map)

            # 2. 解析并执行子工作流
            sub_ctx = ctx  # MVP阶段：仅支持共享状态，未来可扩展状态隔离
            logger.info("Subflow call: %s (share_state=%s)", ref, share_state)
            sf_result = self.execute_ref(ref, mapped_items, sub_ctx)
            sf_result_items = sf_result.items

            # 3. 根据 output_map 将子流程的结果映射回父流程的 items
            merged_items = self._apply_output_map(items, sf_result_items, output_map)
            logs = sf_result.logs + [f"Subflow[{ref}] executed"]
            return NodeResult(items=merged_items, logs=logs, metrics={"type": "Subflow", "items_out": len(merged_items)})

        # --- 原子节点分派 ---
        # 从注册中心获取节点类，实例化并执行
        cls = self.registry.get(t)
        node: Node = cls(params=spec.get("params"))
        return node.safe_run(items, ctx)

    @staticmethod
    def _build_node_map(doc: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """为IR文档构建节点ID到节点定义的映射。与IRLoader中的方法重复，可考虑重构。"""
        nodes = doc.get("nodes") or []
        node_map: Dict[str, Dict[str, Any]] = {}
        for spec in nodes:
            nid = spec.get("id")
            if not nid:
                raise IRValidationError("IR node missing 'id'")
            if nid in node_map:
                raise IRValidationError(f"Duplicate node id: {nid}")
            node_map[nid] = spec
        return node_map

    @staticmethod
    def _get_node_spec(node_map: Dict[str, Dict[str, Any]], node_id: str) -> Dict[str, Any]:
        """从节点映射中安全地获取指定ID的节点定义。"""
        try:
            return node_map[node_id]
        except KeyError as exc:
            raise IRValidationError(f"Unknown node id: {node_id}") from exc

    @staticmethod
    def _evaluate_condition(expr: str, items: List[Dict[str, Any]]) -> bool:
        """
        在一个受限的环境中安全地评估'If'节点的条件表达式。
        
        当前实现使用受限的 eval，只暴露了 'item' (第一个item), 'items' 和 'len'。
        注意：eval 存在安全风险，未来应替换为更安全的表达式引擎（如jmespath）。
        """
        env = {"__builtins__": None}
        local = {
            "item": items[0] if items else {},
            "items": items,
            "len": len,  # 允许使用 len(items)
        }
        try:
            return bool(eval(expr, env, local))
        except Exception:
            return False

    @staticmethod
    def _apply_input_map(items: List[Dict[str, Any]], input_map: Dict[str, str]) -> List[Dict[str, Any]]:
        """将父流程的 items 字段根据 input_map 映射为子流程的输入。"""
        if not input_map:
            return [dict(it) for it in items] # 无映射则直接浅拷贝
        mapped: List[Dict[str, Any]] = []
        for it in items:
            out: Dict[str, Any] = dict(it)
            for src, dst in input_map.items():
                if src in it:
                    out[dst] = it[src]
            mapped.append(out)
        return mapped

    @staticmethod
    def _apply_output_map(
        parent_items: List[Dict[str, Any]],
        sub_items: List[Dict[str, Any]],
        output_map: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """将子流程的输出 items 字段根据 output_map 合并回父流程的 items。"""
        if not output_map:
            return [dict(si) for si in sub_items] # 无映射则直接返回子流程结果
        merged: List[Dict[str, Any]] = []
        # 按索引对齐；如果长度不一致，则以较短的为准
        n = min(len(parent_items), len(sub_items))
        for i in range(n):
            base = dict(parent_items[i]) # 从父 item 开始
            si = sub_items[i]
            for src, dst in output_map.items():
                if src in si:
                    base[dst] = si[src] # 将子 item 的字段合并进来
            merged.append(base)
        # 如果子流程的输出比父流程多，将多余的部分直接附加到结果中
        for i in range(n, len(sub_items)):
            merged.append(dict(sub_items[i]))
        return merged