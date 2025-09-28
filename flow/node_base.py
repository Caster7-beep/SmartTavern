import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, ClassVar


@dataclass
class NodeContext:
    """
    节点执行的上下文环境。
    
    该对象由执行器（Executor）创建并注入到每个节点的 run 方法中，
    提供了对会话状态、共享资源（如LLM适配器、配置等）以及日志记录器的访问。
    """
    session_id: str  # 当前会话的唯一标识符
    state: Any  # 状态管理器实例 (StateManager)，由执行器注入
    resources: Dict[str, Any] = field(default_factory=dict)  # 共享资源池，例如LLM客户端、代码函数等
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))  # 日志记录器实例

    def get_resource(self, name: str) -> Any:
        """从资源池中安全地获取一个共享资源。"""
        return self.resources.get(name)


@dataclass
class NodeResult:
    """
    原子节点执行后返回的结果封装。
    
    包含处理后的数据项（items）、执行日志、性能指标和错误信息。
    执行器会根据此结果来决定工作流的下一步走向。
    """
    items: List[Dict[str, Any]]  # 节点处理后输出的数据项列表
    logs: List[str] = field(default_factory=list)  # 执行过程中产生的日志信息
    metrics: Dict[str, Any] = field(default_factory=dict)  # 性能指标，如执行耗时、输入/输出数量
    errors: List[str] = field(default_factory=list)  # 执行过程中捕获的错误信息


class Node(ABC):
    """
    所有原子节点的抽象基类。
    
    定义了节点的统一接口和基础行为。每个具体的节点（如LLM调用、数据转换等）
    都必须继承自该类并实现 run 方法。
    """
    type_name: ClassVar[str] = "Base"  # 节点的类型名称，用于在IR中标识和在注册中心中查找

    def __init__(self, params: Optional[Dict[str, Any]] = None) -> None:
        """
        初始化节点实例。
        
        Args:
            params: 从工作流IR的节点定义中传入的参数。
        """
        self.params: Dict[str, Any] = params or {}

    @abstractmethod
    def run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        """
        节点的具体执行逻辑，由子类实现。
        
        重要：此方法不应直接修改输入的 items 列表（in-place mutation）。
        
        Args:
            items: 上一节点传入的数据项列表。
            ctx: 当前执行的上下文环境。
            
        Returns:
            一个 NodeResult 对象，包含处理结果。
        """
        """Process items and return a NodeResult. Must not mutate input list in-place."""

    def safe_run(self, items: List[Dict[str, Any]], ctx: NodeContext) -> NodeResult:
        """
        安全执行节点的包装器。
        
        该方法会处理一些通用逻辑，如：
        - 确保输入 items 的格式正确。
        - 捕获执行过程中的异常并将其封装到 NodeResult 中。
        - 自动记录执行耗时等基本性能指标。
        """
        """Wrapper to enforce invariants and capture metrics/errors."""
        start = time.time()
        try:
            normalized_items = self._ensure_items(items)
            result = self.run(normalized_items, ctx)
            if not isinstance(result, NodeResult):
                raise TypeError(f"{self.type_name}.run must return NodeResult")
            # 自动填充通用性能指标
            result.metrics.setdefault("type", self.type_name)
            result.metrics.setdefault("duration_ms", int((time.time() - start) * 1000))
            result.metrics.setdefault("items_in", len(normalized_items))
            result.metrics.setdefault("items_out", len(result.items))
            return result
        except Exception as exc:
            ctx.logger.error("Node %s failed: %s", self.type_name, exc, exc_info=True)
            # 发生异常时，返回一个包含错误信息的NodeResult，并将输入items原样输出
            return NodeResult(items=self._ensure_items(items), logs=[f"error:{exc}"], metrics={
                "type": self.type_name,
                "duration_ms": int((time.time() - start) * 1000),
                "items_in": len(items or []),
                "items_out": len(items or []),
            }, errors=[str(exc)])

    @staticmethod
    def _ensure_items(items: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        确保输入的数据项（items）是一个包含字典的列表，并进行浅拷贝以防止原地修改。
        
        如果输入为 None，则返回一个空列表。
        如果输入格式不正确，则抛出 TypeError。
        """
        if items is None:
            return []
        if not isinstance(items, list):
            raise TypeError("items must be a list of dict")
        out: List[Dict[str, Any]] = []
        for it in items:
            if not isinstance(it, dict):
                raise TypeError("each item must be a dict")
            out.append(dict(it))  # shallow copy
        return out