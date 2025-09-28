import importlib
import logging
import pkgutil
from typing import Any, Dict, Iterable, List, Optional, Type

from flow.node_base import Node

logger = logging.getLogger(__name__)


class NodeRegistry:
    """
    节点注册中心。
    
    负责管理节点类型名称（如 "LLMChat"）到其对应节点类（如 LLMChatNode）的映射。
    支持手动注册和通过包扫描自动发现节点。
    """
    def __init__(self) -> None:
        """初始化一个空的注册表。"""
        self._registry: Dict[str, Type[Node]] = {}

    def register(self, type_name: str, cls: Type[Node], *, override: bool = False) -> None:
        """
        手动注册一个节点类型。
        
        Args:
            type_name: 节点的类型名称，用于在IR中引用。
            cls: 节点类，必须是 Node 的子类。
            override: 如果为 True，则允许覆盖已存在的同名节点类型。
            
        Raises:
            ValueError: 如果 type_name 无效或已被注册且 override=False。
            TypeError: 如果 cls 不是 Node 的子类。
        """
        if not isinstance(type_name, str) or not type_name:
            raise ValueError("type_name must be non-empty str")
        if not isinstance(cls, type) or not issubclass(cls, Node):
            raise TypeError("cls must be subclass of Node")
        if not override and type_name in self._registry and self._registry[type_name] is not cls:
            raise ValueError(f"Node type '{type_name}' already registered to {self._registry[type_name]}")
        self._registry[type_name] = cls
        logger.debug("Registered node type '%s' → %s", type_name, cls.__name__)

    def get(self, type_name: str) -> Type[Node]:
        """
        根据类型名称获取已注册的节点类。
        
        Args:
            type_name: 节点的类型名称。
            
        Returns:
            对应的节点类。
            
        Raises:
            KeyError: 如果该类型名称未被注册。
        """
        try:
            return self._registry[type_name]
        except KeyError as exc:
            known = ", ".join(sorted(self._registry.keys())) or "<none>"
            raise KeyError(f"Unknown node type '{type_name}'. Known: {known}") from exc

    def known_types(self) -> List[str]:
        """返回所有已注册的节点类型名称列表。"""
        return sorted(self._registry.keys())

    def clear(self) -> None:
        """清空整个注册表。"""
        self._registry.clear()

    def auto_register_module_nodes(self, module: Any) -> int:
        """
        自动扫描并注册单个模块中所有有效的节点类。
        
        一个有效的节点类是指：
        - 是 Node 的子类。
        - 不是 Node 基类本身。
        
        Args:
            module: 要扫描的Python模块对象。
            
        Returns:
            成功注册的节点数量。
        """
        count = 0
        for name in dir(module):
            obj = getattr(module, name, None)
            if isinstance(obj, type) and issubclass(obj, Node) and obj is not Node:
                # 优先使用类中定义的 type_name 属性，否则回退到类名
                type_name = getattr(obj, "type_name", None) or obj.__name__
                self.register(type_name, obj, override=True)
                count += 1
        return count

    def discover(self, packages: Optional[Iterable[str]] = None) -> int:
        """
        在指定的Python包下自动发现并注册所有节点类。
        
        该方法会递归地遍历包下的所有子模块，并调用 auto_register_module_nodes 进行注册。
        
        Args:
            packages: 一个包含包名的可迭代对象。如果为 None，则默认为 ["flow.nodes"]。
            
        Returns:
            总共注册的节点数量。
        """
        total = 0
        packages = list(packages) if packages else ["flow.nodes"]
        for pkg_name in packages:
            try:
                pkg = importlib.import_module(pkg_name)
            except Exception as exc:
                logger.debug("Skip package %s: %s", pkg_name, exc)
                continue
            if not hasattr(pkg, "__path__"):
                # 如果是单个模块文件而不是包
                total += self.auto_register_module_nodes(pkg)
                continue
            # 遍历包及其子包
            for m in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
                _, mod_name, is_pkg = m
                if is_pkg:
                    continue
                try:
                    mod = importlib.import_module(mod_name)
                except Exception as exc:
                    logger.warning("Failed to import module %s: %s", mod_name, exc)
                    continue
                total += self.auto_register_module_nodes(mod)
        logger.info("Discovery completed. Total node classes registered: %d", total)
        return total


# 创建一个默认的全局注册中心实例，方便在项目各处直接使用
default_registry = NodeRegistry()


def register(type_name: str, cls: Type[Node], *, override: bool = False) -> None:
    """全局便捷函数，用于向默认注册中心注册节点。"""
    default_registry.register(type_name, cls, override=override)


def get(type_name: str) -> Type[Node]:
    """全局便捷函数，用于从默认注册中心获取节点类。"""
    return default_registry.get(type_name)


def discover(packages: Optional[Iterable[str]] = None) -> int:
    """全局便捷函数，用于在默认注册中心上执行节点自动发现。"""
    return default_registry.discover(packages)


def known_types() -> List[str]:
    """全局便捷函数，用于获取默认注册中心所有已知的节点类型。"""
    return default_registry.known_types()


def as_node(type_name: Optional[str] = None):
    """
    一个类装饰器，用于将一个类自动注册到默认注册中心。
    
    示例:
        @as_node("MyCustomNode")
        class MyNode(Node):
            ...
    """
    def decorator(cls: Type[Node]) -> Type[Node]:
        tn = type_name or getattr(cls, "type_name", None) or cls.__name__
        default_registry.register(tn, cls, override=True)
        return cls
    return decorator