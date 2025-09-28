import copy
import logging
import threading
from typing import Any, Dict, Iterable, List, Optional, Set

logger = logging.getLogger(__name__)


class StateManager:
    """
    状态管理器。
    
    核心设计是“双态管理”，维护两份状态：
    1. LSS (Last Stable State): 最新稳定状态，通常与持久化层同步。
    2. Working State: 工作状态，反映了当前工作流执行中的最新变更，可能包含未持久化的数据。
    
    此外，它还支持“异步更新回退”机制：
    - 当一个状态字段（key）被标记为正在异步更新（pending）时，任何需要读取该字段用于
      生成提示（prompt）的操作，都会自动回退去读取LSS中的旧值，以避免使用不一致的中间状态。
    - 这种机制对于需要长时间运行的后台更新（如知识库更新、角色状态的复杂计算）非常重要。
    
    所有操作都是线程安全的。
    """
    """Working/LSS双态状态管理，支持异步更新pending回退。"""

    def __init__(self, initial_state: Dict[str, Any]) -> None:
        """
        初始化状态管理器。
        
        Args:
            initial_state: 初始的世界状态或会话状态。
        """
        self._lss = copy.deepcopy(initial_state)  # 最新稳定状态
        self._working_state = copy.deepcopy(initial_state)  # 工作状态
        self._state_lock = threading.Lock()  # 用于保护 _lss 和 _working_state 的锁
        self._pending_keys: Set[str] = set()  # 记录正在进行异步更新的状态键
        self._pending_lock = threading.Lock()  # 用于保护 _pending_keys 的锁

    # 基础读取
    def get_working_state(self) -> Dict[str, Any]:
        """获取当前的工作状态（Working State）的深拷贝。"""
        with self._state_lock:
            return copy.deepcopy(self._working_state)

    def get_for_prompt(self) -> Dict[str, Any]:
        """
        获取用于构造提示（Prompt）的状态。
        
        这个方法是“异步更新回退”机制的核心。它会返回工作状态的一份拷贝，
        但对于那些被标记为“pending”的键，其值会从LSS中获取，确保了提示内容的一致性。
        """
        """返回用于构造提示的状态，pending键使用LSS值进行回退。"""
        with self._state_lock:
            prompt_state = copy.deepcopy(self._working_state)
        with self._pending_lock:
            pending_keys = list(self._pending_keys)
        if pending_keys:
            logger.debug("Prompt fallback: using LSS for keys %s", pending_keys)
            for key in pending_keys:
                if key in self._lss:
                    prompt_state[key] = self._lss[key]
        return prompt_state

    # 同步写入
    def update_state_sync(self, updates: Dict[str, Any]) -> None:
        """
        同步更新状态。
        
        该操作会同时更新工作状态（Working State）和最新稳定状态（LSS）。
        适用于需要立即生效且一致的常规状态变更。
        """
        with self._state_lock:
            self._working_state.update(updates)
            self._lss.update(updates)
        logger.info("Sync state update: %s", list(updates.keys()))

    # 异步写入生命周期
    def start_async_update(self, keys: Iterable[str]) -> None:
        """
        开始一个异步更新过程。
        
        将指定的键（keys）标记为“pending”状态。
        """
        keys_list: List[str] = list(keys)
        with self._pending_lock:
            for key in keys_list:
                self._pending_keys.add(key)
        logger.info("Async update started; pending keys: %s", keys_list)

    def complete_async_update(self, updates: Dict[str, Any]) -> None:
        """
        完成一个异步更新过程。
        
        将更新应用到工作状态和LSS，并从“pending”集合中移除相应的键。
        """
        keys = list(updates.keys())
        with self._state_lock:
            self._working_state.update(updates)
            self._lss.update(updates)
        with self._pending_lock:
            for key in keys:
                self._pending_keys.discard(key)
        logger.info("Async update committed; keys: %s", keys)

    # 便捷API
    def read(self, keys: Optional[Iterable[str]] = None, *, for_prompt: bool = False) -> Dict[str, Any]:
        """
        一个便捷的读取API。
        
        Args:
            keys: 要读取的状态键列表。如果为 None，则返回所有状态。
            for_prompt: 如果为 True，则调用 get_for_prompt 获取带有回退逻辑的状态。
            
        Returns:
            一个包含所请求状态键值对的字典。
        """
        state = self.get_for_prompt() if for_prompt else self.get_working_state()
        if keys is None:
            return state
        out: Dict[str, Any] = {}
        for k in keys:
            if k in state:
                out[k] = state[k]
        return out

    def write_sync(self, updates: Dict[str, Any]) -> None:
        """update_state_sync 的别名，提供更简洁的调用方式。"""
        self.update_state_sync(updates)