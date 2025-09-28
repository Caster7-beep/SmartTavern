import sys
import logging
from pathlib import Path
from typing import Any, Dict, List

# Ensure project root importability
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import yaml  # noqa: E402

from flow.registry import NodeRegistry  # noqa: E402
from flow.ir import IRLoader  # noqa: E402
from flow.executor import FlowExecutor  # noqa: E402
from flow.state_manager import StateManager  # noqa: E402
from flow.node_base import NodeContext  # noqa: E402
from services.llm_adapter import llm_adapter  # noqa: E402


def select_context(item: Dict[str, Any], ctx: NodeContext) -> Dict[str, Any]:
    """示例 Code 函数：根据状态与用户输入生成 messages 与上下文槽位。"""
    state_for_prompt = ctx.state.get_for_prompt() if hasattr(ctx.state, "get_for_prompt") else {}
    parts = [f"{k}={state_for_prompt.get(k)}" for k in sorted(state_for_prompt.keys())]
    system_content = "[world_state]\n" + "\n".join(parts) if parts else "[world_state]\n(empty)"
    user_text = str(item.get("user_input", "")).strip()
    messages: List[Dict[str, str]] = [{"role": "system", "content": system_content}]
    if user_text:
        messages.append({"role": "user", "content": user_text})
    return {
        "messages": messages,
        "context_slots": {"state_for_prompt": state_for_prompt},
    }


def load_initial_state() -> Dict[str, Any]:
    cfg_path = ROOT / "config" / "world_config.yaml"
    if not cfg_path.exists():
        # Fallback minimal state if world_config is missing
        return {"location": "酒馆", "turn_count": 0, "protagonist_mood": "中性"}
    with cfg_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return (data.get("initial_state") or {}) if isinstance(data, dict) else {}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger = logging.getLogger("new_flow_test")

    # 1) 准备依赖：注册节点与加载IR
    registry = NodeRegistry()
    # 自动发现 flow.nodes.* 下的所有 Node 子类
    registry.discover()

    loader = IRLoader(schema_path=str(ROOT / "schemas" / "ir.schema.json"))
    # 加载主工作流与子工作流目录
    loader.load_dirs([ROOT / "config" / "workflows", ROOT / "config" / "workflows" / "subflows"])

    # 2) 准备上下文：状态管理与资源
    initial_state = load_initial_state()
    state = StateManager(initial_state)
    ctx = NodeContext(
        session_id="test-session",
        state=state,
        resources={
            "llm": llm_adapter,           # 使用现有适配器（失败时会有mock回退）
            "code_funcs": {"select_context": select_context},
        },
    )

    # 3) 准备输入 items
    items = [{"user_input": "我推开酒馆的门，环顾四周，打算与老板交谈。"}]

    # 4) 执行主工作流
    executor = FlowExecutor(registry=registry, loader=loader)
    result = executor.execute_ref("main@1", items, ctx)

    # 5) 输出结果与状态快照
    final_items = result.items
    current_state = state.get_working_state()
    logger.info("Final items: %s", final_items)
    logger.info("State snapshot: %s", current_state)

    # 6) 简单断言
    assert isinstance(final_items, list) and len(final_items) >= 1
    assert "narrative" in final_items[0], "期望子工作流生成 narrative 字段"
    assert "last_narrative" in current_state, "期望写回状态字段 last_narrative"

    print("Assertions passed: narrative produced and state updated.")


if __name__ == "__main__":
    main()