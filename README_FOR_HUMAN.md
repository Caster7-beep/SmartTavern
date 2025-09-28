# SmartTavern 技术架构文档

## 1. 概述

本项目是一个专为LLM文字冒险游戏设计的、可配置的工作流引擎。其核心目标是通过将业务逻辑从代码中解耦到数据（YAML文件）中，实现高度的灵活性和可扩展性。开发者可以通过定义工作流中间表示（IR）来编排复杂的交互逻辑，而无需修改核心引擎代码。

## 2. 核心架构 (The `flow` package)

新架构围绕“Flow”模型构建，其设计灵感借鉴了n8n的IO模型和声明式工作流思想。主要组件如下：

### 2.1. 节点 (`flow.node_base.py` & `flow/nodes/`)

-   **`Node` (抽象基类)**: 所有操作单元的基类，定义了 `run(items, ctx)` 接口。节点被设计为近乎纯函数，接收 `items` (一个字典列表) 并返回一个新的 `NodeResult` 对象，不应原地修改输入。
-   **`NodeContext`**: 节点的执行上下文，由执行器注入。它包含了对 `session_id`、状态管理器 (`StateManager`) 和共享资源（如LLM适配器、白名单函数）的访问权限。
-   **原子节点库 (`flow/nodes/`)**:
    -   **`CodeNode`**: 执行白名单中的Python函数，用于复杂的逻辑，如动态提示构建。
    -   **`LLMChatNode`**: 封装了对 `LLMAdapter` 的调用。
    -   **`ReadState`/`WriteState`**: 提供对状态管理器的读写接口。
    -   **`Map`/`Filter`/`Merge`/`Split`**: 基于 `jmespath` 的数据转换节点。

### 2.2. 节点注册中心 (`flow.registry.py`)

-   **`NodeRegistry`**: 维护从节点类型字符串（如 `"LLMChat"`）到 `Node` 子类的映射。
-   **自动发现**: 引擎启动时，`discover()` 方法会自动扫描 `flow.nodes` 包下的所有模块，查找 `Node` 的子类并使用其 `type_name` 或类名进行注册。这使得添加新节点无需修改任何注册代码。

### 2.3. 工作流中间表示 (IR) (`flow.ir.py`)

-   **格式**: 工作流使用YAML定义，其结构由 `schemas/ir.schema.json` 强制校验。
-   **核心结构**:
    -   `id`: 工作流的唯一标识。
    -   `version`: 版本号。
    -   `entry`: 入口节点的ID。
    -   `nodes`: 节点定义列表。
-   **`IRLoader`**: 负责从磁盘加载、校验YAML文件，并将其索引为 `id@version` 的形式，同时为每个工作流构建一个节点ID到节点定义的快速查找映射（Node Map）。

### 2.4. 执行器 (`flow.executor.py`)

-   **`FlowExecutor`**: 引擎的核心，扮演解释器的角色。
-   **执行流程**:
    1.  接收到一个工作流引用 (`ref`) 或文档 (`doc`) 后，它会找到入口节点。
    2.  通过 `_run_spec` 方法递归地遍历节点。
    3.  **组合语义处理**: 当遇到特定类型（`Sequence`, `If`, `Subflow`）的节点时，执行器会实现其组合逻辑。例如，`Sequence` 会按顺序执行其 `children` 列表中的节点，并将前一个节点的输出作为后一个节点的输入。
    4.  **原子节点分派**: 当遇到原子节点类型时，它会从 `NodeRegistry` 获取对应的类，实例化该节点，并调用其 `safe_run` 方法。

### 2.5. 状态管理 (`flow.state_manager.py`)

-   **`StateManager`**: 实现了双态管理模型，确保数据一致性。
    -   **`_lss` (Last Stable State)**: 最新稳定状态，通常与持久化层同步。
    -   **`_working_state`**: 工作状态，反映当前流程中的变更。
-   **异步更新回退**:
    -   通过 `start_async_update(keys)` 可将某些状态键标记为 `pending`。
    -   当调用 `get_for_prompt()` 时，管理器会返回 `_working_state` 的一个副本，但会将所有 `pending` 的键的值替换为 `_lss` 中的旧值。这对于避免在生成提示时使用到不完整的、正在后台更新的状态至关重要。
    -   更新完成后，通过 `complete_async_update(updates)` 提交变更并移除 `pending` 标记。

## 3. 数据流 (I/O Model)

-   **`items`**: 工作流中数据的载体是 `List[Dict[str, Any]]`。每个节点接收一个 `items` 列表，处理后返回一个新的 `items` 列表。
-   **n8n模型**: 这种“项数组”模型允许节点进行一对一、一对多（如 `Split`）、多对一（未来可实现的 `Join`）和多对多（如 `Map`）的数据转换，具有高度的灵活性。

## 4. 已移除的旧版模块

早期版本中存在 `core.Engine`、`workflow.WorkflowRunner` 与 `prompt_engine.PromptConstructor` 等模块，用于承载旧的会话管理与模板逻辑。随着 Flow 架构成为唯一实现，这些目录已从代码库移除，仅在此文档中保留描述以记录设计演进。

