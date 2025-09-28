import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml

try:
    import jsonschema  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError("jsonschema is required. Please add 'jsonschema' to requirements.txt") from exc

logger = logging.getLogger(__name__)


class IRValidationError(ValueError):
    """当工作流中间表示（IR）文档验证失败时抛出的特定异常。"""
    pass


class IRLoader:
    """
    工作流中间表示（IR）加载器。
    
    负责从文件系统加载、验证和索引工作流的IR文档（通常是YAML格式）。
    它使用JSON Schema来确保IR文档的结构正确性，并将加载的流程缓存在内存中以便快速访问。
    """

    def __init__(self, schema_path: str = "schemas/ir.schema.json") -> None:
        """
        初始化加载器。
        
        Args:
            schema_path: IR的JSON Schema文件路径。
        """
        self.schema_path = Path(schema_path)
        self._schema: Dict[str, Any] = self._load_schema(self.schema_path)
        # 索引: "flow_id@version" -> IR文档 (dict)
        self._flows: Dict[str, Dict[str, Any]] = {}
        # 节点映射缓存: "flow_id@version" -> {node_id: node_spec}
        self._node_maps: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def _load_schema(self, path: Path) -> Dict[str, Any]:
        """从磁盘加载IR的JSON Schema文件。"""
        if not path.exists():
            raise FileNotFoundError(f"IR JSON Schema not found at {path}")
        try:
            with path.open("r", encoding="utf-8") as fh:
                text = fh.read()
            # 优先尝试解析JSON，失败则回退到YAML（为开发者提供便利）
            try:
                data = json.loads(text)
            except Exception:
                data = yaml.safe_load(text) or {}
            if not isinstance(data, dict):
                raise ValueError("IR schema must be a JSON object")
            return data
        except Exception as exc:
            raise RuntimeError(f"Failed to load IR schema from {path}: {exc}") from exc

    # -------- Public API --------

    def load_dirs(self, dirs: Iterable[str | Path]) -> int:
        """
        从指定的多个目录中加载所有 .yaml/.yml 文件，并进行验证和索引。
        
        Args:
            dirs: 包含目录路径的可迭代对象。
            
        Returns:
            成功加载并注册的工作流数量。
        """
        count = 0
        for d in dirs:
            base = Path(d)
            if not base.exists():
                logger.info("Skip non-existent IR dir: %s", base)
                continue
            for path in base.rglob("*.yml"):
                count += self._load_file(path)
            for path in base.rglob("*.yaml"):
                count += self._load_file(path)
        logger.info("IRLoader loaded %d flow(s) from %s", count, ", ".join(map(str, dirs)))
        return count

    def load_file(self, path: str | Path) -> str:
        """加载单个IR文件，并返回其引用标识（id@version）。"""
        return self._load_file(Path(path))

    def validate(self, doc: Dict[str, Any]) -> None:
        """
        根据加载的JSON Schema验证一个IR文档。
        
        Args:
            doc: 要验证的IR文档（字典形式）。
            
        Raises:
            IRValidationError: 如果验证失败。
        """
        try:
            jsonschema.validate(instance=doc, schema=self._schema)
        except jsonschema.ValidationError as exc:  # type: ignore
            raise IRValidationError(f"IR schema validation failed: {exc.message}") from exc

    def register(self, doc: Dict[str, Any]) -> str:
        """
        验证并注册一个IR文档。
        
        注册过程包括：
        1. 验证文档结构。
        2. 将文档存入内部索引。
        3. 构建并缓存该文档的节点ID到节点定义的映射，以加速后续查找。
        
        Args:
            doc: 要注册的IR文档。
            
        Returns:
            该工作流的引用标识（id@version）。
        """
        self.validate(doc)
        ref = self._ref_of(doc)
        self._flows[ref] = doc
        self._node_maps[ref] = self._build_node_map(doc)
        logger.debug("Registered IR flow %s", ref)
        return ref

    def get(self, ref: str) -> Dict[str, Any]:
        """根据引用标识（id@version）获取已注册的IR文档。"""
        try:
            return self._flows[ref]
        except KeyError as exc:
            raise KeyError(f"Flow not found: {ref}") from exc

    def node_map(self, ref: str) -> Dict[str, Dict[str, Any]]:
        """获取指定工作流的节点ID到节点定义的映射。"""
        try:
            return self._node_maps[ref]
        except KeyError:
            # 如果缓存中没有，尝试动态构建
            doc = self.get(ref)
            node_map = self._build_node_map(doc)
            self._node_maps[ref] = node_map
            return node_map

    def list_flows(self) -> List[str]:
        """返回所有已注册工作流的引用标识列表。"""
        return sorted(self._flows.keys())

    # -------- Helpers --------

    def _load_file(self, path: Path) -> int:
        """加载并注册单个IR文件，处理可能的错误。"""
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
            if not isinstance(data, dict):
                raise IRValidationError(f"IR file must be a mapping root: {path}")
            self.register(data)
            logger.info("Loaded IR: %s", path)
            return 1
        except IRValidationError as exc:
            logger.error("Invalid IR file %s: %s", path, exc)
            return 0
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to load IR file %s: %s", path, exc, exc_info=True)
            return 0

    @staticmethod
    def _ref_of(doc: Dict[str, Any]) -> str:
        """从IR文档中生成标准的引用标识 'id@version'。"""
        flow_id = doc.get("id")
        version = doc.get("version")
        if not flow_id or version is None:
            raise IRValidationError("IR doc requires 'id' and 'version'")
        return f"{flow_id}@{int(version)}"

    @staticmethod
    def _build_node_map(doc: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """为IR文档构建一个从节点ID到节点定义的快速查找映射。"""
        nodes = doc.get("nodes") or []
        node_map: Dict[str, Dict[str, Any]] = {}
        for spec in nodes:
            nid = spec.get("id")
            if not nid:
                raise IRValidationError("IR node missing 'id'")
            if nid in node_map:
                raise IRValidationError(f"Duplicate node id in IR: {nid}")
            node_map[nid] = spec
        return node_map

    @staticmethod
    def entry_node_ids(doc: Dict[str, Any]) -> List[str]:
        """获取IR文档中定义的入口节点ID。"""
        entry = doc.get("entry")
        if not entry:
            raise IRValidationError("IR doc missing 'entry'")
        return [entry]

    def export_json(self, ref: str, ensure_ascii: bool = False) -> str:
        """将已注册的工作流序列化为格式化的JSON字符串，主要用于调试。"""
        return json.dumps(self.get(ref), ensure_ascii=ensure_ascii, indent=2)