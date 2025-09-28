import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import requests
import yaml

logger = logging.getLogger(__name__)


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            nv = dict(out[k])
            nv.update(v)
            out[k] = nv
        else:
            out[k] = v
    return out


class LLMAdapter:
    """HTTP adapter with support for:
    - OpenAI-style Chat Completions (default): /v1/chat/completions
    - Google Gemini generateContent (auto-detected by base_url/endpoint_path)
    """

    def __init__(self, config_path: str = "config/api_config.yaml") -> None:
        self.config_path = Path(config_path)
        self.secrets_path = Path("config/api_secrets.yaml")
        self._session = requests.Session()
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"LLM API config not found at {self.config_path}")
        with self.config_path.open("r", encoding="utf-8") as stream:
            base_cfg = yaml.safe_load(stream) or {}

        # Overlay secrets file if present
        cfg = dict(base_cfg)
        if self.secrets_path.exists():
            try:
                with self.secrets_path.open("r", encoding="utf-8") as s:
                    secrets = yaml.safe_load(s) or {}
                cfg = _deep_merge(cfg, secrets)
                logger.info("Loaded LLM secrets overlay from %s", self.secrets_path)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to read secrets overlay %s: %s", self.secrets_path, exc)

        # Environment variable override for password/api key
        env_password = (
            os.getenv("SMARTTAVERN_LLM_PASSWORD")
            or os.getenv("LLM_API_KEY")
            or os.getenv("OPENAI_API_KEY")
            or os.getenv("BEARER_TOKEN")
        )
        if env_password:
            cfg["password"] = env_password
            logger.info("LLM password/api_key loaded from environment variable")

        return cfg

    @property
    def _base_url(self) -> str:
        base_url = self._config.get("base_url")
        if not base_url:
            raise ValueError("base_url is required in LLM API config")
        return base_url

    def _request_url(self) -> str:
        """OpenAI-style endpoint URL."""
        endpoint = (self._config.get("endpoint_path") or "/v1/chat/completions").strip()
        if not endpoint:
            return self._base_url
        if "://" in endpoint:
            return endpoint
        normalized_endpoint = endpoint.lstrip("/")
        normalized_base = self._base_url.rstrip("/")
        if normalized_base.endswith(normalized_endpoint):
            return normalized_base
        return f"{normalized_base}/{normalized_endpoint}"

    def _resolve_model(self, model_name: str) -> str:
        models = self._config.get("models") or {}
        return models.get(model_name, model_name)

    # ---------------- Gemini support ----------------

    def _is_gemini_config(self) -> bool:
        base = (self._config.get("base_url") or "").lower()
        ep = (self._config.get("endpoint_path") or "").lower()
        return ("generativelanguage.googleapis.com" in base) or (":generatecontent" in ep)

    def _gemini_request_url(self, actual_model: str) -> str:
        """Compose Gemini generateContent URL.
        If endpoint_path provided, use it; otherwise, build from model.
        """
        endpoint = (self._config.get("endpoint_path") or "").strip()
        if not endpoint:
            # default Gemini endpoint
            endpoint = f"/v1beta/models/{actual_model}:generateContent"
        if "://" in endpoint:
            url = endpoint
        else:
            normalized_endpoint = endpoint.lstrip("/")
            normalized_base = self._base_url.rstrip("/")
            url = f"{normalized_base}/{normalized_endpoint}"
        return url

    @staticmethod
    def _convert_messages_to_gemini_payload(messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """Map OpenAI-style messages -> Gemini generateContent payload.
        Conforms to official cURL sample:
          - 'contents' is an array of objects with 'parts': [{'text': ...}]
          - omit 'role' for user messages; assistant messages may include role='model'
          - system messages are concatenated into 'systemInstruction.parts[].text' (camelCase)
        """
        system_texts: List[str] = []
        contents: List[Dict[str, Any]] = []
        for msg in messages or []:
            role = (msg.get("role") or "").strip()
            content = str(msg.get("content") or "")
            if role == "system":
                if content:
                    system_texts.append(content)
                continue

            # Build content object matching sample format
            content_obj: Dict[str, Any] = {"parts": [{"text": content}]}
            if role == "assistant":
                # Explicitly tag assistant as model (optional; user messages omit role)
                content_obj["role"] = "model"
            contents.append(content_obj)

        payload: Dict[str, Any] = {"contents": contents}
        if system_texts:
            # Use camelCase 'systemInstruction' as per API spec
            payload["systemInstruction"] = {
                "parts": [{"text": "\n\n".join(system_texts)}]
            }
        return payload

    @staticmethod
    def _extract_gemini_text(data: Mapping[str, Any]) -> Optional[str]:
        try:
            candidates = data.get("candidates")
            if not candidates:
                return None
            content = candidates[0].get("content") or {}
            parts = content.get("parts") or []
            texts: List[str] = []
            for p in parts:
                t = p.get("text")
                if isinstance(t, str):
                    texts.append(t)
            return "\n".join([t for t in texts if t])
        except Exception:  # noqa: BLE001
            return None

    # ---------------- Public call ----------------

    def call_model(self, messages: List[Dict[str, str]], model_name: str) -> str:
        timeout = self._config.get("timeout", 30)
        actual_model = self._resolve_model(model_name)

        if self._is_gemini_config():
            # Google Gemini path
            url = self._gemini_request_url(actual_model)
            payload = self._convert_messages_to_gemini_payload(messages)
            headers = {"Content-Type": "application/json"}
            # Prefer header style as per official sample cURL
            api_key = self._config.get("password") or self._config.get("api_key")
            if api_key:
                headers["x-goog-api-key"] = api_key
                logger.debug("Gemini: using x-goog-api-key header")
            else:
                logger.warning("Gemini config detected but no API key found in 'password' or 'api_key'; request may fail")

            # Log payload shape (avoid sensitive data)
            try:
                first_content = (payload.get("contents") or [{}])[0]
                preview = {"has_systemInstruction": "systemInstruction" in payload, "first_content_keys": list(first_content.keys())}
                logger.info("Gemini request preview: url=%s payload_keys=%s preview=%s", url, list(payload.keys()), preview)
            except Exception:
                pass

            try:
                response = self._session.post(url, json=payload, headers=headers, timeout=timeout)
                response.raise_for_status()
                data = response.json()
                content = self._extract_gemini_text(data)
                if content:
                    return content
                raise ValueError("Gemini response missing candidates/content text")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Gemini call failed for model %s: %s; falling back to mock response", actual_model, exc)
                return self._mock_response(messages, model_name)

        # Default OpenAI-style Chat Completions
        payload_oa: Dict[str, Any] = {
            "model": actual_model,
            "messages": messages,
        }
        headers_oa = {"Content-Type": "application/json"}
        password = self._config.get("password")
        if password:
            headers_oa["Authorization"] = f"Bearer {password}"

        try:
            response = self._session.post(self._request_url(), json=payload_oa, headers=headers_oa, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            content = self._extract_content(data)
            if content:
                return content
            raise ValueError("LLM response missing message content")
        except Exception as exc:  # noqa: BLE001 - fallback is intentional here
            logger.warning("LLM call failed for model %s: %s; falling back to mock response", actual_model, exc)
            return self._mock_response(messages, model_name)

    @staticmethod
    def _extract_content(data: Mapping[str, Any]) -> Optional[str]:
        try:
            choices = data.get("choices")
            if not choices:
                return None
            message = choices[0].get("message")
            if not message:
                return None
            return message.get("content")
        except Exception:  # noqa: BLE001
            return None

    def _mock_response(self, messages: List[Dict[str, str]], model_name: str) -> str:
        system_prompt = next((msg["content"] for msg in messages if msg.get("role") == "system"), "")
        if model_name == "narrative-llm":
            response = "【AI回复】霓虹灯汇成电流，湿冷空气裹挟着铁锈味。你拉紧风衣，在数据黑市的霓虹缝隙中前行。"
            if "疲惫且谨慎" in system_prompt:
                response += "\n(调试信息：使用了初始/Fallback心境)"
            elif "【更新后的心境】" in system_prompt:
                response += "\n(调试信息：使用了更新后的心境)"
            return response
        if model_name == "analyzer-llm":
            last_user_input = messages[-1]["content"] if messages else ""
            if "攻击" in last_user_input or "逃跑" in last_user_input:
                return "【更新后的心境】肾上腺素飙升，极度紧张且充满攻击性"
            return "【更新后的心境】保持警惕，但稍微放松了一些"
        return f"【未知模型回复】{model_name}"


llm_adapter = LLMAdapter()
