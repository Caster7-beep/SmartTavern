import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from services.llm_adapter import LLMAdapter  # noqa: E402


class _FakeResponse:
    def __init__(self, payload_preview: Dict[str, Any]) -> None:
        self._preview = payload_preview

    def raise_for_status(self) -> None:
        # Simulate HTTP 200
        return None

    def json(self) -> Dict[str, Any]:
        # Minimal Gemini success shape
        return {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "OK: payload accepted and format matched sample"}
                        ]
                    }
                }
            ]
        }


class _FakeSession:
    def __init__(self) -> None:
        self.captured: Dict[str, Any] = {}

    def post(self, url: str, json: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None, timeout: Optional[float] = None):
        # Capture call
        self.captured = {
            "url": url,
            "json": json,
            "headers": headers or {},
            "timeout": timeout,
        }

        # Validate URL target: must be Gemini generateContent endpoint
        assert ":generateContent" in url, f"URL should be a Gemini generateContent endpoint, got: {url}"

        # Validate headers include x-goog-api-key (as per sample cURL)
        assert "x-goog-api-key" in self.captured["headers"], "Missing 'x-goog-api-key' header"
        assert isinstance(self.captured["headers"]["x-goog-api-key"], str) and len(self.captured["headers"]["x-goog-api-key"]) > 0, "Empty API key header"

        # Validate payload shape against sample cURL
        body = self.captured["json"] or {}
        assert "contents" in body, "Payload must contain 'contents' array"
        contents = body["contents"]
        assert isinstance(contents, list) and len(contents) >= 1, "'contents' must be a non-empty list"

        first = contents[0]
        assert isinstance(first, dict), "each element of 'contents' must be an object"
        # Sample cURL omits role for user messages; we ensure 'role' is not required and if present it's ok.
        # Preferably, for user messages, our adapter omits the role key.
        assert "role" not in first, "For user messages, 'role' should be omitted to match sample cURL"
        assert "parts" in first and isinstance(first["parts"], list) and len(first["parts"]) >= 1, "'parts' must be a non-empty list"
        assert isinstance(first["parts"][0], dict) and "text" in first["parts"][0], "'parts[0]' must be an object with 'text' field"

        # Return a fake 200 response
        return _FakeResponse({"keys": list((json or {}).keys())})


def main() -> None:
    # Use real configuration; the adapter will read config/api_config.yaml and config/api_secrets.yaml
    adapter = LLMAdapter(config_path=str(ROOT / "config" / "api_config.yaml"))

    # Monkeypatch HTTP session with our fake session to capture and validate the outgoing request
    fake_session = _FakeSession()
    adapter._session = fake_session  # type: ignore[attr-defined]

    # Build a minimal OpenAI-style messages list that should be converted to Gemini payload
    messages: List[Dict[str, str]] = [
        {"role": "user", "content": "Explain how AI works in a few words"}
    ]

    # Model name should map via config to 'gemini-2.5-flash'
    result = adapter.call_model(messages, "narrative-llm")

    # If we reach here, assertions passed
    print("Gemini payload verified. Adapter returned:", result)


if __name__ == "__main__":
    main()