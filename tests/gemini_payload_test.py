import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from services.llm_adapter import LLMAdapter  # noqa: E402






def main() -> None:
    # Use real configuration; the adapter will read config/llm_config.json and config/llm_secrets.json
    adapter = LLMAdapter(config_path=str(ROOT / "config" / "llm_config.json"))


    # Build a minimal OpenAI-style messages list that should be converted to Gemini payload
    messages: List[Dict[str, str]] = [
        {"role": "user", "content": "Explain how AI works in a few words"}
    ]

    # Model name should map via config to 'gemini-2.5-flash'
    result = adapter.call_model(messages, "narrative-llm")
    assert isinstance(result, str) and len(result) > 0, "Empty response from Gemini"
    print("Gemini HTTP call succeeded. Adapter returned:", result)


if __name__ == "__main__":
    main()