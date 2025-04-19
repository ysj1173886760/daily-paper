from typing import Dict, Any
from .base import YamlConfig


class LLMConfig(YamlConfig):
    model_name: str = "gpt-3.5-turbo"
    api_key: str = ""
    base_url: str = ""
    temperature: float = 0.7
    max_tokens: int = 2000
