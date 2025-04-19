from typing import Dict, Any, Optional
import yaml
from pathlib import Path
from daily_paper.core.config.llm import LLMConfig
from daily_paper.core.config.storage import StorageConfig
from daily_paper.core.config.base import YamlConfig

class Config(YamlConfig):
    llm: LLMConfig = LLMConfig()
    storage: StorageConfig = StorageConfig()
    feishu_webhook_url: str = ""

    @classmethod
    def parse(cls, config_path: str):
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(path, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)
            return Config(**config_dict)

    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'Config':
        return cls.parse(yaml_path)

# 为了方便使用，导出主要的类
__all__ = ['Config', 'LLMConfig', 'StorageConfig']
