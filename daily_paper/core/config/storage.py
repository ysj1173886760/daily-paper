from typing import Dict, Any
from .base import YamlConfig


class StorageConfig(YamlConfig):
    storage_type: str = "local"
    base_path: str = "./data"
