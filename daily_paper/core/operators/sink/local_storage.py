from typing import Any, List, Callable, Dict, TypeVar, Tuple
from pathlib import Path
import json
from datetime import datetime

from daily_paper.core.operators.base import Operator

class LocalStorage(Operator):
    """保存键值对数据到本地存储的算子"""
    
    def __init__(self, storage_dir: str, storage_namespace: str, key_value_getter: Callable[[Any], Tuple[str, Any]] = None):
        """初始化LocalStorage
        
        Args:
            storage_dir: 存储目录
            key_value_getter: 从输入数据中提取key和value的函数
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.key_value_getter = key_value_getter
        self.storage_namespace = storage_namespace
        
    async def process(self, items: List[Any]) -> List[Any]:
        """将数据保存到本地存储
        
        Args:
            items: 输入数据列表
            
        Returns:
            List[Any]: 输入的数据列表
        """
        if not self.key_value_getter:
            raise ValueError("key_value_getter not provided")
            
        # 读取现有数据
        existing_data = {}
        if self.storage_dir.exists():
            storage_file = self.storage_dir / f"{self.storage_namespace}.json"
            if storage_file.exists():
                with open(storage_file, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
        
        # 处理新数据
        for item in items:
            key, value = self.key_value_getter(item)
            # 更新或添加新数据
            existing_data[key] = {
                "value": value,
                "stored_at": datetime.now().isoformat()
            }
            
        # 保存所有数据
        storage_file = self.storage_dir / f"{self.storage_namespace}.json"
        with open(storage_file, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)
                
        return items 