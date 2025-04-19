from typing import Any, List, Callable, Dict, TypeVar, Tuple
from pathlib import Path
import json
from datetime import datetime

from daily_paper.core.operators.base import Operator


class LocalStorage:
    """本地存储基类，处理文件路径和存储相关的通用逻辑"""

    def __init__(self, storage_dir: str, storage_namespace: str):
        """初始化LocalStorage

        Args:
            storage_dir: 存储目录
            storage_namespace: 存储命名空间
        """
        self.storage_dir = Path(storage_dir)
        self.storage_namespace = storage_namespace
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    @property
    def storage_file(self) -> Path:
        """获取存储文件路径"""
        return self.storage_dir / f"{self.storage_namespace}.json"

    def read_storage(self) -> Dict[str, Any]:
        """读取存储文件中的所有数据

        Returns:
            Dict[str, Any]: 存储的数据字典
        """
        if not self.storage_file.exists():
            return {}

        with open(self.storage_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_storage(self, data: Dict[str, Any]):
        """写入数据到存储文件

        Args:
            data: 要存储的数据字典
        """
        with open(self.storage_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


class LocalStorageWriter(Operator, LocalStorage):
    """保存键值对数据到本地存储的算子"""

    def __init__(
        self,
        storage_dir: str,
        storage_namespace: str,
        key_value_getter: Callable[[Any], Tuple[str, Any]] = None,
    ):
        """初始化LocalStorageWriter

        Args:
            storage_dir: 存储目录
            storage_namespace: 存储命名空间
            key_value_getter: 从输入数据中提取key和value的函数
        """
        LocalStorage.__init__(self, storage_dir, storage_namespace)
        self.key_value_getter = key_value_getter

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
        existing_data = self.read_storage()

        # 处理新数据
        for item in items:
            key, value = self.key_value_getter(item)
            # 更新或添加新数据
            existing_data[key] = {
                "value": value,
                "stored_at": datetime.now().isoformat(),
            }

        # 保存所有数据
        self.write_storage(existing_data)

        return items


class LocalStorageReader(Operator, LocalStorage):
    """从本地存储读取键值对数据的算子"""

    def __init__(
        self,
        storage_dir: str,
        storage_namespace: str,
        value_reader: Callable[[str, Dict[str, Any]], Any] = None,
    ):
        """初始化LocalStorageReader

        Args:
            storage_dir: 存储目录
            storage_namespace: 存储命名空间
            value_reader: 将存储的键值对转换为输出数据的函数，接收 (key, value_dict) 作为参数
                         value_dict 包含 'value' 和 'stored_at' 两个字段
        """
        LocalStorage.__init__(self, storage_dir, storage_namespace)
        self.value_reader = value_reader or (lambda k, v: v)

    async def process(self, items: List[Any]) -> List[Any]:
        """从本地存储读取数据

        Args:
            items: 输入数据列表（在这个算子中不会被使用）

        Returns:
            List[Any]: 从存储中读取并转换后的数据列表
        """
        stored_data = self.read_storage()

        # 使用value_reader函数转换每个存储的键值对
        result = []
        for key, value_dict in stored_data.items():
            transformed_value = self.value_reader(key, value_dict["value"])
            result.append(transformed_value)

        return result
