from typing import Any, List, Set, Dict, Literal
import json
from pathlib import Path
from enum import Enum

from daily_paper.core.operators.base import Operator
from daily_paper.core.common.logger import logger

class IDState(str, Enum):
    """ID的状态枚举"""

    PENDING = "pending"
    FINISHED = "finished"


class StateManager:
    """状态管理器，用于管理ID处理状态"""

    def __init__(self, base_dir: str, namespace: str):
        """初始化状态管理器

        Args:
            base_dir: 状态存储根目录
            namespace: 命名空间，用于区分不同类型的ID
        """
        self.storage_dir = Path(base_dir) / "pending_states"
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.storage_dir / f"{namespace}_states.json"

    def _load_states(self) -> Dict[str, IDState]:
        """加载所有ID的状态"""
        if not self.state_file.exists():
            return {}

        with open(self.state_file, "r", encoding="utf-8") as f:
            states = json.load(f)
            return {k: IDState(v) for k, v in states.items()}

    def _save_states(self, states: Dict[str, IDState]):
        """保存所有ID的状态"""
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump({k: v.value for k, v in states.items()}, f)

    def get_pending_ids(self) -> Set[str]:
        """获取待处理的ID集合"""
        states = self._load_states()
        return {id for id, state in states.items() if state == IDState.PENDING}

    def is_finished(self, id: str) -> bool:
        """判断ID是否已处理完成"""
        states = self._load_states()
        if id not in states:
            return False
        return states[id] == IDState.FINISHED

    def store_pending_ids(self, ids: List[str]):
        """存储待处理的ID，已完成的ID不会被重新标记为pending"""
        states = self._load_states()

        # 只更新那些不是FINISHED状态的ID
        for id in ids:
            if states.get(id) != IDState.FINISHED:
                states[id] = IDState.PENDING

        self._save_states(states)

    def mark_as_finished(self, ids: List[str]):
        """将ID标记为已完成"""
        states = self._load_states()

        # 将指定的ID标记为完成状态
        for id in ids:
            states[id] = IDState.FINISHED

        self._save_states(states)


class InsertPendingIDs(Operator):
    """将ID标记为待处理状态的算子"""

    def __init__(self, base_dir: str, namespace: str):
        """初始化InsertPendingIDs

        Args:
            base_dir: 状态存储根目录
            namespace: 命名空间，用于区分不同类型的ID
        """
        self.state_manager = StateManager(base_dir, namespace)

    async def process(self, ids: List[str]) -> List[str]:
        """将ID添加到待处理状态

        Args:
            ids: ID列表

        Returns:
            List[str]: 输入的ID列表
        """
        self.state_manager.store_pending_ids(ids)
        return ids


class GetAllPendingIDs(Operator):
    """获取所有待处理ID的算子"""

    def __init__(self, base_dir: str, namespace: str):
        """初始化GetAllPendingIDs

        Args:
            base_dir: 状态存储根目录
            namespace: 命名空间，用于区分不同类型的ID
        """
        self.state_manager = StateManager(base_dir, namespace)

    async def process(self, _: Any) -> List[str]:
        """获取所有待处理的ID

        Returns:
            List[str]: 待处理的ID列表
        """
        return list(self.state_manager.get_pending_ids())


class MarkIDsAsFinished(Operator):
    """将对象标记为处理完成的算子"""

    def __init__(
        self, base_dir: str, namespace: str, id_getter: callable = lambda x: x
    ):
        """初始化MarkIDsAsFinished

        Args:
            base_dir: 状态存储根目录
            namespace: 命名空间，用于区分不同类型的ID
            id_getter: 从对象中获取ID的函数，默认直接返回对象本身
        """
        self.state_manager = StateManager(base_dir, namespace)
        self.id_getter = id_getter

    async def process(self, items: List[Any]) -> List[Any]:
        """将对象标记为已完成

        Args:
            items: 对象列表

        Returns:
            List[Any]: 输入的对象列表
        """
        ids = [self.id_getter(item) for item in items]
        self.state_manager.mark_as_finished(ids)
        return items


class FilterFinishedIDs(Operator):
    """过滤对象列表，只保留ID为未处理完成状态的对象的算子"""

    def __init__(
        self, base_dir: str, namespace: str, id_getter: callable = lambda x: x
    ):
        """初始化FilterFinishedIDs

        Args:
            base_dir: 状态存储根目录
            namespace: 命名空间，用于区分不同类型的ID
            id_getter: 从对象中获取ID的函数，默认直接返回对象本身
        """
        self.state_manager = StateManager(base_dir, namespace)
        self.id_getter = id_getter

    async def process(self, items: List[Any]) -> List[Any]:
        """过滤对象列表，只返回ID为未处理完成状态的对象

        Args:
            items: 需要过滤的对象列表

        Returns:
            List[Any]: ID为未处理完成状态的对象列表
        """
        return [
            item
            for item in items
            if not self.state_manager.is_finished(self.id_getter(item))
        ]
