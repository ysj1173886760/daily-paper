from typing import Any, List, Set
import json
from pathlib import Path

from daily_paper.core.operators.base import Operator


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
        self.state_file = self.storage_dir / f"{namespace}.json"
        
    def get_pending_ids(self) -> Set[str]:
        """获取待处理的ID集合"""
        if not self.state_file.exists():
            return set()
            
        with open(self.state_file, "r", encoding="utf-8") as f:
            return set(json.load(f))
            
    def store_pending_ids(self, ids: List[str]):
        """存储待处理的ID"""
        pending_ids = self.get_pending_ids()
        pending_ids.update(ids)
        
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(list(pending_ids), f)
            
    def mark_as_finished(self, ids: List[str]):
        """将ID标记为已完成"""
        pending_ids = self.get_pending_ids()
        pending_ids.difference_update(ids)
        
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(list(pending_ids), f)


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
    """将ID标记为处理完成的算子"""
    
    def __init__(self, base_dir: str, namespace: str):
        """初始化MarkIDsAsFinished
        
        Args:
            base_dir: 状态存储根目录
            namespace: 命名空间，用于区分不同类型的ID
        """
        self.state_manager = StateManager(base_dir, namespace)
        
    async def process(self, ids: List[str]) -> List[str]:
        """将ID标记为已完成
        
        Args:
            ids: ID列表
            
        Returns:
            List[str]: 输入的ID列表
        """
        self.state_manager.mark_as_finished(ids)
        return ids