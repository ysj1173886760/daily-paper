from typing import Any, List, Set
import json
from pathlib import Path

from daily_paper.core.operators.base import Operator


class StateManager:
    """状态管理器，用于管理论文处理状态"""
    
    def __init__(self, storage_dir: str = ".state"):
        """初始化状态管理器
        
        Args:
            storage_dir: 状态存储目录
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
    def _get_state_file(self, stage_name: str) -> Path:
        """获取状态文件路径"""
        return self.storage_dir / f"{stage_name}.json"
        
    def get_pending_ids(self, stage_name: str) -> Set[str]:
        """获取待处理的论文ID集合"""
        state_file = self._get_state_file(stage_name)
        if not state_file.exists():
            return set()
            
        with open(state_file, "r", encoding="utf-8") as f:
            return set(json.load(f))
            
    def store_pending_ids(self, stage_name: str, paper_ids: List[str]):
        """存储待处理的论文ID"""
        state_file = self._get_state_file(stage_name)
        pending_ids = self.get_pending_ids(stage_name)
        pending_ids.update(paper_ids)
        
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(list(pending_ids), f)
            
    def mark_as_finished(self, stage_name: str, paper_ids: List[str]):
        """将论文ID标记为已完成"""
        state_file = self._get_state_file(stage_name)
        pending_ids = self.get_pending_ids(stage_name)
        pending_ids.difference_update(paper_ids)
        
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(list(pending_ids), f)


# 全局状态管理器实例
_state_manager = StateManager()


class InsertPendingIDs(Operator):
    """将论文ID标记为待处理状态的算子"""
    
    def __init__(self, stage_name: str):
        """初始化InsertPendingIDs
        
        Args:
            stage_name: 处理阶段名称
        """
        self.stage_name = stage_name
        
    async def process(self, paper_ids: List[str]) -> List[str]:
        """将论文ID添加到待处理状态
        
        Args:
            paper_ids: 论文ID列表
            
        Returns:
            List[str]: 输入的论文ID列表
        """
        _state_manager.store_pending_ids(self.stage_name, paper_ids)
        return paper_ids


class GetAllPendingIDs(Operator):
    """获取所有待处理论文ID的算子"""
    
    def __init__(self, stage_name: str):
        """初始化GetAllPendingIDs
        
        Args:
            stage_name: 处理阶段名称
        """
        self.stage_name = stage_name
        
    async def process(self, _: Any) -> List[str]:
        """获取所有待处理的论文ID
        
        Returns:
            List[str]: 待处理的论文ID列表
        """
        return list(_state_manager.get_pending_ids(self.stage_name))


class MarkIDsAsFinished(Operator):
    """将论文ID标记为处理完成的算子"""
    
    def __init__(self, stage_name: str):
        """初始化MarkIDsAsFinished
        
        Args:
            stage_name: 处理阶段名称
        """
        self.stage_name = stage_name
        
    async def process(self, paper_ids: List[str]) -> List[str]:
        """将论文ID标记为已完成
        
        Args:
            paper_ids: 论文ID列表
            
        Returns:
            List[str]: 输入的论文ID列表
        """
        _state_manager.mark_as_finished(self.stage_name, paper_ids)
        return paper_ids 