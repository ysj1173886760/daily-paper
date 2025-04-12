from typing import Any, List
import json
from pathlib import Path
from datetime import datetime

from daily_paper.core.operators.base import Operator
from daily_paper.core.models import Paper


class LocalSource(Operator):
    """从本地存储读取论文数据的算子"""
    
    def __init__(self, storage_path: str):
        """初始化LocalSource
        
        Args:
            storage_path: 本地存储路径
        """
        self.storage_path = Path(storage_path)
        
    async def process(self, _: Any) -> List[Paper]:
        """从本地存储读取论文数据
        
        Returns:
            List[Paper]: 论文列表
        """
        if not self.storage_path.exists():
            return []
            
        papers = []
        for file_path in self.storage_path.glob("*.json"):
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                paper = Paper(
                    paper_id=data["paper_id"],
                    title=data["title"],
                    url=data["url"],
                    abstract=data["abstract"],
                    authors=data["authors"],
                    category=data["category"],
                    publish_date=datetime.strptime(data["publish_date"], "%Y-%m-%d").date(),
                    update_date=datetime.strptime(data["update_date"], "%Y-%m-%d").date(),
                    summary=data.get("summary"),
                    pushed=data.get("pushed", False)
                )
                papers.append(paper)
                
        return papers 