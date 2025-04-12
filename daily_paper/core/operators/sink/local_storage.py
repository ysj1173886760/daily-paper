from typing import Any, List
import json
from pathlib import Path
from datetime import datetime

from daily_paper.core.operators.base import Operator
from daily_paper.core.models import Paper


class LocalStorage(Operator):
    """保存到本地存储的算子"""
    
    def __init__(self, storage_dir: str = "papers"):
        """初始化LocalStorage
        
        Args:
            storage_dir: 存储目录
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
    async def process(self, papers: List[Paper]) -> List[Paper]:
        """将论文保存到本地存储
        
        Args:
            papers: 论文列表
            
        Returns:
            List[Paper]: 输入的论文列表
        """
        for paper in papers:
            # 使用论文ID作为文件名
            file_path = self.storage_dir / f"{paper.id}.json"
            
            # 将Paper对象转换为字典
            paper_dict = {
                "paper_id": paper.id,
                "title": paper.title,
                "url": paper.url,
                "abstract": paper.abstract,
                "authors": paper.authors,
                "category": paper.category,
                "publish_date": paper.publish_date.isoformat(),
                "update_date": paper.update_date.isoformat(),
                "summary": paper.summary,
                "pushed": paper.pushed,
                "stored_at": datetime.now().isoformat()
            }
            
            # 保存到文件
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(paper_dict, f, ensure_ascii=False, indent=2)
                
        return papers 