from datetime import date
from typing import Optional
from dataclasses import dataclass

@dataclass
class Paper:
    """论文数据模型"""
    paper_id: str          # 论文唯一标识
    title: str            # 论文标题
    url: str             # 论文链接
    abstract: str        # 论文摘要
    authors: str   # 作者列表
    category: str        # 论文类别
    publish_date: date   # 发布日期
    update_date: date    # 更新日期
    comments: Optional[str] = None  # 评论