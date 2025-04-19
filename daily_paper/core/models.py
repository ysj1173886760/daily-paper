from datetime import date
from typing import Optional
from dataclasses import dataclass

@dataclass
class Paper:
    """论文数据模型"""
    id: str          # 论文唯一标识
    title: str            # 论文标题
    url: str             # 论文链接
    abstract: str        # 论文摘要
    authors: str   # 作者列表
    category: str        # 论文类别
    publish_date: str    # 发布日期
    update_date: str     # 更新日期

@dataclass
class PaperWithSummary(Paper):
    summary: str
