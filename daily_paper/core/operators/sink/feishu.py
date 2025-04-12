from typing import Any, Union, List
import json
import aiohttp

from daily_paper.core.operators.base import Operator
from daily_paper.core.models import Paper


class FeishuPusher(Operator):
    """推送到飞书的算子"""
    
    def __init__(self, webhook_url: str):
        """初始化FeishuPusher
        
        Args:
            webhook_url: 飞书机器人的Webhook地址
        """
        self.webhook_url = webhook_url
        
    async def process(self, content: Union[str, List[Paper]]) -> Union[str, List[Paper]]:
        """推送内容到飞书
        
        Args:
            content: 要推送的内容，可以是字符串或论文列表
            
        Returns:
            Union[str, List[Paper]]: 输入的内容
        """
        if isinstance(content, list):
            # 如果输入是论文列表，生成摘要信息
            message = "# 论文更新提醒\n\n"
            for paper in content:
                message += f"## {paper.title}\n"
                message += f"作者：{', '.join(paper.authors)}\n"
                message += f"链接：{paper.url}\n"
                if paper.summary:
                    message += f"摘要：{paper.summary}\n"
                message += "\n---\n\n"
        else:
            # 如果输入是字符串（如每日简报），直接使用
            message = content
            
        # 构造飞书消息
        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {
                    "wide_screen_mode": True
                },
                "elements": [{
                    "tag": "markdown",
                    "content": message
                }]
            }
        }
        
        # 发送到飞书
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status != 200:
                    raise RuntimeError(f"Failed to push to Feishu: {await response.text()}")
                
        return content 