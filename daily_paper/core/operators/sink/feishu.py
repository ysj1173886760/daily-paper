from typing import Any, Union, List
import json
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential
from daily_paper.core.operators.base import Operator
from daily_paper.core.models import Paper
import requests
from daily_paper.core.common import logger
from typing import Callable, Tuple


@retry(stop=stop_after_attempt(100), wait=wait_exponential(multiplier=1, min=1, max=10))
def send_to_feishu_with_retry(webhook_url: str, message: dict):
    """带重试机制的飞书消息推送"""
    response = requests.post(webhook_url, json=message, timeout=10)
    response.raise_for_status()


class FeishuPusher(Operator):
    """推送到飞书的算子"""

    def __init__(
        self,
        webhook_url: str,
        title_and_content_getter: Callable[[Any], Tuple[str, str]],
    ):
        """初始化FeishuPusher

        Args:
            webhook_url: 飞书机器人的Webhook地址
        """
        self.webhook_url = webhook_url
        self.title_and_content_getter = title_and_content_getter

    async def single_content_push_feishu(self, content: Any) -> bool:
        """推送单个内容到飞书"""
        title, content = self.title_and_content_getter(content)
        message = {
            "msg_type": "interactive",
            "card": {
                "elements": [
                    {"tag": "div", "text": {"content": f"{content}", "tag": "lark_md"}}
                ],
                "header": {"title": {"content": f"{title}", "tag": "plain_text"}},
            },
        }
        try:
            send_to_feishu_with_retry(self.webhook_url, message)
            logger.info(f"飞书推送成功: {title}")
            return True
        except Exception as e:
            logger.error(f"飞书推送失败: {str(e)}")
            return False

    async def process(self, content: List[Any]) -> List[Tuple[Any, bool]]:
        """推送内容到飞书

        Args:
            content: 要推送的内容，可以是字符串或论文列表

        Returns:
            List[Tuple[Any, bool]]: 输入的内容和推送结果
        """
        # sequential push
        result = []
        for c in content:
            result.append((c, await self.single_content_push_feishu(c)))

        return result
