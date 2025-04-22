import asyncio
from dataclasses import asdict
from typing import Any, List, Tuple
import openai
from daily_paper.core.operators.base import Operator
from daily_paper.core.models import Paper, PaperWithSummary
from daily_paper.core.common import logger
from daily_paper.core.config import LLMConfig
from tqdm.asyncio import tqdm_asyncio


class AbstractBasedLLMFilter(Operator):
    """使用LLM过滤论文的算子"""

    def __init__(self, llm_config: LLMConfig, target_topic: str):
        """初始化LLMFilter

        Args:
            api_key: OpenAI API密钥
            model: 使用的模型名称
        """
        self.client = openai.AsyncOpenAI(
            api_key=llm_config.api_key, base_url=llm_config.base_url
        )
        self.model = llm_config.model_name
        self.target_topic = target_topic
        self.semaphore = asyncio.Semaphore(llm_config.max_concurrent_requests)

    async def filter_paper(self, paper: Paper) -> bool:
        # 修正冒号为英文格式，使用标准签名语法
        prompt = "请判断以下论文是否属于用户关注的领域\n"
        prompt += f"如果是，回答YES，否则回答NO\n"
        prompt += f"用户关注的领域是：{self.target_topic}\n"
        prompt += f"论文的摘要：{paper.abstract}\n"
        logger.debug(f"prompt: {prompt}")
        async with self.semaphore:
            result = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是一位论文过滤专家，专精于通过论文的摘要判断论文是否属于用户关注的领域。"},
                    {"role": "user", "content": prompt},
                ],
            )
            llm_response = result.choices[0].message.content
        
        is_filtered = "NO" in llm_response
        if is_filtered:
            logger.debug(f"论文 {paper.title} 被过滤")
        else:
            logger.debug(f"论文 {paper.title} 被保留")

        return is_filtered

    async def process(
        self, papers: list[Paper]
    ) -> list[Tuple[Paper, bool]]:
        # 使用asyncio.gather并行处理所有论文
        logger.info(f"过滤 {len(papers)} 篇论文")

        tasks = [self.filter_paper(paper) for paper in papers]
        filtered_results = await tqdm_asyncio.gather(*tasks, desc="过滤论文", total=len(tasks))

        result = []
        for paper, filtered in zip(papers, filtered_results):
            result.append((paper, filtered))
        return result
