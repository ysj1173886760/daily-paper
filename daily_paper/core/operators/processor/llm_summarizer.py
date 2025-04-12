from typing import Any, List
import openai
from daily_paper.core.operators.base import Operator
from daily_paper.core.models import Paper


class LLMSummarizer(Operator):
    """使用LLM生成论文摘要的算子"""
    
    def __init__(self, api_key: str, model: str = "gpt-4"):
        """初始化LLMSummarizer
        
        Args:
            api_key: OpenAI API密钥
            model: 使用的模型名称
        """
        self.client = openai.AsyncOpenAI(api_key=api_key)
        self.model = model
        
    async def process(self, papers: List[Paper]) -> List[Paper]:
        """为论文生成摘要
        
        Args:
            papers: 论文列表
            
        Returns:
            List[Paper]: 添加了摘要的论文列表
        """
        for paper in papers:
            if not paper.summary:  # 只处理没有摘要的论文
                prompt = f"""请用中文总结以下学术论文的主要内容，包括：
                1. 研究问题
                2. 主要方法
                3. 关键发现
                4. 创新点
                
                论文标题：{paper.title}
                论文摘要：{paper.abstract}
                """
                
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "你是一个专业的学术论文分析助手。"},
                        {"role": "user", "content": prompt}
                    ]
                )
                
                paper.summary = response.choices[0].message.content
                
        return papers 