import pytest
import os
from pathlib import Path
from daily_paper.core.operators.processor.llm_summarizer import LLMSummarizer
from daily_paper.core.models import Paper, PaperWithSummary
from daily_paper.core.config import LLMConfig
from daily_paper.core.common import logger


@pytest.fixture
def llm_config():
    return LLMConfig(
        api_key=os.environ.get("LLM_API_KEY"),
        base_url=os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1"),
        model_name=os.environ.get("CHAT_MODEL_NAME", "gpt-3.5-turbo"),
    )


@pytest.fixture
def summarizer(llm_config):
    return LLMSummarizer(llm_config)


@pytest.fixture
def test_paper_text():
    test_data_path = (
        Path(__file__).parent.parent.parent.parent / "test_data" / "test_paper.txt"
    )
    with open(test_data_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.mark.slow
@pytest.mark.asyncio
async def test_summarize_paper(summarizer: LLMSummarizer, test_paper_text: str):
    # 测试单篇论文摘要生成
    summary = await summarizer.summarize_paper(test_paper_text)
    assert isinstance(summary, str)
    assert len(summary) > 0

    # 验证摘要内容包含关键信息
    logger.info(f"summary: {summary}")
