import os
import pytest
import asyncio
import tempfile
import shutil
from daily_paper.core.operators.processor.paper_reader import PaperReader
from daily_paper.core.models import Paper
from daily_paper.core.common import logger
from datetime import date
from contextlib import asynccontextmanager


@pytest.fixture(scope="function")
def temp_dir():
    """创建临时目录的fixture"""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    # 测试结束后清理临时目录
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture(scope="function")
def paper_reader(temp_dir):
    """创建PaperReader实例的fixture"""
    reader = PaperReader(cache_dir=temp_dir, max_workers=5)
    return reader


@asynccontextmanager
async def reader_lifecycle(reader: PaperReader):
    """PaperReader生命周期管理"""
    await reader.setup()
    try:
        yield reader
    finally:
        await reader.cleanup()


@pytest.mark.slow
@pytest.mark.asyncio
async def test_paper_download_and_process(paper_reader):
    """测试论文下载和处理功能"""
    async with reader_lifecycle(paper_reader) as reader:
        # 这里需要用户提供实际的论文列表
        test_papers = [
            Paper(
                id="2504.07624",
                title="ConceptFormer: Towards Efficient Use of Knowledge-Graph Embeddings in Large Language Models",
                url="http://arxiv.org/abs/2504.07624",
                authors="Joel Barmettler, Abraham Bernstein, Luca Rossetto",
                abstract="none",
                category="cs.CL",
                publish_date=date(2025, 4, 10).strftime("%Y-%m-%d"),
                update_date=date(2025, 4, 10).strftime("%Y-%m-%d"),
            )
        ]

        # 处理论文
        results = await reader.process(test_papers)

        # 验证结果
        assert len(results) == len(test_papers)
        for paper, text in results:
            assert isinstance(text, str)
            assert len(text) > 0  # 确保提取到了文本

            logger.info(f"title: {paper.title}, 摘要: {text}")
