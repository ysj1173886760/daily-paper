import pytest
from daily_paper.core.operators.datasource.arxiv import ArxivSource
from daily_paper.core.models import Paper
from daily_paper.core.common.logger import logger


@pytest.mark.slow
@pytest.mark.asyncio
async def test_arxiv_source_process():
    """测试 ArxivSource 实际获取论文数据"""
    source = ArxivSource(topic="LLM", max_results=3)

    # 执行处理
    papers = await source.process(None)

    # 验证结果数量
    assert len(papers) == 3
    assert all(isinstance(p, Paper) for p in papers)


@pytest.mark.slow
@pytest.mark.asyncio
async def test_arxiv_source_with_invalid_topic():
    """测试 ArxivSource 处理无效主题"""
    source = ArxivSource(topic="ysj_can_fly", search_limit=3)

    # 执行处理
    papers = await source.process(None)
    assert len(papers) == 0


@pytest.mark.slow
@pytest.mark.asyncio
async def test_arxiv_source_with_list_topic():
    """测试 ArxivSource 处理列表主题"""
    source = ArxivSource(
        topic=["RAG", "Retrieval-Augmented Generation"], search_limit=10
    )

    # 执行处理
    papers = await source.process(None)
    assert len(papers) == 10
    assert all(isinstance(p, Paper) for p in papers)

    # 打印论文内容供人工检查
    for i, paper in enumerate(papers, 1):
        logger.info(f"\n论文 {i}:")
        logger.info(f"{paper}")


@pytest.mark.slow
@pytest.mark.asyncio
async def test_arxiv_source_with_offset():
    """测试 ArxivSource 处理偏移量"""
    # source = ArxivSource(topic="\"Memory\" AND \"LLM\"", search_offset=0, search_limit=1000)
    # papers = await source.process(None)

    total_paper_list = []
    batch_size = 100
    for offset in range(0, 1000, batch_size):
        source = ArxivSource(topic="\"Memory\" AND \"LLM\"", search_offset=offset, search_limit=batch_size, should_retry_when_empty=True)
        papers = await source.process(None)
        assert len(papers) == batch_size
        total_paper_list.extend(papers)

    assert len(total_paper_list) == 1000
