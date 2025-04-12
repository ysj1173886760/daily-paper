import pytest
from daily_paper.core.operators.datasource.arxiv import ArxivSource
from daily_paper.core.models import Paper
from daily_paper.core.common.logger import logger

@pytest.mark.asyncio
async def test_arxiv_source_process():
    """测试 ArxivSource 实际获取论文数据"""
    source = ArxivSource(topic="LLM", max_results=3)
    
    # 执行处理
    papers = await source.process(None)
    
    # 验证结果数量
    assert len(papers) == 3
    assert all(isinstance(p, Paper) for p in papers)
    
    # # 打印论文内容供人工检查
    # for i, paper in enumerate(papers, 1):
    #     logger.info(f"\n论文 {i}:")
    #     logger.info(f"ID: {paper.paper_id}")
    #     logger.info(f"标题: {paper.title}")
    #     logger.info(f"作者: {', '.join(paper.authors)}")
    #     logger.info(f"分类: {paper.category}")
    #     logger.info(f"发布日期: {paper.publish_date}")
    #     logger.info(f"更新日期: {paper.update_date}")
    #     logger.info(f"摘要: {paper.abstract}\n")

@pytest.mark.asyncio
async def test_arxiv_source_with_invalid_topic():
    """测试 ArxivSource 处理无效主题"""
    source = ArxivSource(topic="ysj_can_fly", max_results=3)
    
    # 执行处理
    papers = await source.process(None)
    assert len(papers) == 0

@pytest.mark.asyncio
async def test_arxiv_source_with_list_topic():
    """测试 ArxivSource 处理列表主题"""
    source = ArxivSource(topic=["RAG", "Retrieval-Augmented Generation"], max_results=10)
    
    # 执行处理
    papers = await source.process(None)
    assert len(papers) == 10
    assert all(isinstance(p, Paper) for p in papers)

    # 打印论文内容供人工检查
    for i, paper in enumerate(papers, 1):
        logger.info(f"\n论文 {i}:")
        logger.info(f"Title: {paper.title}")