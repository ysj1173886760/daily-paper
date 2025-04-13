from daily_paper.core.pipeline import DAGPipeline
from daily_paper.core.operators.datasource.arxiv import ArxivSource
from daily_paper.core.operators.processor.paper_reader import PaperReader
from daily_paper.core.operators.processor.llm_summarizer import LLMSummarizer
from daily_paper.core.operators.state.pending import FilterFinishedIDs, MarkIDsAsFinished, InsertPendingIDs
from daily_paper.core.models import Paper, PaperWithSummary
from daily_paper.core.config import LLMConfig
from daily_paper.core.config import Config
from daily_paper.core.common import logger
from daily_paper.core.operators.storage.local_storage import LocalStorageWriter, LocalStorageReader
import os
import asyncio
import argparse

def id_getter(x: Paper):
    return x.id

async def create_paper_summarize_pipeline(config: Config) -> DAGPipeline:
    """创建论文处理pipeline
    
    包含以下步骤：
    1. 从Arxiv获取论文
    2. 使用PaperReader读取论文
    3. 使用LLMSummarizer总结论文
    
    Returns:
        DAGPipeline: 配置好的pipeline实例
    """
    pipeline = DAGPipeline()
    
    # 添加数据源算子
    pipeline.add_operator(
        name="arxiv_source",
        operator=ArxivSource(topic=["RAG", "Retrieval Augmented Generation"], max_results=3),
        dependencies=None
    )

    pipeline.add_operator(
        name="filter_pending_ids",
        operator=FilterFinishedIDs(base_dir=os.path.join(config.storage.base_path, "state"), namespace="arxiv", id_getter=id_getter),
        dependencies=["arxiv_source"]
    )

    # only read the unprocessed papers
    pipeline.add_operator(
        name="paper_reader",
        operator=PaperReader(os.path.join(config.storage.base_path, "paper_caches")),
        dependencies=["filter_pending_ids"]
    )
    
    # 添加论文总结算子
    pipeline.add_operator(
        name="paper_summarizer",
        operator=LLMSummarizer(config.llm),
        dependencies=["paper_reader"]
    )

    def kv_getter(x: PaperWithSummary):
      return x.id, x.summary

    pipeline.add_operator(
        name="save_paper_summaries",
        operator=LocalStorageWriter(storage_dir=os.path.join(config.storage.base_path, "paper_summaries"), storage_namespace="paper_summaries", key_value_getter=kv_getter),
        dependencies=["paper_summarizer"]
    )

    # mark the processed papers as finished
    pipeline.add_operator(
        name="mark_processed_papers",
        operator=MarkIDsAsFinished(base_dir=os.path.join(config.storage.base_path, "state"), namespace="arxiv", id_getter=id_getter),
        dependencies=["save_paper_summaries"]
    )

    return pipeline

async def create_paper_push_pipeline(config: Config) -> DAGPipeline:
    """创建论文推送pipeline
    
    包含以下步骤：
    1. 从本地存储读取论文摘要
    2. 使用FeishuPush算子推送论文摘要到飞书
    3. 将推送成功的论文摘要标记为已推送
    """
    pipeline = DAGPipeline()

    def convert_to_paper_with_summary(key: str, value: dict):
      return PaperWithSummary(**value)

    pipeline.add_operator(
        name="read_paper_summaries",
        operator=LocalStorageReader(storage_dir=os.path.join(config.storage.base_path, "paper_summaries"), storage_namespace="paper_summaries", value_reader=convert_to_paper_with_summary),
        dependencies=None
    )

    # filter the papers that have been pushed
    pipeline.add_operator(
        name="filter_pushed_papers",
        operator=FilterFinishedIDs(base_dir=os.path.join(config.storage.base_path, "state"), namespace="push", id_getter=id_getter),
        dependencies=["read_paper_summaries"]
    )

    # TODO(ysj): impl me
    # pipeline.add_operator(
    #     name="push_paper_summaries",
    #     operator=FeishuPush(config.feishu), 
    #     dependencies=["filter_pushed_papers"]
    # )

    pipeline.add_operator(
        name="mark_pushed_papers",
        operator=MarkIDsAsFinished(base_dir=os.path.join(config.storage.base_path, "state"), namespace="push", id_getter=id_getter),
        dependencies=["push_paper_summaries"]
    )

    return pipeline

async def run_paper_pipeline(config_path: str):
    """运行论文处理pipeline"""
    config = Config.from_yaml(config_path)
    pipeline: DAGPipeline = await create_paper_summarize_pipeline(config)
    results = await pipeline.execute()

    logger.info(f"Pipeline completed with {len(results)} results")
    logger.info(results)
    return results

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", type=str, default="config.yaml")
    args = args.parse_args()

    asyncio.run(run_paper_pipeline(args.config))
