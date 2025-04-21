from daily_paper.core.pipeline import DAGPipeline
from daily_paper.core.operators.datasource.arxiv import ArxivSource
from daily_paper.core.operators.processor.paper_reader import PaperReader
from daily_paper.core.operators.processor.llm_summarizer import LLMSummarizer
from daily_paper.core.operators.processor.custom_processor import CustomProcessor
from daily_paper.core.operators.state.pending import (
    FilterFinishedIDs,
    MarkIDsAsFinished,
    InsertPendingIDs,
)
from daily_paper.core.models import Paper, PaperWithSummary
from daily_paper.core.config import LLMConfig
from daily_paper.core.operators.sink.feishu import FeishuPusher
from daily_paper.core.config import Config
from daily_paper.core.common import logger
from daily_paper.core.operators.storage.local_storage import (
    LocalStorageWriter,
    LocalStorageReader,
)
import os
import asyncio
import argparse
from typing import Tuple, List, Any
from dataclasses import asdict
from daily_paper.core.operators.processor.abstract_based_llm_filter import AbstractBasedLLMFilter
import logging

def id_getter(x: Paper):
    return x.id

async def create_paper_filter_pipeline(config: Config) -> DAGPipeline:
    """创建论文过滤pipeline"""
    pipeline = DAGPipeline()

    pipeline.add_operator(
        name="arxiv_source",
        operator=ArxivSource(
            topic=config.arxiv_topic_list, search_offset=config.arxiv_search_offset, search_limit=config.arxiv_search_limit
        ),
        dependencies=None,
    )

    pipeline.add_operator(
        name="filter_arxiv_papers",
        operator=FilterFinishedIDs(
            base_dir=os.path.join(config.storage.base_path, "state"),
            namespace="arxiv_llm_filter",
            id_getter=id_getter,
        ),
        dependencies=["arxiv_source"],
    )

    pipeline.add_operator(
        name="llm_filter",
        operator=AbstractBasedLLMFilter(config.llm, config.llm_filter_topic),
        dependencies=["filter_arxiv_papers"],
    )

    def kv_getter(x: Tuple[Paper, bool]):
        filtered = x[1]
        if not filtered:
            return x[0].id, asdict(x[0])
        else:
            return x[0].id, None

    pipeline.add_operator(
        name="save_filtered_papers",
        operator=LocalStorageWriter(
            storage_dir=os.path.join(config.storage.base_path, "filtered_papers"),
            storage_namespace="filtered_papers",
            key_value_getter=kv_getter,
        ),
        dependencies=["llm_filter"],
    )

    def paper_with_filter_status_id_getter(x: Tuple[Paper, bool]):
        return x[0].id

    pipeline.add_operator(
        name="mark_filtered_papers",
        operator=MarkIDsAsFinished(
            base_dir=os.path.join(config.storage.base_path, "state"),
            namespace="arxiv_llm_filter",
            id_getter=paper_with_filter_status_id_getter,  
        ),
        dependencies=["save_filtered_papers"],
    )
    
    return pipeline

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

    if not config.enable_llm_filter:
        # 添加数据源算子
        pipeline.add_operator(
            name="paper_source",
            operator=ArxivSource(
                topic=config.arxiv_topic_list, search_offset=config.arxiv_search_offset, search_limit=config.arxiv_search_limit
            ),
            dependencies=None,
        )
    else:
        def convert_to_paper(key: str, value: dict):
            return Paper(**value)

        pipeline.add_operator(
            name="paper_source",
            operator=LocalStorageReader(
                storage_dir=os.path.join(config.storage.base_path, "filtered_papers"),
                storage_namespace="filtered_papers",
                value_reader=convert_to_paper,
            ),
            dependencies=None,
        )

    pipeline.add_operator(
        name="filter_pending_ids",
        operator=FilterFinishedIDs(
            base_dir=os.path.join(config.storage.base_path, "state"),
            namespace="arxiv",
            id_getter=id_getter,
        ),
        dependencies=["paper_source"],
    )

    pipeline.add_operator(
        name="limit_batch_size",
        operator=CustomProcessor(lambda x: x[:config.process_batch_size]),
        dependencies=["filter_pending_ids"],
    )

    # only read the unprocessed papers
    pipeline.add_operator(
        name="paper_reader",
        operator=PaperReader(os.path.join(config.storage.base_path, "paper_caches")),
        dependencies=["limit_batch_size"],
    )

    # 添加论文总结算子
    pipeline.add_operator(
        name="paper_summarizer",
        operator=LLMSummarizer(config.llm),
        dependencies=["paper_reader"],
    )

    def kv_getter(x: PaperWithSummary):
        return x.id, asdict(x)

    pipeline.add_operator(
        name="save_paper_summaries",
        operator=LocalStorageWriter(
            storage_dir=os.path.join(config.storage.base_path, "paper_summaries"),
            storage_namespace="paper_summaries",
            key_value_getter=kv_getter,
        ),
        dependencies=["paper_summarizer"],
    )

    # mark the processed papers as finished
    pipeline.add_operator(
        name="mark_processed_papers",
        operator=MarkIDsAsFinished(
            base_dir=os.path.join(config.storage.base_path, "state"),
            namespace="arxiv",
            id_getter=id_getter,
        ),
        dependencies=["save_paper_summaries"],
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
        operator=LocalStorageReader(
            storage_dir=os.path.join(config.storage.base_path, "paper_summaries"),
            storage_namespace="paper_summaries",
            value_reader=convert_to_paper_with_summary,
        ),
        dependencies=None,
    )

    # filter the papers that have been pushed
    pipeline.add_operator(
        name="filter_pushed_papers",
        operator=FilterFinishedIDs(
            base_dir=os.path.join(config.storage.base_path, "state"),
            namespace="push",
            id_getter=id_getter,
        ),
        dependencies=["read_paper_summaries"],
    )

    def order_by_update_date(x: List[PaperWithSummary]) -> List[PaperWithSummary]:
        return sorted(x, key=lambda y: y.update_date)

    pipeline.add_operator(
        name="order_by_update_date",
        operator=CustomProcessor(order_by_update_date),
        dependencies=["filter_pushed_papers"],
    )

    def title_and_content_getter(x: PaperWithSummary) -> Tuple[str, str]:
        title = "📄 新论文推荐"
        content = f"**{x.title}**\n"
        content += f"**更新时间**: {x.update_date}\n\n"
        content += f"👤 {x.authors}\n\n"
        content += f"💡 AI总结：{x.summary}...\n\n"
        content += f"---\n"
        content += f"📎 [论文原文]({x.url})"
        return title, content

    pipeline.add_operator(
        name="push_paper_summaries",
        operator=FeishuPusher(config.feishu_webhook_url, title_and_content_getter),
        dependencies=["order_by_update_date"],
    )

    def filter_out_failed_papers(
        x: List[Tuple[PaperWithSummary, bool]],
    ) -> List[PaperWithSummary]:
        return [y[0] for y in x if y[1] is True]

    pipeline.add_operator(
        name="filter_out_push_failed_papers",
        operator=CustomProcessor(filter_out_failed_papers),
        dependencies=["push_paper_summaries"],
    )

    pipeline.add_operator(
        name="mark_pushed_papers",
        operator=MarkIDsAsFinished(
            base_dir=os.path.join(config.storage.base_path, "state"),
            namespace="push",
            id_getter=id_getter,
        ),
        dependencies=["filter_out_push_failed_papers"],
    )

    return pipeline


async def run_paper_summarize_pipeline(config_path: str):
    """运行论文处理pipeline"""
    config = Config.from_yaml(config_path)
    total_results = []
    # TODO(ysj): use sink to collect results
    while True:
      pipeline: DAGPipeline = await create_paper_summarize_pipeline(config)
      results = await pipeline.execute()
      logger.info(f"Paper Summarize Pipeline small batch completed with {len(results)} results")
      total_results.extend(results)
      if len(results) == 0:
        break

    logger.info(f"Paper Summarize Pipeline completed with {len(total_results)} results")

    return total_results


async def run_paper_push_pipeline(config_path: str):
    config = Config.from_yaml(config_path)
    pipeline: DAGPipeline = await create_paper_push_pipeline(config)
    results = await pipeline.execute()
    logger.info(f"Paper Push Pipeline completed with {len(results)} results")
    return results

async def run_paper_filter_pipeline(config_path: str):
    config = Config.from_yaml(config_path)
    pipeline: DAGPipeline = await create_paper_filter_pipeline(config)
    results = await pipeline.execute()
    logger.info(f"Paper Filter Pipeline completed with {len(results)} results")
    return results

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", type=str, default="config.yaml")
    args = args.parse_args()

    # 配置日志记录
    logging.basicConfig(
        level=logging.INFO,  # 设置日志级别为 INFO
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # 输出到控制台
        ]
    )

    arxiv_logger = logging.getLogger('arxiv')
    arxiv_logger.setLevel(logging.INFO)


    # asyncio.run(run_paper_filter_pipeline(args.config))
    # asyncio.run(run_paper_summarize_pipeline(args.config))
    asyncio.run(run_paper_push_pipeline(args.config))