import argparse
import logging
import asyncio
import os
from daily_paper.core.config import Config
from daily_paper.core.workflow.daily_paper_workflow import DAGPipeline
from daily_paper.core.operators.datasource.arxiv import ArxivSource
from daily_paper.core.operators.storage.local_storage import LocalStorageWriter
from daily_paper.core.models import Paper
from daily_paper.core.common.logger import logger
from dataclasses import asdict

async def run_pipeline(config: Config):
    """创建arxiv源pipeline"""
    source_operator = ArxivSource(topic=config.arxiv_topic_list, search_offset=config.arxiv_search_offset, search_limit=config.arxiv_search_limit)

    def kv_getter(x: Paper):
      return x.id, asdict(x)

    writer = LocalStorageWriter(
        storage_dir=os.path.join(config.storage.base_path, "fetched_papers"),
        storage_namespace="fetched_papers",
        key_value_getter=kv_getter,
    )

    # batch process
    write_batch_size = 100
    paper_list = []
    async for paper in source_operator.stream_process(None):
        paper_list.append(paper)
        if len(paper_list) >= write_batch_size:
            await writer.process(paper_list)
            paper_list = []
            logger.info(f"写入 {write_batch_size} 篇论文")

    if len(paper_list) > 0:
        await writer.process(paper_list)

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

    config = Config.from_yaml(args.config)

    asyncio.run(run_pipeline(config))