from typing import Any, List
import arxiv

from daily_paper.core.operators.base import Operator
from daily_paper.core.models import Paper
from daily_paper.core.common.logger import logger

ARXIV_URL = "http://arxiv.org/"


def get_authors(authors, first_author=False):
    if first_author:
        return str(authors[0])  # 显式转换为字符串
    return ", ".join(str(author) for author in authors)  # 确保所有元素都是字符串


class ArxivSource(Operator):
    """从Arxiv获取论文数据的算子"""

    def __init__(self, topic: str | List[str], max_results: int = 100):
        """初始化ArxivSource

        Args:
            topic: 要搜索的主题，可以是单个字符串或字符串列表。如果是列表，将使用 OR 连接进行搜索
            max_results: 最大返回结果数
        """
        if isinstance(topic, list):
            self.topic = " OR ".join(f'"{t}"' for t in topic)
        else:
            self.topic = f'"{topic}"' if " OR " not in topic else topic
        self.max_results = max_results
        logger.info(
            f"初始化 ArxivSource: topic={self.topic}, max_results={max_results}"
        )

    async def process(self, _: Any) -> List[Paper]:
        """从Arxiv获取论文数据

        Returns:
            List[Paper]: 论文列表
        """
        paper_list = []
        logger.info(f"开始从 Arxiv 获取论文数据: topic={self.topic}")

        client = arxiv.Client()
        search = arxiv.Search(
            query=self.topic,
            max_results=self.max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate,
        )

        for result in client.results(search):
            paper_id = result.get_short_id()
            paper_title = result.title
            paper_url = result.entry_id
            paper_abstract = result.summary.replace("\n", " ")
            paper_authors = get_authors(result.authors)
            primary_category = result.primary_category
            publish_time = result.published.date()
            update_time = result.updated.date()
            comments = result.comment

            # logger.info(f"Time = {update_time} title = {paper_title} author = {paper_first_author}")

            # eg: 2108.09112v1 -> 2108.09112
            ver_pos = paper_id.find("v")
            if ver_pos == -1:
                paper_key = paper_id
            else:
                paper_key = paper_id[0:ver_pos]
            paper_url = ARXIV_URL + "abs/" + paper_key

            arxiv_paper = Paper(
                id=paper_key,
                title=paper_title,
                url=paper_url,
                abstract=paper_abstract,
                authors=paper_authors,
                category=primary_category,
                publish_date=publish_time.strftime("%Y-%m-%d"),
                update_date=update_time.strftime("%Y-%m-%d"),
            )
            paper_list.append(arxiv_paper)

        return paper_list
