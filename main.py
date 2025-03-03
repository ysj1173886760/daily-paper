import logging
from typing import TypedDict, Optional
import datetime
import arxiv
import pandas as pd
from pathlib import Path
from datetime import datetime
import os
from PyPDF2 import PdfReader
import dspy
import pandas as pd
from pathlib import Path

ARXIV_URL = "http://arxiv.org/"

LLM_API_KEY = os.getenv("LLM_API_KEY")
CHAT_MODEL_NAME = os.getenv("CHAT_MODEL_NAME")

FILTER_FILE_NAME = "data/daily_papers.parquet"

class ArxivPaper(TypedDict):
    paper_id: str
    paper_title: str
    paper_url: str
    paper_abstract: str
    paper_authors: str
    paper_first_author: str
    primary_category: str
    publish_time: datetime.date
    update_time: datetime.date
    comments: Optional[str]

def get_authors(authors, first_author = False):
    output = str()
    if first_author == False:
        output = ", ".join(str(author) for author in authors)
    else:
        output = authors[0]
    return output

def get_daily_papers(query, max_results) -> dict[str, ArxivPaper]:
    paper_result = {}
    search_engine = arxiv.Search(
        query = query,
        max_results = max_results,
        sort_by = arxiv.SortCriterion.SubmittedDate
    )

    for result in search_engine.results():
        paper_id            = result.get_short_id()
        paper_title         = result.title
        paper_url           = result.entry_id
        paper_abstract      = result.summary.replace("\n"," ")
        paper_authors       = get_authors(result.authors)
        paper_first_author  = get_authors(result.authors, first_author = True)
        primary_category    = result.primary_category
        publish_time        = result.published.date()
        update_time         = result.updated.date()
        comments            = result.comment


        logging.info(f"Time = {update_time} title = {paper_title} author = {paper_first_author}")

        # eg: 2108.09112v1 -> 2108.09112
        ver_pos = paper_id.find('v')
        if ver_pos == -1:
            paper_key = paper_id
        else:
            paper_key = paper_id[0:ver_pos]
        paper_url = ARXIV_URL + 'abs/' + paper_key

        arxiv_paper = ArxivPaper(
          paper_id=paper_id,
          paper_title=paper_title,
          paper_url=paper_url,
          paper_abstract=paper_abstract,
          paper_authors=paper_authors,
          paper_first_author=paper_first_author,
          primary_category=primary_category,
          publish_time=publish_time,
          update_time=update_time,
          comments=comments
        )
        paper_result[paper_key] = arxiv_paper

    return paper_result

def save_to_parquet(papers: dict[str, ArxivPaper]):
    """保存论文数据到parquet文件（增量写入）"""
    Path("data").mkdir(exist_ok=True)
    filename = FILTER_FILE_NAME
    
    # 读取已有数据（如果文件存在）
    existing_df = pd.DataFrame()
    if Path(filename).exists():
        try:
            existing_df = pd.read_parquet(filename)
        except Exception as e:
            logging.warning(f"Error reading existing file: {str(e)}")
    
    # 合并新旧数据
    new_df = pd.DataFrame.from_dict(papers, orient='index')
    combined_df = pd.concat([existing_df, new_df], ignore_index=False)
    
    # 去重（保留最后出现的记录）
    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    
    # 保存更新后的数据
    combined_df.to_parquet(filename, engine='pyarrow')
    logging.info(f"Saved {len(combined_df)} papers (added {len(papers)} new) to {filename}")

def filter_existing_papers(new_papers: dict[str, ArxivPaper]) -> dict[str, ArxivPaper]:
    """过滤已存在的论文（单文件版本）"""
    existing_ids = set()
    filename = FILTER_FILE_NAME
    
    # 检查并读取单个文件
    if Path(filename).exists():
        try:
            df = pd.read_parquet(filename)
            if not df.empty and 'paper_id' in df.columns:
                existing_ids.update(df['paper_id'].tolist())
        except Exception as e:
            logging.warning(f"Error reading {filename}: {str(e)}")
    
    # 过滤新论文
    return {k: v for k, v in new_papers.items() if v['paper_id'] not in existing_ids}

class PaperAnalysis(dspy.Signature):
    """分析论文内容并结构化提取核心贡献，同时判断是否属于某一个特定领域"""
    input_paper_text: str = dspy.InputField(desc="论文全文文本")
    input_domain: str = dspy.InputField(desc="领域名")
    output_domain: bool = dspy.OutputField(desc="是否属于用户给定的领域, 返回true或false")

if __name__ == "__main__":
  # 获取今日论文
  new_papers = get_daily_papers("RAG", 5)
  
  # 过滤已存在论文
  filtered_papers = filter_existing_papers(new_papers)
  
  # 保存新论文到parquet
  if filtered_papers:
      save_to_parquet(filtered_papers)
      print(f"保存了{len(filtered_papers)}篇新论文")
  else:
      print("没有发现新论文")