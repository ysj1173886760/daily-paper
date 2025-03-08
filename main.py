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
import requests
from tqdm import tqdm  # 新增进度条导入
from functools import wraps
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

ARXIV_URL = "http://arxiv.org/"

LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_BASE_URL = os.getenv("LLM_BASE_URL")
CHAT_MODEL_NAME = os.getenv("CHAT_MODEL_NAME")

FILTER_FILE_NAME = "data/daily_papers.parquet"

def sync_timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000
        print(f"function: {func.__name__} execution time: {elapsed_time:.2f} millisecond")
        return result
    return wrapper

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
    if first_author:
        return str(authors[0])  # 显式转换为字符串
    return ", ".join(str(author) for author in authors)  # 确保所有元素都是字符串

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
    
    # 合并新旧数据时添加summary字段
    new_df = pd.DataFrame.from_dict(papers, orient='index')
    new_df['summary'] = None  # 新增summary列初始化
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
    """分析论文摘要并判断是否属于某一个特定领域"""
    input_paper_text: str = dspy.InputField(desc="论文的摘要")
    input_domain: str = dspy.InputField(desc="领域名")
    output_domain: bool = dspy.OutputField(desc="是否属于用户给定的领域, 返回true或false")

def analyze_paper(paper: ArxivPaper, domain: str) -> bool:
    """
    使用PaperAnalysis分析论文是否属于特定领域。
    
    :param paper: 要分析的论文，类型为ArxivPaper
    :param domain: 要判断的领域名
    :return: 如果论文属于该领域返回True，否则返回False
    """
    analysis = PaperAnalysis()
    result = analysis(input_paper_text=paper['paper_abstract'], input_domain=domain)
    return result.output_domain

@sync_timer
def summarize_paper(lm, paper_text) -> str:
    # 修正冒号为英文格式，使用标准签名语法
    prompt = f"用中文帮我介绍一下这篇文章: {paper_text}"
    summary = lm(prompt)
    return summary

@sync_timer
def extract_text_from_pdf(pdf_path):
    """提取PDF文本内容"""
    try:
        with open(pdf_path, 'rb') as f:
            reader = PdfReader(f)
            return '\n'.join([page.extract_text() for page in reader.pages])
    except Exception as e:
        print(f"Error reading {pdf_path}: {str(e)}")
        return ""

@sync_timer
def download_paper(url: str, paper_id: str, save_dir: str):
    """下载并保存PDF论文"""
    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, f"{paper_id}.pdf")
    
    if os.path.exists(file_path):
        print(f"文件已存在，跳过下载: {paper_id}")
        return
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"成功下载: {paper_id}")
    except Exception as e:
        print(f"下载失败 {paper_id}: {str(e)}")

async def process_single_paper(executor, lm, paper, row_index):
    """并发处理单篇论文的异步任务"""
    loop = asyncio.get_event_loop()
    
    # 下载论文（使用线程池执行阻塞IO）
    pdf_url = paper['paper_url'].replace('abs', 'pdf')
    await loop.run_in_executor(executor, download_paper, pdf_url, paper['paper_id'], 'papers')
    
    # 提取文本
    pdf_path = os.path.join('papers', f"{paper['paper_id']}.pdf")
    paper_text = await loop.run_in_executor(executor, extract_text_from_pdf, pdf_path)
    
    # 总结论文
    summary = await loop.run_in_executor(executor, summarize_paper, lm, paper_text)
    
    return row_index, summary

if __name__ == "__main__":
    # 配置dspy
    lm = dspy.LM("openai/" + CHAT_MODEL_NAME, api_base=LLM_BASE_URL, api_key=LLM_API_KEY, temperature=0.2)
    dspy.configure(lm=lm)

    # 获取今日论文
    new_papers = get_daily_papers("RAG", 20)

    # 过滤已存在论文
    filtered_papers = filter_existing_papers(new_papers)

    save_to_parquet(filtered_papers)
    print(f"保存了{len(filtered_papers)}篇新论文")
    
    # 读取保存的论文数据
    df = pd.read_parquet(FILTER_FILE_NAME)

    # 添加缺失的summary列（兼容旧数据）
    if 'summary' not in df.columns:
        df['summary'] = None

    # 过滤掉已经有summary字段的论文
    papers_without_summary = df[df['summary'].isna()]

    # 创建线程池执行器
    executor = ThreadPoolExecutor(max_workers=20)
    # 修复事件循环创建方式
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # 准备所有任务
    tasks = []
    for index, row in papers_without_summary.iterrows():
        paper = ArxivPaper(
            paper_id=row['paper_id'],
            paper_title=row['paper_title'],
            paper_url=row['paper_url'],
            paper_abstract=row['paper_abstract'],
            paper_authors=row['paper_authors'],
            paper_first_author=row['paper_first_author'],
            primary_category=row['primary_category'],
            publish_time=row['publish_time'],
            update_time=row['update_time'],
            comments=row['comments']
        )
        tasks.append(process_single_paper(executor, lm, paper, index))
    
    # 使用tqdm显示并发任务进度
    from tqdm.asyncio import tqdm_asyncio
    results = loop.run_until_complete(
        tqdm_asyncio.gather(*tasks, desc="并发处理论文", total=len(tasks))
    )
    
    # 批量更新结果
    for index, summary in results:
        df.at[index, 'summary'] = summary

    # 保存更新后的DataFrame
    df.to_parquet(FILTER_FILE_NAME, engine='pyarrow')  # 新增保存操作

    # 示例：分析第一篇过滤后的论文是否属于特定领域
    # if filtered_papers:
    #     first_paper = next(iter(filtered_papers.values()))
    #     is_in_domain = analyze_paper(first_paper, "RAG")
    #     print(f"第一篇论文是否属于RAG领域: {is_in_domain}")