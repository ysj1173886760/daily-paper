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
import ast

ARXIV_URL = "http://arxiv.org/"

LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_BASE_URL = os.getenv("LLM_BASE_URL")
CHAT_MODEL_NAME = os.getenv("CHAT_MODEL_NAME")
FEISHU_WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL")

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
    """保存论文数据到parquet文件（增加pushed字段）"""
    Path("data").mkdir(exist_ok=True)
    filename = FILTER_FILE_NAME
    
    # 读取已有数据（如果文件存在）
    existing_df = pd.DataFrame()
    if Path(filename).exists():
        try:
            existing_df = pd.read_parquet(filename)
        except Exception as e:
            logging.warning(f"Error reading existing file: {str(e)}")
    
    # 合并新旧数据时添加pushed字段
    new_df = pd.DataFrame.from_dict(papers, orient='index')
    new_df['summary'] = None
    new_df['pushed'] = False  # 新增推送状态字段
    combined_df = pd.concat([existing_df, new_df], ignore_index=False)
    
    # 去重（保留最后出现的记录）并保存
    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    combined_df.to_parquet(filename, engine='pyarrow')

def send_to_feishu(paper: ArxivPaper, summary: str) -> bool:
    """发送单篇论文到飞书（返回是否成功）"""
    if not FEISHU_WEBHOOK_URL:
        logging.error("飞书Webhook地址未配置")
        return

    formatted_summary = summary.replace("\\n", "\n")
    
    message = {
        "msg_type": "interactive",
        "card": {
            "elements": [{
                "tag": "div",
                "text": {
                    "content": f"**{paper['paper_title']}**\n"
                               f"**更新时间**: {paper['update_time']}\n\n"
                               f"👤 {paper['paper_authors']}\n\n"
                               f"💡 AI总结：{formatted_summary}...\n\n"
                               f"---\n"
                               f"📎 [论文原文]({paper['paper_url']})",
                    "tag": "lark_md"
                }
            }],
            "header": {
                "title": {
                    "content": "📄 新论文推荐",
                    "tag": "plain_text"
                }
            }
        }
    }

    try:
        send_to_feishu_with_retry(message)
        logging.info(f"飞书推送成功: {paper['paper_id']}")
        return True
    except Exception as e:
        logging.error(f"飞书推送失败: {str(e)}")
        return False

def push_to_feishu(df: pd.DataFrame) -> pd.DataFrame:
    """批量推送未发送论文并更新状态"""
    # 筛选需要推送的论文
    to_push = df[(df['pushed'] == False) & 
                (df['summary'].notna())].copy()
    
    if to_push.empty:
        logging.info("没有需要推送的新论文")
        return df
    
    # 按时间排序（旧到新）
    sorted_df = to_push.sort_values('update_time', ascending=True)
    
    # 批量处理推送
    success_indices = []
    for index, row in sorted_df.iterrows():
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
        if send_to_feishu(paper, row['summary']):
            success_indices.append(index)
    
    # 批量更新推送状态
    if success_indices:
        df.loc[success_indices, 'pushed'] = True
        df.to_parquet(FILTER_FILE_NAME, engine='pyarrow')
        logging.info(f"成功更新{len(success_indices)}篇论文推送状态")
    
    return df

# 主流程修改
if __name__ == "__main__":
    # 配置dspy
    lm = dspy.LM("openai/" + CHAT_MODEL_NAME, api_base=LLM_BASE_URL, api_key=LLM_API_KEY, temperature=0.2)
    dspy.configure(lm=lm)

    # 获取今日论文
    new_papers = get_daily_papers("\"RAG\" OR \"Retrieval-Augmented Generation\"", 200)

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

    print(f"需要处理{len(papers_without_summary)}篇新论文")

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
    df.to_parquet(FILTER_FILE_NAME, engine='pyarrow')
    
    # 新增飞书推送（只推送本次处理的论文）
    # 按update_time从旧到新排序
    sorted_papers = df.loc[papers_without_summary.index].sort_values('update_time', ascending=True)
    
    for index, row in sorted_papers.iterrows():
        if pd.notna(row['summary']):
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
            send_to_feishu(paper, row['summary'], index)  # 传入df索引

    # 示例：分析第一篇过滤后的论文是否属于特定领域
    # if filtered_papers:
    #     first_paper = next(iter(filtered_papers.values()))
    #     is_in_domain = analyze_paper(first_paper, "RAG")
    #     print(f"第一篇论文是否属于RAG领域: {is_in_domain}")

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

def summarize_paper(lm, paper_text) -> str:
    # 修正冒号为英文格式，使用标准签名语法
    prompt = f"用中文帮我介绍一下这篇文章: {paper_text}"
    summary = lm(prompt)
    return summary[0]

def extract_text_from_pdf(pdf_path):
    """提取PDF文本内容（增加双解析引擎）"""
    try:
        # 尝试使用PyPDF2解析
        with open(pdf_path, 'rb') as f:
            reader = PdfReader(f)
            return '\n'.join([page.extract_text() for page in reader.pages])
    except Exception as pdf_error:
        print(f"PyPDF2解析失败，尝试备用解析引擎: {pdf_path}")
        try:
            # 备选方案1：使用pdfplumber（需要安装）
            import pdfplumber
            with pdfplumber.open(pdf_path) as pdf:
                return '\n'.join([page.extract_text() for page in pdf.pages])
        except Exception as plumber_error:
            try:
                # 备选方案2：使用PyMuPDF（需要安装）
                import fitz  # PyMuPDF的导入名称
                doc = fitz.open(pdf_path)
                return '\n'.join([page.get_text() for page in doc])
            except Exception as fitz_error:
                error_msg = (
                    f"PDF解析全部失败: {pdf_path}\n"
                    f"PyPDF2错误: {str(pdf_error)}\n"
                    f"pdfplumber错误: {str(plumber_error)}\n"
                    f"PyMuPDF错误: {str(fitz_error)}"
                )
                print(error_msg)
                return ""

def download_paper(url: str, paper_id: str, save_dir: str, retries=3):
    """下载并保存PDF论文（增加重试机制）"""
    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, f"{paper_id}.pdf")
    
    if os.path.exists(file_path):
        print(f"文件已存在，跳过下载: {paper_id}")
        return
    
    for attempt in range(retries):
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # 增加文件完整性校验
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    downloaded += len(chunk)
                    f.write(chunk)
                    
            # 简单校验文件完整性
            if total_size > 0 and downloaded != total_size:
                raise IOError("文件大小不匹配，可能下载不完整")
                
            print(f"成功下载: {paper_id}")
            return
        except Exception as e:
            if attempt < retries - 1:
                print(f"下载失败 {paper_id}，第{attempt+1}次重试...")
                time.sleep(2)
            else:
                print(f"下载最终失败 {paper_id}: {str(e)}")
                try:
                    os.remove(file_path)
                except:
                    pass

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

from tenacity import retry, wait_exponential, stop_after_attempt

@retry(stop=stop_after_attempt(100), wait=wait_exponential(multiplier=1, min=1, max=10))
def send_to_feishu_with_retry(message):
    """带重试机制的飞书消息推送"""
    response = requests.post(
        FEISHU_WEBHOOK_URL,
        json=message,
        timeout=10
    )
    response.raise_for_status()