import logging
from typing import TypedDict, Optional
import arxiv
import pandas as pd
from pathlib import Path
import datetime
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
import argparse

ARXIV_URL = "http://arxiv.org/"

LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_BASE_URL = os.getenv("LLM_BASE_URL")
CHAT_MODEL_NAME = os.getenv("CHAT_MODEL_NAME")
FEISHU_WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL")
MAX_PAPER_TEXT_LENGTH = 128000

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

def save_to_parquet(papers: dict[str, ArxivPaper], meta_file: str):
    """保存论文数据到parquet文件（增加pushed字段）"""
    Path("data").mkdir(exist_ok=True)
    
    # 读取已有数据（如果文件存在）
    existing_df = pd.DataFrame()
    if Path(meta_file).exists():
        try:
            existing_df = pd.read_parquet(meta_file)
        except Exception as e:
            logging.warning(f"Error reading existing file: {str(e)}")
    
    # 合并新旧数据时添加pushed字段
    new_df = pd.DataFrame.from_dict(papers, orient='index')
    new_df['summary'] = None
    new_df['pushed'] = False  # 新增推送状态字段
    combined_df = pd.concat([existing_df, new_df], ignore_index=False)
    
    # 去重（保留最后出现的记录）并保存
    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    combined_df.to_parquet(meta_file, engine='pyarrow')

def send_to_feishu(paper: ArxivPaper, summary: str) -> bool:
    """发送单篇论文到飞书（返回是否成功）"""
    if not FEISHU_WEBHOOK_URL:
        logging.error("飞书Webhook地址未配置")
        return False

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

def push_to_feishu(df: pd.DataFrame, meta_file: str) -> pd.DataFrame:
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
        else:
            logging.error(f"飞书推送失败: {paper['paper_id']} {paper['paper_title']}")
    
    # 批量更新推送状态
    if success_indices:
        df.loc[success_indices, 'pushed'] = True
        df.to_parquet(meta_file, engine='pyarrow')
        logging.info(f"成功更新{len(success_indices)}篇论文推送状态")
    
    return df

def filter_existing_papers(new_papers: dict[str, ArxivPaper], meta_file: str) -> dict[str, ArxivPaper]:
    """过滤已存在的论文（参数化版本）"""
    existing_ids = set()
    
    if Path(meta_file).exists():
        try:
            df = pd.read_parquet(meta_file)
            if not df.empty and 'paper_id' in df.columns:
                existing_ids.update(df['paper_id'].tolist())
        except Exception as e:
            logging.warning(f"Error reading {meta_file}: {str(e)}")
    
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
            text = '\n'.join([page.extract_text() for page in reader.pages])
            # 新增Unicode清理
            return text.encode('utf-8', 'ignore').decode('utf-8')  # 过滤无效字符
    except Exception as pdf_error:
        print(f"PyPDF2解析失败，尝试备用解析引擎: {pdf_path}")
        try:
            # 备选方案1：使用pdfplumber（需要安装）
            import pdfplumber
            with pdfplumber.open(pdf_path) as pdf:
                text = '\n'.join([page.extract_text() for page in pdf.pages])
                return text.encode('utf-8', 'ignore').decode('utf-8')  # 过滤无效字符
        except Exception as plumber_error:
            try:
                # 备选方案2：使用PyMuPDF（需要安装）
                import fitz  # PyMuPDF的导入名称
                doc = fitz.open(pdf_path)
                text = '\n'.join([page.get_text() for page in doc])
                return text.encode('utf-8', 'ignore').decode('utf-8')  # 过滤无效字符
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
    
    # 新增文本截断逻辑
    truncated_text = paper_text
    if len(paper_text) > MAX_PAPER_TEXT_LENGTH:
        print(f"论文截断警告: {paper['paper_title']}（ID: {paper['paper_id']}）文本长度 {len(paper_text)} 字符，已截断")
        truncated_text = paper_text[:MAX_PAPER_TEXT_LENGTH] + "[...截断...]"
    
    # 总结论文（使用截断后的文本）
    summary = await loop.run_in_executor(executor, summarize_paper, lm, truncated_text)
    
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

def generate_daily_summary(lm, df: pd.DataFrame, target_date: datetime.date = None) -> str:
    """生成指定日期的简报并推荐3篇论文"""
    # 默认使用当天日期
    target_date = target_date or datetime.date.today()
    
    # 筛选目标日期推送的论文
    daily_papers = df[(df['update_time'] == target_date)]

    if (len(daily_papers) == 0):
        return None
    
    # 构建汇总文本
    combined_text = "今日论文汇总：\n\n"
    for counter, (idx, row) in enumerate(daily_papers.iterrows(), 1):  # 改用enumerate生成序号
        combined_text += f"【论文{counter}】{row['paper_title']}\nAI总结：{row['summary']}...\n\n"
    
    # LLM生成简报
    prompt = (
        f"请将以下论文汇总信息整理成一份结构清晰的每日简报（使用中文）：\n{combined_text}\n"
        "要求：\n1. 分领域总结研究趋势\n2. 用简洁的bullet points呈现\n3. 推荐3篇最值得阅读的论文并说明理由\n4. 领域相关趋势列出相关论文标题\n5. 论文标题用英文表达\n"
        "6.只输出分领域研究趋势总结和推荐阅读论文，不需要输出其他内容\n7.论文标题输出时不要省略"
    )
    return lm(prompt)[0]

def push_daily_summary(lm, df: pd.DataFrame, target_date: datetime.date = None):
    """推送指定日期的总结报告"""
    daily_report = generate_daily_summary(lm, df, target_date)
    if daily_report == None:
      print(f"{target_date} 没有需要推送的日报")
      return

    print(f"\n=== {target_date or '每日'}简报 ===")
    print(daily_report)
    
    if FEISHU_WEBHOOK_URL:
        target_date_display = target_date or datetime.date.today()
        message = {
            "msg_type": "interactive",
            "card": {
                "elements": [{
                    "tag": "div",
                    "text": {
                        "content": f"📅 AI论文简报({target_date_display}){daily_report}",
                        "tag": "lark_md"
                    }
                }],
                "header": {
                    "title": {
                        "content": f"{target_date_display} 论文日报",
                        "tag": "plain_text"
                    }
                }
            }
        }
        send_to_feishu_with_retry(message)

def process_papers_and_generate_summaries(lm, df: pd.DataFrame) -> pd.DataFrame:
    """处理论文下载并生成摘要（返回更新后的DataFrame）"""
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

    return df

def generate_weekly_summary_if_sunday(lm, df):
    """如果是周日则生成周报，否则生成日报"""
    today = datetime.date.today()
    
    if today.weekday() == 6:  # 周日（0=周一，6=周日）
        print("检测到周日，生成本周所有日报")
        # 遍历过去一周（周一到周日）
        for i in range(6, -1, -1):
            past_day = today - datetime.timedelta(days=i)
            push_daily_summary(lm, df, past_day)
    else:
        print("生成今日日报")
        push_daily_summary(lm, df, today)

# 主流程修改
def main(query: str, 
        max_results: int,
        meta_file: str,
        lm: dspy.LM):
    """主流程函数（参数化版本）"""

    # 获取今日论文
    new_papers = get_daily_papers(query, max_results)

    # 过滤已存在论文
    filtered_papers = filter_existing_papers(new_papers, meta_file)

    save_to_parquet(filtered_papers, meta_file)
    print(f"保存了{len(filtered_papers)}篇新论文")
    
    # 读取保存的论文数据
    df = pd.read_parquet(meta_file)

    # 处理论文并生成摘要
    df = process_papers_and_generate_summaries(lm, df)

    # TODO(ysj): filter paper by user specified summary
    
    # 保存更新后的DataFrame
    df.to_parquet(meta_file, engine='pyarrow')
    
    # df = reset_recent_pushed_status(df, 7)
    
    push_to_feishu(df, meta_file)
    
    # generate_weekly_summary_if_sunday(lm, df)


def reset_recent_pushed_status(df: pd.DataFrame, days: int, meta_file: str) -> pd.DataFrame:
    """重置推送状态（参数化版本）"""
    # 计算日期范围
    cutoff_date = datetime.date.today() - datetime.timedelta(days=days)
    
    # 筛选需要重置的记录（使用loc避免链式赋值警告）
    mask = df['update_time'] >= cutoff_date
    reset_count = df.loc[mask, 'pushed'].sum()
    
    # 执行状态重置
    df.loc[mask, 'pushed'] = FalsWithSummarye
    
    # 保存更新到文件
    df.to_parquet(meta_file, engine='pyarrow')
    logging.info(f"已重置最近{days}天内{reset_count}篇论文的推送状态")
    return df

def rag_papers(lm):
    main("\"RAG\" OR \"Retrieval-Augmented Generation\"", 40, "data/daily_papers.parquet", lm)

def kg_papers(lm):
    main("\"knowledge-graph\" OR \"knowledge graph\"", 40, "data/daily_papers_kg.parquet", lm)

if __name__ == "__main__":
    # 配置dspy
    lm = dspy.LM("openai/" + CHAT_MODEL_NAME, api_base=LLM_BASE_URL, api_key=LLM_API_KEY, temperature=0.2)
    dspy.configure(lm=lm)

    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=str, default="", help="任务名称")
    args = parser.parse_args()

    if args.task == "rag":
        rag_papers(lm)
    elif args.task == "kg":
        kg_papers(lm)
    else:
        print("未知任务")
