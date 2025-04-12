import os
import requests
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import asyncio
from tqdm.asyncio import tqdm_asyncio
from tenacity import retry, wait_exponential, stop_after_attempt

from daily_paper.core.operators.base.operator import Operator
from daily_paper.core.common import logger
from daily_paper.core.models import Paper

class PaperReader(Operator):
    """论文下载和PDF解析算子
    
    将输入的论文列表下载为PDF并解析为文本内容。
    """
    
    def __init__(self, cache_dir: str = "papers", max_workers: int = 20):
        """
        初始化PaperReader
        
        Args:
            save_dir: PDF文件保存目录
            max_workers: 并发下载的最大worker数
        """
        super().__init__()
        self.cache_dir = cache_dir
        self.max_workers = max_workers
        self.executor = None

    async def setup(self):
        """初始化资源"""
        os.makedirs(self.cache_dir, exist_ok=True)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

    async def cleanup(self):
        """清理资源"""
        if self.executor:
            self.executor.shutdown()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def _download_paper(self, url: str, paper_id: str) -> str:
        """
        下载单篇论文
        
        Args:
            url: 论文URL
            paper_id: 论文ID
            
        Returns:
            str: 保存的文件路径
        """
        file_path = os.path.join(self.cache_dir, f"{paper_id}.pdf")

        logger.info(f"下载论文: {url} {paper_id}")
        
        if os.path.exists(file_path):
            logger.info(f"文件已存在，跳过下载: {paper_id}")
            return file_path
        
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # 文件完整性校验
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                downloaded += len(chunk)
                f.write(chunk)
                
        # 校验文件大小
        if total_size > 0 and downloaded != total_size:
            raise IOError("文件大小不匹配，可能下载不完整")
            
        logger.info(f"成功下载: {paper_id}")
        return file_path

    def _extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        从PDF中提取文本，使用多个解析引擎保证可靠性
        
        Args:
            pdf_path: PDF文件路径
            
        Returns:
            str: 提取的文本内容
        """
        try:
            # 尝试使用PyPDF2解析
            from PyPDF2 import PdfReader
            with open(pdf_path, 'rb') as f:
                reader = PdfReader(f)
                text = '\n'.join([page.extract_text() for page in reader.pages])
                return text.encode('utf-8', 'ignore').decode('utf-8')
        except Exception as pdf_error:
            logger.warning(f"PyPDF2解析失败，尝试备用解析引擎: {pdf_path}")
            try:
                # 备选方案1：使用pdfplumber
                import pdfplumber
                with pdfplumber.open(pdf_path) as pdf:
                    text = '\n'.join([page.extract_text() for page in pdf.pages])
                    return text.encode('utf-8', 'ignore').decode('utf-8')
            except Exception as plumber_error:
                try:
                    # 备选方案2：使用PyMuPDF
                    import fitz
                    doc = fitz.open(pdf_path)
                    text = '\n'.join([page.get_text() for page in doc])
                    return text.encode('utf-8', 'ignore').decode('utf-8')
                except Exception as fitz_error:
                    error_msg = (
                        f"PDF解析全部失败: {pdf_path}\n"
                        f"PyPDF2错误: {str(pdf_error)}\n"
                        f"pdfplumber错误: {str(plumber_error)}\n"
                        f"PyMuPDF错误: {str(fitz_error)}"
                    )
                    logger.error(error_msg)
                    return ""

    async def _process_single_paper(self, paper: Paper) -> tuple:
        """
        处理单篇论文的异步任务
        
        Args:
            paper: 论文信息字典
            
        Returns:
            tuple: (paper_id, 提取的文本内容)
        """
        # 将arxiv的abs链接转换为pdf链接
        pdf_url = paper.url.replace('abs', 'pdf')
        
        try:
            # 下载论文
            pdf_path = await asyncio.get_event_loop().run_in_executor(
                self.executor, 
                self._download_paper,
                pdf_url,
                paper.id
            )
            
            # 提取文本
            paper_text = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self._extract_text_from_pdf,
                pdf_path
            )
            
            return paper, paper_text
            
        except Exception as e:
            logger.error(f"处理论文失败 {paper.id}: {str(e)}")
            return paper, ""

    async def process(self, papers: list[Paper]) -> list[tuple[Paper, str]]:
        """
        处理论文列表
        
        Args:
            papers: 论文信息列表
            
        Returns:
            list[tuple[Paper, str]]: 论文ID到提取文本的映射
        """
        if not papers:
            return []
            
        # 准备所有任务
        tasks = [
            self._process_single_paper(paper)
            for paper in papers
        ]
        
        # 使用tqdm显示进度
        results = await tqdm_asyncio.gather(
            *tasks,
            desc="处理论文",
            total=len(tasks)
        )
        
        return results
