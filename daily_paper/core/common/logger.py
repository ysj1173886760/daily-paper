from loguru import logger
import sys
import os

def setup_logger():
    """配置日志记录器
    
    配置内容包括：
    1. 输出到控制台，级别为 INFO
    2. 输出到文件，级别为 DEBUG，按天轮转
    """
    # 移除默认的处理器
    logger.remove()
    
    # 添加控制台输出
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    
    # 创建日志目录
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # 添加文件输出
    logger.add(
        os.path.join(log_dir, "daily_paper_{time:YYYY-MM-DD}.log"),
        rotation="00:00",  # 每天轮转
        retention="30 days",  # 保留30天
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        encoding="utf-8"
    )

# 初始化日志配置
setup_logger()

__all__ = ['logger'] 