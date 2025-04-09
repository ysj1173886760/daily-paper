import os
from pathlib import Path

# 基础路径配置
BASE_DIR = Path(__file__).resolve().parent

# API配置
API_V1_STR = "/api/v1"
PROJECT_NAME = "Daily Paper"

# 数据目录配置
DATA_DIR = os.path.join(BASE_DIR.parent, "data")
PAPERS_DIR = os.path.join(BASE_DIR.parent, "papers")

# 确保必要的目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PAPERS_DIR, exist_ok=True)

# 数据库配置（如果需要的话）
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./daily_paper.db") 