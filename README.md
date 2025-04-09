# Daily Paper

一个用于获取和管理学术论文的应用程序。

## 项目结构

```
daily-paper/
├── backend/                 # 后端主目录
│   ├── app/                # 应用代码
│   │   ├── api/           # API路由
│   │   ├── core/          # 核心业务逻辑
│   │   ├── models/        # 数据模型
│   │   └── services/      # 服务层
│   ├── tests/             # 测试目录
│   ├── requirements.txt    # 依赖管理
│   └── config.py          # 配置文件
├── frontend/              # 前端代码
└── README.md             # 项目文档
```

## 后端启动指南

1. 创建虚拟环境：
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# 或
.venv\Scripts\activate  # Windows
```

2. 安装依赖：
```bash
cd backend
pip install -r requirements.txt
```

3. 启动服务器：
```bash
cd backend
python -m uvicorn app.main:app --reload
```

服务器将在 http://localhost:8000 启动，API文档可在 http://localhost:8000/docs 查看。

## 前端启动指南

请参考 frontend 目录下的说明文档。