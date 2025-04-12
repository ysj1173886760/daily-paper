# Daily Paper 系统设计文档

## 1. 系统概述

Daily Paper 是一个基于算子(Operator)的论文处理流水线系统，用于自动化收集、分析和推送学术论文。系统采用模块化设计，通过可组合的算子来构建灵活的处理流程。

## 2. 核心概念

### 2.1 Paper 数据模型

```python
class Paper:
    paper_id: str          # 论文唯一标识
    title: str            # 论文标题
    url: str             # 论文链接
    abstract: str        # 论文摘要
    authors: List[str]   # 作者列表
    category: str        # 论文类别
    publish_date: Date   # 发布日期
    update_date: Date    # 更新日期
    summary: Optional[str] # AI生成的摘要
    pushed: bool         # 是否已推送
```

### 2.2 算子(Operator)体系

#### 基础算子接口
```python
class Operator:
    def process(self, input_data: Any) -> Any:
        """处理输入数据并返回结果"""
        pass
```

#### 核心算子类型

1. **数据源算子 (DataSource)**
   - ArxivSource: 从Arxiv获取特定主题的论文
   - LocalSource: 从本地存储读取论文数据

2. **处理算子 (Processor)**
   - LLMSummarizer: 使用LLM生成论文摘要
   - TopicFilter: 使用LLM进行主题过滤
   - DailyDigest: 生成每日论文简报
   - WeeklyDigest: 生成每周论文总结

3. **状态管理算子 (StateManager)**
   - InsertPendingIDs: 将待处理的论文ID插入pending状态
   - GetAllPendingIDs: 获取所有处于pending状态的论文ID
   - MarkIDsAsFinished: 将已处理的论文ID标记为完成

4. **输出算子 (Sink)**
   - FeishuPusher: 推送到飞书
   - LocalStorage: 保存到本地存储
   - DatabaseSink: 保存到数据库

## 3. 流水线设计

### 3.1 Pipeline 框架

Pipeline框架采用DAG（有向无环图）结构设计，支持算子的并行执行和依赖管理。

核心类设计：
```python
class OperatorStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass
class OperatorNode:
    operator: Operator
    name: str
    dependencies: Set[str]
    status: OperatorStatus = OperatorStatus.PENDING

class DAGPipeline:
    def __init__(self):
        self.operators: Dict[str, OperatorNode] = {}
        self.execution_order: List[Set[str]] = []  # 每层可并行执行的算子集合
    
    def add_operator(self, name: str, operator: Operator, dependencies: List[str] = None):
        """添加算子到DAG中"""
        pass
    
    async def execute(self, initial_data: Any = None) -> Dict[str, Any]:
        """按照拓扑顺序执行算子，同一层级并行执行"""
        pass
```

主要特性：
1. **DAG结构**：支持复杂的算子依赖关系
2. **并行执行**：同一层级的算子可并行处理
3. **状态管理**：跟踪每个算子的执行状态
4. **异步支持**：支持同步/异步算子混合使用

### 3.2 示例流水线

```python
# 创建DAG流水线示例
pipeline = DAGPipeline()

# 数据源算子
pipeline.add_operator("arxiv_source", ArxivSource(topic="RAG"))

# 并行处理算子
pipeline.add_operator("summarizer", LLMSummarizer(), ["arxiv_source"])
pipeline.add_operator("topic_filter", TopicFilter(domain="RAG"), ["arxiv_source"])

# 汇聚处理
pipeline.add_operator("pending_marker", InsertPendingIDs("push_pending"), 
                     ["summarizer", "topic_filter"])

# 存储算子
pipeline.add_operator("storage", LocalStorage(), ["pending_marker"])
```

执行流程示意：
```
arxiv_source
    ├── summarizer ──┐
    └── topic_filter ┴── pending_marker --> storage
```

### 3.3 状态管理设计

状态管理算子的核心实现：

```python
class InsertPendingIDs(Operator):
    """将论文ID标记为待处理状态"""
    def __init__(self, stage_name: str):
        self.stage_name = stage_name
    
    def process(self, papers: List[Paper]) -> List[Paper]:
        pending_ids = [paper.id for paper in papers]
        store_pending_ids(self.stage_name, pending_ids)
        return papers

class GetAllPendingIDs(Operator):
    """获取所有待处理的论文ID"""
    def __init__(self, stage_name: str):
        self.stage_name = stage_name
    
    def process(self, _) -> List[str]:
        return get_pending_ids(self.stage_name)

class MarkIDsAsFinished(Operator):
    """将论文ID标记为处理完成"""
    def __init__(self, stage_name: str):
        self.stage_name = stage_name
    
    def process(self, paper_ids: List[str]) -> List[str]:
        mark_as_finished(self.stage_name, paper_ids)
        return paper_ids
```

状态管理算子的优势：
1. 通用性：可用于任何需要状态追踪的场景
2. 解耦合：状态管理与业务逻辑分离
3. 原子性：每个状态变更都是独立的原子操作
4. 可追踪：支持多阶段处理的状态追踪

## 4. 扩展性设计

### 4.1 新增算子
系统支持以下类型的算子扩展：
- 新的数据源（如其他论文库）
- 新的处理器（如情感分析、关键词提取）
- 新的输出端（如其他通讯工具）

### 4.2 配置化
- 支持通过配置文件定义流水线
- 支持动态加载算子
- 支持算子参数配置

## 5. 存储设计

### 5.1 本地存储
- 使用Parquet格式存储论文数据
- 支持增量更新
- 支持历史记录追踪

### 5.2 状态管理
- 记录论文处理状态
- 支持断点续传
- 支持重试机制

## 6. 监控和日志

### 6.1 系统监控
- 算子执行时间
- 处理论文数量
- 错误率统计

### 6.2 日志记录
- 操作日志
- 错误日志
- 性能日志

## 7. 后续优化方向

1. 并行处理支持
   - 算子级并行
   - 数据级并行

2. 错误处理增强
   - 重试策略
   - 降级策略
   - 报警机制

3. 智能调度
   - 基于负载的动态调度
   - 基于优先级的调度
   - 定时任务支持
