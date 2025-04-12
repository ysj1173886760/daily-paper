from typing import Dict, List, Set, Any, Optional
import asyncio
from collections import defaultdict

from daily_paper.core.operators.base import Operator, OperatorNode, OperatorStatus


class DAGPipeline:
    """DAG流水线实现"""
    
    def __init__(self):
        self.operators: Dict[str, OperatorNode] = {}
        self.execution_order: List[Set[str]] = []
        
    def add_operator(self, name: str, operator: Operator, dependencies: Optional[List[str]] = None):
        """添加算子到DAG中
        
        Args:
            name: 算子名称
            operator: 算子实例
            dependencies: 依赖的算子名称列表
        """
        if name in self.operators:
            raise ValueError(f"Operator with name {name} already exists")
            
        if dependencies is None:
            dependencies = set()
        else:
            # 验证依赖的算子是否存在
            for dep in dependencies:
                if dep not in self.operators:
                    raise ValueError(f"Dependency {dep} does not exist")
            dependencies = set(dependencies)
            
        self.operators[name] = OperatorNode(
            operator=operator,
            name=name,
            dependencies=dependencies
        )
        
        # 重新计算执行顺序
        self._compute_execution_order()
        
    def _compute_execution_order(self):
        """计算算子的执行顺序，生成可并行执行的层级"""
        self.execution_order = []
        remaining = set(self.operators.keys())
        
        while remaining:
            # 找出当前可执行的算子（所有依赖都已完成）
            executable = set()
            for name in remaining:
                if all(dep not in remaining for dep in self.operators[name].dependencies):
                    executable.add(name)
                    
            if not executable:
                # 如果没有可执行的算子但还有剩余算子，说明存在循环依赖
                raise ValueError("Circular dependency detected in pipeline")
                
            self.execution_order.append(executable)
            remaining -= executable
            
    async def execute(self, initial_data: Any = None) -> Dict[str, Any]:
        """执行流水线
        
        Args:
            initial_data: 初始输入数据
            
        Returns:
            Dict[str, Any]: 每个算子的执行结果
        """
        # 重置所有算子状态
        for op in self.operators.values():
            op.status = OperatorStatus.PENDING
            op.result = None
            
        results = {}
        if initial_data is not None:
            results["initial"] = initial_data
            
        # 按层级执行算子
        for layer in self.execution_order:
            # 同一层级的算子可以并行执行
            tasks = []
            for op_name in layer:
                op_node = self.operators[op_name]
                # 收集所有依赖的结果
                input_data = initial_data
                if op_node.dependencies:
                    # 如果有依赖，使用依赖的结果作为输入
                    deps_results = [results[dep] for dep in op_node.dependencies]
                    if len(deps_results) == 1:
                        input_data = deps_results[0]
                    else:
                        input_data = deps_results
                
                op_node.status = OperatorStatus.RUNNING
                tasks.append(self._execute_operator(op_node, input_data))
                
            # 等待当前层级的所有算子执行完成
            layer_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 处理执行结果
            for op_name, result in zip(layer, layer_results):
                op_node = self.operators[op_name]
                if isinstance(result, Exception):
                    op_node.status = OperatorStatus.FAILED
                    raise result
                else:
                    op_node.status = OperatorStatus.COMPLETED
                    op_node.result = result
                    results[op_name] = result
                    
        return results
    
    async def _execute_operator(self, op_node: OperatorNode, input_data: Any) -> Any:
        """执行单个算子
        
        Args:
            op_node: 算子节点
            input_data: 输入数据
            
        Returns:
            Any: 算子执行结果
        """
        try:
            return await op_node.operator.process(input_data)
        except Exception as e:
            op_node.status = OperatorStatus.FAILED
            raise e 