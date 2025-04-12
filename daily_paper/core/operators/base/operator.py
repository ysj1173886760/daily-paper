from enum import Enum
from typing import Any, Optional, Set
from dataclasses import dataclass


class OperatorStatus(Enum):
    """算子状态枚举"""
    PENDING = "PENDING"    # 待执行
    RUNNING = "RUNNING"    # 执行中
    COMPLETED = "COMPLETED"  # 执行完成
    FAILED = "FAILED"      # 执行失败


class Operator:
    """基础算子接口
    
    所有具体的算子都应该继承这个基类并实现process方法。
    算子应该是无状态的，所有状态应该通过输入参数传入和通过返回值传出。
    """
    
    async def process(self, input_data: Any) -> Any:
        """处理输入数据并返回结果
        
        这是算子的核心方法，所有子类必须实现这个方法。
        该方法应该是幂等的，即对于相同的输入，多次调用应该产生相同的输出。
        
        Args:
            input_data: 输入数据，可以是任意类型
            
        Returns:
            Any: 处理后的结果，可以是任意类型
            
        Raises:
            NotImplementedError: 子类必须实现这个方法
        """
        raise NotImplementedError("Operator must implement process method")
    
    async def setup(self):
        """算子初始化方法
        
        在算子首次使用前调用，用于初始化资源。
        子类可以重写这个方法来进行必要的初始化工作。
        """
        pass
    
    async def cleanup(self):
        """算子清理方法
        
        在算子不再使用时调用，用于清理资源。
        子类可以重写这个方法来进行必要的清理工作。
        """
        pass
    
    def __str__(self) -> str:
        """返回算子的字符串表示"""
        return f"{self.__class__.__name__}"
    
    def __repr__(self) -> str:
        """返回算子的详细字符串表示"""
        return f"{self.__class__.__name__}()"


@dataclass
class OperatorNode:
    """DAG中的算子节点
    
    用于在DAG中表示一个算子节点，包含算子实例、名称、依赖关系和执行状态等信息。
    
    Attributes:
        operator: 算子实例
        name: 算子名称
        dependencies: 依赖的其他算子的名称集合
        status: 算子的执行状态
        result: 算子的执行结果
        error: 执行过程中的错误信息
    """
    operator: Operator
    name: str
    dependencies: Set[str]
    status: OperatorStatus = OperatorStatus.PENDING
    result: Optional[Any] = None
    error: Optional[Exception] = None
    
    def __str__(self) -> str:
        """返回节点的字符串表示"""
        return f"{self.name}({self.operator.__class__.__name__})[{self.status.value}]"
    
    def reset(self):
        """重置节点状态"""
        self.status = OperatorStatus.PENDING
        self.result = None
        self.error = None 