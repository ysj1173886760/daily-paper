from typing import Any, Callable, List
from dataclasses import dataclass, field
from daily_paper.core.operators.base import Operator

@dataclass
class CustomProcessor(Operator):
    """自定义处理器，接受一个 lambda 函数来处理列表数据。
    
    继承自基础 Operator 类，实现异步处理方法。
    
    Attributes:
        processor_func: 用户定义的处理函数，接受 List[Any] 作为输入，返回 List[Any]
    """
    processor_func: Callable[[List[Any]], List[Any]] = field()
    
    async def process(self, input_data: Any) -> Any:
        """异步处理输入的列表数据。
        
        Args:
            input_data: 需要处理的输入列表
            
        Returns:
            处理后的列表
            
        Raises:
            ValueError: 当输入不是列表类型时抛出
            RuntimeError: 当处理过程发生错误时抛出
        """
        if not isinstance(input_data, list):
            raise ValueError("输入数据必须是列表类型")
            
        try:
            return self.processor_func(input_data)
        except Exception as e:
            raise RuntimeError(f"处理数据时发生错误: {str(e)}")
    
    def __repr__(self) -> str:
        """返回处理器的详细字符串表示"""
        return f"{self.__class__.__name__}(processor_func={self.processor_func})"
