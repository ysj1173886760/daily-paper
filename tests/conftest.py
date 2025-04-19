import pytest
import asyncio


@pytest.fixture(scope="session")
def event_loop():
    """创建一个会话级别的事件循环"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
