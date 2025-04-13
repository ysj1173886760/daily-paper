import pytest
from pathlib import Path
from typing import List

import pytest

from daily_paper.core.operators.state.pending import (
    StateManager,
    InsertPendingIDs,
    GetAllPendingIDs,
    MarkIDsAsFinished,
    IDState,
)


@pytest.fixture
def state_manager(tmp_path: Path) -> StateManager:
    """创建一个使用临时目录的StateManager实例"""
    return StateManager(str(tmp_path), "test")


@pytest.fixture
def sample_ids() -> List[str]:
    """返回示例ID列表"""
    return ["id1", "id2", "id3"]


def test_state_manager_init(tmp_path: Path):
    """测试StateManager的初始化"""
    manager = StateManager(str(tmp_path), "test")
    storage_dir = tmp_path / "pending_states"
    assert storage_dir.exists()
    assert storage_dir.is_dir()


def test_state_manager_store_and_get_pending(state_manager: StateManager, sample_ids: List[str]):
    """测试存储和获取待处理ID"""
    state_manager.store_pending_ids(sample_ids)
    pending_ids = state_manager.get_pending_ids()
    assert pending_ids == set(sample_ids)


def test_state_manager_mark_as_finished(state_manager: StateManager, sample_ids: List[str]):
    """测试将ID标记为已完成"""
    # 先存储为pending状态
    state_manager.store_pending_ids(sample_ids)
    
    # 标记部分ID为完成
    finished_ids = sample_ids[:2]
    state_manager.mark_as_finished(finished_ids)
    
    # 验证状态
    pending_ids = state_manager.get_pending_ids()
    assert pending_ids == {sample_ids[2]}
    
    # 验证完成的ID不会被重新标记为pending
    state_manager.store_pending_ids(finished_ids)
    pending_ids = state_manager.get_pending_ids()
    assert pending_ids == {sample_ids[2]}


@pytest.mark.asyncio
async def test_insert_pending_ids_operator(tmp_path: Path, sample_ids: List[str]):
    """测试InsertPendingIDs算子"""
    operator = InsertPendingIDs(str(tmp_path), "test")
    result = await operator.process(sample_ids)
    
    # 验证返回值
    assert result == sample_ids
    
    # 验证状态
    pending_ids = operator.state_manager.get_pending_ids()
    assert pending_ids == set(sample_ids)


@pytest.mark.asyncio
async def test_get_all_pending_ids_operator(tmp_path: Path, sample_ids: List[str]):
    """测试GetAllPendingIDs算子"""
    # 先插入一些pending ID
    insert_op = InsertPendingIDs(str(tmp_path), "test")
    await insert_op.process(sample_ids)
    
    # 测试获取
    get_op = GetAllPendingIDs(str(tmp_path), "test")
    result = await get_op.process(None)
    assert set(result) == set(sample_ids)


@pytest.mark.asyncio
async def test_mark_ids_as_finished_operator(tmp_path: Path, sample_ids: List[str]):
    """测试MarkIDsAsFinished算子"""
    # 先插入pending ID
    insert_op = InsertPendingIDs(str(tmp_path), "test")
    await insert_op.process(sample_ids)
    
    # 标记部分ID为完成
    finished_ids = sample_ids[:2]
    mark_op = MarkIDsAsFinished(str(tmp_path), "test")
    result = await mark_op.process(finished_ids)
    
    # 验证返回值
    assert result == finished_ids
    
    # 验证状态
    get_op = GetAllPendingIDs(str(tmp_path), "test")
    pending_ids = await get_op.process(None)
    assert set(pending_ids) == {sample_ids[2]}


@pytest.mark.asyncio
async def test_operator_workflow(tmp_path: Path, sample_ids: List[str]):
    """测试完整的操作流程"""
    # 创建operators
    insert_op = InsertPendingIDs(str(tmp_path), "test")
    get_op = GetAllPendingIDs(str(tmp_path), "test")
    mark_op = MarkIDsAsFinished(str(tmp_path), "test")
    
    # 1. 插入pending IDs
    await insert_op.process(sample_ids)
    
    # 2. 获取所有pending IDs
    pending_ids = await get_op.process(None)
    assert set(pending_ids) == set(sample_ids)
    
    # 3. 标记部分ID为完成
    finished_ids = sample_ids[:2]
    await mark_op.process(finished_ids)
    
    # 4. 再次获取pending IDs，验证状态
    remaining_pending = await get_op.process(None)
    assert set(remaining_pending) == {sample_ids[2]}
    
    # 5. 尝试重新插入已完成的ID
    await insert_op.process(finished_ids)
    final_pending = await get_op.process(None)
    assert set(final_pending) == {sample_ids[2]}  # 已完成的ID不应该变回pending状态
