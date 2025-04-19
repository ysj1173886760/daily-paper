import pytest
from pathlib import Path
import yaml
import tempfile
import os

from daily_paper.core.config import Config, LLMConfig, StorageConfig


def test_llm_config():
    """测试LLMConfig的默认值和自定义值"""
    # 测试默认值
    default_config = LLMConfig()
    assert default_config.model_name == "gpt-3.5-turbo"
    assert default_config.api_key == ""
    assert default_config.base_url == ""
    assert default_config.temperature == 0.7
    assert default_config.max_tokens == 2000

    # 测试自定义值
    custom_config = LLMConfig(
        model_name="gpt-4",
        api_key="test-key",
        base_url="https://api.test.com",
        temperature=0.5,
        max_tokens=1000,
    )
    assert custom_config.model_name == "gpt-4"
    assert custom_config.api_key == "test-key"
    assert custom_config.base_url == "https://api.test.com"
    assert custom_config.temperature == 0.5
    assert custom_config.max_tokens == 1000


def test_config_file_loading():
    """测试配置文件加载功能"""
    # 创建临时配置文件
    config_data = {
        "llm": {"model_name": "gpt-4", "api_key": "test-key", "temperature": 0.5},
        "storage": {"base_path": "/tmp/test"},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(config_data, f)
        config_path = f.name

    try:
        # 测试从文件加载配置
        config = Config.from_yaml(config_path)

        # 验证LLM配置
        assert config.llm.model_name == "gpt-4"
        assert config.llm.api_key == "test-key"
        assert config.llm.temperature == 0.5
        assert config.llm.base_url == ""  # 默认值

        # 验证Storage配置
        assert config.storage.base_path == "/tmp/test"

    finally:
        # 清理临时文件
        os.unlink(config_path)


def test_config_file_not_found():
    """测试配置文件不存在的情况"""
    with pytest.raises(FileNotFoundError):
        config = Config.from_yaml("non_existent_config.yaml")


def test_empty_config():
    """测试空配置的情况"""
    config = Config()
    assert isinstance(config.llm, LLMConfig)
    assert isinstance(config.storage, StorageConfig)
    assert config.llm.model_name == "gpt-3.5-turbo"  # 默认值
