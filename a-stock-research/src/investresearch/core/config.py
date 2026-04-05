"""配置加载器 - YAML配置 + 环境变量覆盖"""

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from .exceptions import ConfigurationError


class Config:
    """系统配置管理器（单例）

    首次实例化时加载所有YAML文件并应用环境变量覆盖。
    后续实例化返回同一对象。
    """

    _instance: "Config | None" = None
    _loaded: bool = False

    def __new__(cls) -> "Config":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not self._loaded:
            self._config: dict[str, Any] = {}
            self._load()

    @classmethod
    def _load(cls) -> None:
        """加载配置"""
        instance = cls._instance
        if instance is None:
            instance = cls()

        # 加载.env文件
        env_path = Path(__file__).parent.parent.parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=False)

        # 确定配置目录
        config_dir = Path(__file__).parent.parent.parent.parent / "config"
        if not config_dir.exists():
            # 开发时可能直接在src目录运行
            raise ConfigurationError(
                f"配置目录不存在: {config_dir}",
                config_path=str(config_dir),
            )

        # 加载YAML配置文件
        config_files = ["settings.yaml", "agents.yaml", "data_sources.yaml"]
        for filename in config_files:
            filepath = config_dir / filename
            if filepath.exists():
                with open(filepath, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                    key = filename.replace(".yaml", "")
                    if key == "settings":
                        instance._config.update(data)
                    else:
                        instance._config[key] = data

        # 环境变量覆盖
        instance._apply_env_overrides()

        cls._loaded = True

    def _apply_env_overrides(self) -> None:
        """用环境变量覆盖敏感配置"""
        llm_config = self._config.get("llm", {})
        providers = llm_config.get("providers", {})

        for _provider_name, provider_config in providers.items():
            api_key_env = provider_config.get("api_key_env", "")
            if api_key_env and api_key_env in os.environ:
                provider_config["api_key"] = os.environ[api_key_env]

            base_url_env = provider_config.get("base_url_env", "")
            if base_url_env and base_url_env in os.environ:
                provider_config["base_url"] = os.environ[base_url_env]
            elif "default_base_url" in provider_config:
                provider_config.setdefault("base_url", provider_config["default_base_url"])

    def get(self, key_path: str, default: Any = None) -> Any:
        """获取配置值，支持点号路径

        Example:
            config.get("system.name")  -> "A股投研多Agent系统"
            config.get("llm.providers.bailian.models.qwen3-plus")
        """
        keys = key_path.split(".")
        value: Any = self._config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        return value

    def get_llm_config(self, provider: str) -> dict[str, Any]:
        """获取LLM提供商配置"""
        providers = self._config.get("llm", {}).get("providers", {})
        if provider not in providers:
            raise ConfigurationError(f"未找到LLM提供商配置: {provider}")
        return providers[provider]

    def get_layer_model(self, layer: str, task: str | None = None) -> str:
        """获取指定层使用的模型标识

        Args:
            layer: "data_layer" | "analysis_layer" | "decision_layer"
            task: 可选任务名覆盖，如 "financial", "valuation"
        """
        layer_config = self._config.get("llm", {}).get("layer_models", {}).get(layer, {})
        if isinstance(layer_config, str):
            return layer_config
        if task and task in layer_config:
            return layer_config[task]
        return layer_config.get("default", layer_config.get("fallback", "qwen3-coder"))

    def get_agent_config(self, layer: str, agent_name: str) -> dict[str, Any]:
        """获取Agent特定配置"""
        return (
            self._config.get("agents", {})
            .get(layer, {})
            .get(agent_name, {})
        )

    def get_chroma_config(self) -> dict[str, Any]:
        """获取ChromaDB知识库配置"""
        return self._config.get("storage", {}).get("chroma", {})

    def get_knowledge_base_config(self) -> dict[str, Any]:
        """获取知识库配置"""
        return self._config.get("knowledge_base", {})

    def get_watch_list_path(self) -> str:
        """获取跟踪列表文件路径"""
        kb = self.get_knowledge_base_config()
        return kb.get("watch_list_path", "./data/watch_list.json")

    def get_alert_thresholds(self) -> dict[str, Any]:
        """获取预警阈值配置"""
        kb = self.get_knowledge_base_config()
        return kb.get("alert_thresholds", {})

    @property
    def all(self) -> dict[str, Any]:
        """返回完整配置字典"""
        return self._config

    @classmethod
    def reset(cls) -> None:
        """重置单例（仅用于测试）"""
        cls._instance = None
        cls._loaded = False


# 全局配置实例
config = Config()
