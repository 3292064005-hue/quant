"""配置加载器。"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from a_share_quant.config.models import AppConfig


class ConfigLoaderError(RuntimeError):
    """配置加载失败时抛出。"""


class ConfigLoader:
    """YAML 配置加载器。"""

    @staticmethod
    def load(path: str | Path) -> AppConfig:
        """从 YAML 文件读取应用配置。

        Args:
            path: 主配置文件路径。当前约定主入口为 `configs/app.yaml`。

        Returns:
            `AppConfig` 实例。

        Raises:
            ConfigLoaderError: 当路径不存在、无法解析或内容不是映射结构时抛出。
        """
        config_path = Path(path)
        payload = ConfigLoader._load_yaml_mapping(config_path)
        if config_path.name == "app.yaml":
            payload = ConfigLoader._merge_companion_files(config_path, payload)
        return AppConfig.model_validate(payload)

    @staticmethod
    def _merge_companion_files(config_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
        config_dir = config_path.parent
        merged = dict(payload)
        companion_map = {
            "data": config_dir / "data.yaml",
            "risk": config_dir / "risk.yaml",
            "backtest": config_dir / "backtest.yaml",
        }
        for section, file_path in companion_map.items():
            if file_path.exists():
                merged[section] = ConfigLoader._deep_merge_dicts(merged.get(section, {}), ConfigLoader._load_yaml_mapping(file_path))
        broker_provider = str(merged.get("broker", {}).get("provider", "mock")).lower()
        broker_file = config_dir / "broker" / f"{broker_provider}.yaml"
        if broker_provider != "mock" and broker_file.exists():
            merged["broker"] = ConfigLoader._deep_merge_dicts(merged.get("broker", {}), ConfigLoader._load_yaml_mapping(broker_file))
        return merged

    @staticmethod
    def _load_yaml_mapping(path: Path) -> dict[str, Any]:
        if not path.exists():
            raise ConfigLoaderError(f"配置文件不存在: {path}")
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload: Any = yaml.safe_load(handle) or {}
        except yaml.YAMLError as exc:
            raise ConfigLoaderError(f"配置解析失败: {path}") from exc
        if not isinstance(payload, dict):
            raise ConfigLoaderError(f"配置根节点必须是映射结构: {path}")
        return payload

    @staticmethod
    def _deep_merge_dicts(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base)
        for key, value in incoming.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = ConfigLoader._deep_merge_dicts(merged[key], value)
            else:
                merged[key] = value
        return merged
