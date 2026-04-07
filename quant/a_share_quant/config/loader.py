"""配置加载器。"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from a_share_quant.config.models import AppConfig
from a_share_quant.core.path_utils import normalize_runtime_paths


class ConfigLoaderError(RuntimeError):
    """配置加载失败时抛出。"""


class ConfigLoader:
    """YAML 配置加载器。

    合并规则：
        默认值 < companion 文件 < app.yaml 主配置。

    这样可以保证：
        1. `data.yaml` / `risk.yaml` / `backtest.yaml` / `broker/<provider>.yaml` 能提供按主题拆分的基线配置。
        2. `app.yaml` 作为总入口时，用户在主配置中的覆盖值一定生效，不会被 companion 文件反向覆盖。
    """

    @staticmethod
    def load(path: str | Path) -> AppConfig:
        """从 YAML 文件读取应用配置。

        Args:
            path: 主配置文件路径。当前约定主入口为 ``configs/app.yaml``。

        Returns:
            ``AppConfig`` 实例。所有运行时路径字段会在此阶段被解析为绝对路径。

        Raises:
            ConfigLoaderError: 当路径不存在、无法解析、内容不是映射结构或路径模式非法时抛出。
        """
        config_path = Path(path).resolve()
        payload = ConfigLoader._load_yaml_mapping(config_path)
        if config_path.name == "app.yaml":
            payload = ConfigLoader._merge_companion_files(config_path, payload)
        try:
            payload = normalize_runtime_paths(payload, config_path=config_path)
        except ValueError as exc:
            raise ConfigLoaderError(str(exc)) from exc
        return AppConfig.model_validate(payload)

    @staticmethod
    def _merge_companion_files(config_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
        """合并 app.yaml 与分主题 companion 文件。

        Boundary Behavior:
            - 若 companion 文件不存在，则直接忽略。
            - 仅当 broker.provider 为非 mock 且对应 broker 配置文件存在时，才会加载该文件。
            - app.yaml 中显式声明的字段优先级最高。
        """
        config_dir = config_path.parent
        base_payload: dict[str, Any] = {}
        companion_map = {
            "data": config_dir / "data.yaml",
            "risk": config_dir / "risk.yaml",
            "backtest": config_dir / "backtest.yaml",
        }
        for section, file_path in companion_map.items():
            if file_path.exists():
                base_payload[section] = ConfigLoader._load_yaml_mapping(file_path)

        broker_provider = str(payload.get("broker", {}).get("provider", "mock")).lower()
        broker_file = config_dir / "broker" / f"{broker_provider}.yaml"
        if broker_provider != "mock" and broker_file.exists():
            base_payload["broker"] = ConfigLoader._load_yaml_mapping(broker_file)

        return ConfigLoader._deep_merge_dicts(base_payload, payload)

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
