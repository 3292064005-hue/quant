"""配置加载器。"""
from __future__ import annotations

from importlib import resources
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
        默认值 < companion 文件 < 基础配置链 < 当前主配置。

    其中基础配置链由 ``extends`` 明确指定，可用于把 ``paper/live`` 变体配置建立在
    ``configs/app.yaml`` 之上，避免维护整份重复 YAML。
    """

    @staticmethod
    def load(path: str | Path) -> AppConfig:
        """从 YAML 文件读取应用配置。

        Args:
            path: 主配置文件路径。默认约定主入口为 ``configs/app.yaml``。

        Returns:
            ``AppConfig`` 实例。所有运行时路径字段会在此阶段被解析为绝对路径。

        Raises:
            ConfigLoaderError: 当路径不存在、无法解析、内容不是映射结构、extends 非法或路径模式非法时抛出。
        """
        config_path = ConfigLoader._resolve_config_path(path)
        payload = ConfigLoader._load_with_extends(config_path, stack=())
        try:
            payload = normalize_runtime_paths(payload, config_path=config_path)
        except ValueError as exc:
            raise ConfigLoaderError(str(exc)) from exc
        return AppConfig.model_validate(payload)


    @staticmethod
    def _resolve_config_path(path: str | Path) -> Path:
        """解析配置路径；当外部路径缺失时回退到 wheel 内置配置目录。

        Boundary Behavior:
            - 绝对路径只接受真实存在的文件，不做隐式映射。
            - 相对路径若在当前工作目录不存在，源码态优先回退到仓库 ``configs/`` 单一真相源。
            - 若仓库配置不可用，则回退到安装态 ``a_share_quant.resources/configs``。
            - 支持 ``configs/*.yaml`` 与同目录 companion/broker 配置的安装态加载。
        """
        candidate = Path(path)
        if candidate.exists():
            return candidate.resolve()
        if candidate.is_absolute():
            raise ConfigLoaderError(f"配置文件不存在: {candidate}")

        candidate_parts = list(candidate.parts)
        if candidate_parts[:1] == ["configs"]:
            candidate_parts = candidate_parts[1:]

        repo_config_root = Path(__file__).resolve().parents[2] / "configs"
        repo_candidate = repo_config_root.joinpath(*candidate_parts) if candidate_parts else repo_config_root
        if repo_candidate.exists():
            return repo_candidate.resolve()

        resource_root = Path(str(resources.files("a_share_quant.resources").joinpath("configs")))
        bundled = resource_root.joinpath(*candidate_parts) if candidate_parts else resource_root
        if bundled.exists():
            return bundled.resolve()
        raise ConfigLoaderError(f"配置文件不存在: {candidate.resolve()}")

    @staticmethod
    def _load_with_extends(path: Path, *, stack: tuple[Path, ...]) -> dict[str, Any]:
        """递归解析 ``extends`` 并应用 companion 文件。"""
        if path in stack:
            chain = " -> ".join(item.name for item in (*stack, path))
            raise ConfigLoaderError(f"检测到循环 extends 配置链: {chain}")
        payload = ConfigLoader._load_yaml_mapping(path)
        extends_value = payload.pop("extends", None)
        merged_base: dict[str, Any] = {}
        if extends_value is not None:
            if isinstance(extends_value, (str, Path)):
                extend_refs = [extends_value]
            elif isinstance(extends_value, list) and all(isinstance(item, (str, Path)) for item in extends_value):
                extend_refs = list(extends_value)
            else:
                raise ConfigLoaderError(f"extends 必须是字符串或字符串列表: {path}")
            for ref in extend_refs:
                ref_path = Path(ref)
                if not ref_path.is_absolute():
                    ref_path = (path.parent / ref_path).resolve()
                merged_base = ConfigLoader._deep_merge_dicts(
                    merged_base,
                    ConfigLoader._load_with_extends(ref_path, stack=(*stack, path)),
                )
        if path.name == "app.yaml":
            payload = ConfigLoader._merge_companion_files(path, payload)
        return ConfigLoader._deep_merge_dicts(merged_base, payload)

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
