from __future__ import annotations

from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap_storage_context
from a_share_quant.config.loader import ConfigLoader
from a_share_quant.services.strategy_service import StrategyService


def test_strategy_service_persists_registry_metadata(temp_config_dir: Path) -> None:
    config = ConfigLoader.load(str(temp_config_dir / "app.yaml"))
    with bootstrap_storage_context(str(temp_config_dir / "app.yaml")) as context:
        service = StrategyService(config, context.strategy_repository)
        strategy = service.build_default()
        saved = service.get_saved_strategy_definition(strategy.strategy_id)
        assert saved is not None
        assert saved["strategy_type"] == type(strategy).__name__
        assert saved["class_path"] == "builtin.top_n_momentum"
        assert saved["version"] == config.strategy.version
        assert saved["params"]["lookback"] == config.strategy.lookback
        assert saved["component_manifest"]["factor_component"] == "builtin.momentum"
        assert "momentum" in saved["capability_tags"]
        assert saved["strategy_blueprint"]["factor"] == "builtin.momentum"
        enabled = service.list_enabled_strategy_definitions()
        assert any(item["strategy_id"] == strategy.strategy_id for item in enabled)


def test_strategy_service_supports_generic_strategy_params(temp_config_dir: Path, tmp_path: Path, monkeypatch) -> None:
    module_path = tmp_path / "ext_strategy_module.py"
    module_path.write_text(
        """
class CustomStrategy:
    def __init__(self, strategy_id, foo):
        self.strategy_id = strategy_id
        self.foo = foo

    def required_history_bars(self):
        return 1

    def should_rebalance(self, eligible_trade_index):
        return True

    def generate_targets(self, history_by_symbol, current_date, securities):
        return []
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    app_path = temp_config_dir / "app.yaml"
    payload = ConfigLoader._load_yaml_mapping(app_path)
    payload.setdefault("strategy", {})["class_path"] = "ext_strategy_module:CustomStrategy"
    payload["strategy"]["params"] = {"foo": "bar-value"}
    app_path.write_text(__import__("yaml").safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")

    config = ConfigLoader.load(str(app_path))
    with bootstrap_storage_context(str(app_path)) as context:
        service = StrategyService(config, context.strategy_repository)
        strategy = service.build_default()
        assert strategy.foo == "bar-value"
        saved = service.get_saved_strategy_definition(config.strategy.strategy_id)
        assert saved is not None
        assert saved["params"]["foo"] == "bar-value"
        assert saved["params"]["strategy_id"] == config.strategy.strategy_id
        assert saved["component_manifest"]["signal_component"] == "builtin.direct_targets"
        assert "external_strategy" in saved["capability_tags"]
        assert strategy._component_manifest["portfolio_construction_component"] == "builtin.portfolio_engine"
        assert saved["strategy_blueprint"]["signal"] == "builtin.direct_targets"
