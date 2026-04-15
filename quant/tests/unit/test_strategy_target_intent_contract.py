from __future__ import annotations

from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap_storage_context
from a_share_quant.config.loader import ConfigLoader
from a_share_quant.domain.models import TargetIntent
from a_share_quant.services.strategy_service import StrategyService


class _RecordingPluginManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def emit_target_intents_generated(self, context, strategy_id: str, intents: list[TargetIntent], payload: dict[str, object]) -> None:
        self.calls.append((strategy_id, dict(payload)))


def test_strategy_blueprint_declares_target_intent_contract(temp_config_dir: Path) -> None:
    config = ConfigLoader.load(str(temp_config_dir / "app.yaml"))
    with bootstrap_storage_context(str(temp_config_dir / "app.yaml")) as context:
        plugin_manager = _RecordingPluginManager()
        service = StrategyService(
            config,
            context.strategy_repository,
            research_run_repository=context.research_run_repository,
            plugin_manager=plugin_manager,
            plugin_context=context,
        )
        strategy = service.build_default()
        saved = service.get_saved_strategy_definition(strategy.strategy_id)
        assert saved is not None
        assert saved["strategy_blueprint"]["target_intent_contract"] == "target_intent.v1"
        runtime = strategy._execution_runtime
        assert runtime is not None
        assert hasattr(runtime, "generate_target_intents")
