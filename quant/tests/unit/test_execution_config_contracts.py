from __future__ import annotations

from typing import Any

import pytest

from a_share_quant.app.bootstrap import bootstrap
from a_share_quant.config.models import BacktestExecutionSection


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('fill_model', 'unknown'),
        ('slippage_model', 'unknown'),
        ('fee_model', 'unknown'),
        ('tax_model', 'unknown'),
    ],
)
def test_execution_model_selectors_reject_unsupported_values(field: str, value: str) -> None:
    kwargs: dict[str, Any] = {field: value}
    with pytest.raises(ValueError):
        BacktestExecutionSection(**kwargs)


def test_bootstrap_execution_engine_uses_declared_builtin_models(tmp_path) -> None:
    config_path = tmp_path / 'app.yaml'
    config_path.write_text(
        '\n'.join(
            [
                'app:',
                '  runtime_mode: research_backtest',
                'data:',
                '  provider: csv',
                f'  reports_dir: {tmp_path.as_posix()}/reports',
                'database:',
                f'  path: {tmp_path.as_posix()}/db.sqlite3',
                'broker:',
                '  provider: mock',
                'backtest:',
                '  slippage_bps: 5.0',
                '  fee_bps: 3.0',
                '  tax_bps: 10.0',
                '  execution:',
                '    fill_model: volume_share',
                '    slippage_model: bps',
                '    fee_model: broker_bps',
                '    tax_model: a_share_sell_tax',
            ]
        ),
        encoding='utf-8',
    )
    with bootstrap(str(config_path)) as context:
        engine = context.require_backtest_service().engine.execution_engine
        assert engine.fill_model.__class__.__name__ == 'VolumeShareFillModel'
        assert engine.slippage_model.__class__.__name__ == 'BpsSlippageModel'
        assert engine.fee_model.__class__.__name__ == 'BpsFeeModel'
        assert engine.tax_model.__class__.__name__ == 'AShareSellTaxModel'


def test_execution_event_mode_rejects_unimplemented_sync() -> None:
    with pytest.raises(ValueError):
        BacktestExecutionSection(event_mode='sync')
