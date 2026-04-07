"""QMT 适配器。"""
from __future__ import annotations

from a_share_quant.adapters.broker.contract_adapter import MappedBrokerAdapter


class QMTAdapter(MappedBrokerAdapter):
    """对接 QMT Python 客户端的边界适配器。"""

    provider_name = "qmt"
