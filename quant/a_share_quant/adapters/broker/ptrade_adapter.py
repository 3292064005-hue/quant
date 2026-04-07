"""PTrade 适配器。"""
from __future__ import annotations

from a_share_quant.adapters.broker.contract_adapter import MappedBrokerAdapter


class PTradeAdapter(MappedBrokerAdapter):
    """对接 PTrade Python 客户端的边界适配器。"""

    provider_name = "ptrade"
