#!/usr/bin/env python3
"""Operator 交易会话恢复入口。"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from a_share_quant.cli import main_operator_reconcile_session as main

if __name__ == "__main__":
    raise SystemExit(main())
