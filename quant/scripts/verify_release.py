#!/usr/bin/env python3
"""统一的发布前质量门禁脚本。"""
from __future__ import annotations

import ast
import configparser
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import tokenize
import venv
import zipfile
from pathlib import Path
from typing import Iterable, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_STATIC_ROOTS = ("a_share_quant", "scripts", "tests")
_EXPECTED_CONSOLE_SCRIPTS = {
    "a-share-quant",
    "a-share-quant-check-runtime",
    "a-share-quant-init-db",
    "a-share-quant-sync-market-data",
    "a-share-quant-daily-run",
    "a-share-quant-generate-report",
    "a-share-quant-launch-ui",
    "a-share-quant-research",
    "a-share-quant-operator-snapshot",
    "a-share-quant-operator-submit-order",
    "a-share-quant-operator-reconcile-session",
    "a-share-quant-operator-sync-session",
    "a-share-quant-operator-run-supervisor",
}
sys.dont_write_bytecode = True


def _pythonpath_with(*paths: Path) -> str:
    existing = os.environ.get("PYTHONPATH", "")
    parts = [str(path) for path in paths if path]
    if existing:
        parts.append(existing)
    return os.pathsep.join(parts)



def _run(
    command: list[str],
    *,
    cwd: Path | str | None = None,
    suppress_output: bool = False,
    env_extra: dict[str, str] | None = None,
) -> None:
    stdout = subprocess.DEVNULL if suppress_output else None
    stderr = subprocess.STDOUT if suppress_output else None
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    if env_extra:
        env.update(env_extra)
    completed = subprocess.run(command, cwd=cwd or PROJECT_ROOT, stdout=stdout, stderr=stderr, env=env)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)



def _run_capture(
    command: list[str],
    *,
    cwd: Path | str | None = None,
    env_extra: dict[str, str] | None = None,
) -> str:
    """执行命令并返回标准输出文本。"""
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    if env_extra:
        env.update(env_extra)
    completed = subprocess.run(command, cwd=cwd or PROJECT_ROOT, capture_output=True, text=True, env=env)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)
    return completed.stdout



def _read_installed_console_scripts(install_root: Path) -> set[str]:
    """读取安装态 wheel 的 console_scripts 声明。"""
    candidates = sorted(install_root.glob("**/*.dist-info/entry_points.txt"))
    if not candidates:
        raise SystemExit("安装态 wheel 缺少 entry_points.txt")
    parser = configparser.ConfigParser()
    parser.read(candidates[-1], encoding="utf-8")
    if not parser.has_section("console_scripts"):
        return set()
    return set(parser["console_scripts"].keys())



def _resolve_venv_python(venv_dir: Path) -> Path:
    scripts_dir = "Scripts" if os.name == "nt" else "bin"
    executable = "python.exe" if os.name == "nt" else "python"
    return venv_dir / scripts_dir / executable



def _resolve_console_script_path(venv_dir: Path, script_name: str) -> Path:
    scripts_dir = venv_dir / ("Scripts" if os.name == "nt" else "bin")
    suffix = ".exe" if os.name == "nt" else ""
    candidate = scripts_dir / f"{script_name}{suffix}"
    if not candidate.exists():
        raise SystemExit(f"安装态 launcher 缺失: {script_name}")
    return candidate



def _write_runtime_bound_config(template_path: Path, output_path: Path) -> Path:
    """把模板配置绑定到临时 runtime 目录，避免发布门禁污染源码树。"""
    from a_share_quant.config.loader import ConfigLoader
    import yaml

    config = ConfigLoader.load(str(template_path))
    payload = config.model_dump(mode="json")
    runtime_root = output_path.parent / "runtime"
    runtime_root.mkdir(parents=True, exist_ok=True)
    payload.setdefault("app", {})["logs_dir"] = str(runtime_root / "logs")
    payload.setdefault("data", {})["storage_dir"] = str(runtime_root / "data")
    payload.setdefault("data", {})["reports_dir"] = str(runtime_root / "reports")
    payload.setdefault("database", {})["path"] = str(runtime_root / "a_share_quant.db")
    payload.setdefault("operator", {})["supervisor_scan_interval_seconds"] = 0.05
    payload.setdefault("operator", {})["supervisor_idle_timeout_seconds"] = 0.2
    output_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return output_path



def _parse_literal_payload(text: str) -> dict[str, object]:
    """解析 CLI 打印的 Python dict 文本。"""
    payload = ast.literal_eval(text.strip())
    if not isinstance(payload, dict):
        raise SystemExit(f"命令输出不是 dict: {text!r}")
    return payload



def _run_operator_acceptance_smoke(*, command_prefix: list[str] | None = None, cwd: Path | str | None = None) -> None:
    """执行仓内自带 operator acceptance profile 的发布前烟雾链。"""
    command_prefix = command_prefix or [sys.executable]
    with tempfile.TemporaryDirectory(prefix="verify_operator_") as temp_dir:
        temp_root = Path(temp_dir)
        config_path = _write_runtime_bound_config(PROJECT_ROOT / "configs/operator_paper_trade_demo.yaml", temp_root / "operator_demo.yaml")
        sample_csv = PROJECT_ROOT / "sample_data/daily_bars.csv"
        _run([*command_prefix, "scripts/init_db.py", "--config", str(config_path)] if command_prefix == [sys.executable] else [*command_prefix, "--config", str(config_path)], cwd=cwd, suppress_output=True)
        if command_prefix == [sys.executable]:
            _run([*command_prefix, "scripts/sync_market_data.py", "--config", str(config_path), "--csv", str(sample_csv), "--provider", "csv"], cwd=cwd, suppress_output=True)
            _run([*command_prefix, "scripts/operator_snapshot.py", "--config", str(config_path)], cwd=cwd, suppress_output=True)
            submit_output = _run_capture([
                *command_prefix,
                "scripts/operator_submit_order.py",
                "--config",
                str(config_path),
                "--symbol",
                "600000.SH",
                "--side",
                "BUY",
                "--price",
                "10.50",
                "--quantity",
                "100",
                "--trade-date",
                "2026-01-05",
            ], cwd=cwd)
        else:
            _run([*command_prefix, "--config", str(config_path)], cwd=cwd, suppress_output=True)
            sync_cmd = _resolve_console_script_path(Path(command_prefix[0]).parents[1], "a-share-quant-sync-market-data")
            snapshot_cmd = _resolve_console_script_path(Path(command_prefix[0]).parents[1], "a-share-quant-operator-snapshot")
            submit_cmd = _resolve_console_script_path(Path(command_prefix[0]).parents[1], "a-share-quant-operator-submit-order")
            sync_session_cmd = _resolve_console_script_path(Path(command_prefix[0]).parents[1], "a-share-quant-operator-sync-session")
            supervisor_cmd = _resolve_console_script_path(Path(command_prefix[0]).parents[1], "a-share-quant-operator-run-supervisor")
            _run([str(sync_cmd), "--config", str(config_path), "--provider", "csv", "--csv", str(sample_csv)], cwd=cwd, suppress_output=True)
            _run([str(snapshot_cmd), "--config", str(config_path)], cwd=cwd, suppress_output=True)
            submit_output = _run_capture([
                str(submit_cmd),
                "--config",
                str(config_path),
                "--symbol",
                "600000.SH",
                "--side",
                "BUY",
                "--price",
                "10.50",
                "--quantity",
                "100",
                "--trade-date",
                "2026-01-05",
            ], cwd=cwd)
        submit_payload = json.loads(submit_output)
        session_id = submit_payload["session"]["session_id"]
        if submit_payload["session"]["status"] != "RECOVERY_REQUIRED":
            raise SystemExit("operator acceptance smoke: submit_order 未进入 RECOVERY_REQUIRED")

        if command_prefix == [sys.executable]:
            sync_output = _run_capture([
                *command_prefix,
                "scripts/operator_sync_session.py",
                "--config",
                str(config_path),
                "--session-id",
                session_id,
            ], cwd=cwd)
        else:
            sync_session_cmd = _resolve_console_script_path(Path(command_prefix[0]).parents[1], "a-share-quant-operator-sync-session")
            sync_output = _run_capture([
                str(sync_session_cmd),
                "--config",
                str(config_path),
                "--session-id",
                session_id,
            ], cwd=cwd)
        sync_payload = json.loads(sync_output)
        if sync_payload["session"]["status"] != "COMPLETED":
            raise SystemExit("operator acceptance smoke: sync_session 未完成会话")

        if command_prefix == [sys.executable]:
            submit_output_2 = _run_capture([
                *command_prefix,
                "scripts/operator_submit_order.py",
                "--config",
                str(config_path),
                "--symbol",
                "600000.SH",
                "--side",
                "BUY",
                "--price",
                "10.50",
                "--quantity",
                "100",
                "--trade-date",
                "2026-01-05",
            ], cwd=cwd)
            submit_payload_2 = json.loads(submit_output_2)
            session_id_2 = submit_payload_2["session"]["session_id"]
            supervisor_output = _run_capture([
                *command_prefix,
                "scripts/operator_run_supervisor.py",
                "--config",
                str(config_path),
                "--session-id",
                session_id_2,
                "--max-loops",
                "1",
                "--stop-when-idle",
            ], cwd=cwd)
        else:
            submit_cmd = _resolve_console_script_path(Path(command_prefix[0]).parents[1], "a-share-quant-operator-submit-order")
            supervisor_cmd = _resolve_console_script_path(Path(command_prefix[0]).parents[1], "a-share-quant-operator-run-supervisor")
            submit_output_2 = _run_capture([
                str(submit_cmd),
                "--config",
                str(config_path),
                "--symbol",
                "600000.SH",
                "--side",
                "BUY",
                "--price",
                "10.50",
                "--quantity",
                "100",
                "--trade-date",
                "2026-01-05",
            ], cwd=cwd)
            submit_payload_2 = json.loads(submit_output_2)
            session_id_2 = submit_payload_2["session"]["session_id"]
            supervisor_output = _run_capture([
                str(supervisor_cmd),
                "--config",
                str(config_path),
                "--session-id",
                session_id_2,
                "--max-loops",
                "1",
                "--stop-when-idle",
            ], cwd=cwd)
        supervisor_payload = json.loads(supervisor_output)
        if session_id_2 not in supervisor_payload.get("completed_session_ids", []):
            raise SystemExit("operator acceptance smoke: supervisor 未完成会话")



def _write_optional_surface_shims(shim_root: Path) -> None:
    """写入可选运行面的本地 shim 包，驱动真实代码路径。"""
    pyqt_dir = shim_root / "PySide6"
    pyqt_dir.mkdir(parents=True, exist_ok=True)
    (pyqt_dir / "__init__.py").write_text("from . import QtWidgets\n", encoding="utf-8")
    (pyqt_dir / "QtWidgets.py").write_text(
        """
class _WidgetBase:
    def __init__(self, *args, **kwargs):
        self.children = []
        self.args = args
        self.kwargs = kwargs
    def addWidget(self, widget, *args):
        self.children.append((widget, args))
    def addStretch(self, *_args):
        return None
    def setWordWrap(self, *_args):
        return None
    def setHorizontalHeaderLabels(self, labels):
        self.labels = list(labels)
    def setItem(self, *_args):
        return None
    def resizeColumnsToContents(self):
        return None
    def setEditTriggers(self, *_args):
        return None
    def addTab(self, widget, title):
        self.children.append((widget, title))
    def setWindowTitle(self, title):
        self.title = title
    def setCentralWidget(self, widget):
        self.central_widget = widget
    def resize(self, *_args):
        return None
    def show(self):
        return None

class QApplication:
    def __init__(self, *_args, **_kwargs):
        pass
    def exec(self):
        return 0

class QWidget(_WidgetBase):
    pass

class QMainWindow(_WidgetBase):
    pass

class QTabWidget(_WidgetBase):
    pass

class QLabel(_WidgetBase):
    pass

class QVBoxLayout(_WidgetBase):
    pass

class QGridLayout(_WidgetBase):
    pass

class QGroupBox(_WidgetBase):
    pass

class QTableWidget(_WidgetBase):
    class EditTrigger:
        NoEditTriggers = 0
    pass

class QTableWidgetItem:
    def __init__(self, text):
        self.text = text
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (shim_root / "tushare.py").write_text(
        """
import pandas as pd

class _Client:
    def stock_basic(self, exchange="", list_status="L", fields=""):
        return pd.DataFrame([
            {"ts_code": "600000.SH", "name": "浦发银行", "exchange": "SSE", "market": "主板", "list_status": list_status, "list_date": "19991110", "delist_date": None},
            {"ts_code": "000001.SZ", "name": "平安银行", "exchange": "SZSE", "market": "主板", "list_status": list_status, "list_date": "19910403", "delist_date": None},
        ])
    def stock_st(self, trade_date):
        return pd.DataFrame(columns=["ts_code"])
    def trade_cal(self, exchange, start_date, end_date):
        return pd.DataFrame([
            {"exchange": exchange, "cal_date": start_date, "is_open": 1, "pretrade_date": None},
            {"exchange": exchange, "cal_date": end_date, "is_open": 1, "pretrade_date": start_date},
        ])
    def daily(self, ts_code=None, start_date=None, end_date=None):
        codes = [code for code in (ts_code or "600000.SH,000001.SZ").split(",") if code]
        rows = []
        for code in codes:
            rows.append({"ts_code": code, "trade_date": start_date, "open": 10.0, "high": 10.8, "low": 9.8, "close": 10.5, "vol": 1000, "amount": 10000, "pre_close": 10.0})
            rows.append({"ts_code": code, "trade_date": end_date, "open": 10.5, "high": 10.9, "low": 10.1, "close": 10.7, "vol": 1200, "amount": 12000, "pre_close": 10.5})
        return pd.DataFrame(rows)
    def stk_limit(self, ts_code=None, start_date=None, end_date=None):
        codes = [code for code in (ts_code or "600000.SH,000001.SZ").split(",") if code]
        rows = []
        for code in codes:
            rows.append({"ts_code": code, "trade_date": start_date, "up_limit": 11.0, "down_limit": 9.0})
            rows.append({"ts_code": code, "trade_date": end_date, "up_limit": 11.5, "down_limit": 9.5})
        return pd.DataFrame(rows)

def pro_api(_token):
    return _Client()
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (shim_root / "akshare.py").write_text(
        """
import pandas as pd

def stock_info_a_code_name():
    return pd.DataFrame([
        {"code": "600000", "name": "浦发银行"},
        {"code": "000001", "name": "平安银行"},
    ])

def stock_zh_a_hist(symbol, period="daily", start_date=None, end_date=None, adjust=""):
    return pd.DataFrame([
        {"日期": start_date, "开盘": 10.0, "收盘": 10.5, "最高": 10.8, "最低": 9.8, "成交量": 1000, "成交额": 10000, "涨跌额": 0.5},
        {"日期": end_date, "开盘": 10.5, "收盘": 10.7, "最高": 10.9, "最低": 10.1, "成交量": 1200, "成交额": 12000, "涨跌额": 0.2},
    ])
""".strip()
        + "\n",
        encoding="utf-8",
    )



def _run_optional_surface_smokes() -> list[str]:
    """执行可选运行面的真实代码路径 smoke。"""
    with tempfile.TemporaryDirectory(prefix="verify_optional_") as temp_dir:
        temp_root = Path(temp_dir)
        shim_root = temp_root / "shims"
        shim_root.mkdir(parents=True, exist_ok=True)
        _write_optional_surface_shims(shim_root)
        env_extra = {
            "PYTHONPATH": _pythonpath_with(shim_root, PROJECT_ROOT),
            "TUSHARE_TOKEN": "demo-token",
        }
        config_path = _write_runtime_bound_config(PROJECT_ROOT / "configs/app.yaml", temp_root / "app.yaml")
        _run([sys.executable, "scripts/init_db.py", "--config", str(config_path)], suppress_output=True, env_extra=env_extra)
        _run([sys.executable, "scripts/launch_ui.py", "--config", str(config_path)], suppress_output=True, env_extra=env_extra)

        tushare_output = _run_capture(
            [
                sys.executable,
                "scripts/sync_market_data.py",
                "--config",
                str(config_path),
                "--provider",
                "tushare",
                "--start-date",
                "20260102",
                "--end-date",
                "20260105",
                "--symbols",
                "600000.SH,000001.SZ",
            ],
            env_extra=env_extra,
        )
        tushare_payload = _parse_literal_payload(tushare_output)
        if tushare_payload.get("provider") != "tushare" or int(tushare_payload.get("bar_count", 0) or 0) <= 0:
            raise SystemExit("optional smoke: tushare 真实代码路径未返回有效结果")

        akshare_output = _run_capture(
            [
                sys.executable,
                "scripts/sync_market_data.py",
                "--config",
                str(config_path),
                "--provider",
                "akshare",
                "--start-date",
                "20260102",
                "--end-date",
                "20260105",
                "--symbols",
                "600000.SH,000001.SZ",
            ],
            env_extra=env_extra,
        )
        akshare_payload = _parse_literal_payload(akshare_output)
        if akshare_payload.get("provider") != "akshare" or int(akshare_payload.get("bar_count", 0) or 0) <= 0:
            raise SystemExit("optional smoke: akshare 真实代码路径未返回有效结果")
    return []



def _optional_module_command(module_name: str, *args: str) -> list[str] | None:
    """返回可执行的 ``python -m`` 命令；若模块未安装则返回 ``None``。"""
    if importlib.util.find_spec(module_name) is None:
        return None
    return [sys.executable, "-m", module_name, *args]



def _iter_python_files(roots: Sequence[Path]) -> Iterable[Path]:
    """遍历静态门禁要检查的 Python 文件。"""
    for root in roots:
        if not root.exists():
            continue
        if root.is_file():
            if root.suffix == ".py":
                yield root
            continue
        for file_path in sorted(root.rglob("*.py")):
            yield file_path



def _check_python_syntax(file_path: Path) -> str | None:
    """以只读方式校验单个 Python 文件语法。"""
    try:
        with tokenize.open(file_path) as handle:
            source = handle.read()
        compile(source, str(file_path), "exec")
        return None
    except (SyntaxError, UnicodeDecodeError, ValueError) as exc:
        return str(exc)



def _run_builtin_static_gate(*, roots: Sequence[Path] | None = None) -> None:
    """执行仓内自给的基础静态门禁。"""
    selected_roots = list(roots) if roots is not None else [PROJECT_ROOT / item for item in _DEFAULT_STATIC_ROOTS]
    failures: list[str] = []
    for file_path in _iter_python_files(selected_roots):
        error_message = _check_python_syntax(file_path)
        if error_message is None:
            continue
        try:
            display_path = file_path.relative_to(PROJECT_ROOT)
        except ValueError:
            display_path = file_path
        failures.append(f"{display_path}: {error_message}")
    if failures:
        joined = "\n".join(failures)
        raise SystemExit(f"基础静态门禁失败，存在无法编译的 Python 文件:\n{joined}")



def _run_optional_quality_tools() -> list[str]:
    """执行增强静态门禁；未安装工具时返回跳过说明。"""
    skipped: list[str] = []
    optional_commands = {
        "ruff": ("check", "."),
        "mypy": (".",),
    }
    for module_name, args in optional_commands.items():
        command = _optional_module_command(module_name, *args)
        if command is None:
            skipped.append(module_name)
            continue
        _run(command)
    return skipped



def _run_installed_wheel_smoke(temp_dir: str) -> None:
    """验证安装态 wheel 的 import、bundled config 与 launcher stub。"""
    wheel_dir = Path(temp_dir) / "wheel"
    venv_dir = Path(temp_dir) / "venv"
    wheel_dir.mkdir(parents=True, exist_ok=True)
    _run([sys.executable, "-m", "pip", "wheel", ".", "--no-deps", "-w", str(wheel_dir)], suppress_output=True)
    wheels = sorted(wheel_dir.glob("*.whl"))
    if not wheels:
        raise SystemExit("wheel 构建成功但未产出 .whl 文件")

    venv.EnvBuilder(with_pip=True, clear=True).create(venv_dir)
    venv_python = _resolve_venv_python(venv_dir)
    _run([str(venv_python), "-m", "pip", "install", "--no-deps", str(wheels[-1])], suppress_output=True)

    smoke_code = (
        "import a_share_quant; "
        "from a_share_quant.config.loader import ConfigLoader; "
        "cfg = ConfigLoader.load('configs/operator_paper_trade_demo.yaml'); "
        "assert cfg.app.runtime_mode == 'paper_trade'; "
        "assert cfg.broker.client_factory; "
        "print(a_share_quant.__name__)"
    )
    _run([str(venv_python), "-c", smoke_code], cwd=temp_dir, suppress_output=True)

    installed_scripts = _read_installed_console_scripts(venv_dir)
    missing = sorted(_EXPECTED_CONSOLE_SCRIPTS - installed_scripts)
    if missing:
        raise SystemExit(f"安装态 wheel 缺少 console_scripts: {missing}")

    for script_name in sorted(_EXPECTED_CONSOLE_SCRIPTS):
        launcher = _resolve_console_script_path(venv_dir, script_name)
        _run([str(launcher), "--help"], cwd=temp_dir, suppress_output=True)

    # 真实执行生成后的 launcher stub，而非只校验 entry_points 元数据。
    runtime_config = _write_runtime_bound_config(PROJECT_ROOT / "configs/operator_paper_trade_demo.yaml", Path(temp_dir) / "installed_operator_demo.yaml")
    check_runtime_cmd = _resolve_console_script_path(venv_dir, "a-share-quant-check-runtime")
    _run([str(check_runtime_cmd), "--config", str(runtime_config), "--strict"], cwd=temp_dir, suppress_output=True)

    init_db_cmd = _resolve_console_script_path(venv_dir, "a-share-quant-init-db")
    _run([str(init_db_cmd), "--config", str(runtime_config)], cwd=temp_dir, suppress_output=True)
    _run_operator_acceptance_smoke(command_prefix=[str(init_db_cmd)], cwd=temp_dir)



def _verify_clean_release_archive() -> None:
    """构建并检查干净发布包，避免 staging/egg-info/缓存污染回归。"""
    with tempfile.TemporaryDirectory(prefix="verify_release_") as temp_dir:
        archive_path = Path(temp_dir) / "release.zip"
        _run([sys.executable, "scripts/build_clean_release.py", "--source", ".", "--output", str(archive_path)])
        with zipfile.ZipFile(archive_path) as handle:
            names = handle.namelist()
            bad_member = handle.testzip()
        if bad_member is not None:
            raise SystemExit(f"clean release zip 损坏，首个坏成员: {bad_member}")
        forbidden_fragments = ["__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", "runtime/"]
        for fragment in forbidden_fragments:
            if any(fragment in name for name in names):
                raise SystemExit(f"clean release 仍包含污染物片段: {fragment}")
        if any(name.endswith(".egg-info/") or ".egg-info/" in name for name in names):
            raise SystemExit("clean release 仍包含 egg-info 元数据目录")
        if any(part.startswith(".") and part.endswith("_staging") for name in names for part in Path(name).parts):
            raise SystemExit("clean release 仍包含隐藏 staging 目录")



def main() -> int:
    """执行全仓发布前质量门禁。"""
    _run_builtin_static_gate()
    skipped_tools = _run_optional_quality_tools()
    _run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"])
    _run([sys.executable, "-m", "a_share_quant", "--help"], suppress_output=True)
    _run([sys.executable, "scripts/operator_snapshot.py", "--help"], suppress_output=True)
    _run([sys.executable, "scripts/operator_submit_order.py", "--help"], suppress_output=True)
    _run([sys.executable, "scripts/operator_reconcile_session.py", "--help"], suppress_output=True)
    _run([sys.executable, "scripts/operator_sync_session.py", "--help"], suppress_output=True)
    _run([sys.executable, "scripts/operator_run_supervisor.py", "--help"], suppress_output=True)
    _run_operator_acceptance_smoke()
    with tempfile.TemporaryDirectory(prefix="verify_wheel_") as temp_dir:
        _run_installed_wheel_smoke(temp_dir)
    _verify_clean_release_archive()
    skipped_optional_surfaces = _run_optional_surface_smokes()
    skipped_messages: list[str] = []
    if skipped_tools:
        skipped_messages.append(
            "增强静态门禁已跳过：" + ", ".join(skipped_tools) + "。若要执行完整 ruff/mypy 校验，请先安装 requirements-dev.txt 或 project optional dependency 'dev'。"
        )
    if skipped_optional_surfaces:
        skipped_messages.append("可选运行面 smoke 已跳过：" + ", ".join(skipped_optional_surfaces) + "。")
    for message in skipped_messages:
        print(f"[verify_release] {message}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
