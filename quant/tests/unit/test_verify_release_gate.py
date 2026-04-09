from __future__ import annotations

import contextlib
import io
import sys
from pathlib import Path

import pytest

from scripts import verify_release


def test_optional_module_command_returns_none_for_missing_module(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(verify_release.importlib.util, "find_spec", lambda name: None)
    assert verify_release._optional_module_command("ruff", "check", ".") is None


def test_run_builtin_static_gate_rejects_syntax_error(tmp_path: Path) -> None:
    bad_file = tmp_path / "broken.py"
    bad_file.write_text("def broken(:\n    pass\n", encoding="utf-8")
    with pytest.raises(SystemExit) as exc_info:
        verify_release._run_builtin_static_gate(roots=[bad_file])
    assert "基础静态门禁失败" in str(exc_info.value)


def test_run_optional_quality_tools_reports_skipped_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(verify_release, "_optional_module_command", lambda *args: None)
    skipped = verify_release._run_optional_quality_tools()
    assert skipped == ["ruff", "mypy"]



def test_run_builtin_static_gate_does_not_create_pycache(tmp_path: Path) -> None:
    package_dir = tmp_path / "pkg"
    package_dir.mkdir()
    good_file = package_dir / "module.py"
    good_file.write_text("x = 1\n", encoding="utf-8")

    verify_release._run_builtin_static_gate(roots=[package_dir])

    assert not any(path.name == "__pycache__" for path in tmp_path.rglob("__pycache__"))



def test_run_injects_no_bytecode_env(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(command, cwd=None, stdout=None, stderr=None, env=None):
        captured["command"] = command
        captured["cwd"] = cwd
        captured["env"] = env

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(verify_release.subprocess, "run", fake_run)
    verify_release._run(["python", "-V"], suppress_output=True)

    assert captured["cwd"] == verify_release.PROJECT_ROOT
    assert isinstance(captured["env"], dict)
    assert captured["env"]["PYTHONDONTWRITEBYTECODE"] == "1"


def test_verify_release_invokes_pytest_without_cacheprovider(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(command: list[str], *, suppress_output: bool = False) -> None:
        calls.append(command)

    monkeypatch.setattr(verify_release, "_run", fake_run)
    monkeypatch.setattr(verify_release, "_run_builtin_static_gate", lambda *args, **kwargs: None)
    monkeypatch.setattr(verify_release, "_run_optional_quality_tools", lambda: [])
    monkeypatch.setattr(verify_release, "_run_operator_acceptance_smoke", lambda: None)
    monkeypatch.setattr(verify_release, "_run_installed_wheel_smoke", lambda temp_dir: None)
    monkeypatch.setattr(verify_release, "_verify_clean_release_archive", lambda: None)
    monkeypatch.setattr(verify_release, "_run_optional_surface_smokes", lambda: [])

    assert verify_release.main() == 0
    assert [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"] in calls


def test_run_optional_surface_smokes_returns_no_skips() -> None:
    assert verify_release._run_optional_surface_smokes() == []


def test_run_installed_wheel_smoke_executes_launcher_stubs(tmp_path: Path) -> None:
    verify_release._run_installed_wheel_smoke(str(tmp_path))
