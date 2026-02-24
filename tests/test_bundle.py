from __future__ import annotations

from pathlib import Path
import types

import pytest

from regspec_machine.bundle import (
    DesktopBundleConfig,
    build_pyinstaller_command,
    parse_bundle_args,
    resolve_bundle_executable,
    run_bundle_build,
)


def _mk_project(tmp_path: Path) -> Path:
    root = tmp_path / "proj"
    pkg_dir = root / "regspec_machine"
    desktop = pkg_dir / "desktop.py"
    desktop_entry = pkg_dir / "desktop_entry.py"
    pkg_dir.mkdir(parents=True, exist_ok=True)
    desktop.write_text("def main():\n    return 0\n", encoding="utf-8")
    desktop_entry.write_text(
        "from regspec_machine.desktop import main\nif __name__ == '__main__':\n    raise SystemExit(main())\n",
        encoding="utf-8",
    )
    return root


def test_parse_bundle_args_defaults() -> None:
    cfg = parse_bundle_args([])
    assert cfg.name == "regspec-desktop"
    assert cfg.onefile is False
    assert cfg.clean is True
    assert cfg.smoke_check is True


def test_parse_bundle_args_rejects_blank_name() -> None:
    with pytest.raises(ValueError, match="--name is required"):
        parse_bundle_args(["--name", "   "])


def test_build_pyinstaller_command_contains_expected_flags(tmp_path: Path) -> None:
    root = _mk_project(tmp_path)
    cfg = DesktopBundleConfig(
        project_root=str(root),
        python_bin="/usr/bin/python3",
        name="desk-ut",
        onefile=True,
        windowed=True,
        clean=True,
        smoke_check=True,
    )
    cmd = build_pyinstaller_command(cfg)
    assert cmd[:3] == ("/usr/bin/python3", "-m", "PyInstaller")
    assert "--name" in cmd and "desk-ut" in cmd
    assert "--onefile" in cmd
    assert "--windowed" in cmd
    assert "--clean" in cmd
    assert "--collect-all" in cmd and "regspec_machine" in cmd
    assert str(root / "regspec_machine" / "desktop_entry.py") in cmd


def test_resolve_bundle_executable_paths(tmp_path: Path) -> None:
    root = _mk_project(tmp_path)
    cfg = DesktopBundleConfig(
        project_root=str(root),
        python_bin="python",
        name="deskapp",
        onefile=False,
        dist_dir="dist",
    )
    linux_path = resolve_bundle_executable(cfg, os_name="linux")
    win_path = resolve_bundle_executable(cfg, os_name="windows")
    assert linux_path == (root / "dist" / "deskapp" / "deskapp")
    assert win_path == (root / "dist" / "deskapp" / "deskapp.exe")


def test_run_bundle_build_executes_build_and_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _mk_project(tmp_path)
    cfg = DesktopBundleConfig(
        project_root=str(root),
        python_bin="python",
        name="deskapp",
        onefile=False,
        dist_dir="dist",
        work_dir="work",
        spec_dir="spec",
        clean=False,
        smoke_check=True,
    )
    calls = []

    def fake_find_spec(name: str):
        return object() if name == "PyInstaller" else None

    def fake_runner(cmd, cwd):
        calls.append((tuple(cmd), cwd))
        # Simulate generated executable after build.
        if "PyInstaller" in cmd:
            exe = root / "dist" / "deskapp" / "deskapp"
            exe.parent.mkdir(parents=True, exist_ok=True)
            exe.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("regspec_machine.bundle.importlib.util.find_spec", fake_find_spec)
    out = run_bundle_build(cfg, runner=fake_runner)
    assert out.build_returncode == 0
    assert out.smoke_returncode == 0
    assert Path(out.bundle_executable).is_file()
    assert Path(out.manifest_json).is_file()
    assert len(calls) == 2
    assert calls[1][0][-1] == "--help"


def test_run_bundle_build_requires_pyinstaller(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _mk_project(tmp_path)
    cfg = DesktopBundleConfig(
        project_root=str(root),
        python_bin="python",
    )
    monkeypatch.setattr("regspec_machine.bundle.importlib.util.find_spec", lambda _name: None)
    with pytest.raises(RuntimeError, match="PyInstaller is not installed"):
        run_bundle_build(cfg, runner=lambda _cmd, _cwd: types.SimpleNamespace(returncode=0, stdout="", stderr=""))
