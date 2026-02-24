"""Desktop bundle automation (L6.2 build path)."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import importlib.util
import json
from pathlib import Path
import platform
import subprocess
import sys
from typing import Any, Callable, Optional, Sequence, Tuple


@dataclass(frozen=True)
class DesktopBundleConfig:
    project_root: str
    python_bin: str
    name: str = "regspec-desktop"
    onefile: bool = False
    windowed: bool = False
    clean: bool = True
    smoke_check: bool = True
    dist_dir: str = "build/dist"
    work_dir: str = "build/work"
    spec_dir: str = "build/spec"
    manifest_json: str = ""


@dataclass(frozen=True)
class BundleBuildResult:
    build_command: Tuple[str, ...]
    smoke_command: Tuple[str, ...]
    bundle_executable: str
    manifest_json: str
    build_returncode: int
    smoke_returncode: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_bundle_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="regspec-build-desktop",
        description="Build desktop wrapper executable using PyInstaller.",
    )
    parser.add_argument("--project-root", default=".", help="project root path")
    parser.add_argument("--python-bin", default=sys.executable, help="python executable for build")
    parser.add_argument("--name", default="regspec-desktop", help="bundle name")
    parser.add_argument("--onefile", action="store_true", help="build one-file executable")
    parser.add_argument("--windowed", action="store_true", help="build windowed executable")
    parser.add_argument("--no-clean", action="store_true", help="disable clean build")
    parser.add_argument("--no-smoke-check", action="store_true", help="skip executable --help smoke check")
    parser.add_argument("--dist-dir", default="build/dist", help="PyInstaller dist dir")
    parser.add_argument("--work-dir", default="build/work", help="PyInstaller work dir")
    parser.add_argument("--spec-dir", default="build/spec", help="PyInstaller spec dir")
    parser.add_argument("--manifest-json", default="", help="manifest json output path")
    return parser


def parse_bundle_args(argv: Optional[Sequence[str]] = None) -> DesktopBundleConfig:
    args = build_bundle_parser().parse_args(argv)
    name = str(args.name).strip()
    if not name:
        raise ValueError("--name is required")
    return DesktopBundleConfig(
        project_root=str(args.project_root).strip() or ".",
        python_bin=str(args.python_bin).strip() or sys.executable,
        name=name,
        onefile=bool(args.onefile),
        windowed=bool(args.windowed),
        clean=not bool(args.no_clean),
        smoke_check=not bool(args.no_smoke_check),
        dist_dir=str(args.dist_dir).strip() or "build/dist",
        work_dir=str(args.work_dir).strip() or "build/work",
        spec_dir=str(args.spec_dir).strip() or "build/spec",
        manifest_json=str(args.manifest_json).strip(),
    )


def _desktop_script_path(project_root: Path) -> Path:
    return (project_root / "regspec_machine" / "desktop_entry.py").resolve()


def build_pyinstaller_command(config: DesktopBundleConfig) -> Tuple[str, ...]:
    project_root = Path(config.project_root).expanduser().resolve()
    script_path = _desktop_script_path(project_root)
    if not script_path.is_file():
        raise FileNotFoundError(f"desktop entry script not found: {script_path}")

    dist_dir = (project_root / config.dist_dir).resolve()
    work_dir = (project_root / config.work_dir).resolve()
    spec_dir = (project_root / config.spec_dir).resolve()

    cmd = [
        config.python_bin,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--name",
        config.name,
        "--distpath",
        str(dist_dir),
        "--workpath",
        str(work_dir),
        "--specpath",
        str(spec_dir),
        "--collect-all",
        "regspec_machine",
        "--hidden-import",
        "uvicorn",
        "--hidden-import",
        "fastapi",
        "--hidden-import",
        "starlette",
        "--hidden-import",
        "pydantic",
    ]
    if config.clean:
        cmd.append("--clean")
    cmd.append("--onefile" if config.onefile else "--onedir")
    cmd.append("--windowed" if config.windowed else "--console")
    cmd.append(str(script_path))
    return tuple(cmd)


def resolve_bundle_executable(
    config: DesktopBundleConfig,
    *,
    os_name: str = "",
) -> Path:
    project_root = Path(config.project_root).expanduser().resolve()
    dist_root = (project_root / config.dist_dir).resolve()
    sys_name = (os_name or platform.system()).strip().lower()
    exe_name = f"{config.name}.exe" if sys_name.startswith("win") else config.name
    if config.onefile:
        return dist_root / exe_name
    return dist_root / config.name / exe_name


def _default_runner(cmd: Sequence[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(cmd),
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )


def run_bundle_build(
    config: DesktopBundleConfig,
    *,
    runner: Optional[
        Callable[[Sequence[str], Path], subprocess.CompletedProcess[str]]
    ] = None,
) -> BundleBuildResult:
    if importlib.util.find_spec("PyInstaller") is None:
        raise RuntimeError(
            "PyInstaller is not installed. Install with: pip install 'regspec-machine[build]'"
        )

    project_root = Path(config.project_root).expanduser().resolve()
    dist_root = (project_root / config.dist_dir).resolve()
    dist_root.mkdir(parents=True, exist_ok=True)

    run = runner or _default_runner
    build_cmd = build_pyinstaller_command(config)
    build_proc = run(build_cmd, project_root)
    if int(build_proc.returncode) != 0:
        raise RuntimeError(
            f"PyInstaller build failed (rc={int(build_proc.returncode)}):\n{str(build_proc.stderr)[-4000:]}"
        )

    executable = resolve_bundle_executable(config)
    smoke_cmd: Tuple[str, ...] = tuple()
    smoke_rc = 0
    if config.smoke_check:
        if not executable.is_file():
            raise RuntimeError(f"bundle executable not found after build: {executable}")
        smoke_cmd = (str(executable), "--help")
        smoke_proc = run(smoke_cmd, project_root)
        smoke_rc = int(smoke_proc.returncode)
        if smoke_rc != 0:
            raise RuntimeError(
                f"desktop smoke check failed (rc={smoke_rc}):\n{str(smoke_proc.stderr)[-4000:]}"
            )

    manifest_path = (
        Path(config.manifest_json).expanduser().resolve()
        if str(config.manifest_json).strip()
        else (dist_root / f"{config.name}_bundle_manifest.json")
    )
    result = BundleBuildResult(
        build_command=tuple(build_cmd),
        smoke_command=smoke_cmd,
        bundle_executable=str(executable),
        manifest_json=str(manifest_path),
        build_returncode=int(build_proc.returncode),
        smoke_returncode=int(smoke_rc),
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(result.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    config = parse_bundle_args(argv)
    result = run_bundle_build(config)
    print(f"[regspec-build-desktop] build ok: {result.bundle_executable}")
    print(f"[regspec-build-desktop] manifest: {result.manifest_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
