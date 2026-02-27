"""Cross-OS local launcher for the L4/L5 service."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Sequence
import webbrowser

from .api import create_app
from .contracts import RUN_MODES
from .module_input import get_git_commit
from .ui_page import build_ui_page_html


@dataclass(frozen=True)
class ConsoleLaunchConfig:
    host: str
    port: int
    workspace_root: str
    events_jsonl: str
    max_attempts: int
    reload: bool
    open_browser: bool
    out_ui_html: str
    ui_html_only: bool

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def ui_url(self) -> str:
        return f"{self.base_url}/ui"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="regspec-console",
        description="Run regspec-machine API/UI local operator console.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="bind port (default: 8000)")
    parser.add_argument(
        "--workspace-root",
        default="",
        help="TwinPaper workspace root (auto-detected when empty)",
    )
    parser.add_argument(
        "--events-jsonl",
        default="",
        help="optional lifecycle event log path",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=2,
        help="max retry attempts per run",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="enable uvicorn auto-reload (dev only)",
    )
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="open /ui in default browser after startup",
    )
    parser.add_argument(
        "--out-ui-html",
        default="",
        help="optional path to export current /ui page as static HTML",
    )
    parser.add_argument(
        "--ui-html-only",
        action="store_true",
        help="write --out-ui-html and exit without starting server",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> ConsoleLaunchConfig:
    args = build_parser().parse_args(argv)
    if int(args.port) <= 0:
        raise ValueError("--port must be > 0")
    if int(args.max_attempts) < 1:
        raise ValueError("--max-attempts must be >= 1")
    out_ui_html = str(args.out_ui_html).strip()
    if bool(args.ui_html_only) and not out_ui_html:
        raise ValueError("--ui-html-only requires --out-ui-html")
    return ConsoleLaunchConfig(
        host=str(args.host).strip() or "127.0.0.1",
        port=int(args.port),
        workspace_root=str(args.workspace_root).strip(),
        events_jsonl=str(args.events_jsonl).strip(),
        max_attempts=int(args.max_attempts),
        reload=bool(args.reload),
        open_browser=bool(args.open_browser),
        out_ui_html=out_ui_html,
        ui_html_only=bool(args.ui_html_only),
    )


def _write_ui_html_snapshot(path_text: str) -> str:
    out = Path(path_text).expanduser()
    if not out.is_absolute():
        out = (Path.cwd() / out).resolve()
    else:
        out = out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    html = build_ui_page_html(run_modes=RUN_MODES)
    out.write_text(html, encoding="utf-8")
    return str(out)


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    uvicorn_run: Optional[Callable[..., None]] = None,
) -> int:
    config = parse_args(argv)

    if config.out_ui_html:
        written = _write_ui_html_snapshot(config.out_ui_html)
        print(f"[regspec-console] UI HTML exported: {written}")
        if config.ui_html_only:
            return 0

    app = create_app(
        workspace_root=config.workspace_root or None,
        events_jsonl=config.events_jsonl or None,
        max_attempts=config.max_attempts,
    )

    run = uvicorn_run
    if run is None:
        try:
            import uvicorn  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "uvicorn is required for regspec-console. Install with: pip install 'regspec-machine[api]'"
            ) from exc
        run = uvicorn.run

    if config.open_browser:
        webbrowser.open(config.ui_url)

    try:
        import regspec_machine as pkg

        pkg_path = Path(getattr(pkg, "__file__", "") or "").resolve()
        if pkg_path.name:
            print(f"[regspec-console] package: {pkg_path}")
    except Exception:
        pass

    commit = get_git_commit(cwd=Path(__file__).resolve().parents[1])
    if commit:
        print(f"[regspec-console] git_commit: {commit}")

    print(f"[regspec-console] serving: {config.base_url}")
    print(f"[regspec-console] UI: {config.ui_url}")
    run(app, host=config.host, port=config.port, reload=config.reload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
