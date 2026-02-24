"""Desktop wrapper PoC for cross-OS operator experience."""

import argparse
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from dataclasses import dataclass
from typing import Callable, Optional, Sequence, Tuple

from .api import create_app


@dataclass(frozen=True)
class DesktopLaunchConfig:
    host: str
    port: int
    workspace_root: str
    events_jsonl: str
    max_attempts: int
    title: str
    width: int
    height: int
    startup_timeout_sec: float

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def ui_url(self) -> str:
        return f"{self.base_url}/ui"

    @property
    def health_url(self) -> str:
        return f"{self.base_url}/healthz"


def build_desktop_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="regspec-desktop",
        description="Launch regspec-machine as a local desktop window (L6.2 PoC).",
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
        "--title",
        default="RegSpec-Machine Console",
        help="desktop window title",
    )
    parser.add_argument("--width", type=int, default=1320, help="window width")
    parser.add_argument("--height", type=int, default=860, help="window height")
    parser.add_argument(
        "--startup-timeout-sec",
        type=float,
        default=30.0,
        help="wait timeout before UI open",
    )
    return parser


def parse_desktop_args(argv: Optional[Sequence[str]] = None) -> DesktopLaunchConfig:
    args = build_desktop_parser().parse_args(argv)
    if int(args.port) <= 0:
        raise ValueError("--port must be > 0")
    if int(args.max_attempts) < 1:
        raise ValueError("--max-attempts must be >= 1")
    if int(args.width) < 480:
        raise ValueError("--width must be >= 480")
    if int(args.height) < 360:
        raise ValueError("--height must be >= 360")
    if float(args.startup_timeout_sec) <= 0:
        raise ValueError("--startup-timeout-sec must be > 0")
    return DesktopLaunchConfig(
        host=str(args.host).strip() or "127.0.0.1",
        port=int(args.port),
        workspace_root=str(args.workspace_root).strip(),
        events_jsonl=str(args.events_jsonl).strip(),
        max_attempts=int(args.max_attempts),
        title=str(args.title).strip() or "RegSpec-Machine Console",
        width=int(args.width),
        height=int(args.height),
        startup_timeout_sec=float(args.startup_timeout_sec),
    )


def _default_start_server(app, host: str, port: int):
    try:
        import uvicorn  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "uvicorn is required for regspec-desktop. Install with: pip install 'regspec-machine[api]'"
        ) from exc

    config = uvicorn.Config(app, host=host, port=int(port), log_level="info")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True, name="regspec-uvicorn")
    thread.start()
    return server, thread


def _default_wait_health(url: str, timeout_sec: float) -> bool:
    deadline = time.time() + float(timeout_sec)
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                if int(getattr(resp, "status", 200)) == 200:
                    return True
        except urllib.error.URLError:
            pass
        except Exception:
            pass
        time.sleep(0.15)
    return False


def _default_open_webview(*, url: str, title: str, width: int, height: int) -> bool:
    try:
        import webview  # type: ignore
    except Exception:
        return False
    webview.create_window(title=title, url=url, width=width, height=height, resizable=True)
    webview.start()
    return True


def _default_open_browser(url: str) -> None:
    webbrowser.open(url)


def _run_desktop(
    config: DesktopLaunchConfig,
    *,
    start_server: Callable[..., Tuple[object, threading.Thread]],
    wait_health: Callable[[str, float], bool],
    open_webview: Callable[..., bool],
    open_browser: Callable[[str], None],
) -> int:
    app = create_app(
        workspace_root=config.workspace_root or None,
        events_jsonl=config.events_jsonl or None,
        max_attempts=config.max_attempts,
    )
    server, thread = start_server(app, config.host, config.port)
    print(f"[regspec-desktop] serving: {config.base_url}")
    print(f"[regspec-desktop] UI: {config.ui_url}")
    if not wait_health(config.health_url, config.startup_timeout_sec):
        setattr(server, "should_exit", True)
        thread.join(timeout=3.0)
        raise RuntimeError(f"server health check timeout: {config.health_url}")

    try:
        opened = open_webview(
            url=config.ui_url,
            title=config.title,
            width=config.width,
            height=config.height,
        )
        if not opened:
            print("[regspec-desktop] pywebview not available; opening browser fallback.")
            open_browser(config.ui_url)
            while thread.is_alive():
                thread.join(timeout=0.5)
    finally:
        setattr(server, "should_exit", True)
        thread.join(timeout=3.0)
    return 0


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    start_server: Optional[Callable[..., Tuple[object, threading.Thread]]] = None,
    wait_health: Optional[Callable[[str, float], bool]] = None,
    open_webview: Optional[Callable[..., bool]] = None,
    open_browser: Optional[Callable[[str], None]] = None,
) -> int:
    config = parse_desktop_args(argv)
    return _run_desktop(
        config,
        start_server=start_server or _default_start_server,
        wait_health=wait_health or _default_wait_health,
        open_webview=open_webview or _default_open_webview,
        open_browser=open_browser or _default_open_browser,
    )


if __name__ == "__main__":
    raise SystemExit(main())

