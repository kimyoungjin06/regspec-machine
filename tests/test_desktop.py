from __future__ import annotations

import types

import pytest

from regspec_machine.desktop import main, parse_desktop_args


class _FakeThread:
    def __init__(self) -> None:
        self._alive = False
        self.join_calls = 0

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout=None) -> None:
        self.join_calls += 1
        self._alive = False


def test_parse_desktop_defaults() -> None:
    cfg = parse_desktop_args([])
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 8000
    assert cfg.max_attempts == 2
    assert cfg.width == 1320
    assert cfg.height == 860


def test_parse_desktop_invalid_values() -> None:
    with pytest.raises(ValueError, match="--port must be > 0"):
        parse_desktop_args(["--port", "0"])
    with pytest.raises(ValueError, match="--max-attempts must be >= 1"):
        parse_desktop_args(["--max-attempts", "0"])
    with pytest.raises(ValueError, match="--width must be >= 480"):
        parse_desktop_args(["--width", "320"])
    with pytest.raises(ValueError, match="--height must be >= 360"):
        parse_desktop_args(["--height", "200"])


def test_desktop_main_prefers_webview(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {}
    fake_thread = _FakeThread()
    fake_server = types.SimpleNamespace(should_exit=False)

    def fake_create_app(**kwargs):
        calls["create_app_kwargs"] = kwargs
        return object()

    def fake_start_server(app, host, port):
        calls["start_server"] = {"app": app, "host": host, "port": port}
        return fake_server, fake_thread

    def fake_wait_health(url, timeout):
        calls["wait_health"] = {"url": url, "timeout": timeout}
        return True

    def fake_open_webview(**kwargs):
        calls["open_webview"] = kwargs
        return True

    def fake_open_browser(url):
        calls["open_browser"] = url

    monkeypatch.setattr("regspec_machine.desktop.create_app", fake_create_app)
    rc = main(
        [
            "--host",
            "0.0.0.0",
            "--port",
            "9001",
            "--workspace-root",
            "/tmp/ws",
            "--events-jsonl",
            "/tmp/events.jsonl",
            "--max-attempts",
            "3",
            "--title",
            "DeskUT",
        ],
        start_server=fake_start_server,
        wait_health=fake_wait_health,
        open_webview=fake_open_webview,
        open_browser=fake_open_browser,
    )
    assert rc == 0
    assert calls["start_server"]["host"] == "0.0.0.0"
    assert calls["start_server"]["port"] == 9001
    assert calls["create_app_kwargs"]["workspace_root"] == "/tmp/ws"
    assert calls["create_app_kwargs"]["events_jsonl"] == "/tmp/events.jsonl"
    assert calls["create_app_kwargs"]["max_attempts"] == 3
    assert calls["open_webview"]["title"] == "DeskUT"
    assert "open_browser" not in calls
    assert fake_server.should_exit is True
    assert fake_thread.join_calls >= 1


def test_desktop_main_browser_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {}
    fake_thread = _FakeThread()
    fake_server = types.SimpleNamespace(should_exit=False)

    def fake_create_app(**kwargs):
        calls["create_app_kwargs"] = kwargs
        return object()

    def fake_start_server(app, host, port):
        calls["start_server"] = {"app": app, "host": host, "port": port}
        return fake_server, fake_thread

    def fake_wait_health(url, timeout):
        calls["wait_health"] = {"url": url, "timeout": timeout}
        return True

    def fake_open_webview(**kwargs):
        calls["open_webview"] = kwargs
        return False

    def fake_open_browser(url):
        calls["open_browser"] = url

    monkeypatch.setattr("regspec_machine.desktop.create_app", fake_create_app)
    rc = main(
        [],
        start_server=fake_start_server,
        wait_health=fake_wait_health,
        open_webview=fake_open_webview,
        open_browser=fake_open_browser,
    )
    assert rc == 0
    assert calls["open_browser"].endswith("/ui")
    assert fake_server.should_exit is True
    assert fake_thread.join_calls >= 1


def test_desktop_main_health_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_thread = _FakeThread()
    fake_server = types.SimpleNamespace(should_exit=False)

    def fake_create_app(**_kwargs):
        return object()

    def fake_start_server(_app, _host, _port):
        return fake_server, fake_thread

    def fake_wait_health(_url, _timeout):
        return False

    monkeypatch.setattr("regspec_machine.desktop.create_app", fake_create_app)
    with pytest.raises(RuntimeError, match="server health check timeout"):
        main(
            ["--startup-timeout-sec", "0.1"],
            start_server=fake_start_server,
            wait_health=fake_wait_health,
            open_webview=lambda **_kwargs: True,
            open_browser=lambda _url: None,
        )
    assert fake_server.should_exit is True
    assert fake_thread.join_calls >= 1

