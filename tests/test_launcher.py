from __future__ import annotations

import pytest

from regspec_machine.launcher import main, parse_args


def test_launcher_parse_defaults() -> None:
    cfg = parse_args([])
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 8000
    assert cfg.max_attempts == 2
    assert cfg.reload is False


def test_launcher_parse_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="--port must be > 0"):
        parse_args(["--port", "0"])
    with pytest.raises(ValueError, match="--max-attempts must be >= 1"):
        parse_args(["--max-attempts", "0"])


def test_launcher_main_calls_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {}

    def fake_create_app(**kwargs):
        called["create_kwargs"] = kwargs
        return object()

    def fake_uvicorn_run(app, *, host, port, reload):
        called["run_app"] = app
        called["run_kwargs"] = {
            "host": host,
            "port": port,
            "reload": reload,
        }

    monkeypatch.setattr("regspec_machine.launcher.create_app", fake_create_app)
    rc = main(
        [
            "--host",
            "0.0.0.0",
            "--port",
            "9001",
            "--workspace-root",
            "/tmp/workspace",
            "--events-jsonl",
            "/tmp/events.jsonl",
            "--max-attempts",
            "3",
            "--reload",
        ],
        uvicorn_run=fake_uvicorn_run,
    )
    assert rc == 0
    assert called["create_kwargs"]["workspace_root"] == "/tmp/workspace"
    assert called["create_kwargs"]["events_jsonl"] == "/tmp/events.jsonl"
    assert called["create_kwargs"]["max_attempts"] == 3
    assert called["run_kwargs"]["host"] == "0.0.0.0"
    assert called["run_kwargs"]["port"] == 9001
    assert called["run_kwargs"]["reload"] is True

