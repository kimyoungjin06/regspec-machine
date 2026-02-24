from __future__ import annotations

import pytest

from regspec_machine.launcher import main, parse_args


def test_launcher_parse_defaults() -> None:
    cfg = parse_args([])
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 8000
    assert cfg.max_attempts == 2
    assert cfg.reload is False
    assert cfg.out_ui_html == ""
    assert cfg.ui_html_only is False


def test_launcher_parse_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="--port must be > 0"):
        parse_args(["--port", "0"])
    with pytest.raises(ValueError, match="--max-attempts must be >= 1"):
        parse_args(["--max-attempts", "0"])
    with pytest.raises(ValueError, match="--ui-html-only requires --out-ui-html"):
        parse_args(["--ui-html-only"])


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


def test_launcher_main_ui_html_only_exports_and_exits(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out_html = tmp_path / "ui" / "console_snapshot.html"
    called = {"create_app": False, "uvicorn_run": False}

    def fake_create_app(**_kwargs):
        called["create_app"] = True
        return object()

    def fake_uvicorn_run(*_args, **_kwargs):
        called["uvicorn_run"] = True

    monkeypatch.setattr("regspec_machine.launcher.create_app", fake_create_app)
    rc = main(
        [
            "--out-ui-html",
            str(out_html),
            "--ui-html-only",
        ],
        uvicorn_run=fake_uvicorn_run,
    )
    assert rc == 0
    assert out_html.is_file()
    text = out_html.read_text(encoding="utf-8")
    assert "<!doctype html>" in text.lower()
    assert "RegSpec-Machine Operator Console" in text
    assert called["create_app"] is False
    assert called["uvicorn_run"] is False
