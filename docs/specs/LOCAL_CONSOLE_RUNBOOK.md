# Local Console Runbook (L6.1)

## Goal

Start `regspec-machine` API + browser console (`/ui`) with one command on Linux/macOS/Windows.

## Prerequisites

- Python 3.10+
- API dependencies installed:

```bash
python -m pip install -e .[api]
```

## Start

```bash
regspec-console --workspace-root /path/to/TwinPaper --open-browser
```

Fallback:

```bash
python -m regspec_machine.launcher --workspace-root /path/to/TwinPaper --open-browser
```

Default endpoint:

- API root: `http://127.0.0.1:8000`
- UI page: `http://127.0.0.1:8000/ui`

## Useful options

- `--host 0.0.0.0` expose to LAN
- `--port 8010` change port
- `--events-jsonl /tmp/regspec_events.jsonl` persist run lifecycle events
- `--max-attempts 3` orchestrator retry limit
- `--reload` auto-reload for local development

## Smoke check

1. Open `/ui` and submit a dry run.
2. Verify run appears in monitor table.
3. Open summary panel and confirm `state` transitions.

## Baseline Compare Export (Standard Path)

After comparing `nooption` vs `singlex` in `/ui`, use:

- `Save compare to outputs/`

This writes both files to:

- `outputs/reports/regspec_compare/*.json`
- `outputs/reports/regspec_compare/*.md`

The export includes governance and promotion checks, and blocks promotion when either branch fails governance.
