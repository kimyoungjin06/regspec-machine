# Desktop Wrapper PoC (L6.2)

## Scope

- Local desktop packaging path that reuses existing L4 API and L5 UI.
- Cross-OS strategy: native window preferred (`pywebview`), browser fallback when unavailable.

## Entrypoint

- Script: `regspec-desktop`
- Module fallback: `python -m regspec_machine.desktop`
- Build script: `regspec-build-desktop` (PyInstaller automation)

## Startup flow

1. Build FastAPI app with `create_app(...)`.
2. Start uvicorn server in background thread.
3. Wait for `/healthz` readiness.
4. Open native window (`pywebview`) to `/ui`.
5. If native window is unavailable, open default browser to `/ui`.

## Why this PoC

- Keeps existing L1~L5 contracts unchanged.
- Avoids immediate heavy Electron/Tauri integration cost.
- Gives non-CLI users an app-like entry while preserving API testability.

## Build Automation

Install build dependency:

```bash
python -m pip install -e .[build]
```

Build:

```bash
regspec-build-desktop --project-root /path/to/regspec-machine
```

Options:
- `--onefile`
- `--windowed`
- `--no-smoke-check`
- `--manifest-json /custom/path/manifest.json`
