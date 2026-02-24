"""L4 API layer over RunOrchestrator.

This module keeps the request/status/result payloads aligned with
`regspec_machine.contracts` while exposing a small FastAPI surface for
agent/UI integrations.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from .contracts import RUN_MODES, RunRequestContract
from .engine import PresetEngine
from .orchestrator import RunOrchestrator
from .ui_page import build_ui_page_html


def _utc_now_isoz() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_path_map(
    *,
    workspace_root: Path,
    artifact_map: Mapping[str, str],
) -> tuple[Dict[str, str], Dict[str, bool]]:
    resolved: Dict[str, str] = {}
    existing: Dict[str, bool] = {}
    for key, raw in artifact_map.items():
        text = str(raw or "").strip()
        if not text:
            resolved[key] = ""
            existing[key] = False
            continue
        p = Path(text)
        if not p.is_absolute():
            p = workspace_root / p
        p = p.resolve()
        resolved[key] = str(p)
        existing[key] = p.is_file()
    return resolved, existing


def _dispatch_run(orchestrator: RunOrchestrator, run_id: str, *, dry_run: bool) -> None:
    try:
        orchestrator.execute(run_id, dry_run=bool(dry_run))
    except Exception:
        # API dispatch should never crash the request path; run state already
        # reflects failures via orchestrator transitions.
        return


def create_app(
    *,
    orchestrator: Optional[RunOrchestrator] = None,
    engine: Optional[PresetEngine] = None,
    workspace_root: Optional[Path | str] = None,
    events_jsonl: Optional[Path | str] = None,
    max_attempts: int = 2,
) -> Any:
    """Create FastAPI app for run submission and lifecycle control."""
    try:
        from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
        from fastapi.responses import HTMLResponse, JSONResponse
    except Exception as exc:
        raise RuntimeError(
            "FastAPI dependencies are missing. Install with: pip install 'regspec-machine[api]'"
        ) from exc

    orch = orchestrator
    if orch is None:
        resolved_root = Path(workspace_root).expanduser().resolve() if workspace_root else None
        eng = engine or PresetEngine(workspace_root=resolved_root)
        orch = RunOrchestrator(engine=eng, max_attempts=max_attempts, events_jsonl=events_jsonl)

    def _workspace_root() -> Path:
        eng = getattr(orch, "engine", None)
        root = getattr(eng, "workspace_root", None)
        if root is None:
            return Path.cwd().resolve()
        return Path(root).resolve()

    app = FastAPI(
        title="regspec-machine API",
        version="0.1.0",
        description="L4 execution API for nooption/singlex/paired run orchestration.",
    )
    app.state.orchestrator = orch

    @app.get("/healthz")
    def healthz() -> Dict[str, Any]:
        return {
            "ok": True,
            "timestamp_utc": _utc_now_isoz(),
            "run_count": len(orch.list_snapshots()),
        }

    @app.get("/runs")
    def list_runs(
        state: str = Query(default=""),
        limit: int = Query(default=200, ge=1, le=1000),
    ) -> Dict[str, Any]:
        rows = orch.list_snapshots(state=state)
        rows_sorted = sorted(
            rows,
            key=lambda row: str(row.status.updated_at_utc),
            reverse=True,
        )
        payload_rows = []
        for row in rows_sorted[: int(limit)]:
            result_counts = row.result.counts if row.result is not None else {}
            payload_rows.append(
                {
                    "run_id": row.request.run_id,
                    "mode": row.request.mode,
                    "state": row.status.state,
                    "attempt": int(row.status.attempt),
                    "created_at_utc": row.status.created_at_utc,
                    "updated_at_utc": row.status.updated_at_utc,
                    "progress_stage": row.status.progress_stage,
                    "progress_message": row.status.progress_message,
                    "progress_fraction": row.status.progress_fraction,
                    "returncode": row.returncode,
                    "has_result": row.result is not None,
                    "counts": result_counts,
                }
            )
        return {
            "state_filter": str(state).strip(),
            "rows": payload_rows,
            "total_rows": len(payload_rows),
            "allowed_modes": list(RUN_MODES),
        }

    @app.post("/runs", status_code=202)
    def post_run(
        payload: Dict[str, Any],
        background_tasks: BackgroundTasks,
        execute: bool = Query(default=True),
        dry_run: bool = Query(default=False),
    ) -> Dict[str, Any]:
        try:
            request = RunRequestContract.from_payload(payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        preexisting_same_run = orch.get_status(request.run_id) is not None
        try:
            status = orch.submit(request)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        idempotent_reused = bool(request.idempotency_key) and (
            status.run_id != request.run_id or preexisting_same_run
        )
        dispatched = bool(execute) and status.state == "queued"
        if dispatched:
            background_tasks.add_task(_dispatch_run, orch, status.run_id, dry_run=bool(dry_run))

        return {
            "request": request.as_dict(),
            "status": status.as_dict(),
            "idempotent_reused": idempotent_reused,
            "dispatched": dispatched,
        }

    @app.get("/runs/{run_id}")
    def get_run_status(run_id: str) -> Dict[str, Any]:
        status = orch.get_status(run_id)
        if status is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        return {"status": status.as_dict()}

    @app.get("/runs/{run_id}/result")
    def get_run_result(run_id: str) -> Any:
        status = orch.get_status(run_id)
        if status is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        result = orch.get_result(run_id)
        if result is None:
            return JSONResponse(
                status_code=202,
                content={
                    "status": status.as_dict(),
                    "result": None,
                },
            )
        return {
            "status": status.as_dict(),
            "result": result.as_dict(),
        }

    @app.get("/runs/{run_id}/summary")
    def get_run_summary(run_id: str) -> Any:
        status = orch.get_status(run_id)
        if status is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        result = orch.get_result(run_id)
        if result is None:
            return JSONResponse(
                status_code=202,
                content={"status": status.as_dict(), "summary": None},
            )
        return {
            "status": status.as_dict(),
            "summary": {
                "run_id": result.run_id,
                "mode": result.mode,
                "state": result.state,
                "counts": result.counts,
                "governance_checks": result.governance_checks,
                "audit_hashes": result.audit_hashes,
                "timestamp_utc": result.timestamp_utc,
            },
        }

    @app.post("/runs/{run_id}/cancel")
    def cancel_run(run_id: str, reason: str = Query(default="cancel requested")) -> Dict[str, Any]:
        try:
            status = orch.cancel(run_id, reason=reason)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"status": status.as_dict()}

    @app.post("/runs/{run_id}/retry")
    def retry_run(run_id: str, dry_run: bool = Query(default=False)) -> Dict[str, Any]:
        try:
            execution = orch.retry(run_id, dry_run=bool(dry_run))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "status": execution.status.as_dict(),
            "result": execution.result.as_dict(),
            "returncode": int(execution.returncode),
        }

    @app.get("/runs/{run_id}/artifacts")
    def get_artifacts(run_id: str) -> Any:
        status = orch.get_status(run_id)
        if status is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        result = orch.get_result(run_id)
        if result is None:
            return JSONResponse(
                status_code=202,
                content={"status": status.as_dict(), "artifacts": None},
            )

        artifact_map = result.artifacts.as_dict()
        resolved_map, existing_map = _to_path_map(
            workspace_root=_workspace_root(),
            artifact_map=artifact_map,
        )
        return {
            "status": status.as_dict(),
            "artifacts": artifact_map,
            "resolved_artifacts": resolved_map,
            "artifact_exists": existing_map,
        }

    @app.get("/ui", response_class=HTMLResponse)
    def ui_index() -> HTMLResponse:
        return HTMLResponse(content=build_ui_page_html(run_modes=RUN_MODES), status_code=200)

    return app
