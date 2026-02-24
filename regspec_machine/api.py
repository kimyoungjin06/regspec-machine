"""L4 API layer over RunOrchestrator.

This module keeps the request/status/result payloads aligned with
`regspec_machine.contracts` while exposing a small FastAPI surface for
agent/UI integrations.
"""

import csv
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


def _resolve_artifact_path(*, workspace_root: Path, raw: str) -> Optional[Path]:
    text = str(raw or "").strip()
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute():
        path = workspace_root / path
    return path.resolve()


def _safe_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if out != out:  # NaN
        return None
    return out


def _extract_review_metrics(*, workspace_root: Path, artifacts: Mapping[str, str]) -> Dict[str, Any]:
    top_inf_path = _resolve_artifact_path(
        workspace_root=workspace_root,
        raw=str(artifacts.get("top_models_inference_csv", "")),
    )
    rst_path = _resolve_artifact_path(
        workspace_root=workspace_root,
        raw=str(artifacts.get("restart_stability_csv", "")),
    )

    validated_candidate_count = 0
    support_candidate_count = 0
    exploratory_count = 0
    p_best = None
    q_best = None
    p_best_candidate_id = ""

    if top_inf_path is not None and top_inf_path.is_file():
        with top_inf_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                tier = str(row.get("candidate_tier", "")).strip().lower()
                if tier == "validated_candidate":
                    validated_candidate_count += 1
                elif tier == "support_candidate":
                    support_candidate_count += 1
                elif tier == "exploratory":
                    exploratory_count += 1

                p_val = _safe_float(row.get("p_boot_validation", row.get("p_value_validation")))
                q_val = _safe_float(row.get("q_value_validation", row.get("q_value")))
                if p_val is not None and (p_best is None or p_val < p_best):
                    p_best = p_val
                    p_best_candidate_id = str(row.get("candidate_id", "")).strip()
                if q_val is not None and (q_best is None or q_val < q_best):
                    q_best = q_val

    restart_rows = 0
    restart_validated_rate_max = None
    restart_validated_rate_mean = None
    validated_rates = []
    if rst_path is not None and rst_path.is_file():
        with rst_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                restart_rows += 1
                rate = _safe_float(row.get("validated_rate"))
                if rate is None:
                    continue
                validated_rates.append(rate)
                if restart_validated_rate_max is None or rate > restart_validated_rate_max:
                    restart_validated_rate_max = rate
        if validated_rates:
            restart_validated_rate_mean = sum(validated_rates) / float(len(validated_rates))

    return {
        "validated_candidate_count": int(validated_candidate_count),
        "support_candidate_count": int(support_candidate_count),
        "exploratory_count": int(exploratory_count),
        "best_p_validation": p_best,
        "best_q_validation": q_best,
        "best_p_candidate_id": p_best_candidate_id,
        "restart_rows": int(restart_rows),
        "restart_validated_rate_max": restart_validated_rate_max,
        "restart_validated_rate_mean": restart_validated_rate_mean,
        "top_models_inference_exists": bool(top_inf_path and top_inf_path.is_file()),
        "restart_stability_exists": bool(rst_path and rst_path.is_file()),
    }


def _build_review_payload(
    *,
    workspace_root: Path,
    result_payload: Mapping[str, Any],
) -> Dict[str, Any]:
    artifacts = result_payload.get("artifacts", {})
    counts = result_payload.get("counts", {})
    governance_checks = result_payload.get("governance_checks", {})

    metrics = _extract_review_metrics(
        workspace_root=workspace_root,
        artifacts=artifacts if isinstance(artifacts, Mapping) else {},
    )
    search_governance = (
        governance_checks.get("search_governance", {})
        if isinstance(governance_checks, Mapping)
        else {}
    )
    track_consensus_meta = (
        governance_checks.get("track_consensus_meta", {})
        if isinstance(governance_checks, Mapping)
        else {}
    )

    validation_used_for_search = search_governance.get("validation_used_for_search")
    candidate_pool_locked = search_governance.get("candidate_pool_locked_pre_validation")

    # Direction-review style checks for paired mode can be present directly.
    paired_consensus_pass = (
        governance_checks.get("singlex_track_consensus_check_pass")
        if isinstance(governance_checks, Mapping)
        else None
    )

    return {
        "run_id": result_payload.get("run_id"),
        "mode": result_payload.get("mode"),
        "state": result_payload.get("state"),
        "counts": counts,
        "metrics": metrics,
        "governance": {
            "validation_used_for_search_false": validation_used_for_search is False,
            "candidate_pool_locked_pre_validation_true": candidate_pool_locked is True,
            "validation_used_for_search": validation_used_for_search,
            "candidate_pool_locked_pre_validation": candidate_pool_locked,
            "track_consensus_enforced": track_consensus_meta.get("enforce_track_consensus"),
            "track_consensus_demoted_rows": track_consensus_meta.get(
                "n_rows_demoted_from_validated", 0
            ),
            "singlex_track_consensus_check_pass": paired_consensus_pass,
        },
        "artifacts": artifacts,
    }


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

    @app.get("/runs/{run_id}/review")
    def get_run_review(run_id: str) -> Any:
        status = orch.get_status(run_id)
        if status is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        result = orch.get_result(run_id)
        if result is None:
            return JSONResponse(
                status_code=202,
                content={"status": status.as_dict(), "review": None},
            )
        result_payload = result.as_dict()
        review = _build_review_payload(
            workspace_root=_workspace_root(),
            result_payload=result_payload,
        )
        return {
            "status": status.as_dict(),
            "review": review,
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
