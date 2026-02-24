"""L2 execution facade over the preset launcher CLI.

This module keeps CLI/API/UI parity by accepting RunRequestContract and
returning status/result contracts after preset execution.
"""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from .contracts import (
    RunArtifactsContract,
    RunErrorContract,
    RunRequestContract,
    RunResultContract,
    RunStatusContract,
)


PRESET_SCRIPT_RELATIVE = Path("scripts/modeling/run_phase_b_regspec_preset.py")


def _utc_now_isoz() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(text).strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "run"


def _coerce_nonnegative_int(value: Any) -> Optional[int]:
    try:
        out = int(value)
    except Exception:
        return None
    if out < 0:
        return None
    return out


def _normalize_path(root: Path, text: str) -> Path:
    p = Path(str(text).strip())
    if p.is_absolute():
        return p
    return root / p


def _read_json_if_exists(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _expected_scan_outputs(run_id: str) -> Dict[str, str]:
    rid = _slug(run_id)
    top_path = f"outputs/tables/phase_b_bikard_machine_scientist_top_models_{rid}.csv"
    top_inference_path = top_path[:-4] + "_inference.csv" if top_path.endswith(".csv") else top_path
    return {
        "scan_runs_csv": f"outputs/tables/phase_b_bikard_machine_scientist_scan_runs_{rid}.csv",
        "top_models_csv": top_path,
        "top_models_inference_csv": top_inference_path,
        "search_log_jsonl": f"data/metadata/phase_b_bikard_machine_scientist_search_log_{rid}.jsonl",
        "run_summary_json": f"data/metadata/phase_b_bikard_machine_scientist_run_summary_{rid}.json",
        "feasibility_frontier_json": (
            f"data/metadata/phase_b_bikard_machine_scientist_feasibility_frontier_{rid}.json"
        ),
        "feature_registry_json": f"data/metadata/phase_b_bikard_machine_scientist_feature_registry_{rid}.json",
        "restart_stability_csv": f"data/metadata/phase_b_bikard_machine_scientist_restart_stability_{rid}.csv",
    }


def _expected_paired_outputs(run_id: str, skip_direction_review: bool) -> Dict[str, str]:
    rid = _slug(run_id)
    return {
        "paired_summary_json": (
            f"data/metadata/phase_b_bikard_machine_scientist_paired_preset_summary_{rid}.json"
        ),
        "direction_review_json": (
            ""
            if skip_direction_review
            else f"data/metadata/phase_b_bikard_machine_scientist_direction_review_{rid}.json"
        ),
    }


def _expected_overnight_outputs(run_id: str) -> Dict[str, str]:
    rid = _slug(run_id)
    return {
        "overnight_summary_json": (
            f"data/metadata/phase_b_bikard_machine_scientist_overnight_summary_{rid}.json"
        ),
        "overnight_checkpoint_json": (
            f"data/metadata/phase_b_bikard_machine_scientist_overnight_checkpoint_{rid}.json"
        ),
    }


def _resolve_workspace_root(
    workspace_root: Optional[Path | str],
    script_relative: Path,
) -> Path:
    if workspace_root is not None:
        return Path(workspace_root).expanduser().resolve()

    probe_starts = [
        Path.cwd().resolve(),
        Path(__file__).resolve().parent,
    ]
    visited: set[Path] = set()
    for start in probe_starts:
        for candidate in [start, *start.parents]:
            if candidate in visited:
                continue
            visited.add(candidate)
            if (candidate / script_relative).is_file():
                return candidate
    raise FileNotFoundError(f"failed to resolve workspace root for script: {script_relative}")


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


def _default_command_executor(cmd: Sequence[str], cwd: Path) -> CommandResult:
    proc = subprocess.run(
        list(cmd),
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    return CommandResult(
        returncode=int(proc.returncode),
        stdout=str(proc.stdout or ""),
        stderr=str(proc.stderr or ""),
    )


@dataclass(frozen=True)
class EngineExecution:
    request: RunRequestContract
    command: Tuple[str, ...]
    returncode: int
    status: RunStatusContract
    result: RunResultContract
    stdout: str = ""
    stderr: str = ""


class PresetEngine:
    """Contract-first facade that dispatches to preset CLI script."""

    def __init__(
        self,
        *,
        workspace_root: Optional[Path | str] = None,
        preset_script_relative: Path | str = PRESET_SCRIPT_RELATIVE,
        command_executor: Optional[Callable[[Sequence[str], Path], CommandResult]] = None,
    ) -> None:
        script_rel = Path(preset_script_relative)
        if script_rel.is_absolute():
            script_path = script_rel.resolve()
            root = script_path.parents[2] if len(script_path.parents) >= 3 else script_path.parent
        else:
            root = _resolve_workspace_root(workspace_root, script_rel)
            script_path = (root / script_rel).resolve()
        if not script_path.is_file():
            raise FileNotFoundError(f"preset script not found: {script_path}")
        self.workspace_root = root
        self.preset_script = script_path
        self._command_executor = command_executor or _default_command_executor

    def build_command(
        self,
        request: RunRequestContract,
        *,
        dry_run: bool = False,
    ) -> Tuple[str, ...]:
        cmd = [
            request.runner_python,
            str(self.preset_script),
            "--mode",
            request.mode,
            "--run-id",
            request.run_id,
            "--runner-python",
            request.runner_python,
            "--cli-summary-top-n",
            str(int(request.cli_summary_top_n)),
            "--hypothesis-window-years",
            str(request.hypothesis_window_years),
            "--hypothesis-confirmatory-window-years",
            str(request.hypothesis_confirmatory_window_years),
            "--hypothesis-time-series-precheck-mode",
            str(request.hypothesis_time_series_precheck_mode),
            "--hypothesis-auto-confirmatory-policy",
            str(request.hypothesis_auto_confirmatory_policy),
            "--hypothesis-nonconfirmatory-max-tier",
            str(request.hypothesis_nonconfirmatory_max_tier),
            "--hypothesis-time-series-min-positive-events",
            str(int(request.hypothesis_time_series_min_positive_events)),
            "--hypothesis-time-series-min-track-positive-events",
            str(int(request.hypothesis_time_series_min_track_positive_events)),
            "--hypothesis-time-series-min-positive-share",
            str(float(request.hypothesis_time_series_min_positive_share)),
        ]
        if int(request.scan_n_bootstrap) > 0:
            cmd.extend(["--scan-n-bootstrap", str(int(request.scan_n_bootstrap))])
        if int(request.scan_max_features) > 0:
            cmd.extend(["--scan-max-features", str(int(request.scan_max_features))])
        if int(request.refine_n_bootstrap) > 0:
            cmd.extend(["--refine-n-bootstrap", str(int(request.refine_n_bootstrap))])
        if bool(request.paired_legacy_sync_validation):
            cmd.append("--paired-legacy-sync-validation")
        if bool(request.skip_direction_review):
            cmd.append("--skip-direction-review")
        for extra in request.extra_args:
            text = str(extra).strip()
            if text:
                cmd.extend(["--extra-arg", text])
        if dry_run:
            cmd.append("--dry-run")
        return tuple(cmd)

    def _build_expected_artifacts(self, request: RunRequestContract) -> RunArtifactsContract:
        if request.mode == "overnight_validation":
            out = _expected_overnight_outputs(request.run_id)
            return RunArtifactsContract(
                overnight_summary_json=out["overnight_summary_json"],
                overnight_checkpoint_json=out["overnight_checkpoint_json"],
            )
        if request.mode.startswith("paired_"):
            out = _expected_paired_outputs(
                run_id=request.run_id,
                skip_direction_review=bool(request.skip_direction_review),
            )
            return RunArtifactsContract(
                paired_summary_json=out["paired_summary_json"],
                direction_review_json=out["direction_review_json"],
            )
        out = _expected_scan_outputs(request.run_id)
        return RunArtifactsContract(
            scan_runs_csv=out["scan_runs_csv"],
            top_models_csv=out["top_models_csv"],
            top_models_inference_csv=out["top_models_inference_csv"],
            search_log_jsonl=out["search_log_jsonl"],
            run_summary_json=out["run_summary_json"],
            feasibility_frontier_json=out["feasibility_frontier_json"],
            feature_registry_json=out["feature_registry_json"],
            restart_stability_csv=out["restart_stability_csv"],
        )

    def _load_result_payloads(
        self,
        request: RunRequestContract,
        artifacts: RunArtifactsContract,
    ) -> Tuple[Dict[str, int], Dict[str, Any], Dict[str, str]]:
        counts: Dict[str, int] = {}
        governance_checks: Dict[str, Any] = {}
        audit_hashes: Dict[str, str] = {}

        if request.mode == "overnight_validation":
            summary_path = _normalize_path(self.workspace_root, artifacts.overnight_summary_json)
            summary_payload = _read_json_if_exists(summary_path)
            aggregate = (
                summary_payload.get("aggregate", {})
                if isinstance(summary_payload.get("aggregate"), Mapping)
                else {}
            )
            for key, value in dict(aggregate).items():
                iv = _coerce_nonnegative_int(value)
                if iv is not None:
                    counts[str(key)] = iv
            for key in ("status", "checkpoint_json"):
                text = str(summary_payload.get(key, "")).strip()
                if text:
                    governance_checks[f"overnight_{key}"] = text
            for key in ("primary_objective_gate_pass_rate", "best_metrics"):
                if key in aggregate:
                    governance_checks[str(key)] = aggregate.get(key)
            return counts, governance_checks, audit_hashes

        if request.mode.startswith("paired_"):
            paired_path = _normalize_path(self.workspace_root, artifacts.paired_summary_json)
            paired_payload = _read_json_if_exists(paired_path)
            children = paired_payload.get("children", [])
            child_rows = children if isinstance(children, list) else []
            counts["children_total"] = len(child_rows)
            counts["children_ok"] = sum(
                1
                for row in child_rows
                if isinstance(row, Mapping) and str(row.get("status", "")).strip().lower() == "ok"
            )
            counts["children_failed"] = sum(
                1
                for row in child_rows
                if isinstance(row, Mapping) and str(row.get("status", "")).strip().lower() == "failed"
            )
            if artifacts.direction_review_json:
                direction_path = _normalize_path(self.workspace_root, artifacts.direction_review_json)
                direction_payload = _read_json_if_exists(direction_path)
                checks = (
                    direction_payload.get("checks", {})
                    if isinstance(direction_payload.get("checks"), Mapping)
                    else {}
                )
                governance_checks.update({str(k): v for k, v in dict(checks).items()})
            return counts, governance_checks, audit_hashes

        run_summary_path = _normalize_path(self.workspace_root, artifacts.run_summary_json)
        run_summary = _read_json_if_exists(run_summary_path)
        raw_counts = run_summary.get("counts", {})
        if isinstance(raw_counts, Mapping):
            for k, v in raw_counts.items():
                iv = _coerce_nonnegative_int(v)
                if iv is not None:
                    counts[str(k)] = iv

        for key in (
            "search_governance",
            "gate_meta",
            "track_consensus_meta",
            "time_series_precheck",
            "time_series_precheck_policy",
            "validated_gate_meta",
        ):
            val = run_summary.get(key)
            if isinstance(val, Mapping):
                governance_checks[str(key)] = dict(val)

        raw_audit = run_summary.get("audit_hashes", {})
        if isinstance(raw_audit, Mapping):
            audit_hashes = {str(k): str(v) for k, v in raw_audit.items()}

        return counts, governance_checks, audit_hashes

    def execute(
        self,
        request: RunRequestContract,
        *,
        dry_run: bool = False,
    ) -> EngineExecution:
        started_at = _utc_now_isoz()
        cmd = self.build_command(request, dry_run=dry_run)
        result = self._command_executor(cmd, self.workspace_root)

        state = "succeeded" if int(result.returncode) == 0 else "failed"
        error = None
        if state == "failed":
            error = RunErrorContract.create(
                code="PRESET_RUNNER_FAILED",
                message=f"preset runner exited with code {int(result.returncode)}",
                retryable=False,
                details={
                    "returncode": int(result.returncode),
                    "stderr_tail": str(result.stderr)[-4000:],
                },
            )

        status = RunStatusContract.create(
            run_id=request.run_id,
            mode=request.mode,
            state=state,
            created_at_utc=started_at,
            updated_at_utc=_utc_now_isoz(),
            progress_stage="completed",
            progress_message="dry-run completed" if dry_run else "execution completed",
            progress_fraction=1.0,
            error=error,
        )

        artifacts = self._build_expected_artifacts(request)
        counts, governance_checks, audit_hashes = self._load_result_payloads(request, artifacts)
        if dry_run:
            governance_checks["dry_run"] = True
        if request.idempotency_key:
            governance_checks["idempotency_key"] = request.idempotency_key

        run_result = RunResultContract.create(
            run_id=request.run_id,
            mode=request.mode,
            state=state,
            artifacts=artifacts,
            counts=counts,
            governance_checks=governance_checks,
            audit_hashes=audit_hashes,
        )
        return EngineExecution(
            request=request,
            command=cmd,
            returncode=int(result.returncode),
            status=status,
            result=run_result,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    def run_nooption(
        self,
        *,
        run_id: str,
        dry_run: bool = False,
        **payload_overrides: Any,
    ) -> EngineExecution:
        payload: Dict[str, Any] = {
            "mode": "nooption_baseline",
            "run_id": str(run_id),
        }
        payload.update(payload_overrides)
        request = RunRequestContract.from_payload(payload)
        return self.execute(request, dry_run=dry_run)

    def run_singlex(
        self,
        *,
        run_id: str,
        dry_run: bool = False,
        **payload_overrides: Any,
    ) -> EngineExecution:
        payload: Dict[str, Any] = {
            "mode": "singlex_baseline",
            "run_id": str(run_id),
        }
        payload.update(payload_overrides)
        request = RunRequestContract.from_payload(payload)
        return self.execute(request, dry_run=dry_run)

    def run_paired(
        self,
        *,
        run_id: str,
        dry_run: bool = False,
        **payload_overrides: Any,
    ) -> EngineExecution:
        payload: Dict[str, Any] = {
            "mode": "paired_nooption_singlex",
            "run_id": str(run_id),
        }
        payload.update(payload_overrides)
        request = RunRequestContract.from_payload(payload)
        return self.execute(request, dry_run=dry_run)
