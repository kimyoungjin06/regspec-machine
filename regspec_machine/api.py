"""L4 API layer over RunOrchestrator.

This module keeps the request/status/result payloads aligned with
`regspec_machine.contracts` while exposing a small FastAPI surface for
agent/UI integrations.
"""

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time
from typing import Any, Dict, List, Mapping, Optional

from .contracts import (
    RUN_MODES,
    RunArtifactsContract,
    RunRequestContract,
    RunResultContract,
    RunStatusContract,
)
from .dataset_profile import profile_dataset_csv
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


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_bool(value: Any) -> Optional[bool]:
    if value is True:
        return True
    if value is False:
        return False
    if value in (1, "1", "true", "TRUE", "True"):
        return True
    if value in (0, "0", "false", "FALSE", "False"):
        return False
    return None


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(text).strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "run"


def _parse_csv_list(text: str) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    rows = []
    seen = set()
    for part in raw.split(","):
        item = str(part).strip()
        if not item:
            continue
        norm = item.lower()
        if norm in seen:
            continue
        seen.add(norm)
        rows.append(item)
    return rows


def _default_dataset_config() -> Dict[str, Any]:
    return {
        "dataset_path": "",
        "run_id": "",
        "artifact_key": "auto",
        "sample_rows": 20000,
        "top_n": 20,
        "research_mode": True,
        "fixed_y": "",
        "exclude_x_cols": "",
    }


def _dataset_config_path(*, workspace_root: Path) -> Path:
    return workspace_root / "data" / "metadata" / "regspec_machine_dataset_profile_config.json"


def _normalize_dataset_config(raw: Mapping[str, Any]) -> Dict[str, Any]:
    base = _default_dataset_config()
    out: Dict[str, Any] = {}
    out["dataset_path"] = str(raw.get("dataset_path", base["dataset_path"]) or "").strip()
    out["run_id"] = str(raw.get("run_id", base["run_id"]) or "").strip()
    artifact_key = str(raw.get("artifact_key", base["artifact_key"]) or "auto").strip().lower()
    if artifact_key not in {"auto", "scan_runs_csv", "top_models_inference_csv", "top_models_csv"}:
        raise ValueError(
            "artifact_key must be one of: auto, scan_runs_csv, top_models_inference_csv, top_models_csv"
        )
    out["artifact_key"] = artifact_key

    try:
        sample_rows = int(raw.get("sample_rows", base["sample_rows"]))
    except Exception as exc:
        raise ValueError("sample_rows must be an integer") from exc
    if sample_rows < 100 or sample_rows > 500000:
        raise ValueError("sample_rows must be in [100, 500000]")
    out["sample_rows"] = sample_rows

    try:
        top_n = int(raw.get("top_n", base["top_n"]))
    except Exception as exc:
        raise ValueError("top_n must be an integer") from exc
    if top_n < 1 or top_n > 100:
        raise ValueError("top_n must be in [1, 100]")
    out["top_n"] = top_n

    out["research_mode"] = bool(raw.get("research_mode", base["research_mode"]))
    out["fixed_y"] = str(raw.get("fixed_y", base["fixed_y"]) or "").strip()
    out["exclude_x_cols"] = str(raw.get("exclude_x_cols", base["exclude_x_cols"]) or "").strip()
    return out


def _is_nooption_mode(mode: str) -> bool:
    text = str(mode or "").strip().lower()
    return text in {"nooption", "nooption_baseline", "nooption_hypothesis_panel"}


def _is_singlex_mode(mode: str) -> bool:
    text = str(mode or "").strip().lower()
    return text in {"singlex", "singlex_baseline", "singlex_hypothesis_panel"}


def _infer_mode_from_run_id(run_id: str) -> str:
    rid = str(run_id or "").strip().lower()
    if rid.endswith("__singlex_hypothesis_panel") or "singlex_hypothesis_panel" in rid:
        return "singlex_hypothesis_panel"
    if rid.endswith("__nooption_hypothesis_panel") or "nooption_hypothesis_panel" in rid:
        return "nooption_hypothesis_panel"
    if rid.endswith("__singlex") or "singlex" in rid:
        return "singlex_baseline"
    if rid.endswith("__nooption_baseline") or "nooption" in rid:
        return "nooption_baseline"
    if "openexplore_autorefine" in rid:
        return "openexplore_autorefine"
    if "openexplore" in rid:
        return "openexplore"
    return "nooption_baseline"


def _infer_state(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"queued", "running", "succeeded", "failed", "cancelled"}:
        return text
    if text in {"ok", "success", "passed", "done"}:
        return "succeeded"
    if text in {"partial_failure", "error", "failed_exception"}:
        return "failed"
    if text in {"canceled"}:
        return "cancelled"
    return "succeeded"


def _extract_timestamp(payload: Mapping[str, Any]) -> str:
    for key in ("timestamp_utc_finished", "timestamp_utc", "timestamp", "generated_at_utc"):
        text = str(payload.get(key, "")).strip()
        if text:
            return text
    return _utc_now_isoz()


def _extract_int_counts(counts: Any) -> Dict[str, int]:
    if not isinstance(counts, Mapping):
        return {}
    out: Dict[str, int] = {}
    for key, value in counts.items():
        if isinstance(value, bool):
            continue
        try:
            n = int(value)
        except Exception:
            continue
        if n < 0:
            continue
        out[str(key)] = n
    return out


def _extract_governance_checks(payload: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    search_governance = payload.get("search_governance")
    if isinstance(search_governance, Mapping):
        out["search_governance"] = dict(search_governance)
    else:
        validation_used_for_search = payload.get("validation_used_for_search")
        candidate_pool_locked = payload.get("candidate_pool_locked_pre_validation")
        if validation_used_for_search is not None or candidate_pool_locked is not None:
            out["search_governance"] = {
                "validation_used_for_search": validation_used_for_search,
                "candidate_pool_locked_pre_validation": candidate_pool_locked,
            }

    track_consensus_meta = payload.get("track_consensus_meta")
    if isinstance(track_consensus_meta, Mapping):
        out["track_consensus_meta"] = dict(track_consensus_meta)

    direction_review_checks = payload.get("direction_review_checks")
    if isinstance(direction_review_checks, Mapping):
        out.update({str(k): v for k, v in direction_review_checks.items()})

    checks = payload.get("checks")
    if isinstance(checks, Mapping):
        out.update({str(k): v for k, v in checks.items()})

    return out


def _extract_artifacts(
    *,
    payload: Mapping[str, Any],
    source_path: Path,
    source_kind: str,
) -> Dict[str, str]:
    allowed = set(RunArtifactsContract().as_dict().keys())
    out: Dict[str, str] = {}
    outputs = payload.get("outputs")
    if isinstance(outputs, Mapping):
        for key in allowed:
            text = str(outputs.get(key, "")).strip()
            if text:
                out[key] = text

    if source_kind == "run_summary":
        out.setdefault("run_summary_json", str(source_path))
    if source_kind == "paired_summary":
        out.setdefault("paired_summary_json", str(source_path))
        direction_review_json = str(payload.get("direction_review_json", "")).strip()
        if direction_review_json:
            out["direction_review_json"] = direction_review_json

    return out


def _history_entry_from_payload(
    *,
    payload: Mapping[str, Any],
    source_path: Path,
    source_kind: str,
) -> Optional[Dict[str, Any]]:
    run_id = str(payload.get("run_id", "")).strip()
    if not run_id:
        return None

    mode_text = str(payload.get("mode", "")).strip()
    mode = mode_text if mode_text in RUN_MODES else _infer_mode_from_run_id(run_id)
    if mode not in RUN_MODES:
        return None

    state = _infer_state(payload.get("status"))
    timestamp = _extract_timestamp(payload)
    counts = _extract_int_counts(payload.get("counts"))
    governance_checks = _extract_governance_checks(payload)

    try:
        audit_hashes_raw = payload.get("audit_hashes")
        audit_hashes = dict(audit_hashes_raw) if isinstance(audit_hashes_raw, Mapping) else {}
        artifacts = RunArtifactsContract(
            **_extract_artifacts(payload=payload, source_path=source_path, source_kind=source_kind)
        )
        result = RunResultContract.create(
            run_id=run_id,
            mode=mode,
            state=state,
            artifacts=artifacts,
            counts=counts,
            governance_checks=governance_checks,
            audit_hashes=audit_hashes,
            timestamp_utc=timestamp,
        )
        status = RunStatusContract.create(
            run_id=run_id,
            mode=mode,
            state=state,
            created_at_utc=timestamp,
            updated_at_utc=timestamp,
            progress_stage="restored",
            progress_message=f"restored from {source_kind}",
            progress_fraction=1.0,
        )
        request = RunRequestContract.from_payload({"mode": mode, "run_id": run_id})
    except Exception:
        return None
    return {
        "request": request,
        "status": status,
        "result": result,
        "source_kind": source_kind,
    }


def _scan_history_entries(*, workspace_root: Path) -> Dict[str, Dict[str, Any]]:
    meta_root = workspace_root / "data" / "metadata"
    if not meta_root.is_dir():
        return {}

    rows: Dict[str, Dict[str, Any]] = {}
    file_specs = [
        ("run_summary", "phase_b_bikard_machine_scientist_run_summary_*.json"),
        ("paired_summary", "phase_b_bikard_machine_scientist_paired_preset_summary_*.json"),
    ]
    for source_kind, pattern in file_specs:
        for path in sorted(meta_root.glob(pattern)):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, Mapping):
                continue
            entry = _history_entry_from_payload(
                payload=payload,
                source_path=path.resolve(),
                source_kind=source_kind,
            )
            if entry is None:
                continue
            run_id = str(entry["request"].run_id)
            prev = rows.get(run_id)
            if prev is None or str(entry["status"].updated_at_utc) >= str(prev["status"].updated_at_utc):
                rows[run_id] = entry
    return rows


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
                status_validation = str(row.get("status_validation", "")).strip().lower()
                has_status_validation = str(row.get("status_validation", "")).strip() != ""
                is_validation_ok = (not has_status_validation) or status_validation in {
                    "ok",
                    "pass",
                    "passed",
                    "success",
                }
                if not is_validation_ok:
                    continue

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


def _extract_compare_branch(review: Mapping[str, Any]) -> Dict[str, Any]:
    metrics = review.get("metrics", {})
    governance = review.get("governance", {})
    return {
        "run_id": str(review.get("run_id", "")),
        "mode": str(review.get("mode", "")),
        "state": str(review.get("state", "")),
        "validated_candidate_count": _safe_int(metrics.get("validated_candidate_count"), 0),
        "support_candidate_count": _safe_int(metrics.get("support_candidate_count"), 0),
        "best_p_validation": _safe_float(metrics.get("best_p_validation")),
        "best_q_validation": _safe_float(metrics.get("best_q_validation")),
        "restart_validated_rate_max": _safe_float(metrics.get("restart_validated_rate_max")),
        "restart_validated_rate_mean": _safe_float(metrics.get("restart_validated_rate_mean")),
        "validation_used_for_search_false": bool(governance.get("validation_used_for_search_false") is True),
        "candidate_pool_locked_pre_validation_true": bool(
            governance.get("candidate_pool_locked_pre_validation_true") is True
        ),
        "track_consensus_enforced": _safe_bool(governance.get("track_consensus_enforced")),
    }


def _build_compare_checks(
    *,
    nooption_branch: Mapping[str, Any],
    singlex_branch: Mapping[str, Any],
) -> Dict[str, Any]:
    no_state_ok = str(nooption_branch.get("state", "")).strip().lower() == "succeeded"
    sx_state_ok = str(singlex_branch.get("state", "")).strip().lower() == "succeeded"
    both_succeeded = bool(no_state_ok and sx_state_ok)

    no_gov_pass = bool(
        nooption_branch.get("validation_used_for_search_false") is True
        and nooption_branch.get("candidate_pool_locked_pre_validation_true") is True
    )
    sx_gov_pass = bool(
        singlex_branch.get("validation_used_for_search_false") is True
        and singlex_branch.get("candidate_pool_locked_pre_validation_true") is True
    )
    all_governance_pass = bool(no_gov_pass and sx_gov_pass)

    no_validated_gate = _safe_int(nooption_branch.get("validated_candidate_count"), 0) > 0
    no_q = _safe_float(nooption_branch.get("best_q_validation"))
    no_q_gate = bool(no_q is not None and no_q <= 0.10)
    no_restart = _safe_float(nooption_branch.get("restart_validated_rate_max"))
    no_restart_gate = bool(no_restart is not None and no_restart >= 0.50)

    singlex_consensus = bool(singlex_branch.get("track_consensus_enforced") is True)

    nooption_promotion_gate_pass = bool(
        all_governance_pass and no_validated_gate and no_q_gate and no_restart_gate
    )
    primary_objective_gate_pass = bool(
        both_succeeded and all_governance_pass and singlex_consensus and nooption_promotion_gate_pass
    )

    return {
        "both_succeeded": both_succeeded,
        "nooption_governance_pass": no_gov_pass,
        "singlex_governance_pass": sx_gov_pass,
        "all_governance_pass": all_governance_pass,
        "singlex_track_consensus_check_pass": singlex_consensus,
        "nooption_primary_validated_gate_pass": no_validated_gate,
        "nooption_q_gate_pass": no_q_gate,
        "nooption_restart_validated_rate_gate_pass": no_restart_gate,
        "nooption_promotion_gate_pass": nooption_promotion_gate_pass,
        "primary_objective_gate_pass": primary_objective_gate_pass,
    }


def _build_compare_payload(
    *,
    nooption_review: Mapping[str, Any],
    singlex_review: Mapping[str, Any],
) -> Dict[str, Any]:
    nooption_branch = _extract_compare_branch(nooption_review)
    singlex_branch = _extract_compare_branch(singlex_review)
    checks = _build_compare_checks(
        nooption_branch=nooption_branch,
        singlex_branch=singlex_branch,
    )
    return {
        "generated_at_utc": _utc_now_isoz(),
        "nooption": nooption_branch,
        "singlex": singlex_branch,
        "checks": checks,
    }


def _render_compare_markdown(payload: Mapping[str, Any]) -> str:
    nooption = payload.get("nooption", {})
    singlex = payload.get("singlex", {})
    checks = payload.get("checks", {})
    lines = [
        "# Baseline Compare Summary",
        "",
        f"- generated_at_utc: {payload.get('generated_at_utc', '-')}",
        f"- nooption_run_id: {nooption.get('run_id', '-')}",
        f"- singlex_run_id: {singlex.get('run_id', '-')}",
        "",
        "## Checks",
        "",
    ]
    for key in (
        "both_succeeded",
        "nooption_governance_pass",
        "singlex_governance_pass",
        "all_governance_pass",
        "singlex_track_consensus_check_pass",
        "nooption_primary_validated_gate_pass",
        "nooption_q_gate_pass",
        "nooption_restart_validated_rate_gate_pass",
        "nooption_promotion_gate_pass",
        "primary_objective_gate_pass",
    ):
        lines.append(f"- {key}: {checks.get(key)}")
    lines.extend(
        [
            "",
            "## Branch Metrics",
            "",
            "| metric | nooption | singlex |",
            "| --- | --- | --- |",
        ]
    )
    for key in (
        "state",
        "validated_candidate_count",
        "support_candidate_count",
        "best_p_validation",
        "best_q_validation",
        "restart_validated_rate_max",
        "restart_validated_rate_mean",
        "validation_used_for_search_false",
        "candidate_pool_locked_pre_validation_true",
        "track_consensus_enforced",
    ):
        lines.append(f"| {key} | {nooption.get(key)} | {singlex.get(key)} |")
    lines.append("")
    return "\n".join(lines)


def _write_compare_exports(
    *,
    workspace_root: Path,
    payload: Mapping[str, Any],
) -> Dict[str, str]:
    no_id = _slug(str(payload.get("nooption", {}).get("run_id", "")))
    sx_id = _slug(str(payload.get("singlex", {}).get("run_id", "")))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = f"compare_{no_id}__vs__{sx_id}_{stamp}"
    out_dir = workspace_root / "outputs" / "reports" / "regspec_compare"
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"{base}.json"
    md_path = out_dir / f"{base}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_compare_markdown(payload), encoding="utf-8")

    return {
        "output_dir": str(out_dir),
        "json": str(json_path),
        "markdown": str(md_path),
    }


def _report_roots(*, workspace_root: Path) -> Dict[str, Path]:
    base = workspace_root / "outputs" / "reports"
    return {
        "regspec_compare": base / "regspec_compare",
        "regspec_dataset_profile_compare": base / "regspec_dataset_profile_compare",
    }


def _list_saved_reports(
    *,
    workspace_root: Path,
    kind: str,
    limit: int,
) -> List[Dict[str, Any]]:
    kind_text = str(kind or "all").strip().lower()
    allowed = _report_roots(workspace_root=workspace_root)
    if kind_text not in {"all", *allowed.keys()}:
        raise ValueError(
            "kind must be one of: all, regspec_compare, regspec_dataset_profile_compare"
        )

    rows: List[Dict[str, Any]] = []
    roots = allowed.items() if kind_text == "all" else [(kind_text, allowed[kind_text])]
    for report_kind, root in roots:
        if not root.is_dir():
            continue
        for path in root.iterdir():
            if not path.is_file():
                continue
            suffix = path.suffix.lower()
            if suffix not in {".json", ".md"}:
                continue
            stat = path.stat()
            modified_utc = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            rows.append(
                {
                    "kind": report_kind,
                    "file_name": path.name,
                    "relative_path": str(path.resolve().relative_to(workspace_root.resolve())),
                    "size_bytes": int(stat.st_size),
                    "modified_at_utc": modified_utc,
                }
            )

    rows_sorted = sorted(rows, key=lambda r: (str(r["modified_at_utc"]), str(r["relative_path"])), reverse=True)
    return rows_sorted[: max(1, int(limit))]


def _resolve_saved_report_path(
    *,
    workspace_root: Path,
    relative_path: str,
) -> Path:
    rel_text = str(relative_path or "").strip()
    if not rel_text:
        raise ValueError("relative_path is required")
    rel_path = Path(rel_text)
    if rel_path.is_absolute():
        raise ValueError("relative_path must be workspace-relative")

    reports_root = (workspace_root / "outputs" / "reports").resolve()
    path = (workspace_root / rel_path).resolve()
    if not (path == reports_root or reports_root in path.parents):
        raise ValueError("relative_path must be under outputs/reports")
    if path.suffix.lower() not in {".json", ".md"}:
        raise ValueError("relative_path must end with .json or .md")
    if not path.is_file():
        raise FileNotFoundError(f"saved report not found: {rel_path}")
    return path


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

    history_cache: Dict[str, Any] = {
        "loaded_at_monotonic": 0.0,
        "rows": {},
    }

    def _load_history_rows(*, force: bool = False) -> Dict[str, Dict[str, Any]]:
        now = time.monotonic()
        if not force and (now - float(history_cache["loaded_at_monotonic"])) <= 5.0:
            return dict(history_cache["rows"])
        rows = _scan_history_entries(workspace_root=_workspace_root())
        history_cache["rows"] = rows
        history_cache["loaded_at_monotonic"] = now
        return dict(rows)

    def _resolve_run_entry(run_id: str) -> Optional[Dict[str, Any]]:
        status = orch.get_status(run_id)
        if status is not None:
            result = orch.get_result(run_id)
            mode = str(status.mode)
            try:
                request = RunRequestContract.from_payload({"mode": mode, "run_id": str(run_id)})
            except Exception:
                request = RunRequestContract.from_payload(
                    {"mode": _infer_mode_from_run_id(run_id), "run_id": str(run_id)}
                )
            return {
                "request": request,
                "status": status,
                "result": result,
                "source_kind": "live",
            }
        rows = _load_history_rows()
        entry = rows.get(str(run_id))
        if entry is not None:
            return entry
        rows = _load_history_rows(force=True)
        return rows.get(str(run_id))

    def _entry_to_list_row(entry: Mapping[str, Any]) -> Dict[str, Any]:
        request = entry.get("request")
        status = entry.get("status")
        result = entry.get("result")
        returncode = entry.get("returncode")
        if request is None or status is None:
            raise ValueError("invalid run entry: request/status missing")
        result_counts = result.counts if result is not None else {}
        return {
            "run_id": request.run_id,
            "mode": request.mode,
            "state": status.state,
            "attempt": int(status.attempt),
            "created_at_utc": status.created_at_utc,
            "updated_at_utc": status.updated_at_utc,
            "progress_stage": status.progress_stage,
            "progress_message": status.progress_message,
            "progress_fraction": status.progress_fraction,
            "returncode": returncode,
            "has_result": result is not None,
            "counts": result_counts,
            "source": str(entry.get("source_kind", "live")),
        }

    dataset_profile_cache: Dict[str, Any] = {
        "rows": {},
    }

    def _iter_entries_latest(*, include_history: bool = True) -> List[Dict[str, Any]]:
        seen_run_ids = set()
        out: List[Dict[str, Any]] = []

        live_rows = orch.list_snapshots(state="")
        for row in live_rows:
            item = {
                "request": row.request,
                "status": row.status,
                "result": row.result,
                "returncode": row.returncode,
                "source_kind": "live",
            }
            out.append(item)
            seen_run_ids.add(str(row.request.run_id))

        if include_history:
            for entry in _load_history_rows().values():
                run_id_text = str(entry.get("request").run_id) if entry.get("request") is not None else ""
                if not run_id_text or run_id_text in seen_run_ids:
                    continue
                out.append(dict(entry))
                seen_run_ids.add(run_id_text)

        out_sorted = sorted(
            out,
            key=lambda entry: str(entry.get("status").updated_at_utc if entry.get("status") is not None else ""),
            reverse=True,
        )
        return out_sorted

    def _resolve_dataset_path_from_entry(
        *,
        entry: Mapping[str, Any],
        artifact_key: str,
    ) -> Optional[Dict[str, Any]]:
        result = entry.get("result")
        request = entry.get("request")
        if result is None or request is None:
            return None
        artifact_map = result.artifacts.as_dict()
        artifact_key_text = str(artifact_key or "auto").strip().lower()
        allowed_keys = (
            "scan_runs_csv",
            "top_models_inference_csv",
            "top_models_csv",
        )
        if artifact_key_text == "auto":
            ordered_keys = list(allowed_keys)
        else:
            if artifact_key_text not in allowed_keys:
                return None
            ordered_keys = [artifact_key_text]

        for key in ordered_keys:
            resolved = _resolve_artifact_path(
                workspace_root=_workspace_root(),
                raw=str(artifact_map.get(key, "")),
            )
            if resolved is not None and resolved.is_file():
                return {
                    "dataset_path": resolved,
                    "artifact_key": key,
                    "run_id": str(request.run_id),
                    "source": str(entry.get("source_kind", "live")),
                }

        for key in ordered_keys:
            resolved = _resolve_artifact_path(
                workspace_root=_workspace_root(),
                raw=str(artifact_map.get(key, "")),
            )
            if resolved is not None:
                return {
                    "dataset_path": resolved,
                    "artifact_key": key,
                    "run_id": str(request.run_id),
                    "source": str(entry.get("source_kind", "live")),
                }
        return None

    def _resolve_dataset_path_request(
        *,
        dataset_path: str,
        run_id: str,
        artifact_key: str,
    ) -> Optional[Dict[str, Any]]:
        path_text = str(dataset_path or "").strip()
        if path_text:
            p = Path(path_text)
            if not p.is_absolute():
                p = (_workspace_root() / p).resolve()
            else:
                p = p.resolve()
            return {
                "dataset_path": p,
                "artifact_key": "direct_path",
                "run_id": str(run_id or "").strip(),
                "source": "user",
            }

        run_id_text = str(run_id or "").strip()
        artifact_key_text = str(artifact_key or "auto").strip().lower()
        if run_id_text:
            entry = _resolve_run_entry(run_id_text)
            if entry is None:
                return None
            return _resolve_dataset_path_from_entry(entry=entry, artifact_key=artifact_key_text)

        for entry in _iter_entries_latest(include_history=True):
            resolved = _resolve_dataset_path_from_entry(entry=entry, artifact_key=artifact_key_text)
            if resolved is not None:
                return resolved
        return None

    def _cached_dataset_profile(
        *,
        dataset_path: Path,
        sample_rows: int,
        top_n: int,
        research_mode: bool,
        fixed_y: str,
        exclude_x_cols: List[str],
    ) -> Dict[str, Any]:
        stat = dataset_path.stat()
        fixed_y_text = str(fixed_y or "").strip()
        exclude_x_norm = sorted({str(x).strip().lower() for x in exclude_x_cols if str(x).strip()})
        cache_key = "|".join(
            [
                str(dataset_path),
                str(int(stat.st_mtime_ns)),
                str(int(stat.st_size)),
                str(int(sample_rows)),
                str(int(top_n)),
                "research=1" if research_mode else "research=0",
                f"fixed_y={fixed_y_text.lower()}",
                f"exclude_x={','.join(exclude_x_norm)}",
            ]
        )
        row = dataset_profile_cache["rows"].get(cache_key)
        if row is not None:
            cached_payload = dict(row)
            cached_payload["cache_hit"] = True
            return cached_payload
        payload = profile_dataset_csv(
            dataset_path=dataset_path,
            sample_rows=int(sample_rows),
            top_n=int(top_n),
            research_mode=bool(research_mode),
            fixed_y=fixed_y_text,
            exclude_x_cols=list(exclude_x_cols),
        )
        payload["cache_hit"] = False
        dataset_profile_cache["rows"] = {cache_key: payload}
        return dict(payload)

    app = FastAPI(
        title="regspec-machine API",
        version="0.1.0",
        description="L4 execution API for nooption/singlex/paired run orchestration.",
    )
    app.state.orchestrator = orch

    @app.get("/healthz")
    def healthz() -> Dict[str, Any]:
        live_count = len(orch.list_snapshots())
        history_count = len(_load_history_rows())
        return {
            "ok": True,
            "timestamp_utc": _utc_now_isoz(),
            "run_count": int(live_count),
            "history_run_count": int(history_count),
        }

    @app.get("/runs")
    def list_runs(
        state: str = Query(default=""),
        mode: str = Query(default=""),
        run_id_contains: str = Query(default=""),
        include_history: bool = Query(default=True),
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=200, ge=1, le=1000),
    ) -> Dict[str, Any]:
        state_filter = str(state).strip().lower()
        mode_filter = str(mode).strip().lower()
        run_like_filter = str(run_id_contains).strip().lower()
        seen_run_ids = set()
        payload_rows: List[Dict[str, Any]] = []

        live_rows = orch.list_snapshots(state="")
        for row in live_rows:
            item = _entry_to_list_row(
                {
                    "request": row.request,
                    "status": row.status,
                    "result": row.result,
                    "returncode": row.returncode,
                    "source_kind": "live",
                }
            )
            payload_rows.append(item)
            seen_run_ids.add(str(item["run_id"]))

        if include_history:
            for entry in _load_history_rows().values():
                row = _entry_to_list_row(entry)
                run_id_text = str(row["run_id"])
                if run_id_text in seen_run_ids:
                    continue
                payload_rows.append(row)
                seen_run_ids.add(run_id_text)

        filtered_rows: List[Dict[str, Any]] = []
        for row in payload_rows:
            row_state = str(row.get("state", "")).strip().lower()
            row_mode = str(row.get("mode", "")).strip().lower()
            row_run_id = str(row.get("run_id", "")).strip().lower()
            if state_filter and row_state != state_filter:
                continue
            if mode_filter and row_mode != mode_filter:
                continue
            if run_like_filter and run_like_filter not in row_run_id:
                continue
            filtered_rows.append(row)

        rows_sorted = sorted(
            filtered_rows,
            key=lambda row: str(row.get("updated_at_utc", "")),
            reverse=True,
        )
        begin = int(offset)
        end = begin + int(limit)
        page_rows = rows_sorted[begin:end]
        return {
            "state_filter": state_filter,
            "mode_filter": mode_filter,
            "run_id_contains_filter": run_like_filter,
            "include_history": bool(include_history),
            "offset": int(offset),
            "limit": int(limit),
            "rows": page_rows,
            "total_rows": len(page_rows),
            "total_available": len(rows_sorted),
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
        entry = _resolve_run_entry(run_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        status = entry["status"]
        return {"status": status.as_dict(), "source": str(entry.get("source_kind", "live"))}

    @app.get("/runs/{run_id}/result")
    def get_run_result(run_id: str) -> Any:
        entry = _resolve_run_entry(run_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        status = entry["status"]
        result = entry["result"]
        if result is None:
            return JSONResponse(
                status_code=202,
                content={
                    "status": status.as_dict(),
                    "result": None,
                    "source": str(entry.get("source_kind", "live")),
                },
            )
        return {
            "status": status.as_dict(),
            "result": result.as_dict(),
            "source": str(entry.get("source_kind", "live")),
        }

    @app.get("/runs/{run_id}/summary")
    def get_run_summary(run_id: str) -> Any:
        entry = _resolve_run_entry(run_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        status = entry["status"]
        result = entry["result"]
        if result is None:
            return JSONResponse(
                status_code=202,
                content={
                    "status": status.as_dict(),
                    "summary": None,
                    "source": str(entry.get("source_kind", "live")),
                },
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
            "source": str(entry.get("source_kind", "live")),
        }

    @app.get("/runs/{run_id}/review")
    def get_run_review(run_id: str) -> Any:
        entry = _resolve_run_entry(run_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        status = entry["status"]
        result = entry["result"]
        if result is None:
            return JSONResponse(
                status_code=202,
                content={
                    "status": status.as_dict(),
                    "review": None,
                    "source": str(entry.get("source_kind", "live")),
                },
            )
        result_payload = result.as_dict()
        review = _build_review_payload(
            workspace_root=_workspace_root(),
            result_payload=result_payload,
        )
        return {
            "status": status.as_dict(),
            "review": review,
            "source": str(entry.get("source_kind", "live")),
        }

    @app.post("/compare/export")
    def export_compare(payload: Dict[str, Any]) -> Dict[str, Any]:
        nooption_run_id = str(payload.get("nooption_run_id", "")).strip()
        singlex_run_id = str(payload.get("singlex_run_id", "")).strip()
        if not nooption_run_id or not singlex_run_id:
            raise HTTPException(
                status_code=422,
                detail="nooption_run_id and singlex_run_id are required",
            )

        nooption_entry = _resolve_run_entry(nooption_run_id)
        if nooption_entry is None:
            raise HTTPException(status_code=404, detail=f"run not found: {nooption_run_id}")
        singlex_entry = _resolve_run_entry(singlex_run_id)
        if singlex_entry is None:
            raise HTTPException(status_code=404, detail=f"run not found: {singlex_run_id}")

        nooption_status = nooption_entry["status"]
        singlex_status = singlex_entry["status"]
        if not _is_nooption_mode(nooption_status.mode):
            raise HTTPException(
                status_code=422,
                detail=f"nooption_run_id mode mismatch: expected nooption*, got {nooption_status.mode}",
            )
        if not _is_singlex_mode(singlex_status.mode):
            raise HTTPException(
                status_code=422,
                detail=f"singlex_run_id mode mismatch: expected singlex*, got {singlex_status.mode}",
            )

        nooption_result = nooption_entry["result"]
        if nooption_result is None:
            raise HTTPException(status_code=409, detail=f"run result not ready: {nooption_run_id}")
        singlex_result = singlex_entry["result"]
        if singlex_result is None:
            raise HTTPException(status_code=409, detail=f"run result not ready: {singlex_run_id}")

        workspace = _workspace_root()
        nooption_review = _build_review_payload(
            workspace_root=workspace,
            result_payload=nooption_result.as_dict(),
        )
        singlex_review = _build_review_payload(
            workspace_root=workspace,
            result_payload=singlex_result.as_dict(),
        )
        compare_payload = _build_compare_payload(
            nooption_review=nooption_review,
            singlex_review=singlex_review,
        )
        files = _write_compare_exports(workspace_root=workspace, payload=compare_payload)
        return {
            "nooption_run_id": nooption_run_id,
            "singlex_run_id": singlex_run_id,
            "comparison": compare_payload,
            "outputs": files,
            "sources": {
                "nooption": str(nooption_entry.get("source_kind", "live")),
                "singlex": str(singlex_entry.get("source_kind", "live")),
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
        entry = _resolve_run_entry(run_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        status = entry["status"]
        result = entry["result"]
        if result is None:
            return JSONResponse(
                status_code=202,
                content={
                    "status": status.as_dict(),
                    "artifacts": None,
                    "source": str(entry.get("source_kind", "live")),
                },
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
            "source": str(entry.get("source_kind", "live")),
        }

    @app.get("/datasets/candidates")
    def get_dataset_candidates(limit: int = Query(default=30, ge=1, le=200)) -> Dict[str, Any]:
        rows = []
        for entry in _iter_entries_latest(include_history=True):
            request = entry.get("request")
            if request is None:
                continue
            for artifact_key in ("scan_runs_csv", "top_models_inference_csv", "top_models_csv"):
                resolved = _resolve_dataset_path_from_entry(entry=entry, artifact_key=artifact_key)
                if resolved is None:
                    continue
                p = resolved.get("dataset_path")
                if p is None:
                    continue
                rows.append(
                    {
                        "run_id": str(resolved.get("run_id", "")),
                        "mode": str(request.mode),
                        "artifact_key": str(resolved.get("artifact_key", "")),
                        "dataset_path": str(p),
                        "exists": bool(Path(p).is_file()),
                        "source": str(resolved.get("source", "live")),
                        "updated_at_utc": str(entry.get("status").updated_at_utc if entry.get("status") else ""),
                    }
                )
                break
            if len(rows) >= int(limit):
                break
        return {
            "rows": rows[: int(limit)],
            "total_rows": len(rows[: int(limit)]),
        }

    @app.get("/datasets/profile")
    def get_dataset_profile(
        dataset_path: str = Query(default=""),
        run_id: str = Query(default=""),
        artifact_key: str = Query(default="auto"),
        sample_rows: int = Query(default=20000, ge=100, le=500000),
        top_n: int = Query(default=20, ge=1, le=100),
        research_mode: bool = Query(default=True),
        fixed_y: str = Query(default=""),
        exclude_x_cols: str = Query(default=""),
    ) -> Dict[str, Any]:
        artifact_key_text = str(artifact_key or "auto").strip().lower()
        if artifact_key_text not in {"auto", "scan_runs_csv", "top_models_inference_csv", "top_models_csv"}:
            raise HTTPException(
                status_code=422,
                detail="artifact_key must be one of: auto, scan_runs_csv, top_models_inference_csv, top_models_csv",
            )

        resolved = _resolve_dataset_path_request(
            dataset_path=str(dataset_path or "").strip(),
            run_id=str(run_id or "").strip(),
            artifact_key=artifact_key_text,
        )
        if resolved is None:
            raise HTTPException(
                status_code=404,
                detail="dataset path could not be resolved from request; provide dataset_path or valid run_id",
            )
        path = resolved["dataset_path"]
        if not Path(path).is_file():
            raise HTTPException(status_code=404, detail=f"dataset file not found: {path}")

        fixed_y_text = str(fixed_y or "").strip()
        exclude_x_list = _parse_csv_list(exclude_x_cols)

        try:
            payload = _cached_dataset_profile(
                dataset_path=Path(path),
                sample_rows=int(sample_rows),
                top_n=int(top_n),
                research_mode=bool(research_mode),
                fixed_y=fixed_y_text,
                exclude_x_cols=exclude_x_list,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"dataset profile failed: {exc}") from exc

        payload.update(
            {
                "generated_at_utc": _utc_now_isoz(),
                "resolved_dataset_path": str(path),
                "artifact_key": str(resolved.get("artifact_key", "")),
                "run_id": str(resolved.get("run_id", "")),
                "source": str(resolved.get("source", "user")),
            }
        )
        return payload

    @app.get("/datasets/config")
    def get_dataset_config() -> Dict[str, Any]:
        path = _dataset_config_path(workspace_root=_workspace_root())
        if not path.is_file():
            return {
                "exists": False,
                "path": str(path),
                "config": _default_dataset_config(),
            }
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, Mapping):
                raise ValueError("config file must contain a JSON object")
            config = _normalize_dataset_config(raw)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"dataset config read failed: {exc}") from exc
        return {
            "exists": True,
            "path": str(path),
            "config": config,
        }

    @app.post("/datasets/config")
    def save_dataset_config(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            config = _normalize_dataset_config(payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        path = _dataset_config_path(workspace_root=_workspace_root())
        path.parent.mkdir(parents=True, exist_ok=True)
        to_write = dict(config)
        to_write["updated_at_utc"] = _utc_now_isoz()
        path.write_text(json.dumps(to_write, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "saved": True,
            "path": str(path),
            "config": config,
            "updated_at_utc": to_write["updated_at_utc"],
        }

    @app.get("/reports/saved")
    def list_saved_reports(
        kind: str = Query(default="all"),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> Dict[str, Any]:
        try:
            rows = _list_saved_reports(
                workspace_root=_workspace_root(),
                kind=str(kind),
                limit=int(limit),
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {
            "rows": rows,
            "total_rows": int(len(rows)),
            "kind": str(kind),
        }

    @app.get("/reports/read")
    def read_saved_report(
        relative_path: str = Query(default=""),
        max_chars: int = Query(default=200000, ge=1000, le=2000000),
    ) -> Dict[str, Any]:
        try:
            path = _resolve_saved_report_path(
                workspace_root=_workspace_root(),
                relative_path=str(relative_path),
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        text = path.read_text(encoding="utf-8")
        truncated = len(text) > int(max_chars)
        text_out = text[: int(max_chars)] if truncated else text
        parsed_json = None
        if path.suffix.lower() == ".json":
            try:
                parsed_json = json.loads(text)
            except Exception:
                parsed_json = None

        return {
            "relative_path": str(path.resolve().relative_to(_workspace_root().resolve())),
            "size_chars": int(len(text)),
            "truncated": bool(truncated),
            "text": text_out,
            "parsed_json": parsed_json,
        }

    @app.get("/ui", response_class=HTMLResponse)
    def ui_index() -> HTMLResponse:
        return HTMLResponse(content=build_ui_page_html(run_modes=RUN_MODES), status_code=200)

    return app
