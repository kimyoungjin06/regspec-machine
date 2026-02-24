"""L4 API layer over RunOrchestrator.

This module keeps the request/status/result payloads aligned with
`regspec_machine.contracts` while exposing a small FastAPI surface for
agent/UI integrations.
"""

import csv
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
import json
import math
from pathlib import Path
import re
import time
import warnings
from typing import Any, Dict, List, Mapping, Optional

import numpy as np
import pandas as pd
from statsmodels.discrete.conditional_models import ConditionalLogit

from .estimators import prepare_informative_df, standardize_inplace
from .module_input import load_and_prepare_data
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
from .splitter import assign_policy_document_holdout
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

    def _median(values: List[float]) -> Optional[float]:
        if not values:
            return None
        arr = sorted(float(v) for v in values)
        n = len(arr)
        mid = n // 2
        if (n % 2) == 1:
            return arr[mid]
        return (arr[mid - 1] + arr[mid]) / 2.0

    def _mean(values: List[float]) -> Optional[float]:
        if not values:
            return None
        return float(sum(values) / float(len(values)))

    def _is_validation_ok(row: Mapping[str, Any]) -> bool:
        status_validation = str(row.get("status_validation", "")).strip().lower()
        has_status = str(row.get("status_validation", "")).strip() != ""
        if not has_status:
            return True
        return status_validation in {"ok", "pass", "passed", "success"}

    def _mode_scope_match(mode_text: str, scope: str) -> bool:
        mode_lower = str(mode_text or "").strip().lower()
        scope_text = str(scope or "").strip().lower()
        if scope_text in {"", "all"}:
            return True
        if scope_text == "singlex":
            return "singlex" in mode_lower
        if scope_text == "nooption":
            return "nooption" in mode_lower
        if scope_text == "paired":
            return mode_lower.startswith("paired_") or "paired" in mode_lower
        return mode_lower == scope_text

    def _histogram_probability(values: List[float]) -> List[Dict[str, Any]]:
        bins = [
            ("<=0.01", None, 0.01),
            ("0.01-0.05", 0.01, 0.05),
            ("0.05-0.10", 0.05, 0.10),
            ("0.10-0.20", 0.10, 0.20),
            ("0.20-0.50", 0.20, 0.50),
            ("0.50-1.00", 0.50, 1.00),
            (">1.00", 1.00, None),
        ]
        counts = [0 for _ in bins]
        for raw in values:
            v = _safe_float(raw)
            if v is None:
                continue
            for i, (_label, lo, hi) in enumerate(bins):
                if lo is None and v <= float(hi):
                    counts[i] += 1
                    break
                if hi is None and v > float(lo):
                    counts[i] += 1
                    break
                if lo is not None and hi is not None and (v > float(lo)) and (v <= float(hi)):
                    counts[i] += 1
                    break
        return [
            {"bin": label, "count": int(counts[i])}
            for i, (label, _lo, _hi) in enumerate(bins)
        ]

    def _resolve_run_summary_payload(run_id: str) -> tuple[Dict[str, Any], Path]:
        rid = str(run_id or "").strip()
        if not rid:
            raise ValueError("run_id is required")

        workspace = _workspace_root()
        candidates: List[Path] = []
        seen_paths: set[Path] = set()

        def _push(path: Optional[Path]) -> None:
            if path is None:
                return
            p = path.resolve()
            if p in seen_paths:
                return
            seen_paths.add(p)
            candidates.append(p)

        entry = _resolve_run_entry(rid)
        if entry is not None and entry.get("result") is not None:
            try:
                artifact_map = entry["result"].artifacts.as_dict()
                _push(
                    _resolve_artifact_path(
                        workspace_root=workspace,
                        raw=str(artifact_map.get("run_summary_json", "")),
                    )
                )
            except Exception:
                pass

        _push(
            workspace
            / "data"
            / "metadata"
            / f"phase_b_bikard_machine_scientist_run_summary_{rid}.json"
        )

        meta_root = workspace / "data" / "metadata"
        if meta_root.is_dir():
            pattern = f"phase_b_bikard_machine_scientist_run_summary_*{rid}*.json"
            for p in sorted(meta_root.glob(pattern), reverse=True):
                _push(p)

        for path in candidates:
            if not path.is_file():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, Mapping):
                continue
            payload_run_id = str(payload.get("run_id", "")).strip()
            if payload_run_id and payload_run_id != rid:
                continue
            return dict(payload), path

        raise FileNotFoundError(f"run_summary_json not found for run_id: {rid}")

    def _dedupe_feature_list(values: List[str]) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for raw in values:
            item = str(raw or "").strip()
            if not item:
                continue
            if item in seen:
                continue
            seen.add(item)
            out.append(item)
        return out

    def _summarize_distribution(values: List[float]) -> Dict[str, Any]:
        if not values:
            return {
                "n": 0,
                "mean": None,
                "ci_low": None,
                "ci_high": None,
                "std": None,
            }
        arr = np.array(values, dtype=float)
        return {
            "n": int(arr.size),
            "mean": float(arr.mean()),
            "ci_low": float(np.quantile(arr, 0.025)),
            "ci_high": float(np.quantile(arr, 0.975)),
            "std": float(arr.std(ddof=1)) if arr.size > 1 else 0.0,
        }

    def _fit_conditional_logit_metrics(
        fit_df: pd.DataFrame,
        *,
        exog_cols: List[str],
    ) -> Dict[str, Any]:
        if fit_df.empty:
            return {
                "status": "insufficient_data",
                "reason": "empty_informative",
                "n_rows": 0,
                "n_events": 0,
                "n_policy_docs": 0,
                "accuracy_event": None,
                "llf": None,
                "llf_per_event": None,
            }
        if not exog_cols:
            return {
                "status": "insufficient_data",
                "reason": "empty_exog",
                "n_rows": int(len(fit_df)),
                "n_events": int(fit_df["event_id"].astype(str).nunique()),
                "n_policy_docs": int(fit_df["policy_document_id"].astype(str).nunique()),
                "accuracy_event": None,
                "llf": None,
                "llf_per_event": None,
            }

        n_rows = int(len(fit_df))
        n_events = int(fit_df["event_id"].astype(str).nunique())
        n_policy_docs = int(fit_df["policy_document_id"].astype(str).nunique())
        try:
            y = fit_df["y"].astype(int).to_numpy(dtype=int)
            X = fit_df[list(exog_cols)].astype(float).to_numpy(dtype=float)
            groups = fit_df["event_id"].astype(str).to_numpy()
            if X.ndim != 2 or X.shape[0] == 0 or X.shape[1] == 0:
                return {
                    "status": "fit_failed",
                    "reason": "empty_design",
                    "n_rows": n_rows,
                    "n_events": n_events,
                    "n_policy_docs": n_policy_docs,
                    "accuracy_event": None,
                    "llf": None,
                    "llf_per_event": None,
                }
            mdl = ConditionalLogit(y, X, groups=groups)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                res = mdl.fit(disp=False, maxiter=300)

            warning_texts = [f"{w.category.__name__}:{w.message}" for w in caught]
            warning_blob = " | ".join(warning_texts).lower()
            converged = True
            mle_retvals = getattr(res, "mle_retvals", None)
            if isinstance(mle_retvals, Mapping):
                converged = bool(mle_retvals.get("converged", True))
            if not converged:
                return {
                    "status": "fit_failed",
                    "reason": "non_converged",
                    "n_rows": n_rows,
                    "n_events": n_events,
                    "n_policy_docs": n_policy_docs,
                    "accuracy_event": None,
                    "llf": None,
                    "llf_per_event": None,
                }
            if "overflow" in warning_blob:
                return {
                    "status": "fit_failed",
                    "reason": "numerical_overflow",
                    "n_rows": n_rows,
                    "n_events": n_events,
                    "n_policy_docs": n_policy_docs,
                    "accuracy_event": None,
                    "llf": None,
                    "llf_per_event": None,
                }

            params = np.array(res.params, dtype=float)
            if params.ndim == 0:
                params = params.reshape(1)
            if not np.isfinite(params).all():
                return {
                    "status": "fit_failed",
                    "reason": "non_finite_params",
                    "n_rows": n_rows,
                    "n_events": n_events,
                    "n_policy_docs": n_policy_docs,
                    "accuracy_event": None,
                    "llf": None,
                    "llf_per_event": None,
                }

            util = (X @ params).astype(float)
            work = fit_df[["event_id", "y"]].copy()
            work["util"] = util
            correct = 0
            total = 0
            for _eid, group in work.groupby("event_id", dropna=False):
                if len(group) != 2:
                    continue
                pred_idx = int(np.argmax(group["util"].to_numpy(dtype=float)))
                actual_idx = int(np.argmax(group["y"].to_numpy(dtype=float)))
                if pred_idx == actual_idx:
                    correct += 1
                total += 1
            llf = float(res.llf)
            if not math.isfinite(llf):
                return {
                    "status": "fit_failed",
                    "reason": "non_finite_llf",
                    "n_rows": n_rows,
                    "n_events": n_events,
                    "n_policy_docs": n_policy_docs,
                    "accuracy_event": None,
                    "llf": None,
                    "llf_per_event": None,
                }
            n_event_denom = total if total > 0 else n_events
            llf_per_event = llf / float(max(1, n_event_denom))
            return {
                "status": "ok",
                "reason": "",
                "n_rows": n_rows,
                "n_events": n_events,
                "n_policy_docs": n_policy_docs,
                "accuracy_event": (float(correct) / float(total) if total > 0 else None),
                "llf": llf,
                "llf_per_event": llf_per_event,
            }
        except Exception as exc:
            return {
                "status": "fit_failed",
                "reason": str(exc),
                "n_rows": n_rows,
                "n_events": n_events,
                "n_policy_docs": n_policy_docs,
                "accuracy_event": None,
                "llf": None,
                "llf_per_event": None,
            }

    def _bootstrap_step_metrics(
        fit_df: pd.DataFrame,
        *,
        exog_cols: List[str],
        cluster_unit: str,
        n_bootstrap: int,
        seed: int,
    ) -> Dict[str, Any]:
        if fit_df.empty or int(n_bootstrap) <= 0:
            return {
                "attempted": 0,
                "success": 0,
                "accuracy_values": [],
                "llf_per_event_values": [],
            }
        cluster_col = str(cluster_unit or "").strip() or "policy_document_id"
        if cluster_col not in fit_df.columns:
            cluster_col = "policy_document_id"
        if cluster_col not in fit_df.columns:
            return {
                "attempted": 0,
                "success": 0,
                "accuracy_values": [],
                "llf_per_event_values": [],
            }

        cluster_ids = sorted(fit_df[cluster_col].dropna().astype(str).unique().tolist())
        if not cluster_ids:
            return {
                "attempted": 0,
                "success": 0,
                "accuracy_values": [],
                "llf_per_event_values": [],
            }

        cluster_map: Dict[str, pd.DataFrame] = {
            cid: fit_df[fit_df[cluster_col].astype(str) == cid].copy() for cid in cluster_ids
        }
        rng = np.random.default_rng(int(seed))
        accuracy_values: List[float] = []
        llf_per_event_values: List[float] = []
        attempted = 0
        success = 0

        for b in range(int(n_bootstrap)):
            attempted += 1
            sampled = rng.choice(cluster_ids, size=len(cluster_ids), replace=True)
            parts: List[pd.DataFrame] = []
            for j, cid in enumerate(sampled):
                part = cluster_map.get(str(cid))
                if part is None or part.empty:
                    continue
                copied = part.copy()
                copied["event_id"] = copied["event_id"].astype(str) + f"|eqbs{b}_{j}"
                parts.append(copied)
            if not parts:
                continue
            boot = pd.concat(parts, axis=0, ignore_index=True)
            standardize_inplace(boot, exog_cols)
            fit = _fit_conditional_logit_metrics(boot, exog_cols=exog_cols)
            if fit.get("status") != "ok":
                continue
            acc = _safe_float(fit.get("accuracy_event"))
            llf_per = _safe_float(fit.get("llf_per_event"))
            if acc is None or llf_per is None:
                continue
            accuracy_values.append(float(acc))
            llf_per_event_values.append(float(llf_per))
            success += 1

        return {
            "attempted": int(attempted),
            "success": int(success),
            "accuracy_values": accuracy_values,
            "llf_per_event_values": llf_per_event_values,
        }

    def _build_equation_path(
        *,
        run_id: str,
        factors: List[str],
        track: str,
        y_col: str,
        split_role: str,
        include_base_controls: bool,
        include_baseline: bool,
        n_bootstrap: int,
        bootstrap_seed: int,
        max_steps: int,
        split_seed_override: Optional[int],
        split_ratio_override: Optional[float],
    ) -> Dict[str, Any]:
        def _factor_atoms_from_name(name: str) -> List[str]:
            text = str(name or "").strip()
            if not text or text == "(base_controls)":
                return []

            # onehot format: cat__<source_feature>__<sanitized_level_token>
            if text.startswith("cat__"):
                rest = text.replace("cat__", "", 1)
                if "__" in rest:
                    src, _lvl = rest.rsplit("__", 1)
                    src = str(src).strip()
                    if src:
                        return [src]
                return [text]

            # expression format generated by runner:
            # expr__slog1p__<lhs>
            # expr__sq__<lhs>
            # expr__ratio__<lhs>__over__<rhs>
            # expr__diff__<lhs>__minus__<rhs>
            # expr__mul__<lhs>__x__<rhs>
            if text.startswith("expr__slog1p__"):
                lhs = text.replace("expr__slog1p__", "", 1).strip()
                return [lhs] if lhs else [text]
            if text.startswith("expr__sq__"):
                lhs = text.replace("expr__sq__", "", 1).strip()
                return [lhs] if lhs else [text]
            if text.startswith("expr__ratio__"):
                rest = text.replace("expr__ratio__", "", 1)
                if "__over__" in rest:
                    lhs, rhs = rest.split("__over__", 1)
                    atoms = [str(lhs).strip(), str(rhs).strip()]
                    return [a for a in atoms if a] or [text]
                return [text]
            if text.startswith("expr__diff__"):
                rest = text.replace("expr__diff__", "", 1)
                if "__minus__" in rest:
                    lhs, rhs = rest.split("__minus__", 1)
                    atoms = [str(lhs).strip(), str(rhs).strip()]
                    return [a for a in atoms if a] or [text]
                return [text]
            if text.startswith("expr__mul__"):
                rest = text.replace("expr__mul__", "", 1)
                if "__x__" in rest:
                    lhs, rhs = rest.split("__x__", 1)
                    atoms = [str(lhs).strip(), str(rhs).strip()]
                    return [a for a in atoms if a] or [text]
                return [text]

            return [text]

        summary_payload, summary_path = _resolve_run_summary_payload(run_id)
        config = summary_payload.get("config", {})
        if not isinstance(config, Mapping):
            config = {}
        controls_meta = summary_payload.get("controls_meta", {})
        if not isinstance(controls_meta, Mapping):
            controls_meta = {}
        inputs_meta = summary_payload.get("inputs", {})
        if not isinstance(inputs_meta, Mapping):
            inputs_meta = {}

        workspace = _workspace_root()

        def _resolve_input_csv(text: str, *, required: bool) -> Optional[Path]:
            p = _resolve_artifact_path(workspace_root=workspace, raw=str(text or "").strip())
            if p is None or not p.is_file():
                if required:
                    raise FileNotFoundError(f"input dataset not found: {text}")
                return None
            return p

        dyad_base = _resolve_input_csv(str(inputs_meta.get("dyad_base_csv", "")), required=True)
        ext_csv = _resolve_input_csv(str(inputs_meta.get("extension_feature_csv", "")), required=False)
        phase_a_csv = _resolve_input_csv(str(inputs_meta.get("phase_a_covariates_csv", "")), required=False)

        data, load_meta = load_and_prepare_data(
            dyad_base_csv=dyad_base,
            extension_feature_csv=ext_csv,
            phase_a_covariates_csv=phase_a_csv,
        )

        split_seed_cfg = _safe_int(config.get("split_seed"), 20260219)
        split_ratio_cfg = _safe_float(config.get("split_ratio"))
        split_seed = int(split_seed_override) if split_seed_override is not None else int(split_seed_cfg)
        split_ratio = (
            float(split_ratio_override)
            if split_ratio_override is not None
            else (float(split_ratio_cfg) if split_ratio_cfg is not None else 0.80)
        )
        if split_ratio <= 0.0 or split_ratio >= 1.0:
            split_ratio = 0.80

        data, split_meta = assign_policy_document_holdout(
            data,
            seed=int(split_seed),
            discovery_ratio=float(split_ratio),
            method="hash",
        )

        y_text = str(y_col or "y_all").strip() or "y_all"
        if y_text not in data.columns:
            raise ValueError(f"y_col not found in prepared data: {y_text}")

        track_text = str(track or "primary_strict").strip() or "primary_strict"
        if track_text and track_text.lower() not in {"all", "any"}:
            data = data[data["track"].astype(str) == track_text].copy()

        split_role_text = str(split_role or "validation").strip().lower() or "validation"
        if split_role_text not in {"discovery", "validation", "all"}:
            raise ValueError("split_role must be one of: discovery, validation, all")
        if split_role_text in {"discovery", "validation"}:
            data = data[data["split_role"].astype(str) == split_role_text].copy()

        if data.empty:
            raise ValueError("no rows available after applying track/split filters")

        requested_factors = _dedupe_feature_list(list(factors))
        if not requested_factors:
            raise ValueError("factors must include at least one feature")

        base_controls_requested = controls_meta.get("base_controls_used", [])
        if not isinstance(base_controls_requested, list):
            base_controls_requested = []
        base_controls = (
            _dedupe_feature_list([str(x) for x in base_controls_requested if str(x).strip()])
            if include_base_controls
            else []
        )
        base_controls_present = [c for c in base_controls if c in data.columns]

        factors_present = [f for f in requested_factors if f in data.columns]
        factors_missing = [f for f in requested_factors if f not in data.columns]
        if not factors_present:
            raise ValueError("none of the requested factors were found in prepared data columns")

        max_steps_eff = max(1, min(int(max_steps), len(factors_present)))
        selected_factors = factors_present[:max_steps_eff]

        bootstrap_cluster_unit = str(config.get("bootstrap_cluster_unit", "policy_document_id")).strip()
        if not bootstrap_cluster_unit:
            bootstrap_cluster_unit = "policy_document_id"

        step_specs: List[Dict[str, Any]] = []
        if include_baseline and base_controls_present:
            step_specs.append(
                {
                    "step": 0,
                    "added_factor": "(base_controls)",
                    "factors_included": [],
                    "exog_cols": list(base_controls_present),
                    "is_baseline": True,
                }
            )

        for idx, factor in enumerate(selected_factors, start=1):
            step_specs.append(
                {
                    "step": idx,
                    "added_factor": factor,
                    "factors_included": list(selected_factors[:idx]),
                    "exog_cols": _dedupe_feature_list(list(base_controls_present) + list(selected_factors[:idx])),
                    "is_baseline": False,
                }
            )

        step_rows: List[Dict[str, Any]] = []
        previous_row: Optional[Dict[str, Any]] = None
        for spec in step_specs:
            step_index = int(spec.get("step", 0))
            exog_cols = list(spec.get("exog_cols", []))
            fit_df = prepare_informative_df(
                data,
                y_col=y_text,
                exog_cols=exog_cols,
            )
            standardize_inplace(fit_df, exog_cols)
            fit_metrics = _fit_conditional_logit_metrics(fit_df, exog_cols=exog_cols)
            bootstrap_payload = _bootstrap_step_metrics(
                fit_df,
                exog_cols=exog_cols,
                cluster_unit=bootstrap_cluster_unit,
                n_bootstrap=int(n_bootstrap),
                seed=int(bootstrap_seed + step_index * 100003),
            )
            acc_stats = _summarize_distribution(list(bootstrap_payload.get("accuracy_values", [])))
            llf_stats = _summarize_distribution(list(bootstrap_payload.get("llf_per_event_values", [])))

            if acc_stats["mean"] is None:
                point_acc = _safe_float(fit_metrics.get("accuracy_event"))
                if point_acc is not None:
                    acc_stats = {
                        "n": 1,
                        "mean": float(point_acc),
                        "ci_low": float(point_acc),
                        "ci_high": float(point_acc),
                        "std": 0.0,
                    }
            if llf_stats["mean"] is None:
                point_llf = _safe_float(fit_metrics.get("llf_per_event"))
                if point_llf is not None:
                    llf_stats = {
                        "n": 1,
                        "mean": float(point_llf),
                        "ci_low": float(point_llf),
                        "ci_high": float(point_llf),
                        "std": 0.0,
                    }

            row: Dict[str, Any] = {
                "step": step_index,
                "is_baseline": bool(spec.get("is_baseline")),
                "added_factor": str(spec.get("added_factor", "")),
                "contribution_atoms": _factor_atoms_from_name(str(spec.get("added_factor", ""))),
                "factors_included": list(spec.get("factors_included", [])),
                "equation": f"{y_text} ~ " + " + ".join(exog_cols),
                "n_exog_cols": int(len(exog_cols)),
                "n_rows": int(fit_metrics.get("n_rows", 0)),
                "n_events": int(fit_metrics.get("n_events", 0)),
                "n_policy_docs": int(fit_metrics.get("n_policy_docs", 0)),
                "fit_status": str(fit_metrics.get("status", "")),
                "fit_reason": str(fit_metrics.get("reason", "")),
                "accuracy_mean": acc_stats["mean"],
                "accuracy_ci_low": acc_stats["ci_low"],
                "accuracy_ci_high": acc_stats["ci_high"],
                "accuracy_std": acc_stats["std"],
                "llf_per_event_mean": llf_stats["mean"],
                "llf_per_event_ci_low": llf_stats["ci_low"],
                "llf_per_event_ci_high": llf_stats["ci_high"],
                "llf_per_event_std": llf_stats["std"],
                "bootstrap_attempted": int(bootstrap_payload.get("attempted", 0)),
                "bootstrap_success": int(bootstrap_payload.get("success", 0)),
                "delta_accuracy_mean": None,
                "delta_accuracy_ci_low": None,
                "delta_accuracy_ci_high": None,
                "delta_llf_per_event_mean": None,
                "delta_llf_per_event_ci_low": None,
                "delta_llf_per_event_ci_high": None,
                "contribution_accuracy_mean": None,
                "contribution_accuracy_ci_low": None,
                "contribution_accuracy_ci_high": None,
                "contribution_accuracy_abs": None,
                "contribution_accuracy_share_abs": None,
                "contribution_llf_per_event_mean": None,
                "contribution_llf_per_event_ci_low": None,
                "contribution_llf_per_event_ci_high": None,
            }

            prev_acc = _safe_float(previous_row.get("accuracy_mean")) if previous_row is not None else None
            curr_acc = _safe_float(row.get("accuracy_mean"))
            if prev_acc is not None and curr_acc is not None:
                delta = curr_acc - prev_acc
                prev_std = _safe_float(previous_row.get("accuracy_std")) or 0.0
                curr_std = _safe_float(row.get("accuracy_std")) or 0.0
                delta_std = math.sqrt(max(0.0, prev_std * prev_std + curr_std * curr_std))
                row["delta_accuracy_mean"] = float(delta)
                row["delta_accuracy_ci_low"] = float(delta - 1.96 * delta_std)
                row["delta_accuracy_ci_high"] = float(delta + 1.96 * delta_std)
                row["contribution_accuracy_mean"] = float(delta)
                row["contribution_accuracy_ci_low"] = float(delta - 1.96 * delta_std)
                row["contribution_accuracy_ci_high"] = float(delta + 1.96 * delta_std)
                row["contribution_accuracy_abs"] = float(abs(delta))

            prev_llf = _safe_float(previous_row.get("llf_per_event_mean")) if previous_row is not None else None
            curr_llf = _safe_float(row.get("llf_per_event_mean"))
            if prev_llf is not None and curr_llf is not None:
                delta = curr_llf - prev_llf
                prev_std = _safe_float(previous_row.get("llf_per_event_std")) or 0.0
                curr_std = _safe_float(row.get("llf_per_event_std")) or 0.0
                delta_std = math.sqrt(max(0.0, prev_std * prev_std + curr_std * curr_std))
                row["delta_llf_per_event_mean"] = float(delta)
                row["delta_llf_per_event_ci_low"] = float(delta - 1.96 * delta_std)
                row["delta_llf_per_event_ci_high"] = float(delta + 1.96 * delta_std)
                row["contribution_llf_per_event_mean"] = float(delta)
                row["contribution_llf_per_event_ci_low"] = float(delta - 1.96 * delta_std)
                row["contribution_llf_per_event_ci_high"] = float(delta + 1.96 * delta_std)

            step_rows.append(row)
            previous_row = row

        contribution_rows = [
            row
            for row in step_rows
            if not bool(row.get("is_baseline"))
            and _safe_float(row.get("contribution_accuracy_abs")) is not None
        ]
        total_abs_contribution = float(
            sum(_safe_float(row.get("contribution_accuracy_abs")) or 0.0 for row in contribution_rows)
        )
        if total_abs_contribution > 0.0:
            for row in contribution_rows:
                abs_val = _safe_float(row.get("contribution_accuracy_abs")) or 0.0
                row["contribution_accuracy_share_abs"] = float(abs_val / total_abs_contribution)
        else:
            for row in contribution_rows:
                row["contribution_accuracy_share_abs"] = None

        atom_acc_sum: Dict[str, float] = defaultdict(float)
        atom_acc_abs_sum: Dict[str, float] = defaultdict(float)
        atom_llf_sum: Dict[str, float] = defaultdict(float)
        atom_steps: Dict[str, int] = defaultdict(int)
        atom_factors: Dict[str, set[str]] = defaultdict(set)
        for row in contribution_rows:
            atoms_raw = row.get("contribution_atoms")
            atoms = [str(a).strip() for a in atoms_raw if str(a).strip()] if isinstance(atoms_raw, list) else []
            if not atoms:
                continue
            acc_val = _safe_float(row.get("contribution_accuracy_mean"))
            llf_val = _safe_float(row.get("contribution_llf_per_event_mean"))
            if acc_val is None and llf_val is None:
                continue
            per_atom_acc = float(acc_val) / float(len(atoms)) if acc_val is not None else None
            per_atom_llf = float(llf_val) / float(len(atoms)) if llf_val is not None else None
            factor_name = str(row.get("added_factor", "")).strip()
            for atom in atoms:
                if per_atom_acc is not None:
                    atom_acc_sum[atom] += per_atom_acc
                    atom_acc_abs_sum[atom] += abs(per_atom_acc)
                if per_atom_llf is not None:
                    atom_llf_sum[atom] += per_atom_llf
                atom_steps[atom] += 1
                if factor_name:
                    atom_factors[atom].add(factor_name)

        atom_total_abs = float(sum(atom_acc_abs_sum.values()))
        contribution_groups: List[Dict[str, Any]] = []
        for atom in sorted(set(atom_acc_sum.keys()) | set(atom_llf_sum.keys())):
            acc_sum = atom_acc_sum.get(atom, 0.0)
            acc_abs_sum = atom_acc_abs_sum.get(atom, abs(acc_sum))
            share_abs = (acc_abs_sum / atom_total_abs) if atom_total_abs > 0.0 else None
            contribution_groups.append(
                {
                    "feature_atom": atom,
                    "contribution_accuracy_sum": float(acc_sum),
                    "contribution_accuracy_abs_sum": float(acc_abs_sum),
                    "contribution_accuracy_share_abs": float(share_abs) if share_abs is not None else None,
                    "contribution_llf_per_event_sum": float(atom_llf_sum.get(atom, 0.0)),
                    "n_steps": int(atom_steps.get(atom, 0)),
                    "factors": sorted(atom_factors.get(atom, set())),
                }
            )
        contribution_groups.sort(
            key=lambda r: (
                -(abs(_safe_float(r.get("contribution_accuracy_sum")) or 0.0)),
                str(r.get("feature_atom", "")),
            )
        )

        scored_rows = [r for r in step_rows if _safe_float(r.get("accuracy_mean")) is not None]
        best_row = (
            max(scored_rows, key=lambda r: (_safe_float(r.get("accuracy_mean")) or -1.0, -int(r.get("step", 0))))
            if scored_rows
            else None
        )
        pos_rows = [
            r
            for r in contribution_rows
            if (_safe_float(r.get("contribution_accuracy_mean")) or 0.0) > 0.0
        ]
        neg_rows = [
            r
            for r in contribution_rows
            if (_safe_float(r.get("contribution_accuracy_mean")) or 0.0) < 0.0
        ]
        top_positive = (
            max(pos_rows, key=lambda r: _safe_float(r.get("contribution_accuracy_mean")) or float("-inf"))
            if pos_rows
            else None
        )
        top_negative = (
            min(neg_rows, key=lambda r: _safe_float(r.get("contribution_accuracy_mean")) or float("inf"))
            if neg_rows
            else None
        )
        contribution_total_signed = float(
            sum(_safe_float(r.get("contribution_accuracy_mean")) or 0.0 for r in contribution_rows)
        )
        contribution_total_llf = float(
            sum(_safe_float(r.get("contribution_llf_per_event_mean")) or 0.0 for r in contribution_rows)
        )

        return {
            "generated_at_utc": _utc_now_isoz(),
            "run_id": str(run_id),
            "run_summary_path": str(summary_path),
            "track": track_text,
            "split_role": split_role_text,
            "y_col": y_text,
            "settings": {
                "include_base_controls": bool(include_base_controls),
                "include_baseline": bool(include_baseline),
                "n_bootstrap": int(n_bootstrap),
                "bootstrap_seed": int(bootstrap_seed),
                "split_seed": int(split_seed),
                "split_ratio": float(split_ratio),
                "bootstrap_cluster_unit": bootstrap_cluster_unit,
            },
            "inputs": {
                "dyad_base_csv": str(dyad_base),
                "extension_feature_csv": str(ext_csv) if ext_csv else "",
                "phase_a_covariates_csv": str(phase_a_csv) if phase_a_csv else "",
                "load_meta": load_meta,
                "split_meta": split_meta,
            },
            "factors_requested": requested_factors,
            "factors_used": selected_factors,
            "missing_factors": factors_missing,
            "base_controls_used": base_controls_present,
            "steps": step_rows,
            "contribution_groups": contribution_groups,
            "summary": {
                "n_steps": int(len(step_rows)),
                "best_step": int(best_row["step"]) if best_row is not None else None,
                "best_added_factor": str(best_row["added_factor"]) if best_row is not None else "",
                "best_accuracy_mean": _safe_float(best_row.get("accuracy_mean")) if best_row is not None else None,
                "final_step": int(step_rows[-1]["step"]) if step_rows else None,
                "final_accuracy_mean": _safe_float(step_rows[-1].get("accuracy_mean")) if step_rows else None,
                "contribution_accuracy_total_signed": contribution_total_signed,
                "contribution_accuracy_total_abs": total_abs_contribution,
                "contribution_llf_per_event_total_signed": contribution_total_llf,
                "top_positive_contributor": str(top_positive.get("added_factor", "")) if top_positive else "",
                "top_positive_contribution_accuracy": (
                    _safe_float(top_positive.get("contribution_accuracy_mean")) if top_positive else None
                ),
                "top_negative_contributor": str(top_negative.get("added_factor", "")) if top_negative else "",
                "top_negative_contribution_accuracy": (
                    _safe_float(top_negative.get("contribution_accuracy_mean")) if top_negative else None
                ),
                "n_contribution_groups": int(len(contribution_groups)),
            },
        }

    def _build_explorer_summary(
        *,
        mode_scope: str,
        run_id_contains: str,
        q_threshold: float,
        limit_runs: int,
        top_n: int,
    ) -> Dict[str, Any]:
        entries = _iter_entries_latest(include_history=True)
        run_like = str(run_id_contains or "").strip().lower()
        q_gate = float(q_threshold)
        limited_entries: List[Dict[str, Any]] = []
        for entry in entries:
            request = entry.get("request")
            if request is None:
                continue
            run_id_text = str(request.run_id)
            mode_text = str(request.mode)
            if not _mode_scope_match(mode_text, mode_scope):
                continue
            if run_like and run_like not in run_id_text.lower():
                continue
            limited_entries.append(entry)
            if len(limited_entries) >= int(limit_runs):
                break

        totals: Dict[str, Any] = {
            "runs_considered": int(len(limited_entries)),
            "runs_with_inference": 0,
            "candidate_rows": 0,
            "validation_ok_rows": 0,
            "strong_rows_q": 0,
            "best_q_missing_runs": 0,
            "best_p_missing_runs": 0,
        }

        best_q_values: List[float] = []
        best_p_values: List[float] = []
        run_level_rows: List[Dict[str, Any]] = []

        combo_stats: Dict[str, Dict[str, Any]] = {}
        factor_stats: Dict[str, Dict[str, Any]] = {}

        strong_factor_runs: Dict[str, set[str]] = defaultdict(set)
        pair_counts: Dict[tuple[str, str], int] = defaultdict(int)
        strong_run_count = 0

        for entry in limited_entries:
            request = entry.get("request")
            result = entry.get("result")
            if request is None or result is None:
                continue

            artifacts = result.artifacts.as_dict()
            inf_path = _resolve_artifact_path(
                workspace_root=_workspace_root(),
                raw=str(artifacts.get("top_models_inference_csv", "")),
            )
            if inf_path is None or not inf_path.is_file():
                continue

            run_id_text = str(request.run_id)
            mode_text = str(request.mode)
            totals["runs_with_inference"] += 1

            run_best_q = None
            run_best_p = None
            run_validated = 0
            run_support = 0
            run_strong_rows = 0
            run_key_factors: set[str] = set()
            run_strong_factors: set[str] = set()

            try:
                with inf_path.open("r", encoding="utf-8", newline="") as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        totals["candidate_rows"] += 1

                        if not _is_validation_ok(row):
                            continue
                        totals["validation_ok_rows"] += 1

                        p_val = _safe_float(row.get("p_boot_validation", row.get("p_value_validation")))
                        q_val = _safe_float(row.get("q_value_validation", row.get("q_value")))
                        tier = str(row.get("candidate_tier", "")).strip().lower()
                        restart_mean = _safe_float(row.get("validated_rate_restart"))
                        key_factor = str(row.get("key_factor", "")).strip()
                        track = str(row.get("track", "")).strip()
                        context_scope = str(row.get("context_scope", "")).strip()
                        y_col = str(row.get("y_col", "")).strip()
                        spec_id = str(row.get("spec_id", "")).strip()

                        if p_val is not None and (run_best_p is None or p_val < run_best_p):
                            run_best_p = p_val
                        if q_val is not None and (run_best_q is None or q_val < run_best_q):
                            run_best_q = q_val

                        if tier == "validated_candidate":
                            run_validated += 1
                        if tier in {"validated_candidate", "support_candidate"}:
                            run_support += 1

                        if key_factor:
                            run_key_factors.add(key_factor)

                        is_strong = bool(q_val is not None and q_val <= q_gate)
                        if is_strong:
                            totals["strong_rows_q"] += 1
                            run_strong_rows += 1
                            if key_factor:
                                run_strong_factors.add(key_factor)

                        combo_id = "|".join(
                            [
                                track or "-",
                                context_scope or "-",
                                y_col or "-",
                                spec_id or "-",
                                key_factor or "-",
                            ]
                        )
                        combo = combo_stats.get(combo_id)
                        if combo is None:
                            combo = {
                                "combo_id": combo_id,
                                "track": track or "-",
                                "context_scope": context_scope or "-",
                                "y_col": y_col or "-",
                                "spec_id": spec_id or "-",
                                "key_factor": key_factor or "-",
                                "row_count": 0,
                                "strong_count": 0,
                                "validated_count": 0,
                                "support_or_better_count": 0,
                                "run_ids": set(),
                                "p_values": [],
                                "q_values": [],
                                "restart_values": [],
                            }
                            combo_stats[combo_id] = combo
                        combo["row_count"] += 1
                        combo["run_ids"].add(run_id_text)
                        if is_strong:
                            combo["strong_count"] += 1
                        if tier == "validated_candidate":
                            combo["validated_count"] += 1
                        if tier in {"validated_candidate", "support_candidate"}:
                            combo["support_or_better_count"] += 1
                        if p_val is not None:
                            combo["p_values"].append(float(p_val))
                        if q_val is not None:
                            combo["q_values"].append(float(q_val))
                        if restart_mean is not None:
                            combo["restart_values"].append(float(restart_mean))

                        if key_factor:
                            factor = factor_stats.get(key_factor)
                            if factor is None:
                                factor = {
                                    "key_factor": key_factor,
                                    "row_count": 0,
                                    "strong_count": 0,
                                    "validated_count": 0,
                                    "run_ids": set(),
                                    "p_values": [],
                                    "q_values": [],
                                }
                                factor_stats[key_factor] = factor
                            factor["row_count"] += 1
                            factor["run_ids"].add(run_id_text)
                            if is_strong:
                                factor["strong_count"] += 1
                            if tier == "validated_candidate":
                                factor["validated_count"] += 1
                            if p_val is not None:
                                factor["p_values"].append(float(p_val))
                            if q_val is not None:
                                factor["q_values"].append(float(q_val))
            except Exception:
                continue

            if run_best_q is None:
                totals["best_q_missing_runs"] += 1
            else:
                best_q_values.append(float(run_best_q))
            if run_best_p is None:
                totals["best_p_missing_runs"] += 1
            else:
                best_p_values.append(float(run_best_p))

            run_level_rows.append(
                {
                    "run_id": run_id_text,
                    "mode": mode_text,
                    "best_q_validation": run_best_q,
                    "best_p_validation": run_best_p,
                    "validated_candidate_count": int(run_validated),
                    "support_candidate_count": int(run_support),
                    "strong_row_count_q": int(run_strong_rows),
                    "key_factor_count": int(len(run_key_factors)),
                }
            )

            if run_strong_factors:
                strong_run_count += 1
                for fac in run_strong_factors:
                    strong_factor_runs[fac].add(run_id_text)
                for fac_a, fac_b in combinations(sorted(run_strong_factors), 2):
                    pair_counts[(fac_a, fac_b)] += 1

        combo_rows: List[Dict[str, Any]] = []
        for combo in combo_stats.values():
            n_rows = int(combo["row_count"])
            combo_rows.append(
                {
                    "combo_id": combo["combo_id"],
                    "track": combo["track"],
                    "context_scope": combo["context_scope"],
                    "y_col": combo["y_col"],
                    "spec_id": combo["spec_id"],
                    "key_factor": combo["key_factor"],
                    "n_rows": n_rows,
                    "n_runs": int(len(combo["run_ids"])),
                    "q_best": (min(combo["q_values"]) if combo["q_values"] else None),
                    "q_median": _median(combo["q_values"]),
                    "p_best": (min(combo["p_values"]) if combo["p_values"] else None),
                    "p_median": _median(combo["p_values"]),
                    "restart_mean": _mean(combo["restart_values"]),
                    "strong_share_q": (float(combo["strong_count"]) / float(n_rows) if n_rows > 0 else None),
                    "validated_share": (
                        float(combo["validated_count"]) / float(n_rows) if n_rows > 0 else None
                    ),
                    "support_or_better_share": (
                        float(combo["support_or_better_count"]) / float(n_rows) if n_rows > 0 else None
                    ),
                }
            )
        combo_rows.sort(
            key=lambda row: (
                float(row["q_best"]) if row.get("q_best") is not None else 9.99,
                -int(row.get("n_runs", 0)),
                -float(row.get("strong_share_q") or 0.0),
            )
        )

        factor_rows: List[Dict[str, Any]] = []
        for factor in factor_stats.values():
            n_rows = int(factor["row_count"])
            factor_rows.append(
                {
                    "key_factor": factor["key_factor"],
                    "n_rows": n_rows,
                    "n_runs": int(len(factor["run_ids"])),
                    "q_best": (min(factor["q_values"]) if factor["q_values"] else None),
                    "q_median": _median(factor["q_values"]),
                    "p_best": (min(factor["p_values"]) if factor["p_values"] else None),
                    "strong_share_q": (float(factor["strong_count"]) / float(n_rows) if n_rows > 0 else None),
                    "validated_share": (
                        float(factor["validated_count"]) / float(n_rows) if n_rows > 0 else None
                    ),
                }
            )
        factor_rows.sort(
            key=lambda row: (
                float(row["q_best"]) if row.get("q_best") is not None else 9.99,
                -int(row.get("n_runs", 0)),
                -float(row.get("strong_share_q") or 0.0),
            )
        )

        pair_rows: List[Dict[str, Any]] = []
        pair_for_graph: List[Dict[str, Any]] = []
        base_runs = int(strong_run_count)
        for (fac_a, fac_b), co_runs in pair_counts.items():
            a_runs = int(len(strong_factor_runs.get(fac_a, set())))
            b_runs = int(len(strong_factor_runs.get(fac_b, set())))
            if a_runs <= 0 or b_runs <= 0 or base_runs <= 0:
                continue
            union_runs = a_runs + b_runs - int(co_runs)
            jaccard = (float(co_runs) / float(union_runs)) if union_runs > 0 else None
            lift = (float(co_runs * base_runs) / float(a_runs * b_runs)) if (a_runs * b_runs) > 0 else None
            row = {
                "key_factor_a": fac_a,
                "key_factor_b": fac_b,
                "co_runs": int(co_runs),
                "run_share": float(co_runs) / float(base_runs),
                "jaccard": jaccard,
                "lift": lift,
                "support_a_runs": a_runs,
                "support_b_runs": b_runs,
            }
            pair_rows.append(row)
            pair_for_graph.append(row)
        pair_rows.sort(
            key=lambda row: (
                -int(row.get("co_runs", 0)),
                -float(row.get("jaccard") or 0.0),
                -float(row.get("lift") or 0.0),
            )
        )

        adjacency: Dict[str, set[str]] = defaultdict(set)
        min_co = 2 if base_runs >= 5 else 1
        for row in pair_for_graph:
            if int(row.get("co_runs", 0)) < min_co:
                continue
            if float(row.get("jaccard") or 0.0) < 0.35:
                continue
            a = str(row.get("key_factor_a", ""))
            b = str(row.get("key_factor_b", ""))
            if not a or not b:
                continue
            adjacency[a].add(b)
            adjacency[b].add(a)

        clusters: List[Dict[str, Any]] = []
        visited: set[str] = set()
        for start in sorted(adjacency.keys()):
            if start in visited:
                continue
            stack = [start]
            comp: List[str] = []
            visited.add(start)
            while stack:
                node = stack.pop()
                comp.append(node)
                for nxt in sorted(adjacency.get(node, set())):
                    if nxt in visited:
                        continue
                    visited.add(nxt)
                    stack.append(nxt)
            if len(comp) < 2:
                continue
            comp_sorted = sorted(
                comp,
                key=lambda k: (-len(strong_factor_runs.get(k, set())), k),
            )
            run_union: set[str] = set()
            for fac in comp_sorted:
                run_union.update(strong_factor_runs.get(fac, set()))
            clusters.append(
                {
                    "cluster_signature": " | ".join(comp_sorted[:3]),
                    "n_factors": int(len(comp_sorted)),
                    "run_support": int(len(run_union)),
                    "factors": comp_sorted,
                }
            )
        clusters.sort(key=lambda row: (-int(row["run_support"]), -int(row["n_factors"]), str(row["cluster_signature"])))

        run_level_rows.sort(
            key=lambda row: (
                float(row["best_q_validation"]) if row.get("best_q_validation") is not None else 9.99,
                -int(row.get("validated_candidate_count", 0)),
                -int(row.get("strong_row_count_q", 0)),
                str(row.get("run_id", "")),
            )
        )

        totals["distinct_key_factors"] = int(len(factor_rows))
        totals["distinct_combinations"] = int(len(combo_rows))
        totals["strong_runs"] = int(base_runs)

        return {
            "generated_at_utc": _utc_now_isoz(),
            "filters": {
                "mode_scope": str(mode_scope or "all"),
                "run_id_contains": str(run_id_contains or ""),
                "q_threshold": float(q_gate),
                "limit_runs": int(limit_runs),
                "top_n": int(top_n),
            },
            "totals": totals,
            "distributions": {
                "best_q_bins": _histogram_probability(best_q_values),
                "best_p_bins": _histogram_probability(best_p_values),
                "best_q_values_count": int(len(best_q_values)),
                "best_p_values_count": int(len(best_p_values)),
            },
            "top_combinations": combo_rows[: int(top_n)],
            "top_key_factors": factor_rows[: int(top_n)],
            "top_affinity_pairs": pair_rows[: int(top_n)],
            "factor_clusters": clusters[: int(top_n)],
            "run_scatter_points": run_level_rows,
            "top_runs": run_level_rows[: int(top_n)],
        }

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

    @app.get("/explorer/summary")
    def get_explorer_summary(
        mode_scope: str = Query(default="all"),
        run_id_contains: str = Query(default=""),
        q_threshold: float = Query(default=0.10, ge=0.0, le=1.0),
        limit_runs: int = Query(default=200, ge=1, le=1000),
        top_n: int = Query(default=20, ge=5, le=200),
    ) -> Dict[str, Any]:
        return _build_explorer_summary(
            mode_scope=str(mode_scope or "all"),
            run_id_contains=str(run_id_contains or ""),
            q_threshold=float(q_threshold),
            limit_runs=int(limit_runs),
            top_n=int(top_n),
        )

    @app.post("/explorer/equation-path")
    def post_explorer_equation_path(payload: Dict[str, Any]) -> Dict[str, Any]:
        run_id = str(payload.get("run_id", "")).strip()
        if not run_id:
            raise HTTPException(status_code=422, detail="run_id is required")

        track = str(payload.get("track", "primary_strict")).strip() or "primary_strict"
        y_col = str(payload.get("y_col", "y_all")).strip() or "y_all"
        split_role = str(payload.get("split_role", "validation")).strip().lower() or "validation"
        if split_role not in {"discovery", "validation", "all"}:
            raise HTTPException(status_code=422, detail="split_role must be one of: discovery, validation, all")

        include_base_controls = bool(payload.get("include_base_controls", True))
        include_baseline = bool(payload.get("include_baseline", True))
        n_bootstrap = _safe_int(payload.get("n_bootstrap"), 49)
        n_bootstrap = max(0, min(999, n_bootstrap))
        bootstrap_seed = _safe_int(payload.get("bootstrap_seed"), 20260219)
        max_steps = _safe_int(payload.get("max_steps"), 6)
        max_steps = max(1, min(40, max_steps))

        split_seed_override_raw = payload.get("split_seed")
        split_seed_override: Optional[int]
        if split_seed_override_raw in (None, "", "null"):
            split_seed_override = None
        else:
            try:
                split_seed_override = int(split_seed_override_raw)
            except Exception as exc:
                raise HTTPException(status_code=422, detail="split_seed must be an integer") from exc

        split_ratio_override_raw = payload.get("split_ratio")
        split_ratio_override: Optional[float]
        if split_ratio_override_raw in (None, "", "null"):
            split_ratio_override = None
        else:
            try:
                split_ratio_override = float(split_ratio_override_raw)
            except Exception as exc:
                raise HTTPException(status_code=422, detail="split_ratio must be a float in (0,1)") from exc
            if split_ratio_override <= 0.0 or split_ratio_override >= 1.0:
                raise HTTPException(status_code=422, detail="split_ratio must be a float in (0,1)")

        factors_raw = payload.get("factors")
        factors: List[str] = []
        if isinstance(factors_raw, str):
            factors = _dedupe_feature_list(
                [part for chunk in factors_raw.splitlines() for part in chunk.split(",")]
            )
        elif isinstance(factors_raw, (list, tuple)):
            factors = _dedupe_feature_list([str(item) for item in factors_raw])
        elif factors_raw is None:
            factors = []
        else:
            raise HTTPException(status_code=422, detail="factors must be a list or newline/comma string")

        if not factors:
            raise HTTPException(status_code=422, detail="factors must include at least one feature")

        try:
            return _build_equation_path(
                run_id=run_id,
                factors=factors,
                track=track,
                y_col=y_col,
                split_role=split_role,
                include_base_controls=include_base_controls,
                include_baseline=include_baseline,
                n_bootstrap=n_bootstrap,
                bootstrap_seed=bootstrap_seed,
                max_steps=max_steps,
                split_seed_override=split_seed_override,
                split_ratio_override=split_ratio_override,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"equation path build failed: {exc}") from exc

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
