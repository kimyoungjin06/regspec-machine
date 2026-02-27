#!/usr/bin/env python3
"""Preset launcher for Phase-B regspec scan.

This wrapper keeps day-to-day commands short by mapping a small set of modes
to the long option list of the main runner.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _detect_repo_root(start: Path) -> Path:
    cur = start.resolve()
    for cand in [cur, *cur.parents]:
        if (cand / "AGENTS.md").is_file() and (cand / "modules").is_dir():
            return cand
    return Path(__file__).resolve().parents[2]


ROOT = _detect_repo_root(Path(__file__).resolve().parent)
_RUNNER_CANDIDATES = (
    ROOT
    / "modules"
    / "03_regspec_machine"
    / "scripts"
    / "modeling"
    / "run_phase_b_bikard_machine_scientist_scan.py",
    ROOT
    / "scripts"
    / "modeling"
    / "run_phase_b_bikard_machine_scientist_scan.py",
)
RUNNER = next((p for p in _RUNNER_CANDIDATES if p.is_file()), _RUNNER_CANDIDATES[0])


def _utc_ymd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(text).strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "run"


def _build_output_paths(run_id: str) -> Dict[str, str]:
    rid = _slug(run_id)
    return {
        "scan": f"outputs/tables/phase_b_bikard_machine_scientist_scan_runs_{rid}.csv",
        "top": f"outputs/tables/phase_b_bikard_machine_scientist_top_models_{rid}.csv",
        "log": f"data/metadata/phase_b_bikard_machine_scientist_search_log_{rid}.jsonl",
        "summary": f"data/metadata/phase_b_bikard_machine_scientist_run_summary_{rid}.json",
        "frontier": f"data/metadata/phase_b_bikard_machine_scientist_feasibility_frontier_{rid}.json",
        "registry": f"data/metadata/phase_b_bikard_machine_scientist_feature_registry_{rid}.json",
        "stability": f"data/metadata/phase_b_bikard_machine_scientist_restart_stability_{rid}.csv",
    }


def _ensure_parent(paths: List[str]) -> None:
    for raw in paths:
        Path(raw).parent.mkdir(parents=True, exist_ok=True)


def _to_repo_rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        try:
            return str(path.relative_to(ROOT))
        except Exception:
            return str(path)


def _safe_mtime_ns(path: Path) -> int:
    try:
        return int(path.stat().st_mtime_ns)
    except Exception:
        return -1


def _resolve_latest_file(glob_patterns: Sequence[str]) -> Optional[Path]:
    candidates: List[Path] = []
    for pattern in glob_patterns:
        for path in ROOT.glob(pattern):
            if path.is_file():
                candidates.append(path)
    if not candidates:
        return None
    return max(candidates, key=lambda p: (_safe_mtime_ns(p), p.as_posix()))


def _resolve_scan_input_overrides() -> List[Tuple[str, str, str]]:
    """Resolve scan inputs with locked-default first, latest-glob fallback second."""
    specs: Sequence[Tuple[str, str, Sequence[str]]] = (
        (
            "--input-dyad-base-csv",
            "outputs/tables/phase_b_bikard_policy_doc_twin_dyad_base_20260219.csv",
            ("outputs/tables/phase_b_bikard_policy_doc_twin_dyad_base_*.csv",),
        ),
        (
            "--input-extension-feature-csv",
            "data/metadata/metadata_extension_feature_table_overton20260130.csv",
            ("data/metadata/metadata_extension_feature_table_overton*.csv",),
        ),
        (
            "--input-phase-a-covariates-csv",
            "data/processed/phase_a_model_input_strict_pairs_api_backfilled_overton20260130_labeled.csv",
            ("data/processed/phase_a_model_input_strict_pairs_api_backfilled_overton*_labeled.csv",),
        ),
        (
            "--input-policy-split-csv",
            "outputs/tables/phase_b_keyfactor_explorer_policy_split_20260219.csv",
            ("outputs/tables/phase_b_keyfactor_explorer_policy_split_*.csv",),
        ),
    )
    out: List[Tuple[str, str, str]] = []
    for flag, default_rel, fallback_globs in specs:
        default_path = ROOT / default_rel
        if default_path.is_file():
            out.append((flag, default_rel, "default_locked"))
            continue
        latest = _resolve_latest_file(fallback_globs)
        if latest is not None:
            out.append((flag, _to_repo_rel(latest), "fallback_latest_glob"))
            continue
        out.append((flag, default_rel, "missing_default_no_fallback"))
    return out


@dataclass
class _PairChildResult:
    mode: str
    run_id: str
    status: str
    returncode: int
    command: List[str]
    outputs: Dict[str, str]
    error: str = ""


def _resolve_paired_summary_path(run_id: str, out_path: str) -> Path:
    text = str(out_path).strip()
    if text:
        return Path(text)
    rid = _slug(run_id)
    return ROOT / f"data/metadata/phase_b_bikard_machine_scientist_paired_preset_summary_{rid}.json"


def _resolve_direction_review_path(run_id: str, out_path: str) -> Path:
    text = str(out_path).strip()
    if text:
        return Path(text)
    rid = _slug(run_id)
    return ROOT / f"data/metadata/phase_b_bikard_machine_scientist_direction_review_{rid}.json"


def _resolve_paired_yall_contexts_path(run_id: str) -> Path:
    rid = _slug(run_id)
    return ROOT / f"data/metadata/phase_b_bikard_machine_scientist_y_contexts_paired_yall_only_{rid}.json"


def _resolve_overnight_checkpoint_path(run_id: str, out_path: str) -> Path:
    text = str(out_path).strip()
    if text:
        return Path(text)
    rid = _slug(run_id)
    return ROOT / f"data/metadata/phase_b_bikard_machine_scientist_overnight_checkpoint_{rid}.json"


def _resolve_overnight_summary_path(run_id: str, out_path: str) -> Path:
    text = str(out_path).strip()
    if text:
        return Path(text)
    rid = _slug(run_id)
    return ROOT / f"data/metadata/phase_b_bikard_machine_scientist_overnight_summary_{rid}.json"


def _write_paired_summary(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json_payload(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalize_output_path(raw_path: str) -> Path:
    p = Path(str(raw_path).strip())
    if p.is_absolute():
        return p
    return ROOT / p


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _min_nullable(current: Optional[float], new_value: Optional[float]) -> Optional[float]:
    if new_value is None:
        return current
    if current is None:
        return new_value
    return min(current, new_value)


def _resolve_child_run_summary_path(child: Dict[str, Any], run_id: str) -> Tuple[Optional[Path], str]:
    outputs = child.get("outputs", {}) if isinstance(child.get("outputs"), dict) else {}
    for key in ("run_summary_json", "summary"):
        raw = outputs.get(key)
        text = str(raw).strip() if raw is not None else ""
        if text:
            return _normalize_output_path(text), f"child_outputs:{key}"
    rid = str(run_id).strip()
    if rid:
        return _normalize_output_path(
            f"data/metadata/phase_b_bikard_machine_scientist_run_summary_{_slug(rid)}.json"
        ), "run_id_inferred"
    return None, "unresolved"


def _extract_top_models_metrics(path: Path) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "path": str(path),
        "exists": bool(path.is_file()),
        "n_rows": 0,
        "n_validation_ok_rows": 0,
        "n_validated_candidate_rows": 0,
        "n_validated_candidate_rows_primary_strict": 0,
        "n_support_candidate_rows": 0,
        "n_exploratory_rows": 0,
        "min_p_validation": None,
        "min_q_validation": None,
        "best_candidate_id": "",
        "best_candidate_track": "",
        "best_candidate_y_col": "",
        "best_candidate_p_validation": None,
        "best_candidate_q_validation": None,
    }
    if not path.is_file():
        return metrics
    best_q: Optional[float] = None
    best_p: Optional[float] = None
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            metrics["n_rows"] = _safe_int(metrics["n_rows"], 0) + 1
            tier = str(row.get("candidate_tier", "")).strip()
            track = str(row.get("track", "")).strip()
            if tier == "validated_candidate":
                metrics["n_validated_candidate_rows"] = (
                    _safe_int(metrics["n_validated_candidate_rows"], 0) + 1
                )
                if track == "primary_strict":
                    metrics["n_validated_candidate_rows_primary_strict"] = (
                        _safe_int(metrics["n_validated_candidate_rows_primary_strict"], 0) + 1
                    )
            elif tier == "support_candidate":
                metrics["n_support_candidate_rows"] = _safe_int(
                    metrics["n_support_candidate_rows"], 0
                ) + 1
            elif tier == "exploratory":
                metrics["n_exploratory_rows"] = _safe_int(metrics["n_exploratory_rows"], 0) + 1

            status_validation = str(row.get("status_validation", "")).strip().lower()
            p_val = _safe_float(row.get("p_boot_validation"))
            q_val = _safe_float(row.get("q_value_validation"))
            if status_validation == "ok":
                metrics["n_validation_ok_rows"] = _safe_int(metrics["n_validation_ok_rows"], 0) + 1
                metrics["min_p_validation"] = _min_nullable(metrics.get("min_p_validation"), p_val)
                metrics["min_q_validation"] = _min_nullable(metrics.get("min_q_validation"), q_val)

                cand_id = str(row.get("candidate_id", "")).strip()
                q_rank = q_val if q_val is not None else 10**9
                p_rank = p_val if p_val is not None else 10**9
                cur_q_rank = best_q if best_q is not None else 10**9
                cur_p_rank = best_p if best_p is not None else 10**9
                if (q_rank, p_rank) < (cur_q_rank, cur_p_rank):
                    best_q = q_val
                    best_p = p_val
                    metrics["best_candidate_id"] = cand_id
                    metrics["best_candidate_track"] = str(row.get("track", "")).strip()
                    metrics["best_candidate_y_col"] = str(row.get("y_col", "")).strip()
                    metrics["best_candidate_p_validation"] = p_val
                    metrics["best_candidate_q_validation"] = q_val
    return metrics


def _extract_restart_stability_metrics(path: Path) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "path": str(path),
        "exists": bool(path.is_file()),
        "n_candidates": 0,
        "max_validated_rate": None,
        "max_support_or_better_rate": None,
        "top_validated_candidate_id": "",
        "top_support_candidate_id": "",
    }
    if not path.is_file():
        return metrics
    best_validated_rate: Optional[float] = None
    best_support_rate: Optional[float] = None
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            metrics["n_candidates"] = _safe_int(metrics["n_candidates"], 0) + 1
            cand_id = str(row.get("candidate_id", "")).strip()
            v_rate = _safe_float(row.get("validated_rate"))
            s_rate = _safe_float(row.get("support_or_better_rate"))
            cur_max_v = _safe_float(metrics.get("max_validated_rate"))
            cur_max_s = _safe_float(metrics.get("max_support_or_better_rate"))
            if v_rate is not None and (cur_max_v is None or v_rate > cur_max_v):
                metrics["max_validated_rate"] = v_rate
            if s_rate is not None and (cur_max_s is None or s_rate > cur_max_s):
                metrics["max_support_or_better_rate"] = s_rate

            if v_rate is not None and (best_validated_rate is None or v_rate > best_validated_rate):
                best_validated_rate = v_rate
                metrics["top_validated_candidate_id"] = cand_id
            if s_rate is not None and (best_support_rate is None or s_rate > best_support_rate):
                best_support_rate = s_rate
                metrics["top_support_candidate_id"] = cand_id
    return metrics


def _extract_direction_branch_review(child: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(child.get("mode", ""))
    run_id = str(child.get("run_id", ""))
    run_summary_path, run_summary_path_source = _resolve_child_run_summary_path(child, run_id)
    run_summary_exists = bool(run_summary_path is not None and run_summary_path.is_file())
    run_summary: Dict[str, Any] = {}
    if run_summary_exists and run_summary_path is not None:
        run_summary = json.loads(run_summary_path.read_text(encoding="utf-8"))

    counts = run_summary.get("counts", {}) if isinstance(run_summary.get("counts"), dict) else {}
    top_tiers = (
        counts.get("candidate_tier_top_rows_inference")
        if isinstance(counts.get("candidate_tier_top_rows_inference"), dict)
        else {}
    )
    if not top_tiers and isinstance(counts.get("candidate_tier_top_rows"), dict):
        top_tiers = counts.get("candidate_tier_top_rows")
    outputs = run_summary.get("outputs", {}) if isinstance(run_summary.get("outputs"), dict) else {}
    child_outputs = child.get("outputs", {}) if isinstance(child.get("outputs"), dict) else {}

    top_models_inference_raw = str(
        outputs.get("top_models_inference_csv", child_outputs.get("top_models_inference_csv", ""))
    ).strip()
    if not top_models_inference_raw:
        top_models_inference_raw = str(
            outputs.get("top_models_csv", child_outputs.get("top_models_csv", ""))
        ).strip()
    restart_stability_raw = str(
        outputs.get("restart_stability_csv", child_outputs.get("restart_stability_csv", ""))
    ).strip()

    top_models_metrics = _extract_top_models_metrics(
        _normalize_output_path(top_models_inference_raw) if top_models_inference_raw else Path("")
    )
    restart_metrics = _extract_restart_stability_metrics(
        _normalize_output_path(restart_stability_raw) if restart_stability_raw else Path("")
    )
    track_consensus_meta = (
        run_summary.get("track_consensus_meta", {}) if isinstance(run_summary.get("track_consensus_meta"), dict) else {}
    )
    support_or_better_inference = _safe_int(top_tiers.get("validated_candidate"), 0) + _safe_int(
        top_tiers.get("support_candidate"), 0
    )
    singlex_track_consensus_enabled = bool(track_consensus_meta.get("enforce_track_consensus", False))
    singlex_track_consensus_retained = bool(
        singlex_track_consensus_enabled and support_or_better_inference > 0
    )
    return {
        "mode": mode,
        "run_id": run_id,
        "status": str(child.get("status", "")),
        "returncode": _safe_int(child.get("returncode"), 0),
        "run_summary_path": str(run_summary_path) if run_summary_path is not None else "",
        "run_summary_path_source": run_summary_path_source,
        "run_summary_exists": run_summary_exists,
        "scan_rows": _safe_int(counts.get("scan_rows"), 0),
        "top_rows": _safe_int(counts.get("top_rows"), 0),
        "top_rows_inference": _safe_int(counts.get("top_rows_inference"), 0),
        "validated_candidate_inference": _safe_int(top_tiers.get("validated_candidate"), 0),
        "validated_candidate_inference_primary_strict": _safe_int(
            top_models_metrics.get("n_validated_candidate_rows_primary_strict"),
            0,
        ),
        "support_candidate_inference": _safe_int(top_tiers.get("support_candidate"), 0),
        "exploratory_inference": _safe_int(top_tiers.get("exploratory"), 0),
        "support_or_better_inference": support_or_better_inference,
        "best_p_validation": top_models_metrics.get("min_p_validation"),
        "best_q_validation": top_models_metrics.get("min_q_validation"),
        "best_candidate_id": top_models_metrics.get("best_candidate_id", ""),
        "best_candidate_track": top_models_metrics.get("best_candidate_track", ""),
        "best_candidate_y_col": top_models_metrics.get("best_candidate_y_col", ""),
        "restart_max_validated_rate": restart_metrics.get("max_validated_rate"),
        "restart_max_support_or_better_rate": restart_metrics.get("max_support_or_better_rate"),
        "singlex_track_consensus_enabled": singlex_track_consensus_enabled,
        "singlex_track_consensus_demoted_from_validated": _safe_int(
            track_consensus_meta.get("n_rows_demoted_from_validated"), 0
        ),
        "singlex_track_consensus_retained_support_or_better": singlex_track_consensus_retained,
        "has_pq_evidence": bool(
            top_models_metrics.get("min_p_validation") is not None
            or top_models_metrics.get("min_q_validation") is not None
        ),
        "has_restart_stability": bool(restart_metrics.get("n_candidates", 0) > 0),
        "has_inference_rows": bool(_safe_int(counts.get("top_rows_inference"), 0) > 0),
    }


def _build_direction_review_payload(pair_payload: Dict[str, Any]) -> Dict[str, Any]:
    children = pair_payload.get("children", [])
    branches = [
        _extract_direction_branch_review(child)
        for child in children
        if isinstance(child, dict)
    ]
    by_mode = {str(row.get("mode", "")): row for row in branches}
    nooption_branch = next((v for k, v in by_mode.items() if "nooption" in k), {})
    singlex_branch = next((v for k, v in by_mode.items() if "singlex" in k), {})
    all_children_ok = all(str(row.get("status", "")).strip().lower() == "ok" for row in branches)
    ok_rows = [
        row
        for row in branches
        if str(row.get("status", "")).strip().lower() == "ok"
    ]
    required_fields_present = all(
        bool(row.get("has_inference_rows", False) and row.get("has_pq_evidence", False) and row.get("has_restart_stability", False))
        for row in ok_rows
    )
    if not ok_rows:
        required_fields_present = False
    singlex_consensus_check_pass = bool(
        singlex_branch.get("singlex_track_consensus_enabled", False)
        and singlex_branch.get("singlex_track_consensus_retained_support_or_better", False)
    )
    nooption_validated_candidate_inference = _safe_int(
        nooption_branch.get("validated_candidate_inference"),
        0,
    )
    nooption_validated_candidate_inference_primary_strict = _safe_int(
        nooption_branch.get("validated_candidate_inference_primary_strict"),
        0,
    )
    nooption_best_q_validation = _safe_float(nooption_branch.get("best_q_validation"))
    nooption_best_track = str(nooption_branch.get("best_candidate_track", "")).strip()
    nooption_restart_max_validated_rate = _safe_float(nooption_branch.get("restart_max_validated_rate"))
    nooption_q_threshold_for_promotion = 0.10
    nooption_restart_validated_rate_threshold_for_promotion = 0.50
    # Primary gate should reflect existence of validated primary candidates,
    # not whether the globally best-q candidate happens to be primary.
    nooption_primary_validated_gate_pass = bool(
        nooption_validated_candidate_inference_primary_strict > 0
    )
    nooption_q_gate_pass = bool(
        nooption_best_q_validation is not None
        and nooption_best_q_validation <= nooption_q_threshold_for_promotion
    )
    nooption_restart_validated_rate_gate_pass = bool(
        nooption_restart_max_validated_rate is not None
        and nooption_restart_max_validated_rate >= nooption_restart_validated_rate_threshold_for_promotion
    )
    nooption_promotion_gate_pass = bool(
        nooption_primary_validated_gate_pass
        and nooption_q_gate_pass
        and nooption_restart_validated_rate_gate_pass
    )
    primary_objective_gate_pass = bool(
        all_children_ok
        and required_fields_present
        and singlex_consensus_check_pass
        and nooption_promotion_gate_pass
    )
    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "paired": {
            "mode": str(pair_payload.get("mode", "")),
            "run_id": str(pair_payload.get("run_id", "")),
            "status": str(pair_payload.get("status", "")),
            "children_count": len(branches),
        },
        "checks": {
            "all_children_ok": all_children_ok,
            "required_fields_present": required_fields_present,
            "singlex_track_consensus_check_pass": singlex_consensus_check_pass,
            "nooption_primary_validated_gate_pass": nooption_primary_validated_gate_pass,
            "nooption_q_gate_pass": nooption_q_gate_pass,
            "nooption_restart_validated_rate_gate_pass": nooption_restart_validated_rate_gate_pass,
            "nooption_promotion_gate_pass": nooption_promotion_gate_pass,
            "primary_objective_gate_pass": primary_objective_gate_pass,
        },
        "comparison": {
            "nooption_validated_candidate_inference": nooption_validated_candidate_inference,
            "nooption_validated_candidate_inference_primary_strict": (
                nooption_validated_candidate_inference_primary_strict
            ),
            "singlex_validated_candidate_inference": _safe_int(
                singlex_branch.get("validated_candidate_inference"),
                0,
            ),
            "nooption_best_p_validation": nooption_branch.get("best_p_validation"),
            "singlex_best_p_validation": singlex_branch.get("best_p_validation"),
            "nooption_best_q_validation": nooption_best_q_validation,
            "singlex_best_q_validation": singlex_branch.get("best_q_validation"),
            "nooption_restart_max_validated_rate": nooption_restart_max_validated_rate,
            "singlex_restart_max_validated_rate": singlex_branch.get("restart_max_validated_rate"),
            "singlex_track_consensus_demoted_from_validated": _safe_int(
                singlex_branch.get("singlex_track_consensus_demoted_from_validated"),
                0,
            ),
            "nooption_best_candidate_track": nooption_best_track,
            "nooption_q_threshold_for_promotion": nooption_q_threshold_for_promotion,
            "nooption_restart_validated_rate_threshold_for_promotion": (
                nooption_restart_validated_rate_threshold_for_promotion
            ),
        },
        "branches": branches,
    }


def _write_singlex_registry(path: Path, run_id: str) -> None:
    payload = {
        "meta": {
            "source": "preset_singlex",
            "run_id": run_id,
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "feature_registry": [
            {
                "feature_name": "is_academia_origin",
                "allowed_in_scan": 1,
            }
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_yall_only_contexts(
    path: Path,
    run_id: str,
    *,
    source: str = "preset_singlex_baseline",
) -> None:
    payload = {
        "meta": {
            "source": str(source),
            "run_id": run_id,
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "contexts": [
            {
                "context_scope": "all_contexts",
                "y_col": "y_all",
            }
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_hypothesis_window_years(raw: str) -> List[int]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("hypothesis-window-years must be a comma-separated list of positive integers")
    out: List[int] = []
    seen: set[int] = set()
    for token in text.split(","):
        tok = str(token).strip()
        if not tok:
            continue
        try:
            year = int(tok)
        except Exception as exc:
            raise ValueError(
                "hypothesis-window-years must be a comma-separated list of positive integers"
            ) from exc
        if year <= 0:
            raise ValueError("hypothesis-window-years must be positive integers")
        if year in seen:
            continue
        seen.add(year)
        out.append(year)
    if not out:
        raise ValueError("hypothesis-window-years must include at least one positive integer")
    return out


def _resolve_hypothesis_confirmatory_years(
    *,
    hypothesis_window_years: List[int],
    confirmatory_window_years_raw: str,
) -> List[int]:
    if not hypothesis_window_years:
        raise ValueError("hypothesis_window_years is empty")
    text = str(confirmatory_window_years_raw or "").strip()
    if not text:
        return list(hypothesis_window_years)
    confirm_years = _parse_hypothesis_window_years(text)
    allowed = set(int(x) for x in hypothesis_window_years)
    invalid = [int(y) for y in confirm_years if int(y) not in allowed]
    if invalid:
        raise ValueError(
            "hypothesis-confirmatory-window-years must be a subset of --hypothesis-window-years"
        )
    return confirm_years


def _write_hypothesis_panel_contexts(path: Path, run_id: str, years: List[int]) -> None:
    contexts = [
        {
            "context_scope": f"all_contexts_{int(year)}y",
            "y_col": f"y_{int(year)}y",
        }
        for year in years
    ]
    payload = {
        "meta": {
            "source": "preset_hypothesis_panel",
            "run_id": run_id,
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window_years": [int(y) for y in years],
        },
        "contexts": contexts,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_positive_int_csv(raw: str, *, field_name: str) -> List[int]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError(f"{field_name} must be a comma-separated list of positive integers")
    out: List[int] = []
    seen: set[int] = set()
    for token in text.split(","):
        tok = str(token).strip()
        if not tok:
            continue
        try:
            value = int(tok)
        except Exception as exc:
            raise ValueError(
                f"{field_name} must be a comma-separated list of positive integers"
            ) from exc
        if value <= 0:
            raise ValueError(f"{field_name} must be a comma-separated list of positive integers")
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    if not out:
        raise ValueError(f"{field_name} must include at least one positive integer")
    return out


def _overnight_job_key(seed: int, n_bootstrap: int) -> str:
    return f"s{int(seed)}_b{int(n_bootstrap)}"


def _build_overnight_jobs(seed_grid: List[int], bootstrap_ladder: List[int]) -> List[Dict[str, int]]:
    jobs: List[Dict[str, int]] = []
    for n_bootstrap in bootstrap_ladder:
        for seed in seed_grid:
            jobs.append(
                {
                    "seed": int(seed),
                    "n_bootstrap": int(n_bootstrap),
                    "job_key": _overnight_job_key(int(seed), int(n_bootstrap)),
                }
            )
    return jobs


def _build_overnight_aggregate(attempts: List[Dict[str, Any]]) -> Dict[str, Any]:
    n_total = len(attempts)
    n_ok = sum(1 for row in attempts if _safe_int(row.get("returncode"), 1) == 0)
    n_failed = n_total - n_ok
    n_with_direction = 0
    n_gate_pass = 0
    completed_job_keys = {
        str(row.get("job_key", "")).strip()
        for row in attempts
        if str(row.get("job_key", "")).strip()
    }

    best_metrics: Dict[str, Dict[str, Any]] = {
        "nooption_best_q_validation": {
            "value": None,
            "run_id": "",
            "child_run_id": "",
            "seed": None,
            "n_bootstrap": None,
            "job_key": "",
        },
        "singlex_best_q_validation": {
            "value": None,
            "run_id": "",
            "child_run_id": "",
            "seed": None,
            "n_bootstrap": None,
            "job_key": "",
        },
        "nooption_best_p_validation": {
            "value": None,
            "run_id": "",
            "child_run_id": "",
            "seed": None,
            "n_bootstrap": None,
            "job_key": "",
        },
        "singlex_best_p_validation": {
            "value": None,
            "run_id": "",
            "child_run_id": "",
            "seed": None,
            "n_bootstrap": None,
            "job_key": "",
        },
    }

    for row in attempts:
        checks = row.get("direction_review_checks", {})
        if isinstance(checks, dict) and checks:
            n_with_direction += 1
            if bool(checks.get("primary_objective_gate_pass", False)):
                n_gate_pass += 1
        comparison = row.get("direction_review_comparison", {})
        if not isinstance(comparison, dict):
            continue
        for metric_name in (
            "nooption_best_q_validation",
            "singlex_best_q_validation",
            "nooption_best_p_validation",
            "singlex_best_p_validation",
        ):
            value = _safe_float(comparison.get(metric_name))
            if value is None:
                continue
            snapshot = best_metrics[metric_name]
            current = _safe_float(snapshot.get("value"))
            if current is None or value < current:
                snapshot["value"] = value
                snapshot["run_id"] = str(row.get("child_run_id", ""))
                snapshot["child_run_id"] = str(row.get("child_run_id", ""))
                snapshot["seed"] = _safe_int(row.get("seed"), 0)
                snapshot["n_bootstrap"] = _safe_int(row.get("n_bootstrap"), 0)
                snapshot["job_key"] = str(row.get("job_key", ""))

    gate_rate = (float(n_gate_pass) / float(n_with_direction)) if n_with_direction > 0 else None
    return {
        "n_attempts_total": int(n_total),
        "n_attempts_returncode_zero": int(n_ok),
        "n_attempts_returncode_nonzero": int(n_failed),
        "n_attempts_with_direction_review": int(n_with_direction),
        "n_primary_objective_gate_pass": int(n_gate_pass),
        "primary_objective_gate_pass_rate": gate_rate,
        "n_unique_job_keys_completed": int(len(completed_job_keys)),
        "best_metrics": best_metrics,
    }


def _build_paired_child_outputs(run_id: str) -> Dict[str, str]:
    out = _build_output_paths(run_id)
    return {
        "scan_runs_csv": out["scan"],
        "top_models_csv": out["top"],
        "search_log_jsonl": out["log"],
        "run_summary_json": out["summary"],
        "feasibility_frontier_json": out["frontier"],
        "feature_registry_json": out["registry"],
        "restart_stability_csv": out["stability"],
    }


def _build_paired_common_tail(args: argparse.Namespace) -> List[str]:
    common_tail: List[str] = [
        "--runner-python",
        str(args.runner_python),
        "--cli-summary-top-n",
        str(int(args.cli_summary_top_n)),
    ]
    if int(args.scan_n_bootstrap) > 0:
        common_tail.extend(["--scan-n-bootstrap", str(int(args.scan_n_bootstrap))])
    if int(args.scan_max_features) > 0:
        common_tail.extend(["--scan-max-features", str(int(args.scan_max_features))])
    if bool(args.paired_legacy_sync_validation):
        common_tail.append("--extra-arg=--legacy-single-gate-sync-validation")
    for extra in args.extra_arg:
        text = str(extra).strip()
        if text:
            common_tail.append(f"--extra-arg={text}")
    return common_tail


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--mode",
        choices=(
            "nooption",
            "nooption_baseline",
            "singlex",
            "singlex_baseline",
            "singlex_hypothesis_panel",
            "openexplore",
            "openexplore_autorefine",
            "nooption_hypothesis_panel",
            "paired_nooption_singlex",
            "paired_nooption_singlex_hypothesis",
            "overnight_validation",
        ),
        default="paired_nooption_singlex",
    )
    p.add_argument("--run-id", default="")
    p.add_argument("--scan-n-bootstrap", type=int, default=0)
    p.add_argument("--scan-max-features", type=int, default=0)
    p.add_argument("--refine-n-bootstrap", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--runner-python", default=".venv/bin/python")
    p.add_argument("--cli-summary-top-n", type=int, default=10)
    p.add_argument(
        "--paired-legacy-sync-validation",
        action="store_true",
        default=False,
        help=(
            "paired mode only: forward --legacy-single-gate-sync-validation to child runner commands"
        ),
    )
    p.add_argument("--extra-arg", action="append", default=[])
    p.add_argument("--out-paired-summary-json", default="")
    p.add_argument(
        "--out-direction-review-json",
        default="",
        help=(
            "paired mode only: output path for automated direction-review summary "
            "(validated/pq/restart-stability/track-consensus checks)"
        ),
    )
    p.add_argument(
        "--skip-direction-review",
        action="store_true",
        default=False,
        help="paired mode only: skip automated direction-review summary generation",
    )
    p.add_argument(
        "--paired-y-context-alignment",
        choices=("off", "y_all_only"),
        default="y_all_only",
        help="paired_nooption_singlex only: enforce same y contexts across nooption/singlex branches",
    )
    p.add_argument(
        "--paired-run-singlex-on-nooption-failure",
        action="store_true",
        default=True,
        help="paired mode only: run singlex branch even if nooption branch fails",
    )
    p.add_argument(
        "--no-paired-run-singlex-on-nooption-failure",
        dest="paired_run_singlex_on_nooption_failure",
        action="store_false",
        help="paired mode only: skip singlex branch when nooption branch fails",
    )
    p.add_argument(
        "--hypothesis-window-years",
        default="3,5,10",
        help="comma-separated year windows used in *_hypothesis_panel modes",
    )
    p.add_argument(
        "--hypothesis-confirmatory-window-years",
        default="3,5",
        help="comma-separated confirmatory windows (subset of --hypothesis-window-years)",
    )
    p.add_argument(
        "--hypothesis-time-series-precheck-mode",
        choices=("off", "warn", "fail_redundant_confirmatory", "fail_low_support", "fail_any"),
        default="fail_redundant_confirmatory",
        help="time-series precheck mode forwarded in *_hypothesis_panel modes",
    )
    p.add_argument(
        "--hypothesis-auto-confirmatory-policy",
        choices=("off", "drop_redundant", "drop_redundant_and_low_support"),
        default="drop_redundant_and_low_support",
        help="auto confirmatory adjustment policy for *_hypothesis_panel modes",
    )
    p.add_argument(
        "--hypothesis-nonconfirmatory-max-tier",
        choices=("support_candidate", "exploratory"),
        default="exploratory",
        help="tier cap for non-confirmatory y contexts in *_hypothesis_panel modes",
    )
    p.add_argument("--hypothesis-time-series-min-positive-events", type=int, default=20)
    p.add_argument("--hypothesis-time-series-min-track-positive-events", type=int, default=0)
    p.add_argument("--hypothesis-time-series-min-positive-share", type=float, default=0.05)
    p.add_argument(
        "--max-hours",
        type=float,
        default=8.0,
        help=(
            "overnight_validation only: time budget in hours for iterative paired runs; "
            "0 or negative means no time limit"
        ),
    )
    p.add_argument(
        "--seed-grid",
        default="20260219,20260220,20260221,20260222",
        help="overnight_validation only: comma-separated split/bootstrap seeds",
    )
    p.add_argument(
        "--bootstrap-ladder",
        default="49,99,199",
        help="overnight_validation only: comma-separated n-bootstrap ladder",
    )
    p.add_argument(
        "--checkpoint-json",
        default="",
        help="overnight_validation only: checkpoint path for iterative progress",
    )
    p.add_argument(
        "--out-overnight-summary-json",
        default="",
        help="overnight_validation only: aggregate summary output path",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="overnight_validation only: resume from checkpoint-json if present",
    )
    p.add_argument(
        "--stop-on-fatal",
        action="store_true",
        default=False,
        help="overnight_validation only: stop immediately on first non-zero child returncode",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    ymd = _utc_ymd()
    mode = str(args.mode)
    run_id = str(args.run_id).strip()
    if not run_id:
        run_id = f"phase_b_bikard_keyfactor_scan_{mode}_{ymd}"

    if not RUNNER.is_file():
        tried = ", ".join(str(p) for p in _RUNNER_CANDIDATES)
        raise FileNotFoundError(f"runner script not found (tried: {tried})")

    if mode == "overnight_validation":
        max_hours = float(args.max_hours)
        max_seconds: Optional[float] = None if max_hours <= 0 else float(max_hours * 3600.0)
        seed_grid = _parse_positive_int_csv(args.seed_grid, field_name="seed-grid")
        bootstrap_ladder = _parse_positive_int_csv(
            args.bootstrap_ladder, field_name="bootstrap-ladder"
        )
        jobs = _build_overnight_jobs(seed_grid, bootstrap_ladder)
        checkpoint_path = _resolve_overnight_checkpoint_path(run_id, args.checkpoint_json)
        overnight_summary_path = _resolve_overnight_summary_path(run_id, args.out_overnight_summary_json)

        attempts: List[Dict[str, Any]] = []
        started_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if bool(args.resume) and checkpoint_path.is_file():
            resume_payload = _read_json_payload(checkpoint_path)
            if resume_payload:
                prev_mode = str(resume_payload.get("mode", "")).strip()
                prev_run_id = str(resume_payload.get("run_id", "")).strip()
                if prev_mode and prev_mode != mode:
                    raise ValueError(
                        f"resume checkpoint mode mismatch: expected {mode}, found {prev_mode}"
                    )
                if prev_run_id and prev_run_id != run_id:
                    raise ValueError(
                        f"resume checkpoint run_id mismatch: expected {run_id}, found {prev_run_id}"
                    )
                prev_attempts = resume_payload.get("attempts", [])
                if isinstance(prev_attempts, list):
                    attempts = [row for row in prev_attempts if isinstance(row, dict)]
                prev_started_at = str(resume_payload.get("started_at_utc", "")).strip()
                if prev_started_at:
                    started_at_utc = prev_started_at
                prev_config = (
                    resume_payload.get("config", {})
                    if isinstance(resume_payload.get("config"), dict)
                    else {}
                )
                prev_seed_grid = prev_config.get("seed_grid", [])
                prev_bootstrap_ladder = prev_config.get("bootstrap_ladder", [])
                if isinstance(prev_seed_grid, list) and prev_seed_grid:
                    prev_seed_norm = [
                        _safe_int(value, 0) for value in prev_seed_grid if _safe_int(value, 0) > 0
                    ]
                    if prev_seed_norm and prev_seed_norm != seed_grid:
                        raise ValueError(
                            "resume checkpoint seed-grid mismatch; use the same --seed-grid as the checkpoint"
                        )
                if isinstance(prev_bootstrap_ladder, list) and prev_bootstrap_ladder:
                    prev_bootstrap_norm = [
                        _safe_int(value, 0)
                        for value in prev_bootstrap_ladder
                        if _safe_int(value, 0) > 0
                    ]
                    if prev_bootstrap_norm and prev_bootstrap_norm != bootstrap_ladder:
                        raise ValueError(
                            "resume checkpoint bootstrap-ladder mismatch; use the same --bootstrap-ladder as the checkpoint"
                        )

        completed_job_keys: set[str] = set()
        for row in attempts:
            status = str(row.get("status", "")).strip().lower()
            if status not in {"ok", "failed", "partial_failure"}:
                continue
            key = str(row.get("job_key", "")).strip()
            if not key:
                seed = _safe_int(row.get("seed"), 0)
                n_bootstrap = _safe_int(row.get("n_bootstrap"), 0)
                if seed > 0 and n_bootstrap > 0:
                    key = _overnight_job_key(seed, n_bootstrap)
            if key:
                completed_job_keys.add(key)

        pending_jobs = [row for row in jobs if str(row.get("job_key", "")) not in completed_job_keys]
        checkpoint_payload: Dict[str, Any] = {
            "mode": mode,
            "run_id": run_id,
            "status": "running",
            "started_at_utc": started_at_utc,
            "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "config": {
                "max_hours": max_hours,
                "seed_grid": [int(x) for x in seed_grid],
                "bootstrap_ladder": [int(x) for x in bootstrap_ladder],
                "stop_on_fatal": bool(args.stop_on_fatal),
                "resume": bool(args.resume),
                "runner_python": str(args.runner_python),
                "cli_summary_top_n": int(args.cli_summary_top_n),
                "scan_max_features": int(args.scan_max_features),
                "paired_legacy_sync_validation": bool(args.paired_legacy_sync_validation),
                "paired_y_context_alignment": str(args.paired_y_context_alignment),
                "paired_run_singlex_on_nooption_failure": bool(
                    args.paired_run_singlex_on_nooption_failure
                ),
                "skip_direction_review": bool(args.skip_direction_review),
                "extra_arg": [str(x) for x in list(args.extra_arg or []) if str(x).strip()],
            },
            "paths": {
                "checkpoint_json": str(checkpoint_path),
                "overnight_summary_json": str(overnight_summary_path),
            },
            "plan": {
                "jobs_total": int(len(jobs)),
                "jobs_completed_from_checkpoint": int(len(completed_job_keys)),
                "jobs_pending": int(len(pending_jobs)),
            },
            "attempts": attempts,
        }
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        overnight_summary_path.parent.mkdir(parents=True, exist_ok=True)

        preset_script = Path(__file__).resolve()
        rel_script = str(preset_script.relative_to(ROOT))

        if args.dry_run:
            print("[dry-run] overnight_validation plan:")
            print(f"  run_id: {run_id}")
            print(f"  max_hours: {max_hours}")
            print(f"  seed_grid: {seed_grid}")
            print(f"  bootstrap_ladder: {bootstrap_ladder}")
            print(f"  pending_jobs: {len(pending_jobs)} / total_jobs: {len(jobs)}")
            print("  checkpoint_json:", str(checkpoint_path))
            print("  overnight_summary_json:", str(overnight_summary_path))
            for idx, job in enumerate(pending_jobs, start=1):
                seed = int(job["seed"])
                n_bootstrap = int(job["n_bootstrap"])
                child_run_id = f"{run_id}__overnight_b{n_bootstrap}_s{seed}"
                paired_summary = _resolve_paired_summary_path(child_run_id, "")
                direction_review = _resolve_direction_review_path(child_run_id, "")
                cmd = [
                    str(args.runner_python),
                    rel_script,
                    "--mode",
                    "paired_nooption_singlex",
                    "--run-id",
                    child_run_id,
                    "--runner-python",
                    str(args.runner_python),
                    "--scan-n-bootstrap",
                    str(n_bootstrap),
                    "--cli-summary-top-n",
                    str(int(args.cli_summary_top_n)),
                    "--paired-y-context-alignment",
                    str(args.paired_y_context_alignment),
                    "--out-paired-summary-json",
                    str(paired_summary),
                ]
                if not bool(args.skip_direction_review):
                    cmd.extend(["--out-direction-review-json", str(direction_review)])
                if bool(args.skip_direction_review):
                    cmd.append("--skip-direction-review")
                if bool(args.paired_legacy_sync_validation):
                    cmd.append("--paired-legacy-sync-validation")
                if not bool(args.paired_run_singlex_on_nooption_failure):
                    cmd.append("--no-paired-run-singlex-on-nooption-failure")
                if int(args.scan_max_features) > 0:
                    cmd.extend(["--scan-max-features", str(int(args.scan_max_features))])
                cmd.extend(
                    [
                        "--extra-arg=--split-seed",
                        f"--extra-arg={seed}",
                        "--extra-arg=--bootstrap-seed",
                        f"--extra-arg={seed}",
                    ]
                )
                for extra in args.extra_arg:
                    text = str(extra).strip()
                    if text:
                        cmd.append(f"--extra-arg={text}")
                print(f"  [{idx}/{len(pending_jobs)}] " + " ".join(cmd))
            return 0

        _write_paired_summary(checkpoint_path, checkpoint_payload)
        loop_started_monotonic = time.monotonic()
        timed_out = False
        fatal_stopped = False

        for idx, job in enumerate(pending_jobs, start=1):
            elapsed = float(time.monotonic() - loop_started_monotonic)
            if max_seconds is not None and elapsed >= max_seconds:
                timed_out = True
                break

            seed = int(job["seed"])
            n_bootstrap = int(job["n_bootstrap"])
            job_key = str(job["job_key"])
            child_run_id = f"{run_id}__overnight_b{n_bootstrap}_s{seed}"
            paired_summary = _resolve_paired_summary_path(child_run_id, "")
            direction_review = _resolve_direction_review_path(child_run_id, "")
            cmd = [
                str(args.runner_python),
                rel_script,
                "--mode",
                "paired_nooption_singlex",
                "--run-id",
                child_run_id,
                "--runner-python",
                str(args.runner_python),
                "--scan-n-bootstrap",
                str(n_bootstrap),
                "--cli-summary-top-n",
                str(int(args.cli_summary_top_n)),
                "--paired-y-context-alignment",
                str(args.paired_y_context_alignment),
                "--out-paired-summary-json",
                str(paired_summary),
            ]
            if not bool(args.skip_direction_review):
                cmd.extend(["--out-direction-review-json", str(direction_review)])
            if bool(args.skip_direction_review):
                cmd.append("--skip-direction-review")
            if bool(args.paired_legacy_sync_validation):
                cmd.append("--paired-legacy-sync-validation")
            if not bool(args.paired_run_singlex_on_nooption_failure):
                cmd.append("--no-paired-run-singlex-on-nooption-failure")
            if int(args.scan_max_features) > 0:
                cmd.extend(["--scan-max-features", str(int(args.scan_max_features))])
            cmd.extend(
                [
                    "--extra-arg=--split-seed",
                    f"--extra-arg={seed}",
                    "--extra-arg=--bootstrap-seed",
                    f"--extra-arg={seed}",
                ]
            )
            for extra in args.extra_arg:
                text = str(extra).strip()
                if text:
                    cmd.append(f"--extra-arg={text}")

            print(
                f"[overnight] [{idx}/{len(pending_jobs)}] seed={seed} n_bootstrap={n_bootstrap} run_id={child_run_id}"
            )
            print("[overnight] command:", " ".join(cmd))
            started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            iter_started_monotonic = time.monotonic()
            proc = subprocess.run(cmd, cwd=ROOT, check=False)
            returncode = int(proc.returncode)
            finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            elapsed_sec = float(time.monotonic() - iter_started_monotonic)

            attempt: Dict[str, Any] = {
                "job_key": job_key,
                "seed": seed,
                "n_bootstrap": n_bootstrap,
                "child_run_id": child_run_id,
                "status": "ok" if returncode == 0 else "failed",
                "returncode": returncode,
                "started_at_utc": started_at,
                "finished_at_utc": finished_at,
                "elapsed_sec": elapsed_sec,
                "command": cmd,
                "paired_summary_json": str(paired_summary),
                "direction_review_json": (
                    "" if bool(args.skip_direction_review) else str(direction_review)
                ),
            }

            paired_payload = _read_json_payload(paired_summary)
            if paired_payload:
                attempt["paired_status"] = str(paired_payload.get("status", ""))
                children = paired_payload.get("children", [])
                if isinstance(children, list):
                    attempt["children"] = [
                        {
                            "mode": str(row.get("mode", "")),
                            "status": str(row.get("status", "")),
                            "returncode": _safe_int(row.get("returncode"), 0),
                        }
                        for row in children
                        if isinstance(row, dict)
                    ]

            if not bool(args.skip_direction_review):
                direction_payload = _read_json_payload(direction_review)
                checks = (
                    direction_payload.get("checks", {})
                    if isinstance(direction_payload.get("checks"), dict)
                    else {}
                )
                comparison = (
                    direction_payload.get("comparison", {})
                    if isinstance(direction_payload.get("comparison"), dict)
                    else {}
                )
                if checks:
                    attempt["direction_review_checks"] = checks
                if comparison:
                    attempt["direction_review_comparison"] = comparison

            checkpoint_payload["attempts"].append(attempt)
            checkpoint_payload["updated_at_utc"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            _write_paired_summary(checkpoint_path, checkpoint_payload)

            if returncode != 0 and bool(args.stop_on_fatal):
                fatal_stopped = True
                break

        aggregate = _build_overnight_aggregate(
            [
                row
                for row in checkpoint_payload.get("attempts", [])
                if isinstance(row, dict)
            ]
        )
        completed_keys_final = {
            str(row.get("job_key", "")).strip()
            for row in checkpoint_payload.get("attempts", [])
            if isinstance(row, dict)
            and str(row.get("status", "")).strip().lower() in {"ok", "failed", "partial_failure"}
            and str(row.get("job_key", "")).strip()
        }
        pending_after = max(0, int(len(jobs) - len(completed_keys_final)))
        checkpoint_plan = checkpoint_payload.get("plan")
        if isinstance(checkpoint_plan, dict):
            checkpoint_plan["jobs_total"] = int(len(jobs))
            checkpoint_plan["jobs_completed"] = int(len(completed_keys_final))
            checkpoint_plan["jobs_pending"] = int(pending_after)
        aggregate["n_jobs_total"] = int(len(jobs))
        aggregate["n_jobs_completed"] = int(len(completed_keys_final))
        aggregate["n_jobs_pending"] = int(pending_after)

        n_failed = _safe_int(aggregate.get("n_attempts_returncode_nonzero"), 0)
        if n_failed > 0 and fatal_stopped:
            final_status = "failed"
        elif n_failed > 0:
            final_status = "partial_failure"
        elif timed_out and pending_after > 0:
            final_status = "stopped_max_hours"
        elif pending_after > 0:
            final_status = "incomplete"
        else:
            final_status = "ok"

        checkpoint_payload["status"] = final_status
        checkpoint_payload["aggregate"] = aggregate
        checkpoint_payload["updated_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        checkpoint_payload["finished_at_utc"] = checkpoint_payload["updated_at_utc"]
        _write_paired_summary(checkpoint_path, checkpoint_payload)

        overnight_summary_payload: Dict[str, Any] = {
            "mode": mode,
            "run_id": run_id,
            "status": final_status,
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "checkpoint_json": str(checkpoint_path),
            "config": checkpoint_payload.get("config", {}),
            "aggregate": aggregate,
            "attempts": checkpoint_payload.get("attempts", []),
        }
        _write_paired_summary(overnight_summary_path, overnight_summary_payload)

        print("[overnight] checkpoint_json:", str(checkpoint_path))
        print("[overnight] overnight_summary_json:", str(overnight_summary_path))
        print("[overnight] final_status:", final_status)
        print("[overnight] aggregate:", json.dumps(aggregate, ensure_ascii=False))
        return 0 if n_failed == 0 else 2

    if mode in {"paired_nooption_singlex", "paired_nooption_singlex_hypothesis"}:
        preset_script = Path(__file__).resolve()
        rel_script = str(preset_script.relative_to(ROOT))
        if mode == "paired_nooption_singlex_hypothesis":
            nooption_child_mode = "nooption_hypothesis_panel"
            singlex_child_mode = "singlex_hypothesis_panel"
            nooption_run_id = f"{run_id}__nooption_hypothesis_panel"
            singlex_run_id = f"{run_id}__singlex_hypothesis_panel"
        else:
            nooption_child_mode = "nooption_baseline"
            singlex_child_mode = "singlex_baseline"
            nooption_run_id = f"{run_id}__nooption_baseline"
            singlex_run_id = f"{run_id}__singlex"
        nooption_outputs = _build_paired_child_outputs(nooption_run_id)
        singlex_outputs = _build_paired_child_outputs(singlex_run_id)
        paired_summary_path = _resolve_paired_summary_path(run_id, args.out_paired_summary_json)
        direction_review_path = _resolve_direction_review_path(run_id, args.out_direction_review_json)
        direction_review_enabled = not bool(args.skip_direction_review)
        common_tail = _build_paired_common_tail(args)
        paired_y_contexts_path = ""
        if mode == "paired_nooption_singlex" and str(args.paired_y_context_alignment) == "y_all_only":
            paired_yall_contexts_path = _resolve_paired_yall_contexts_path(run_id)
            paired_y_contexts_path = str(paired_yall_contexts_path.relative_to(ROOT))
            if not bool(args.dry_run):
                _write_yall_only_contexts(
                    paired_yall_contexts_path,
                    run_id,
                    source="preset_paired_nooption_singlex",
                )
            common_tail.extend(
                [
                    "--extra-arg=--y-contexts-json",
                    f"--extra-arg={paired_y_contexts_path}",
                    "--extra-arg=--y-contexts-merge-mode",
                    "--extra-arg=replace",
                ]
            )
        if mode == "paired_nooption_singlex_hypothesis":
            common_tail.extend(
                [
                    "--hypothesis-window-years",
                    str(args.hypothesis_window_years),
                    "--hypothesis-confirmatory-window-years",
                    str(args.hypothesis_confirmatory_window_years),
                    "--hypothesis-time-series-precheck-mode",
                    str(args.hypothesis_time_series_precheck_mode),
                    "--hypothesis-auto-confirmatory-policy",
                    str(args.hypothesis_auto_confirmatory_policy),
                    "--hypothesis-nonconfirmatory-max-tier",
                    str(args.hypothesis_nonconfirmatory_max_tier),
                    "--hypothesis-time-series-min-positive-events",
                    str(int(args.hypothesis_time_series_min_positive_events)),
                    "--hypothesis-time-series-min-track-positive-events",
                    str(int(args.hypothesis_time_series_min_track_positive_events)),
                    "--hypothesis-time-series-min-positive-share",
                    str(float(args.hypothesis_time_series_min_positive_share)),
                ]
            )

        cmd_nooption = [
            args.runner_python,
            rel_script,
            "--mode",
            nooption_child_mode,
            "--run-id",
            nooption_run_id,
            *common_tail,
        ]
        cmd_singlex = [
            args.runner_python,
            rel_script,
            "--mode",
            singlex_child_mode,
            "--run-id",
            singlex_run_id,
            *common_tail,
        ]
        if args.dry_run:
            print("[dry-run] paired mode commands:")
            print(f"  [{nooption_child_mode}] " + " ".join(cmd_nooption))
            print(f"  [{singlex_child_mode}] " + " ".join(cmd_singlex))
            print("  [paired_summary_json] " + str(paired_summary_path))
            if direction_review_enabled:
                print("  [direction_review_json] " + str(direction_review_path))
            return 0

        pair_payload: Dict[str, object] = {
            "mode": mode,
            "run_id": run_id,
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": "running",
            "paired_legacy_sync_validation": bool(args.paired_legacy_sync_validation),
            "hypothesis_window_years": str(args.hypothesis_window_years),
            "hypothesis_confirmatory_window_years": str(args.hypothesis_confirmatory_window_years),
            "hypothesis_time_series_precheck_mode": str(args.hypothesis_time_series_precheck_mode),
            "hypothesis_auto_confirmatory_policy": str(args.hypothesis_auto_confirmatory_policy),
            "hypothesis_nonconfirmatory_max_tier": str(args.hypothesis_nonconfirmatory_max_tier),
            "direction_review_enabled": direction_review_enabled,
            "direction_review_json": str(direction_review_path) if direction_review_enabled else "",
            "paired_y_context_alignment": str(args.paired_y_context_alignment),
            "paired_y_contexts_json": paired_y_contexts_path,
            "paired_run_singlex_on_nooption_failure": bool(
                args.paired_run_singlex_on_nooption_failure
            ),
            "commands": {
                nooption_child_mode: cmd_nooption,
                singlex_child_mode: cmd_singlex,
            },
            "children": [],
        }

        def _append_child(result: _PairChildResult) -> None:
            pair_payload["children"].append(
                {
                    "mode": result.mode,
                    "run_id": result.run_id,
                    "status": result.status,
                    "returncode": int(result.returncode),
                    "command": result.command,
                    "outputs": result.outputs,
                    "error": result.error,
                }
            )
            _write_paired_summary(paired_summary_path, pair_payload)

        print("[preset-run] mode:", mode)
        print("[preset-run] run_id:", run_id)
        print("[preset-run] paired_summary_json:", str(paired_summary_path))
        print(f"[preset-run] command({nooption_child_mode}):", " ".join(cmd_nooption))
        nooption_failed = False
        singlex_failed = False
        try:
            subprocess.run(cmd_nooption, cwd=ROOT, check=True)
            _append_child(
                _PairChildResult(
                    mode=nooption_child_mode,
                    run_id=nooption_run_id,
                    status="ok",
                    returncode=0,
                    command=cmd_nooption,
                    outputs=nooption_outputs,
                )
            )
        except subprocess.CalledProcessError as exc:
            nooption_failed = True
            _append_child(
                _PairChildResult(
                    mode=nooption_child_mode,
                    run_id=nooption_run_id,
                    status="failed",
                    returncode=int(exc.returncode),
                    command=cmd_nooption,
                    outputs=nooption_outputs,
                    error=str(exc),
                )
            )

        print(f"[preset-run] command({singlex_child_mode}):", " ".join(cmd_singlex))
        if nooption_failed and not bool(args.paired_run_singlex_on_nooption_failure):
            _append_child(
                _PairChildResult(
                    mode=singlex_child_mode,
                    run_id=singlex_run_id,
                    status="skipped_due_to_nooption_failure",
                    returncode=0,
                    command=cmd_singlex,
                    outputs=singlex_outputs,
                )
            )
        else:
            try:
                subprocess.run(cmd_singlex, cwd=ROOT, check=True)
                _append_child(
                    _PairChildResult(
                        mode=singlex_child_mode,
                        run_id=singlex_run_id,
                        status="ok",
                        returncode=0,
                        command=cmd_singlex,
                        outputs=singlex_outputs,
                    )
                )
            except subprocess.CalledProcessError as exc:
                singlex_failed = True
                _append_child(
                    _PairChildResult(
                        mode=singlex_child_mode,
                        run_id=singlex_run_id,
                        status="failed",
                        returncode=int(exc.returncode),
                        command=cmd_singlex,
                        outputs=singlex_outputs,
                        error=str(exc),
                    )
                )

        pair_payload["status"] = "ok" if not (nooption_failed or singlex_failed) else "partial_failure"
        if direction_review_enabled:
            direction_payload = _build_direction_review_payload(pair_payload)
            _write_paired_summary(direction_review_path, direction_payload)
            pair_payload["direction_review_checks"] = direction_payload.get("checks", {})
            print("[preset-run] direction_review_json:", str(direction_review_path))
            checks = direction_payload.get("checks", {})
            if isinstance(checks, dict):
                print(
                    "[preset-run] direction_review_checks:",
                    json.dumps(checks, ensure_ascii=False),
                )
        pair_payload["timestamp_utc_finished"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_paired_summary(paired_summary_path, pair_payload)
        return 0 if pair_payload["status"] == "ok" else 2

    outputs = _build_output_paths(run_id)
    _ensure_parent(list(outputs.values()))

    cmd: List[str] = [
        args.runner_python,
        str(RUNNER.relative_to(ROOT)),
        "--run-id",
        run_id,
        "--out-scan-runs-csv",
        outputs["scan"],
        "--out-top-models-csv",
        outputs["top"],
        "--out-search-log-jsonl",
        outputs["log"],
        "--out-run-summary-json",
        outputs["summary"],
        "--out-feasibility-frontier-json",
        outputs["frontier"],
        "--out-feature-registry-json",
        outputs["registry"],
        "--out-restart-stability-csv",
        outputs["stability"],
        "--gate-profile",
        "adaptive_production",
        "--print-cli-summary",
        "--cli-summary-top-n",
        str(int(args.cli_summary_top_n)),
    ]
    resolved_scan_inputs = _resolve_scan_input_overrides()
    for flag, path_text, _source in resolved_scan_inputs:
        cmd.extend([str(flag), str(path_text)])

    if mode in {"openexplore", "openexplore_autorefine"}:
        scan_boot = int(args.scan_n_bootstrap) if int(args.scan_n_bootstrap) > 0 else 49
        scan_max = int(args.scan_max_features) if int(args.scan_max_features) > 0 else 120
        cmd.extend(
            [
                "--n-bootstrap",
                str(scan_boot),
                "--scan-max-features",
                str(scan_max),
                "--input-feature-registry-json",
                "data/metadata/phase_b_keyfactor_explorer_feature_registry_refresh_20260220.json",
                "--expression-registry-mode",
                "ms_benchmark_lite",
                "--expression-max-base-features",
                "12",
                "--expression-max-pairs",
                "60",
                "--expression-max-new-features",
                "120",
                "--categorical-encoding-mode",
                "onehot",
                "--categorical-max-levels-per-feature",
                "4",
                "--categorical-min-level-count",
                "10",
                "--categorical-max-new-features",
                "60",
            ]
        )
    elif mode == "singlex":
        scan_boot = int(args.scan_n_bootstrap) if int(args.scan_n_bootstrap) > 0 else 499
        reg_path = ROOT / f"data/metadata/phase_b_bikard_machine_scientist_feature_registry_singlex_{_slug(run_id)}.json"
        _write_singlex_registry(reg_path, run_id)
        cmd.extend(
            [
                "--n-bootstrap",
                str(scan_boot),
                "--input-feature-registry-json",
                str(reg_path.relative_to(ROOT)),
                "--expression-registry-mode",
                "none",
                "--categorical-encoding-mode",
                "none",
            ]
        )
    elif mode == "singlex_baseline":
        scan_boot = int(args.scan_n_bootstrap) if int(args.scan_n_bootstrap) > 0 else 199
        reg_path = ROOT / f"data/metadata/phase_b_bikard_machine_scientist_feature_registry_singlex_{_slug(run_id)}.json"
        yctx_path = ROOT / (
            "data/metadata/phase_b_bikard_machine_scientist_y_contexts_yall_only_"
            f"{_slug(run_id)}.json"
        )
        _write_singlex_registry(reg_path, run_id)
        _write_yall_only_contexts(yctx_path, run_id)
        cmd.extend(
            [
                "--n-bootstrap",
                str(scan_boot),
                "--input-feature-registry-json",
                str(reg_path.relative_to(ROOT)),
                "--expression-registry-mode",
                "none",
                "--categorical-encoding-mode",
                "none",
                "--no-base-controls",
                "--n-restarts",
                "5",
                "--restart-seed-step",
                "1000003",
                "--enforce-track-consensus",
                "--consensus-anchor-track",
                "primary_strict",
                "--consensus-min-anchor-tier",
                "support_candidate",
                "--auto-scale-y-validated-gates",
                "--y-feasibility-mode",
                "fail_below_floor",
                "--y-contexts-json",
                str(yctx_path.relative_to(ROOT)),
                "--y-contexts-merge-mode",
                "replace",
            ]
        )
    elif mode == "singlex_hypothesis_panel":
        scan_boot = int(args.scan_n_bootstrap) if int(args.scan_n_bootstrap) > 0 else 199
        years = _parse_hypothesis_window_years(args.hypothesis_window_years)
        confirm_years = _resolve_hypothesis_confirmatory_years(
            hypothesis_window_years=years,
            confirmatory_window_years_raw=str(args.hypothesis_confirmatory_window_years),
        )
        confirmatory_cols = ",".join(f"y_{int(y)}y" for y in confirm_years)
        reg_path = ROOT / f"data/metadata/phase_b_bikard_machine_scientist_feature_registry_singlex_{_slug(run_id)}.json"
        yctx_path = ROOT / (
            "data/metadata/phase_b_bikard_machine_scientist_y_contexts_hypothesis_panel_"
            f"{_slug(run_id)}.json"
        )
        _write_singlex_registry(reg_path, run_id)
        _write_hypothesis_panel_contexts(yctx_path, run_id, years)
        cmd.extend(
            [
                "--n-bootstrap",
                str(scan_boot),
                "--input-feature-registry-json",
                str(reg_path.relative_to(ROOT)),
                "--expression-registry-mode",
                "none",
                "--categorical-encoding-mode",
                "none",
                "--no-base-controls",
                "--n-restarts",
                "5",
                "--restart-seed-step",
                "1000003",
                "--enforce-track-consensus",
                "--consensus-anchor-track",
                "primary_strict",
                "--consensus-min-anchor-tier",
                "support_candidate",
                "--auto-scale-y-validated-gates",
                "--y-feasibility-mode",
                "fail_below_floor",
                "--derive-y-time-windows",
                "--y-time-window-years",
                str(args.hypothesis_window_years),
                "--time-series-precheck-mode",
                str(args.hypothesis_time_series_precheck_mode),
                "--time-series-auto-confirmatory-policy",
                str(args.hypothesis_auto_confirmatory_policy),
                "--time-series-min-positive-events",
                str(int(args.hypothesis_time_series_min_positive_events)),
                "--time-series-min-track-positive-events",
                str(int(args.hypothesis_time_series_min_track_positive_events)),
                "--time-series-min-positive-share",
                str(float(args.hypothesis_time_series_min_positive_share)),
                "--y-contexts-json",
                str(yctx_path.relative_to(ROOT)),
                "--y-contexts-merge-mode",
                "replace",
                "--confirmatory-y-cols",
                confirmatory_cols,
                "--nonconfirmatory-max-tier",
                str(args.hypothesis_nonconfirmatory_max_tier),
            ]
        )
    elif mode == "nooption_baseline":
        scan_boot = int(args.scan_n_bootstrap) if int(args.scan_n_bootstrap) > 0 else 49
        cmd.extend(
            [
                "--n-bootstrap",
                str(scan_boot),
                "--n-restarts",
                "5",
                "--restart-seed-step",
                "1000003",
                "--auto-scale-y-validated-gates",
                "--y-feasibility-mode",
                "fail_unusable",
                "--skip-discovery-infeasible-track-y",
                "--auto-disable-base-controls-low-capacity",
                "--base-controls-min-events-per-exog",
                "10",
                "--base-controls-min-policy-docs-per-exog",
                "5",
            ]
        )
    elif mode == "nooption_hypothesis_panel":
        scan_boot = int(args.scan_n_bootstrap) if int(args.scan_n_bootstrap) > 0 else 49
        years = _parse_hypothesis_window_years(args.hypothesis_window_years)
        confirm_years = _resolve_hypothesis_confirmatory_years(
            hypothesis_window_years=years,
            confirmatory_window_years_raw=str(args.hypothesis_confirmatory_window_years),
        )
        confirmatory_cols = ",".join(f"y_{int(y)}y" for y in confirm_years)
        yctx_path = ROOT / (
            "data/metadata/phase_b_bikard_machine_scientist_y_contexts_hypothesis_panel_"
            f"{_slug(run_id)}.json"
        )
        _write_hypothesis_panel_contexts(yctx_path, run_id, years)
        cmd.extend(
            [
                "--n-bootstrap",
                str(scan_boot),
                "--n-restarts",
                "5",
                "--restart-seed-step",
                "1000003",
                "--auto-scale-y-validated-gates",
                "--y-feasibility-mode",
                "fail_below_floor",
                "--derive-y-time-windows",
                "--y-time-window-years",
                str(args.hypothesis_window_years),
                "--time-series-precheck-mode",
                str(args.hypothesis_time_series_precheck_mode),
                "--time-series-auto-confirmatory-policy",
                str(args.hypothesis_auto_confirmatory_policy),
                "--time-series-min-positive-events",
                str(int(args.hypothesis_time_series_min_positive_events)),
                "--time-series-min-track-positive-events",
                str(int(args.hypothesis_time_series_min_track_positive_events)),
                "--time-series-min-positive-share",
                str(float(args.hypothesis_time_series_min_positive_share)),
                "--y-contexts-json",
                str(yctx_path.relative_to(ROOT)),
                "--y-contexts-merge-mode",
                "replace",
                "--confirmatory-y-cols",
                confirmatory_cols,
                "--nonconfirmatory-max-tier",
                str(args.hypothesis_nonconfirmatory_max_tier),
                "--skip-discovery-infeasible-track-y",
                "--auto-disable-base-controls-low-capacity",
                "--base-controls-min-events-per-exog",
                "10",
                "--base-controls-min-policy-docs-per-exog",
                "5",
            ]
        )
    elif mode == "nooption":
        if int(args.scan_n_bootstrap) > 0:
            cmd.extend(["--n-bootstrap", str(int(args.scan_n_bootstrap))])
    else:
        raise ValueError(f"unsupported mode: {mode}")

    if mode == "openexplore_autorefine":
        refine_boot = int(args.refine_n_bootstrap) if int(args.refine_n_bootstrap) > 0 else 199
        cmd.extend(
            [
                "--auto-refine-shortlist",
                "--refine-tier-mode",
                "validated_or_support",
                "--refine-max-features",
                "8",
                "--refine-dedupe-mode",
                "atom",
                "--refine-n-bootstrap",
                str(refine_boot),
                "--refine-run-id-suffix",
                "refine",
            ]
        )

    for extra in args.extra_arg:
        text = str(extra).strip()
        if text:
            cmd.append(text)

    for flag, path_text, source in resolved_scan_inputs:
        if source != "default_locked":
            print(f"[preset-run] input-resolve: {flag}={path_text} ({source})")

    if args.dry_run:
        print("[dry-run] command:")
        print(" ".join(cmd))
        print("[dry-run] outputs:")
        for k, v in outputs.items():
            print(f"  {k}: {v}")
        return 0

    print("[preset-run] mode:", mode)
    print("[preset-run] run_id:", run_id)
    print("[preset-run] command:", " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
