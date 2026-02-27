#!/usr/bin/env python3
"""Run key-factor explorer scan for Phase-B Bikard dyad choice model."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
from time import perf_counter
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
import sys


def _detect_repo_root(start: Path) -> Path:
    cur = start.resolve()
    for cand in [cur, *cur.parents]:
        if (cand / "AGENTS.md").is_file() and (cand / "modules").is_dir():
            return cand
    return Path(__file__).resolve().parents[2]


ROOT = _detect_repo_root(Path(__file__).resolve().parent)
MODULE03_ROOT = ROOT / "modules" / "03_regspec_machine"
for _path in (MODULE03_ROOT, ROOT):
    _text = str(_path)
    if _text not in sys.path:
        sys.path.insert(0, _text)

from regspec_machine import (  # noqa: E402
    build_feature_registry,
    get_git_commit,
    load_and_prepare_data,
    load_feature_registry,
    run_key_factor_scan,
    select_shortlist_features_from_top_models,
    sha256_json,
)
from regspec_machine.feature_registry import (  # noqa: E402
    IDENTIFIER_COLUMNS,
    classify_timing,
    is_outcome_like,
    within_event_variation_label,
    within_event_variation_metrics,
)
from regspec_machine.reporting import (  # noqa: E402
    utc_timestamp,
    write_csv,
    write_json,
    write_jsonl,
)
from regspec_machine.fdr import (  # noqa: E402
    attach_bh_qvalues,
)
from regspec_machine.search_engine import (  # noqa: E402
    ScanConfig,
)
from regspec_machine.splitter import (  # noqa: E402
    apply_policy_split_file,
    assign_policy_document_holdout,
)

DEFAULT_Y_CONTEXTS: Tuple[Tuple[str, str], ...] = (
    ("all_contexts", "y_all"),
    ("evidence_use_only", "y_evidence"),
)


def parse_args() -> argparse.Namespace:
    ymd = datetime.now(timezone.utc).strftime("%Y%m%d")
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input-dyad-base-csv",
        default="outputs/tables/phase_b_bikard_policy_doc_twin_dyad_base_20260219.csv",
    )
    p.add_argument(
        "--input-extension-feature-csv",
        default="data/metadata/metadata_extension_feature_table_overton20260130.csv",
    )
    p.add_argument(
        "--input-phase-a-covariates-csv",
        default="data/processed/phase_a_model_input_strict_pairs_api_backfilled_overton20260130_labeled.csv",
    )
    p.add_argument("--input-feature-registry-json", default="")
    p.add_argument("--strict-lock-mode", action="store_true", default=True)
    p.add_argument("--no-strict-lock-mode", dest="strict_lock_mode", action="store_false")
    p.add_argument(
        "--out-scan-runs-csv",
        default=f"outputs/tables/phase_b_bikard_machine_scientist_scan_runs_{ymd}.csv",
    )
    p.add_argument(
        "--out-top-models-csv",
        default=f"outputs/tables/phase_b_bikard_machine_scientist_top_models_{ymd}.csv",
    )
    p.add_argument(
        "--out-top-models-inference-csv",
        default="",
        help="candidate-level inference summary across restarts; default is out-top-models-csv with _inference suffix",
    )
    p.add_argument(
        "--out-search-log-jsonl",
        default=f"data/metadata/phase_b_bikard_machine_scientist_search_log_{ymd}.jsonl",
    )
    p.add_argument(
        "--out-run-summary-json",
        default=f"data/metadata/phase_b_bikard_machine_scientist_run_summary_{ymd}.json",
    )
    p.add_argument(
        "--out-feasibility-frontier-json",
        default=f"data/metadata/phase_b_bikard_machine_scientist_feasibility_frontier_{ymd}.json",
    )
    p.add_argument(
        "--out-feature-registry-json",
        default=f"data/metadata/phase_b_bikard_machine_scientist_feature_registry_{ymd}.json",
    )
    p.add_argument(
        "--out-restart-stability-csv",
        default=f"data/metadata/phase_b_bikard_machine_scientist_restart_stability_{ymd}.csv",
    )
    p.add_argument("--run-id", default=f"phase_b_bikard_keyfactor_scan_{ymd}")
    p.add_argument(
        "--input-policy-split-csv",
        default="outputs/tables/phase_b_keyfactor_explorer_policy_split_20260219.csv",
    )
    p.add_argument("--split-seed", type=int, default=20260219)
    p.add_argument("--split-ratio", type=float, default=0.80)
    p.add_argument("--split-method", choices=("hash", "random"), default="hash")
    p.add_argument("--bootstrap-seed", type=int, default=20260219)
    p.add_argument("--n-bootstrap", type=int, default=499)
    p.add_argument(
        "--bootstrap-cluster-unit",
        choices=("policy_document_id", "pair_id"),
        default="policy_document_id",
    )
    p.add_argument(
        "--optimizer-mode",
        choices=("none", "adam_lite"),
        default="none",
        help="core fit optimizer for conditional logit and bootstrap refits",
    )
    p.add_argument("--optimizer-adam-max-iter", type=int, default=300)
    p.add_argument("--optimizer-adam-learning-rate", type=float, default=0.05)
    p.add_argument("--optimizer-adam-beta1", type=float, default=0.9)
    p.add_argument("--optimizer-adam-beta2", type=float, default=0.999)
    p.add_argument("--optimizer-adam-eps", type=float, default=1e-8)
    p.add_argument("--optimizer-adam-l2", type=float, default=1e-4)
    p.add_argument("--optimizer-adam-min-iter", type=int, default=25)
    p.add_argument("--optimizer-adam-tol", type=float, default=1e-6)
    # Legacy single-gate args: by default override only discovery/estimable gate.
    # Use --legacy-single-gate-sync-validation to also override validation gate.
    p.add_argument("--min-informative-events", type=int, default=None)
    p.add_argument("--min-policy-docs-informative", type=int, default=None)
    p.add_argument(
        "--legacy-single-gate-sync-validation",
        action="store_true",
        default=False,
        help="if set, legacy min-* args also override validation gate thresholds",
    )
    # Two-stage gates aligned with keyfactor registry/config lock.
    p.add_argument("--min-informative-events-estimable", type=int, default=20)
    p.add_argument("--min-policy-docs-informative-estimable", type=int, default=10)
    p.add_argument("--min-informative-events-validated", type=int, default=100)
    p.add_argument("--min-policy-docs-informative-validated", type=int, default=30)
    p.add_argument("--max-top1-policy-doc-share", type=float, default=0.20)
    p.add_argument("--bootstrap-success-min-ratio", type=float, default=0.80)
    p.add_argument("--q-threshold", type=float, default=0.10)
    p.add_argument("--p-threshold", type=float, default=0.05)
    p.add_argument("--complexity-penalty", type=float, default=0.01)
    p.add_argument("--complexity-penalty-auto", action="store_true", default=False)
    p.add_argument("--complexity-penalty-auto-min", type=float, default=0.005)
    p.add_argument("--complexity-penalty-auto-max", type=float, default=0.05)
    p.add_argument("--include-base-controls", action="store_true", default=True)
    p.add_argument("--no-base-controls", dest="include_base_controls", action="store_false")
    p.add_argument(
        "--control-spec-mode",
        choices=("both", "key_only", "key_plus_base_controls"),
        default="both",
        help="which control spec families to scan (default: both)",
    )
    p.add_argument(
        "--base-controls",
        default="",
        help="comma-separated override of base controls (default uses built-in list)",
    )
    p.add_argument(
        "--base-controls-strict",
        action="store_true",
        default=False,
        help="fail fast if any requested base control column is missing",
    )
    p.add_argument("--n-restarts", type=int, default=1)
    p.add_argument("--restart-seed-step", type=int, default=1_000_003)
    # Legacy alias: when set, it fans out to both registry/scan caps.
    p.add_argument("--max-features", type=int, default=None)
    p.add_argument("--registry-max-features", type=int, default=0)
    p.add_argument("--scan-max-features", type=int, default=0)
    p.add_argument(
        "--expression-registry-mode",
        choices=("none", "signed_log1p", "signed_log1p_square", "ms_benchmark_lite"),
        default="none",
    )
    p.add_argument(
        "--expression-max-new-features",
        type=int,
        default=200,
        help="global cap on derived expression features (0 => no cap; use with care)",
    )
    p.add_argument(
        "--expression-max-base-features",
        type=int,
        default=50,
        help="cap on base features eligible for expression expansion (0 => no cap; use with care)",
    )
    p.add_argument(
        "--expression-max-pairs",
        type=int,
        default=500,
        help="cap on base feature pairs considered for binary ops (0 => no cap; use with care)",
    )
    p.add_argument("--expression-min-nonmissing-count", type=int, default=20)
    p.add_argument(
        "--categorical-encoding-mode",
        choices=("none", "onehot"),
        default="none",
    )
    p.add_argument("--categorical-max-levels-per-feature", type=int, default=5)
    p.add_argument("--categorical-min-level-count", type=int, default=10)
    p.add_argument(
        "--categorical-max-new-features",
        type=int,
        default=200,
        help="global cap on onehot features generated (0 => no cap; use with care)",
    )
    p.add_argument("--registry-min-variation-share", type=float, default=0.10)
    p.add_argument("--registry-min-nonmissing-share", type=float, default=0.80)
    p.add_argument("--registry-build-scope", choices=("discovery", "all"), default="discovery")
    p.add_argument("--y-contexts-json", default="")
    p.add_argument(
        "--y-contexts-merge-mode",
        choices=("append", "replace"),
        default="append",
    )
    p.add_argument(
        "--derive-y-time-windows",
        action="store_true",
        default=False,
        help="derive y_<N>y outcomes from y_all and recency_years_alt before resolving y-contexts",
    )
    p.add_argument(
        "--y-time-window-years",
        default="3,5,10",
        help="comma-separated year windows used by --derive-y-time-windows (example: 3,5,10)",
    )
    p.add_argument(
        "--y-feasibility-mode",
        choices=("warn", "fail_unusable", "fail_below_floor"),
        default="fail_unusable",
    )
    p.add_argument(
        "--time-series-precheck-mode",
        choices=("off", "warn", "fail_redundant_confirmatory", "fail_low_support", "fail_any"),
        default="warn",
        help="precheck mode for time-window outcomes before scan",
    )
    p.add_argument(
        "--time-series-min-positive-events",
        type=int,
        default=20,
        help="minimum overall positive events per y_col for precheck low-support alert",
    )
    p.add_argument(
        "--time-series-min-track-positive-events",
        type=int,
        default=0,
        help="minimum per-track positive events per y_col for precheck (0 => use estimable event gate)",
    )
    p.add_argument(
        "--time-series-min-positive-share",
        type=float,
        default=0.05,
        help="minimum overall positive share per y_col for precheck low-support alert",
    )
    p.add_argument(
        "--time-series-auto-confirmatory-policy",
        choices=("off", "drop_redundant", "drop_redundant_and_low_support"),
        default="off",
        help="auto-adjust confirmatory y cols from precheck recommendations",
    )
    p.add_argument(
        "--confirmatory-y-cols",
        default="y_all",
        help="comma-separated y_col list eligible for confirmatory validation in inference aggregation",
    )
    p.add_argument(
        "--nonconfirmatory-max-tier",
        choices=("support_candidate", "exploratory"),
        default="support_candidate",
        help="max tier allowed for y contexts outside confirmatory-y-cols",
    )
    p.add_argument("--skip-discovery-infeasible-track-y", action="store_true", default=False)
    p.add_argument("--auto-disable-base-controls-low-capacity", action="store_true", default=False)
    p.add_argument("--base-controls-min-events-per-exog", type=int, default=10)
    p.add_argument("--base-controls-min-policy-docs-per-exog", type=int, default=5)
    p.add_argument(
        "--gate-profile",
        choices=("strict_production", "feasibility_diagnostic", "adaptive_production"),
        default="adaptive_production",
    )
    p.add_argument("--auto-scale-validated-gates", action="store_true", default=False)
    p.add_argument("--validated-gate-min-events-floor", type=int, default=30)
    p.add_argument("--validated-gate-min-docs-floor", type=int, default=10)
    p.add_argument("--validated-gate-adaptive-doc-ratio", type=float, default=0.75)
    p.add_argument("--validated-gate-adaptive-event-ratio", type=float, default=0.75)
    p.add_argument(
        "--validated-gate-adaptive-rounding",
        choices=("floor", "round", "ceil"),
        default="round",
    )
    p.add_argument("--auto-scale-y-validated-gates", action="store_true", default=False)
    p.add_argument("--y-validated-gate-min-events-floor", type=int, default=3)
    p.add_argument("--y-validated-gate-min-docs-floor", type=int, default=3)
    p.add_argument("--y-validated-gate-adaptive-event-ratio", type=float, default=0.75)
    p.add_argument("--y-validated-gate-adaptive-doc-ratio", type=float, default=0.75)
    p.add_argument(
        "--y-validated-gate-adaptive-rounding",
        choices=("floor", "round", "ceil"),
        default="round",
    )
    p.add_argument(
        "--scan-family-dedupe-mode",
        choices=("none", "feature", "atom"),
        default="atom",
    )
    p.add_argument("--enforce-track-consensus", action="store_true", default=False)
    p.add_argument("--consensus-anchor-track", default="primary_strict")
    p.add_argument(
        "--consensus-min-anchor-tier",
        choices=("support_candidate", "validated_candidate"),
        default="support_candidate",
    )
    p.add_argument("--print-cli-summary", action="store_true", default=False)
    p.add_argument("--cli-summary-top-n", type=int, default=5)
    p.add_argument("--auto-bootstrap-escalation", action="store_true", default=False)
    p.add_argument("--escalation-n-bootstrap", type=int, default=499)
    p.add_argument("--escalation-max-candidates", type=int, default=12)
    p.add_argument("--escalation-p-margin", type=float, default=0.02)
    p.add_argument("--escalation-q-margin", type=float, default=0.03)
    p.add_argument("--escalation-dedupe-mode", choices=("feature", "atom"), default="atom")
    p.add_argument("--escalation-run-id-suffix", default="escalate")
    p.add_argument("--auto-refine-shortlist", action="store_true", default=False)
    p.add_argument(
        "--refine-tier-mode",
        choices=("validated_only", "validated_or_support"),
        default="validated_or_support",
    )
    p.add_argument("--refine-max-features", type=int, default=8)
    p.add_argument("--refine-dedupe-mode", choices=("feature", "atom"), default="atom")
    p.add_argument("--refine-n-bootstrap", type=int, default=499)
    p.add_argument("--refine-run-id-suffix", default="refine")
    p.add_argument(
        "--min-free-space-mb",
        type=int,
        default=1024,
        help="minimum free disk space required at startup and before writing each stage output (set 0 to disable)",
    )
    return p.parse_args()


def _status_counts(rows: List[Dict[str, object]], key: str) -> Dict[str, int]:
    c = Counter(str(r.get(key, "")) for r in rows)
    return dict(sorted(c.items(), key=lambda kv: kv[0]))


def _round_by_mode(value: float, *, mode: str) -> int:
    if mode == "floor":
        return int(math.floor(value))
    if mode == "ceil":
        return int(math.ceil(value))
    return int(round(value))


def _ensure_output_dirs(paths: List[str]) -> None:
    for raw in paths:
        if not str(raw).strip():
            continue
        Path(raw).parent.mkdir(parents=True, exist_ok=True)


def _with_path_suffix(raw_path: str, suffix: str) -> str:
    p = Path(raw_path)
    clean = str(suffix or "").strip()
    if not clean:
        return str(p)
    return str(p.with_name(f"{p.stem}_{clean}{p.suffix}"))


def _existing_path_for_disk_usage(raw_path: str) -> Path:
    p = Path(raw_path)
    for candidate in (p, p.parent, ROOT):
        if candidate.exists():
            return candidate
    return ROOT


def _check_disk_space_or_raise(
    *,
    probe_paths: Sequence[str],
    min_free_space_mb: int,
    stage: str,
) -> Dict[str, object]:
    unique_paths: List[Path] = []
    seen: set[str] = set()
    for raw in probe_paths:
        key = str(raw).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        unique_paths.append(_existing_path_for_disk_usage(key))
    if not unique_paths:
        unique_paths.append(ROOT)

    checks: List[Dict[str, object]] = []
    for path in unique_paths:
        usage = shutil.disk_usage(path)
        total_mb = int(usage.total // (1024 * 1024))
        free_mb = int(usage.free // (1024 * 1024))
        used_pct = float(((usage.total - usage.free) / usage.total) * 100.0) if usage.total else 0.0
        checks.append(
            {
                "path": str(path),
                "total_mb": total_mb,
                "free_mb": free_mb,
                "used_pct": round(used_pct, 2),
            }
        )

    min_free_mb_observed = min(int(item["free_mb"]) for item in checks) if checks else 0
    snapshot = {
        "stage": stage,
        "min_free_space_mb_required": int(min_free_space_mb),
        "min_free_space_mb_observed": int(min_free_mb_observed),
        "checks": checks,
    }
    if int(min_free_space_mb) > 0 and int(min_free_mb_observed) < int(min_free_space_mb):
        raise RuntimeError(
            "insufficient free disk space "
            f"(stage={stage}, required_mb={int(min_free_space_mb)}, observed_min_mb={int(min_free_mb_observed)})"
        )
    return snapshot


def _resolve_feature_caps(args: argparse.Namespace) -> tuple[int, int, Dict[str, object]]:
    registry_cap = int(args.registry_max_features) if args.registry_max_features else 0
    scan_cap = int(args.scan_max_features) if args.scan_max_features else 0
    if args.max_features is not None and int(args.max_features) > 0:
        legacy = int(args.max_features)
        if registry_cap == 0:
            registry_cap = legacy
        if scan_cap == 0:
            scan_cap = legacy
    meta = {
        "legacy_max_features": args.max_features,
        "registry_max_features_effective": registry_cap,
        "scan_max_features_effective": scan_cap,
    }
    return registry_cap, scan_cap, meta


def _resolve_stage_gate_thresholds(
    args: argparse.Namespace,
) -> tuple[int, int, int, int, Dict[str, object]]:
    min_events_estimable = int(args.min_informative_events_estimable)
    min_docs_estimable = int(args.min_policy_docs_informative_estimable)
    min_events_validated_requested = int(args.min_informative_events_validated)
    min_docs_validated_requested = int(args.min_policy_docs_informative_validated)
    sync_validation = bool(args.legacy_single_gate_sync_validation)

    legacy_event_override = (
        int(args.min_informative_events) if args.min_informative_events is not None else None
    )
    legacy_docs_override = (
        int(args.min_policy_docs_informative) if args.min_policy_docs_informative is not None else None
    )
    if legacy_event_override is not None:
        min_events_estimable = int(legacy_event_override)
        if sync_validation:
            min_events_validated_requested = int(legacy_event_override)
    if legacy_docs_override is not None:
        min_docs_estimable = int(legacy_docs_override)
        if sync_validation:
            min_docs_validated_requested = int(legacy_docs_override)

    has_legacy_override = bool(
        legacy_event_override is not None or legacy_docs_override is not None
    )
    if has_legacy_override and sync_validation:
        validated_gate_source = "legacy_single_gate_synced"
    elif has_legacy_override:
        validated_gate_source = "split_gate_with_legacy_estimable_override"
    else:
        validated_gate_source = "split_gate_explicit"

    meta = {
        "legacy_single_gate_sync_validation": int(sync_validation),
        "legacy_single_gate_overrides": {
            "min_informative_events": legacy_event_override,
            "min_policy_docs_informative": legacy_docs_override,
        },
        "legacy_override_applied_to_estimable": int(has_legacy_override),
        "legacy_override_applied_to_validated": int(has_legacy_override and sync_validation),
        "validated_gate_source": validated_gate_source,
    }
    return (
        int(min_events_estimable),
        int(min_docs_estimable),
        int(min_events_validated_requested),
        int(min_docs_validated_requested),
        meta,
    )


def _resolve_base_controls(
    data: pd.DataFrame,
    include_base_controls: bool,
    *,
    base_controls_override: str,
    strict: bool,
) -> tuple[List[str], Dict[str, object]]:
    default_requested = ["pub_year_alt", "recency_years_alt", "pa__log1p_author_count"]
    override_raw = str(base_controls_override or "").strip()
    if override_raw:
        requested = [c.strip() for c in override_raw.split(",")]
        requested = [c for c in requested if c]
        requested_source = "override"
    else:
        requested = list(default_requested)
        requested_source = "default"

    requested = list(dict.fromkeys(requested))  # stable-dedupe
    if not include_base_controls:
        return [], {
            "base_controls_requested": requested,
            "base_controls_requested_source": requested_source,
            "base_controls_override_raw": override_raw,
            "base_controls_strict": bool(strict),
            "base_controls_used": [],
            "missing_base_controls": requested,
        }
    used = [c for c in requested if c in data.columns]
    missing = [c for c in requested if c not in used]
    if strict and missing:
        raise ValueError(f"missing base control columns (strict): {missing}")
    return used, {
        "base_controls_requested": requested,
        "base_controls_requested_source": requested_source,
        "base_controls_override_raw": override_raw,
        "base_controls_strict": bool(strict),
        "base_controls_used": used,
        "missing_base_controls": missing,
    }


def _sanitize_context_scope(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unnamed_context"


def _group_binary_outcome(
    *,
    data: pd.DataFrame,
    source_cols: Sequence[str],
    group_mode: str,
    threshold: int | None,
) -> pd.Series:
    src = data[list(source_cols)].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    positive = src.gt(0.0)
    if group_mode == "all_positive":
        out = positive.all(axis=1)
    elif group_mode == "at_least_k":
        k = int(threshold) if threshold is not None else 1
        k = max(1, min(k, len(source_cols)))
        out = positive.sum(axis=1) >= k
    else:
        out = positive.any(axis=1)
    return out.astype(int)


def _resolve_y_contexts(
    *,
    data: pd.DataFrame,
    y_contexts_json: str,
    merge_mode: str,
) -> tuple[pd.DataFrame, List[Tuple[str, str]], Dict[str, object]]:
    out_data = data.copy()
    context_rows: List[Tuple[str, str]] = list(DEFAULT_Y_CONTEXTS)
    generated_groups: List[Dict[str, object]] = []

    if not str(y_contexts_json).strip():
        return out_data, context_rows, {
            "y_contexts_source": "default",
            "y_contexts_merge_mode": merge_mode,
            "y_contexts_effective_count": len(context_rows),
            "generated_groups": [],
        }

    context_path = Path(y_contexts_json)
    if not context_path.exists():
        raise ValueError(f"y-contexts JSON file not found: {context_path}")
    payload = json.loads(context_path.read_text(encoding="utf-8"))
    raw_contexts: object = payload
    if isinstance(payload, dict):
        raw_contexts = payload.get("contexts", [])
    if not isinstance(raw_contexts, list):
        raise ValueError("y-contexts JSON must be a list or dict.contexts(list)")
    if merge_mode == "replace":
        context_rows = []

    for idx, row in enumerate(raw_contexts):
        if not isinstance(row, dict):
            raise ValueError(f"y-contexts[{idx}] must be an object")
        context_scope = _sanitize_context_scope(
            row.get("context_scope") or row.get("context_name") or row.get("name") or ""
        )
        y_col = str(row.get("y_col", "")).strip()
        source_cols_raw = row.get("source_cols", [])
        source_cols: List[str] = []
        if isinstance(source_cols_raw, list):
            source_cols = [str(c).strip() for c in source_cols_raw if str(c).strip()]

        if source_cols:
            missing = [c for c in source_cols if c not in out_data.columns]
            if missing:
                raise ValueError(
                    f"y-contexts[{idx}] missing source columns: {missing} (context_scope={context_scope})"
                )
            group_mode = str(row.get("group_mode", "any_positive")).strip().lower()
            if group_mode not in {"any_positive", "all_positive", "at_least_k"}:
                raise ValueError(
                    f"y-contexts[{idx}] unsupported group_mode={group_mode}; "
                    "use any_positive|all_positive|at_least_k"
                )
            threshold = row.get("threshold")
            if threshold is not None:
                try:
                    threshold = int(threshold)
                except Exception as exc:
                    raise ValueError(f"y-contexts[{idx}] threshold must be int") from exc
            y_col_effective = y_col or f"y_group__{context_scope}"
            out_data[y_col_effective] = _group_binary_outcome(
                data=out_data,
                source_cols=source_cols,
                group_mode=group_mode,
                threshold=threshold if isinstance(threshold, int) else None,
            )
            context_rows.append((context_scope, y_col_effective))
            generated_groups.append(
                {
                    "context_scope": context_scope,
                    "y_col": y_col_effective,
                    "source_cols": source_cols,
                    "group_mode": group_mode,
                    "threshold": threshold,
                    "positive_share": float(pd.to_numeric(out_data[y_col_effective], errors="coerce").mean()),
                }
            )
            continue

        if not y_col:
            raise ValueError(
                f"y-contexts[{idx}] must provide either y_col or source_cols (context_scope={context_scope})"
            )
        if y_col not in out_data.columns:
            raise ValueError(f"y-contexts[{idx}] y_col not found in data: {y_col}")
        context_rows.append((context_scope, y_col))

    deduped_contexts: List[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    duplicate_count = 0
    for context_scope, y_col in context_rows:
        key = (str(context_scope).strip(), str(y_col).strip())
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        deduped_contexts.append(key)
    if not deduped_contexts:
        raise ValueError("no y contexts available after parsing y-contexts config")

    return out_data, deduped_contexts, {
        "y_contexts_source": str(y_contexts_json),
        "y_contexts_merge_mode": merge_mode,
        "y_contexts_effective_count": len(deduped_contexts),
        "y_contexts_duplicates_dropped": int(duplicate_count),
        "generated_groups": generated_groups,
    }


def _as_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _parse_csv_tokens(raw: object) -> List[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    out: List[str] = []
    for token in text.split(","):
        t = str(token).strip()
        if t:
            out.append(t)
    deduped: List[str] = []
    seen: set[str] = set()
    for token in out:
        if token in seen:
            continue
        seen.add(token)
        deduped.append(token)
    return deduped


def _parse_positive_int_csv(raw: object, *, field_name: str) -> List[int]:
    tokens = _parse_csv_tokens(raw)
    if not tokens:
        return []
    out: List[int] = []
    seen: set[int] = set()
    for token in tokens:
        try:
            value = int(token)
        except Exception as exc:
            raise ValueError(f"{field_name} must be comma-separated positive integers: {raw}") from exc
        if value <= 0:
            raise ValueError(f"{field_name} must be comma-separated positive integers: {raw}")
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _parse_y_window_from_name(y_col: str) -> int | None:
    m = re.match(r"^y_(\d+)y$", str(y_col or "").strip())
    if not m:
        return None
    try:
        value = int(m.group(1))
    except Exception:
        return None
    return value if value > 0 else None


def _detect_time_related_columns(data: pd.DataFrame) -> Dict[str, object]:
    time_name_tokens = ("year", "date", "time", "recency", "published_on", "pub_")
    by_name = [
        str(col)
        for col in data.columns
        if any(tok in str(col).lower() for tok in time_name_tokens)
    ]
    numeric_year_like: List[str] = []
    for col in data.columns:
        s = pd.to_numeric(data[col], errors="coerce")
        nonmissing = int(s.notna().sum())
        if nonmissing == 0:
            continue
        in_range = ((s >= 1800.0) & (s <= 2100.0)).sum()
        share = float(in_range / nonmissing)
        if share >= 0.80:
            numeric_year_like.append(str(col))
    return {
        "by_name": sorted(by_name),
        "numeric_year_like": sorted(set(numeric_year_like)),
    }


def _build_time_series_precheck(
    *,
    data: pd.DataFrame,
    y_contexts: Sequence[Tuple[str, str]],
    confirmatory_y_cols: Sequence[str],
    min_positive_events: int,
    min_track_positive_events: int,
    min_positive_share: float,
) -> Dict[str, object]:
    y_cols_ordered: List[str] = []
    seen_y: set[str] = set()
    for _, y_col in y_contexts:
        y = str(y_col).strip()
        if not y or y in seen_y or y not in data.columns:
            continue
        seen_y.add(y)
        y_cols_ordered.append(y)

    y_support_rows: List[Dict[str, object]] = []
    signature_map: Dict[str, List[str]] = {}
    signature_pos: Dict[str, int] = {}
    low_support_y_cols: List[str] = []
    low_support_track_rows: List[Dict[str, object]] = []
    n_rows = int(len(data))
    has_track = "track" in data.columns

    for y_col in y_cols_ordered:
        series = pd.to_numeric(data[y_col], errors="coerce")
        nonmissing = int(series.notna().sum())
        y_bin = (series.fillna(0.0) > 0).astype(np.uint8)
        n_pos = int(y_bin.sum())
        pos_share = float(n_pos / n_rows) if n_rows > 0 else 0.0
        sign = hashlib.sha256(y_bin.to_numpy(dtype=np.uint8).tobytes()).hexdigest()
        signature_map.setdefault(sign, []).append(y_col)
        signature_pos[sign] = n_pos

        low_overall = bool(n_pos < int(min_positive_events) or pos_share < float(min_positive_share))
        if low_overall:
            low_support_y_cols.append(y_col)

        track_support_min = n_pos
        track_support_by_name: Dict[str, int] = {}
        if has_track:
            for track, g in data.groupby("track"):
                gs = pd.to_numeric(g[y_col], errors="coerce")
                gpos = int((gs.fillna(0.0) > 0).sum())
                track_support_by_name[str(track)] = gpos
                track_support_min = min(track_support_min, gpos)
                if gpos < int(min_track_positive_events):
                    low_support_track_rows.append(
                        {
                            "y_col": y_col,
                            "track": str(track),
                            "n_positive_track": int(gpos),
                            "threshold_track": int(min_track_positive_events),
                        }
                    )

        y_support_rows.append(
            {
                "y_col": y_col,
                "n_rows": n_rows,
                "n_nonmissing": nonmissing,
                "n_positive": n_pos,
                "positive_share": pos_share,
                "min_track_positive": int(track_support_min),
                "track_positive_counts": track_support_by_name,
            }
        )

    redundant_groups: List[Dict[str, object]] = []
    redundant_pairs: List[Dict[str, object]] = []
    for sign, y_cols in signature_map.items():
        if len(y_cols) <= 1:
            continue
        sorted_y = sorted(y_cols)
        year_candidates: List[Tuple[int, str]] = []
        for y in sorted_y:
            yv = _parse_y_window_from_name(y)
            if yv is not None:
                year_candidates.append((yv, y))
        if year_candidates:
            recommended_keep = sorted(year_candidates, key=lambda t: (int(t[0]), str(t[1])))[0][1]
        else:
            recommended_keep = sorted_y[0]
        redundant_groups.append(
            {
                "signature": sign,
                "y_cols": sorted_y,
                "n_positive": int(signature_pos.get(sign, 0)),
                "recommended_keep": recommended_keep,
            }
        )
        for i in range(len(sorted_y)):
            for j in range(i + 1, len(sorted_y)):
                redundant_pairs.append(
                    {
                        "y_col_left": sorted_y[i],
                        "y_col_right": sorted_y[j],
                        "relation": "exact_match",
                        "signature": sign,
                    }
                )

    confirmatory_set = set(str(x).strip() for x in confirmatory_y_cols if str(x).strip())
    if confirmatory_set:
        active_confirmatory = [y for y in y_cols_ordered if y in confirmatory_set]
    else:
        active_confirmatory = list(y_cols_ordered)

    confirmatory_redundant_groups: List[Dict[str, object]] = []
    confirmatory_drop_set: set[str] = set()
    for group in redundant_groups:
        grp_y = [y for y in group["y_cols"] if y in set(active_confirmatory)]
        if len(grp_y) <= 1:
            continue
        keep = str(group["recommended_keep"])
        if keep not in grp_y:
            keep = sorted(grp_y)[0]
        drop = sorted(y for y in grp_y if y != keep)
        confirmatory_drop_set.update(drop)
        confirmatory_redundant_groups.append(
            {
                "y_cols": sorted(grp_y),
                "recommended_keep": keep,
                "recommended_drop": drop,
            }
        )

    recommended_confirmatory = [y for y in active_confirmatory if y not in confirmatory_drop_set]

    low_support_track_pairs = sorted(
        set((str(r["y_col"]), str(r["track"])) for r in low_support_track_rows)
    )
    low_support_track_y_cols = sorted(set(y for y, _ in low_support_track_pairs))

    return {
        "mode": "time_series_precheck_v1",
        "enabled": True,
        "thresholds": {
            "min_positive_events": int(min_positive_events),
            "min_track_positive_events": int(min_track_positive_events),
            "min_positive_share": float(min_positive_share),
        },
        "time_columns_detected": _detect_time_related_columns(data),
        "y_cols_considered": y_cols_ordered,
        "y_support_rows": y_support_rows,
        "low_support_y_cols": sorted(set(low_support_y_cols)),
        "low_support_track_pairs": [
            {"y_col": y, "track": t} for y, t in low_support_track_pairs
        ],
        "low_support_track_y_cols": low_support_track_y_cols,
        "redundant_groups": redundant_groups,
        "redundant_pairs": redundant_pairs,
        "confirmatory_y_cols_requested": sorted(confirmatory_set),
        "confirmatory_y_cols_effective": active_confirmatory,
        "confirmatory_redundant_groups": confirmatory_redundant_groups,
        "recommended_confirmatory_y_cols": recommended_confirmatory,
        "summary": {
            "n_y_cols_considered": int(len(y_cols_ordered)),
            "n_low_support_y_cols": int(len(sorted(set(low_support_y_cols)))),
            "n_low_support_track_pairs": int(len(low_support_track_pairs)),
            "n_redundant_groups": int(len(redundant_groups)),
            "n_confirmatory_redundant_groups": int(len(confirmatory_redundant_groups)),
        },
    }


def _apply_time_series_precheck_mode(
    *,
    mode: str,
    precheck_meta: Dict[str, object],
) -> Dict[str, object]:
    mode_norm = str(mode or "").strip()
    if mode_norm not in {"off", "warn", "fail_redundant_confirmatory", "fail_low_support", "fail_any"}:
        raise ValueError(
            "time-series-precheck-mode must be one of: off, warn, fail_redundant_confirmatory, fail_low_support, fail_any"
        )
    redundant_confirmatory = precheck_meta.get("confirmatory_redundant_groups", []) or []
    low_support_y = precheck_meta.get("low_support_y_cols", []) or []
    low_support_track_pairs = precheck_meta.get("low_support_track_pairs", []) or []
    if mode_norm == "off":
        return {
            "mode": mode_norm,
            "triggered_redundant_confirmatory": False,
            "triggered_low_support": False,
            "notes": [],
        }

    triggered_redundant = bool(redundant_confirmatory)
    triggered_low_support = bool(low_support_y or low_support_track_pairs)
    notes: List[str] = []
    if triggered_redundant:
        notes.append(
            "redundant confirmatory y groups: "
            + "; ".join(
                ",".join(str(y) for y in grp.get("y_cols", []))
                for grp in redundant_confirmatory[:5]
            )
        )
    if triggered_low_support:
        low_y_text = ",".join(str(x) for x in low_support_y[:10]) if low_support_y else ""
        low_track_text = "; ".join(
            f"{r.get('y_col')}@{r.get('track')}" for r in low_support_track_pairs[:10]
        )
        note = "low-support y/track contexts"
        if low_y_text:
            note += f" | y={low_y_text}"
        if low_track_text:
            note += f" | track={low_track_text}"
        notes.append(note)

    should_fail = False
    if mode_norm == "fail_redundant_confirmatory" and triggered_redundant:
        should_fail = True
    elif mode_norm == "fail_low_support" and triggered_low_support:
        should_fail = True
    elif mode_norm == "fail_any" and (triggered_redundant or triggered_low_support):
        should_fail = True

    if should_fail:
        reason = "; ".join(notes) if notes else "time-series precheck failed"
        raise ValueError("time-series precheck failed: " + reason)

    return {
        "mode": mode_norm,
        "triggered_redundant_confirmatory": bool(triggered_redundant),
        "triggered_low_support": bool(triggered_low_support),
        "notes": notes,
    }


def _resolve_effective_confirmatory_y_cols(
    *,
    requested_confirmatory_y_cols: Sequence[str],
    y_contexts: Sequence[Tuple[str, str]],
    precheck_meta: Dict[str, object],
    auto_policy: str,
) -> tuple[List[str], Dict[str, object]]:
    auto_policy_norm = str(auto_policy or "").strip()
    if auto_policy_norm not in {"off", "drop_redundant", "drop_redundant_and_low_support"}:
        raise ValueError(
            "time-series-auto-confirmatory-policy must be one of: off, drop_redundant, drop_redundant_and_low_support"
        )

    y_available: List[str] = []
    seen_y: set[str] = set()
    for _, y_col in y_contexts:
        y = str(y_col).strip()
        if not y or y in seen_y:
            continue
        seen_y.add(y)
        y_available.append(y)
    available_set = set(y_available)

    requested = [str(y).strip() for y in requested_confirmatory_y_cols if str(y).strip()]
    requested_in_scope = [y for y in requested if y in available_set]
    requested_missing = [y for y in requested if y not in available_set]
    active = requested_in_scope if requested_in_scope else list(y_available)

    effective = list(active)
    dropped_redundant: List[str] = []
    dropped_low_support: List[str] = []

    if auto_policy_norm in {"drop_redundant", "drop_redundant_and_low_support"}:
        recommended = [
            str(y).strip()
            for y in precheck_meta.get("recommended_confirmatory_y_cols", [])
            if str(y).strip()
        ]
        recommended_set = set(recommended)
        if recommended:
            effective = [y for y in effective if y in recommended_set]
        dropped_redundant = [y for y in active if y not in effective]

    if auto_policy_norm == "drop_redundant_and_low_support":
        low_support = set(str(y).strip() for y in precheck_meta.get("low_support_y_cols", []) if str(y).strip())
        low_support.update(
            str(y).strip()
            for y in precheck_meta.get("low_support_track_y_cols", [])
            if str(y).strip()
        )
        dropped_low_support = [y for y in effective if y in low_support]
        if dropped_low_support:
            effective = [y for y in effective if y not in low_support]

    fallback_applied = False
    if not effective:
        fallback_applied = True
        if active:
            effective = [active[0]]
        elif y_available:
            effective = [y_available[0]]

    meta = {
        "auto_policy": auto_policy_norm,
        "requested_confirmatory_y_cols": requested,
        "requested_confirmatory_y_cols_in_scope": requested_in_scope,
        "requested_confirmatory_y_cols_missing": requested_missing,
        "available_y_cols": y_available,
        "effective_confirmatory_y_cols": effective,
        "dropped_redundant_y_cols": dropped_redundant,
        "dropped_low_support_y_cols": dropped_low_support,
        "fallback_applied": bool(fallback_applied),
    }
    return effective, meta


def _derive_y_time_window_outcomes(
    data: pd.DataFrame,
    *,
    years: Sequence[int],
) -> tuple[pd.DataFrame, Dict[str, object]]:
    years_clean: List[int] = []
    seen: set[int] = set()
    for year in years:
        y = int(year)
        if y <= 0 or y in seen:
            continue
        seen.add(y)
        years_clean.append(y)
    if not years_clean:
        raise ValueError("derive-y-time-windows enabled but no positive window years were provided")
    required = ["y_all", "recency_years_alt"]
    missing = [c for c in required if c not in data.columns]
    if missing:
        raise ValueError(
            "derive-y-time-windows requires columns missing from input data: "
            + ",".join(missing)
        )

    out = data.copy()
    y_all_bin = (pd.to_numeric(out["y_all"], errors="coerce").fillna(0.0) > 0).astype(int)
    recency = pd.to_numeric(out["recency_years_alt"], errors="coerce")
    generated_rows: List[Dict[str, object]] = []

    for year in years_clean:
        col = f"y_{int(year)}y"
        within_window = recency.notna() & (recency >= 0.0) & (recency <= float(year))
        out[col] = ((y_all_bin == 1) & within_window).astype(int)
        generated = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)
        generated_rows.append(
            {
                "y_col": col,
                "window_years": int(year),
                "n_positive": int(generated.sum()),
                "positive_share": _as_float(generated.mean(), default=0.0),
                "matches_y_all_exactly": bool((generated == y_all_bin).all()),
            }
        )

    return out, {
        "enabled": True,
        "years": years_clean,
        "source_y_col": "y_all",
        "source_recency_col": "recency_years_alt",
        "generated_cols": [f"y_{int(y)}y" for y in years_clean],
        "generated": generated_rows,
    }


def _extract_feature_atoms(feature_name: str) -> List[str]:
    feat = str(feature_name or "").strip()
    if not feat:
        return []
    atoms: List[str] = []
    if feat.startswith("cat__"):
        parts = feat.split("__")
        if len(parts) >= 3:
            atoms.append(parts[1])
    if not atoms:
        for token in re.findall(r"(?:pa|ext)__[a-zA-Z0-9_]+", feat):
            if token not in atoms:
                atoms.append(token)
    if not atoms:
        for token in ("is_academia_origin", "pub_year_alt", "recency_years_alt"):
            if token in feat and token not in atoms:
                atoms.append(token)
    if not atoms:
        atoms.append(feat)
    return atoms


def _scan_family_signature(row: Dict[str, object], *, mode: str) -> str:
    feature = str(row.get("feature_name", "")).strip()
    if mode == "feature":
        return feature
    atoms: List[str] = []
    raw_expr_inputs = row.get("expression_input_features")
    if isinstance(raw_expr_inputs, list):
        atoms.extend(str(v).strip() for v in raw_expr_inputs if str(v).strip())
    cat_source = str(row.get("categorical_source_feature", "")).strip()
    if cat_source:
        atoms.append(cat_source)
    if not atoms:
        atoms.extend(_extract_feature_atoms(feature))
    dedup_atoms = sorted(set(a for a in atoms if a))
    if not dedup_atoms:
        return feature
    return "|".join(dedup_atoms)


def _scan_family_rank(row: Dict[str, object]) -> Tuple[object, ...]:
    feature = str(row.get("feature_name", "")).strip()
    is_expression = int(feature.startswith("expr__") or int(row.get("expression_generated", 0) or 0) == 1)
    is_categorical = int(feature.startswith("cat__") or int(row.get("categorical_generated", 0) or 0) == 1)
    variation = _as_float(row.get("share_events_with_variation"), default=0.0)
    nonmissing = _as_float(row.get("share_events_nonmissing"), default=0.0)
    return (
        is_expression,
        is_categorical,
        -variation,
        -nonmissing,
        len(feature),
        feature,
    )


def _apply_scan_family_dedupe(
    rows: List[Dict[str, object]],
    *,
    mode: str,
) -> tuple[List[Dict[str, object]], Dict[str, object]]:
    if mode not in {"none", "feature", "atom"}:
        raise ValueError("scan-family-dedupe-mode must be one of: none, feature, atom")
    out = [dict(r) for r in rows]
    allowed_idx = [i for i, r in enumerate(out) if int(r.get("allowed_in_scan", 0)) == 1]
    if mode == "none" or not allowed_idx:
        return out, {
            "scan_family_dedupe_mode": mode,
            "n_allowed_before": len(allowed_idx),
            "n_allowed_after": len(allowed_idx),
            "n_signatures_before": len(allowed_idx),
            "n_signatures_after": len(allowed_idx),
            "n_dropped_family_duplicates": 0,
            "sample_kept_signatures": [],
        }

    signature_to_best: Dict[str, int] = {}
    signature_counts: Counter[str] = Counter()
    for idx in allowed_idx:
        row = out[idx]
        signature = _scan_family_signature(row, mode=mode)
        if not signature:
            continue
        signature_counts[signature] += 1
        prev = signature_to_best.get(signature)
        if prev is None:
            signature_to_best[signature] = idx
            continue
        if _scan_family_rank(row) < _scan_family_rank(out[prev]):
            signature_to_best[signature] = idx

    dropped = 0
    for idx in allowed_idx:
        row = out[idx]
        signature = _scan_family_signature(row, mode=mode)
        best_idx = signature_to_best.get(signature)
        if best_idx is None or best_idx == idx:
            continue
        row["allowed_in_scan"] = 0
        reasons = row.get("block_reasons", [])
        if not isinstance(reasons, list):
            reasons = []
        reasons.append("family_deduped")
        row["block_reasons"] = sorted(set(str(x) for x in reasons if str(x).strip()))
        dropped += 1

    allowed_after = sum(1 for r in out if int(r.get("allowed_in_scan", 0)) == 1)
    return out, {
        "scan_family_dedupe_mode": mode,
        "n_allowed_before": len(allowed_idx),
        "n_allowed_after": int(allowed_after),
        "n_signatures_before": int(len(signature_counts)),
        "n_signatures_after": int(len(signature_to_best)),
        "n_dropped_family_duplicates": int(dropped),
        "sample_kept_signatures": sorted(list(signature_to_best.keys()))[:20],
    }


def _informative_capacity_for_y(df: pd.DataFrame, *, y_col: str) -> Dict[str, int]:
    if y_col not in df.columns:
        return {
            "n_two_alt_events": 0,
            "n_informative_events": 0,
            "n_policy_docs_informative": 0,
        }
    use_cols = ["event_id", "policy_document_id", y_col]
    if "track" in df.columns:
        use_cols.append("track")
    use = df[use_cols].copy()
    if "track" in use.columns:
        use["event_key"] = use["track"].astype(str) + "|" + use["event_id"].astype(str)
    else:
        use["event_key"] = use["event_id"].astype(str)
    use[y_col] = pd.to_numeric(use[y_col], errors="coerce").fillna(0.0)
    n_two_alt_events = 0
    informative_events = 0
    informative_docs: List[str] = []
    for _, g in use.groupby("event_key", dropna=False):
        if len(g) != 2:
            continue
        n_two_alt_events += 1
        if int(g[y_col].sum()) != 1:
            continue
        informative_events += 1
        informative_docs.append(str(g["policy_document_id"].iloc[0]))
    return {
        "n_two_alt_events": int(n_two_alt_events),
        "n_informative_events": int(informative_events),
        "n_policy_docs_informative": int(len(set(informative_docs))),
    }


def _build_y_validated_gate_map(
    *,
    data: pd.DataFrame,
    y_contexts: Sequence[Tuple[str, str]],
    min_events_requested: int,
    min_docs_requested: int,
    auto_scale: bool,
    min_events_floor: int,
    min_docs_floor: int,
    event_ratio: float,
    doc_ratio: float,
    rounding_mode: str,
) -> tuple[Dict[str, int], Dict[str, int], Dict[str, object]]:
    val = data[data["split_role"] == "validation"].copy() if "split_role" in data.columns else data.copy()
    track_values = (
        sorted(val["track"].dropna().astype(str).unique().tolist())
        if "track" in val.columns
        else []
    )
    if not track_values:
        track_values = ["all_tracks"]

    y_to_contexts: Dict[str, List[str]] = {}
    for context_scope, y_col in y_contexts:
        y_to_contexts.setdefault(str(y_col), []).append(str(context_scope))

    event_ratio_c = min(max(float(event_ratio), 0.0), 1.0)
    doc_ratio_c = min(max(float(doc_ratio), 0.0), 1.0)
    min_events_floor = max(int(min_events_floor), 1)
    min_docs_floor = max(int(min_docs_floor), 1)

    gate_events_by_y: Dict[str, int] = {}
    gate_docs_by_y: Dict[str, int] = {}
    per_y_meta: Dict[str, object] = {}
    infeasible_under_requested: List[str] = []
    dropped_to_low_floor: List[str] = []
    unusable_y_cols: List[str] = []
    below_floor_y_cols: List[str] = []

    for y_col in sorted(y_to_contexts.keys()):
        total_cap = _informative_capacity_for_y(val, y_col=y_col)
        by_track: Dict[str, Dict[str, int]] = {}
        for track in track_values:
            if track == "all_tracks":
                g = val
            else:
                g = val[val["track"].astype(str) == str(track)].copy()
            by_track[str(track)] = _informative_capacity_for_y(g, y_col=y_col)

        min_track_events = min(int(v["n_informative_events"]) for v in by_track.values()) if by_track else 0
        min_track_docs = min(int(v["n_policy_docs_informative"]) for v in by_track.values()) if by_track else 0
        gate_events = int(min_events_requested)
        gate_docs = int(min_docs_requested)
        adjustments: List[str] = []
        total_events = int(total_cap.get("n_informative_events", 0))
        total_docs = int(total_cap.get("n_policy_docs_informative", 0))

        if min_track_events < int(min_events_requested):
            infeasible_under_requested.append(
                f"{y_col}:events min_track={min_track_events} < requested={int(min_events_requested)}"
            )
        if min_track_docs < int(min_docs_requested):
            infeasible_under_requested.append(
                f"{y_col}:policy_docs min_track={min_track_docs} < requested={int(min_docs_requested)}"
            )

        if auto_scale:
            cand_events = _round_by_mode(float(min_track_events) * event_ratio_c, mode=rounding_mode)
            cand_docs = _round_by_mode(float(min_track_docs) * doc_ratio_c, mode=rounding_mode)
            cand_events = max(min_events_floor, int(cand_events))
            cand_docs = max(min_docs_floor, int(cand_docs))
            # Never exceed track-level informative capacity.
            cap_events = max(min_track_events, 1)
            cap_docs = max(min_track_docs, 1)
            cand_events = min(int(min_events_requested), int(cand_events), int(cap_events))
            cand_docs = min(int(min_docs_requested), int(cand_docs), int(cap_docs))
            if cand_events != int(min_events_requested):
                adjustments.append(f"events {int(min_events_requested)} -> {int(cand_events)}")
            if cand_docs != int(min_docs_requested):
                adjustments.append(f"policy_docs {int(min_docs_requested)} -> {int(cand_docs)}")
            if min_track_events < min_events_floor:
                dropped_to_low_floor.append(
                    f"{y_col}:events min_track={min_track_events} below floor={min_events_floor}"
                )
            if min_track_docs < min_docs_floor:
                dropped_to_low_floor.append(
                    f"{y_col}:policy_docs min_track={min_track_docs} below floor={min_docs_floor}"
                )
            gate_events = int(max(cand_events, 1))
            gate_docs = int(max(cand_docs, 1))

        is_unusable = (total_events <= 0 or total_docs <= 0 or min_track_events <= 0 or min_track_docs <= 0)
        is_below_floor = (min_track_events < min_events_floor or min_track_docs < min_docs_floor)
        if is_unusable:
            unusable_y_cols.append(y_col)
        if is_below_floor:
            below_floor_y_cols.append(y_col)
        feasibility_label = "usable"
        if is_unusable:
            feasibility_label = "unusable"
        elif is_below_floor:
            feasibility_label = "low_capacity"

        gate_events_by_y[y_col] = gate_events
        gate_docs_by_y[y_col] = gate_docs
        per_y_meta[y_col] = {
            "contexts": sorted(set(y_to_contexts.get(y_col, []))),
            "positive_share_validation": _as_float(pd.to_numeric(val[y_col], errors="coerce").mean(), default=0.0)
            if y_col in val.columns
            else None,
            "validation_capacity_total": total_cap,
            "validation_capacity_min_track": {
                "n_informative_events": int(min_track_events),
                "n_policy_docs_informative": int(min_track_docs),
            },
            "validation_capacity_by_track": by_track,
            "validated_gate_effective": {
                "min_informative_events": int(gate_events),
                "min_policy_docs": int(gate_docs),
            },
            "profile_adjustments": adjustments,
            "feasibility_label": feasibility_label,
            "is_unusable": int(is_unusable),
            "is_below_floor": int(is_below_floor),
        }

    meta = {
        "auto_scale_y_validated_gates": bool(auto_scale),
        "requested_gate": {
            "min_informative_events": int(min_events_requested),
            "min_policy_docs": int(min_docs_requested),
        },
        "adaptive_params": {
            "event_ratio": float(event_ratio_c),
            "doc_ratio": float(doc_ratio_c),
            "rounding_mode": str(rounding_mode),
            "min_events_floor": int(min_events_floor),
            "min_docs_floor": int(min_docs_floor),
        },
        "per_y": per_y_meta,
        "infeasible_under_requested": infeasible_under_requested,
        "below_floor_notes": dropped_to_low_floor,
        "unusable_y_cols": sorted(set(unusable_y_cols)),
        "below_floor_y_cols": sorted(set(below_floor_y_cols)),
    }
    return gate_events_by_y, gate_docs_by_y, meta


def _apply_y_feasibility_mode(
    *,
    mode: str,
    y_validated_gate_meta: Dict[str, object],
) -> Dict[str, List[str]]:
    mode_norm = str(mode or "").strip()
    if mode_norm not in {"warn", "fail_unusable", "fail_below_floor"}:
        raise ValueError(
            "y feasibility mode must be one of: warn, fail_unusable, fail_below_floor"
        )
    y_unusable = sorted(
        set(str(x).strip() for x in y_validated_gate_meta.get("unusable_y_cols", []) if str(x).strip())
    )
    y_below_floor = sorted(
        set(str(x).strip() for x in y_validated_gate_meta.get("below_floor_y_cols", []) if str(x).strip())
    )
    if mode_norm == "fail_unusable" and y_unusable:
        raise ValueError(
            "y feasibility check failed (unusable y contexts): " + ", ".join(y_unusable)
        )
    if mode_norm == "fail_below_floor" and (y_unusable or y_below_floor):
        failing = sorted(set([*y_unusable, *y_below_floor]))
        raise ValueError("y feasibility check failed (below floor or unusable): " + ", ".join(failing))
    return {
        "y_unusable_y_cols": y_unusable,
        "y_below_floor_y_cols": y_below_floor,
    }


def _select_bootstrap_escalation_shortlist(
    *,
    top_rows: Sequence[Dict[str, object]],
    p_threshold: float,
    q_threshold: float,
    p_margin: float,
    q_margin: float,
    max_candidates: int,
    dedupe_mode: str,
) -> tuple[List[Dict[str, object]], Dict[str, object]]:
    if dedupe_mode not in {"feature", "atom"}:
        raise ValueError("escalation-dedupe-mode must be one of: feature, atom")
    max_candidates = max(int(max_candidates), 0)
    if max_candidates == 0:
        return [], {"n_borderline_rows": 0, "n_selected_features": 0, "selected_candidate_ids": []}

    restart_ids = sorted(
        {
            int(r.get("restart_id", 0) or 0)
            for r in top_rows
            if int(r.get("restart_id", 0) or 0) > 0
        }
    )
    anchor_restart_id = restart_ids[0] if restart_ids else 0
    p_margin = max(float(p_margin), 0.0)
    q_margin = max(float(q_margin), 0.0)
    p_upper = float(p_threshold) + float(p_margin)
    q_upper = float(q_threshold) + float(q_margin)

    borderline_rows: List[Dict[str, object]] = []
    for row in top_rows:
        if anchor_restart_id > 0 and int(row.get("restart_id", 0) or 0) != anchor_restart_id:
            continue
        if str(row.get("status_validation", "")) != "ok":
            continue
        if str(row.get("candidate_tier", "")) == "validated_candidate":
            continue
        p_val = _as_float(row.get("p_boot_validation"), default=float("inf"))
        q_val = _as_float(row.get("q_value_validation"), default=float("inf"))
        if not math.isfinite(p_val) or not math.isfinite(q_val):
            continue
        if p_val <= float(p_threshold) and q_val <= float(q_threshold):
            continue
        if p_val > p_upper or q_val > q_upper:
            continue
        feature = str(row.get("key_factor", "")).strip()
        if not feature:
            continue
        p_gap = max(0.0, p_val - float(p_threshold))
        q_gap = max(0.0, q_val - float(q_threshold))
        norm_p = p_gap / max(float(p_margin), 1e-12) if p_margin > 0 else (0.0 if p_gap == 0 else float("inf"))
        norm_q = q_gap / max(float(q_margin), 1e-12) if q_margin > 0 else (0.0 if q_gap == 0 else float("inf"))
        closeness = max(norm_p, norm_q)
        r = dict(row)
        r["_borderline_closeness"] = closeness
        borderline_rows.append(r)

    borderline_rows.sort(
        key=lambda r: (
            _as_float(r.get("_borderline_closeness"), default=float("inf")),
            _as_float(r.get("q_value_validation"), default=float("inf")),
            _as_float(r.get("p_boot_validation"), default=float("inf")),
            -abs(_as_float(r.get("beta_validation"), default=0.0)),
            str(r.get("candidate_id", "")),
        )
    )

    selected: List[Dict[str, object]] = []
    selected_candidate_ids: List[str] = []
    selected_signatures: List[str] = []
    seen_signatures: set[str] = set()
    dropped_duplicate_signature = 0
    for rank_idx, row in enumerate(borderline_rows, start=1):
        feature = str(row.get("key_factor", "")).strip()
        if dedupe_mode == "feature":
            signature = feature
        else:
            signature = "|".join(sorted(set(_extract_feature_atoms(feature))))
        if signature in seen_signatures:
            dropped_duplicate_signature += 1
            continue
        seen_signatures.add(signature)
        selected_signatures.append(signature)
        selected_candidate_ids.append(str(row.get("candidate_id", "")))
        selected.append(
            {
                "feature_name": feature,
                "allowed_in_scan": 1,
                "shortlist_rank": rank_idx,
                "shortlist_signature": signature,
                "shortlist_atoms": _extract_feature_atoms(feature),
                "shortlist_source_tier": "borderline_escalation",
                "shortlist_source_candidate_id": str(row.get("candidate_id", "")),
                "shortlist_source_p_validation": _as_float(row.get("p_boot_validation"), default=float("inf")),
                "shortlist_source_q_validation": _as_float(row.get("q_value_validation"), default=float("inf")),
            }
        )
        if len(selected) >= int(max_candidates):
            break

    meta = {
        "anchor_restart_id": int(anchor_restart_id),
        "dedupe_mode": dedupe_mode,
        "max_candidates": int(max_candidates),
        "p_threshold": float(p_threshold),
        "q_threshold": float(q_threshold),
        "p_margin": float(p_margin),
        "q_margin": float(q_margin),
        "n_borderline_rows": int(len(borderline_rows)),
        "n_selected_features": int(len(selected)),
        "n_dropped_duplicate_signature": int(dropped_duplicate_signature),
        "selected_candidate_ids": selected_candidate_ids,
        "selected_signatures": selected_signatures,
        "selected_features": [str(r.get("feature_name", "")) for r in selected],
    }
    return selected, meta


def _tier_rank(tier: object) -> int:
    t = str(tier or "").strip()
    if t == "validated_candidate":
        return 2
    if t == "support_candidate":
        return 1
    return 0


def _apply_track_consensus_gate(
    *,
    top_rows: List[Dict[str, object]],
    scan_rows: List[Dict[str, object]],
    enforce: bool,
    anchor_track: str,
    min_anchor_tier: str,
) -> Dict[str, object]:
    required_rank = _tier_rank(min_anchor_tier)
    anchor_track = str(anchor_track or "").strip()
    groups: Dict[Tuple[object, ...], List[Dict[str, object]]] = {}
    for row in top_rows:
        key = (
            int(row.get("restart_id", 0) or 0),
            str(row.get("context_scope", "")),
            str(row.get("y_col", "")),
            str(row.get("spec_id", "")),
            str(row.get("key_factor", "")),
            str(row.get("control_set", "")),
        )
        groups.setdefault(key, []).append(row)

    n_groups = 0
    n_groups_pass = 0
    n_rows_demoted = 0
    update_by_candidate: Dict[Tuple[int, str], Dict[str, object]] = {}
    for key, rows in groups.items():
        n_groups += 1
        restart_id = int(key[0])
        signature = "|".join(str(v) for v in key)
        anchor_row = None
        for row in rows:
            if str(row.get("track", "")) == anchor_track:
                anchor_row = row
                break
        if anchor_row is None:
            anchor_tier = ""
            anchor_pass = False
            status = "no_anchor_track"
        else:
            anchor_tier = str(anchor_row.get("candidate_tier", ""))
            anchor_pass = _tier_rank(anchor_tier) >= required_rank
            status = "pass" if anchor_pass else "anchor_below_threshold"
        if anchor_pass:
            n_groups_pass += 1

        for row in rows:
            raw_tier = str(row.get("candidate_tier", ""))
            row["candidate_tier_raw"] = raw_tier
            row["track_consensus_signature"] = signature
            row["track_consensus_anchor_track"] = anchor_track
            row["track_consensus_anchor_tier"] = anchor_tier
            row["track_consensus_min_anchor_tier"] = str(min_anchor_tier)
            row["track_consensus_anchor_pass"] = int(anchor_pass)
            row["track_consensus_status"] = status
            row["track_consensus_enforced"] = int(bool(enforce))
            demoted = 0
            if bool(enforce) and raw_tier == "validated_candidate" and not anchor_pass:
                row["candidate_tier"] = "support_candidate"
                demoted = 1
                n_rows_demoted += 1
            row["track_consensus_demoted"] = int(demoted)
            cid = str(row.get("candidate_id", ""))
            if cid:
                update_by_candidate[(restart_id, cid)] = {
                    "candidate_tier": str(row.get("candidate_tier", "")),
                    "candidate_tier_raw": raw_tier,
                    "track_consensus_signature": signature,
                    "track_consensus_anchor_track": anchor_track,
                    "track_consensus_anchor_tier": anchor_tier,
                    "track_consensus_min_anchor_tier": str(min_anchor_tier),
                    "track_consensus_anchor_pass": int(anchor_pass),
                    "track_consensus_status": status,
                    "track_consensus_enforced": int(bool(enforce)),
                    "track_consensus_demoted": int(demoted),
                }

    for row in scan_rows:
        key = (int(row.get("restart_id", 0) or 0), str(row.get("candidate_id", "")))
        patch = update_by_candidate.get(key)
        if not patch:
            continue
        for k, v in patch.items():
            row[k] = v

    return {
        "enforce_track_consensus": bool(enforce),
        "consensus_anchor_track": anchor_track,
        "consensus_min_anchor_tier": str(min_anchor_tier),
        "n_groups": int(n_groups),
        "n_groups_pass_anchor": int(n_groups_pass),
        "n_rows_demoted_from_validated": int(n_rows_demoted),
    }


def _sign_token(value: object) -> int:
    try:
        x = float(value)
    except Exception:
        return 0
    if not math.isfinite(x):
        return 0
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _mode_token(values: Sequence[str]) -> tuple[str, int]:
    tokens = [str(v).strip() for v in values if str(v).strip()]
    if not tokens:
        return "", 0
    counts = Counter(tokens)
    best = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0]
    return str(best[0]), int(best[1])


def _build_restart_inference_rows(
    *,
    top_rows: List[Dict[str, object]],
    n_restarts: int,
    p_threshold: float,
    q_threshold: float,
    confirmatory_y_cols: Sequence[str],
    nonconfirmatory_max_tier: str,
) -> tuple[List[Dict[str, object]], Dict[str, object]]:
    confirmatory_y_set = set(str(v).strip() for v in confirmatory_y_cols if str(v).strip())
    nonconfirmatory_max_tier = str(nonconfirmatory_max_tier or "support_candidate").strip()
    if nonconfirmatory_max_tier not in {"support_candidate", "exploratory"}:
        raise ValueError("nonconfirmatory_max_tier must be one of: support_candidate, exploratory")
    by_candidate: Dict[str, List[Dict[str, object]]] = {}
    for row in top_rows:
        cid = str(row.get("candidate_id", "")).strip()
        if not cid:
            continue
        by_candidate.setdefault(cid, []).append(row)

    rows_out: List[Dict[str, object]] = []
    denominator = max(int(n_restarts), 1)
    for cid, rows in by_candidate.items():
        sorted_rows = sorted(
            rows,
            key=lambda r: (
                int(r.get("restart_id", 0) or 0) if int(r.get("restart_id", 0) or 0) > 0 else 10_000_000,
                str(r.get("track", "")),
                str(r.get("spec_id", "")),
                str(r.get("key_factor", "")),
            ),
        )
        ref = sorted_rows[0]
        restart_ids = sorted({int(r.get("restart_id", 0) or 0) for r in rows if int(r.get("restart_id", 0) or 0) > 0})
        n_present = len(restart_ids)
        if n_restarts <= 1:
            n_present = max(n_present, len(rows))
        presence_rate = float(n_present) / float(denominator)

        status_discovery_vals = [str(r.get("status_discovery", "")) for r in rows]
        status_validation_vals = [str(r.get("status_validation", "")) for r in rows]
        status_discovery_mode, _ = _mode_token(status_discovery_vals)
        status_validation_mode, _ = _mode_token(status_validation_vals)
        discovery_ok_any = any(str(v) == "ok" for v in status_discovery_vals)

        p_val_ok: List[float] = []
        p_disc_all: List[float] = []
        beta_disc: List[float] = []
        beta_val: List[float] = []
        score_disc: List[float] = []
        score_val: List[float] = []
        for r in rows:
            p_disc = _as_float(r.get("p_boot_discovery"), default=float("inf"))
            if math.isfinite(p_disc):
                p_disc_all.append(float(p_disc))
            p_val = _as_float(r.get("p_boot_validation"), default=float("inf"))
            if math.isfinite(p_val):
                if str(r.get("status_validation", "")) == "ok":
                    p_val_ok.append(float(p_val))
            b_disc = _as_float(r.get("beta_discovery"), default=float("nan"))
            if math.isfinite(b_disc):
                beta_disc.append(float(b_disc))
            b_val = _as_float(r.get("beta_validation"), default=float("nan"))
            if math.isfinite(b_val):
                beta_val.append(float(b_val))
            s_disc = _as_float(r.get("score_discovery"), default=float("nan"))
            if math.isfinite(s_disc):
                score_disc.append(float(s_disc))
            s_val = _as_float(r.get("score_validation"), default=float("nan"))
            if math.isfinite(s_val):
                score_val.append(float(s_val))

        p_agg = float(np.median(np.asarray(p_val_ok, dtype=float))) if p_val_ok else None
        p_min = float(np.min(np.asarray(p_val_ok, dtype=float))) if p_val_ok else None
        p_max = float(np.max(np.asarray(p_val_ok, dtype=float))) if p_val_ok else None
        p_mean = float(np.mean(np.asarray(p_val_ok, dtype=float))) if p_val_ok else None
        beta_disc_med = float(np.median(np.asarray(beta_disc, dtype=float))) if beta_disc else None
        beta_val_med = float(np.median(np.asarray(beta_val, dtype=float))) if beta_val else None
        score_disc_med = float(np.median(np.asarray(score_disc, dtype=float))) if score_disc else None
        score_val_med = float(np.median(np.asarray(score_val, dtype=float))) if score_val else None

        tiers_restart = [str(r.get("candidate_tier", "")) for r in rows]
        tier_counts = Counter(tiers_restart)
        modal_tier, modal_tier_count = _mode_token(tiers_restart)
        n_validated_restart = int(tier_counts.get("validated_candidate", 0))
        n_support_or_better_restart = int(
            tier_counts.get("validated_candidate", 0) + tier_counts.get("support_candidate", 0)
        )
        validated_rate_restart = float(n_validated_restart) / float(denominator)
        support_rate_restart = float(n_support_or_better_restart) / float(denominator)
        modal_tier_rate_restart = float(modal_tier_count) / float(max(len(rows), 1))

        row_out: Dict[str, object] = {
            "run_id": ref.get("run_id", ""),
            "candidate_id": cid,
            "track": ref.get("track", ""),
            "context_scope": ref.get("context_scope", ""),
            "y_col": ref.get("y_col", ""),
            "spec_id": ref.get("spec_id", ""),
            "key_factor": ref.get("key_factor", ""),
            "control_set": ref.get("control_set", ""),
            "fdr_family_id": ref.get("fdr_family_id", ""),
            "status_discovery": "ok" if discovery_ok_any else status_discovery_mode,
            "status_validation": "ok" if p_val_ok else status_validation_mode,
            "p_boot_discovery": float(np.median(np.asarray(p_disc_all, dtype=float))) if p_disc_all else None,
            "p_boot_validation": p_agg,
            "q_value_validation": None,
            "beta_discovery": beta_disc_med,
            "beta_validation": beta_val_med,
            "score_discovery": score_disc_med,
            "score_validation": score_val_med,
            "validation_pass_p_raw": 0,
            "validation_pass_q_raw": 0,
            "validation_pass_p": False,
            "validation_pass_q": False,
            "candidate_tier": "support_candidate" if discovery_ok_any else "exploratory",
            "candidate_tier_raw": "support_candidate" if discovery_ok_any else "exploratory",
            "confirmatory_eligible": 0,
            "confirmatory_policy_demoted": 0,
            "confirmatory_policy_reason": "",
            "track_consensus_signature": ref.get("track_consensus_signature", ""),
            "track_consensus_anchor_track": ref.get("track_consensus_anchor_track", ""),
            "track_consensus_anchor_tier": ref.get("track_consensus_anchor_tier", ""),
            "track_consensus_min_anchor_tier": ref.get("track_consensus_min_anchor_tier", ""),
            "track_consensus_anchor_pass": ref.get("track_consensus_anchor_pass", 1),
            "track_consensus_status": ref.get("track_consensus_status", ""),
            "track_consensus_enforced": ref.get("track_consensus_enforced", 0),
            "track_consensus_demoted": 0,
            "candidate_pool_size": ref.get("candidate_pool_size", None),
            "equivalence_hash": ref.get("equivalence_hash", ""),
            "support_only": 1,
            "not_replacing_confirmatory_claim": 1,
            "validation_used_for_search": ref.get("validation_used_for_search", 0),
            "candidate_pool_locked_pre_validation": ref.get("candidate_pool_locked_pre_validation", 1),
            "data_hash": ref.get("data_hash", ""),
            "config_hash": ref.get("config_hash", ""),
            "feature_registry_hash": ref.get("feature_registry_hash", ""),
            "git_commit": ref.get("git_commit", ""),
            "timestamp": ref.get("timestamp", ""),
            "restart_aggregation_mode": "median_p_validation_ok",
            "n_restarts_total": int(denominator),
            "n_restarts_present": int(n_present),
            "presence_rate": round(float(presence_rate), 6),
            "n_validation_ok_restarts": int(len(p_val_ok)),
            "p_boot_validation_min": p_min,
            "p_boot_validation_median": p_agg,
            "p_boot_validation_max": p_max,
            "p_boot_validation_mean": p_mean,
            "candidate_tier_modal_restart": modal_tier,
            "candidate_tier_modal_rate_restart": round(float(modal_tier_rate_restart), 6),
            "validated_rate_restart": round(float(validated_rate_restart), 6),
            "support_or_better_rate_restart": round(float(support_rate_restart), 6),
        }
        rows_out.append(row_out)

    q_input = [
        r
        for r in rows_out
        if str(r.get("status_validation", "")) == "ok" and r.get("p_boot_validation") is not None
    ]
    attach_bh_qvalues(q_input, p_col="p_boot_validation", family_col="fdr_family_id")

    n_candidates_validated = 0
    n_candidates_confirmatory_eligible = 0
    n_candidates_demoted_nonconfirmatory = 0
    for row in rows_out:
        p_val = _as_float(row.get("p_boot_validation"), default=float("inf"))
        q_val = _as_float(row.get("q_value"), default=float("inf"))
        val_ok = str(row.get("status_validation", "")) == "ok" and math.isfinite(p_val)
        p_pass_raw = bool(val_ok and p_val <= float(p_threshold))
        q_pass_raw = bool(val_ok and math.isfinite(q_val) and q_val <= float(q_threshold))
        y_col = str(row.get("y_col", "")).strip()
        confirmatory_eligible = bool(not confirmatory_y_set or y_col in confirmatory_y_set)
        if confirmatory_eligible:
            n_candidates_confirmatory_eligible += 1
        p_pass = bool(p_pass_raw and confirmatory_eligible)
        q_pass = bool(q_pass_raw and confirmatory_eligible)
        row["q_value_validation"] = row.get("q_value", None)
        if "q_value" in row:
            del row["q_value"]
        row["validation_pass_p_raw"] = int(p_pass_raw)
        row["validation_pass_q_raw"] = int(q_pass_raw)
        row["validation_pass_p"] = int(p_pass)
        row["validation_pass_q"] = int(q_pass)
        row["confirmatory_eligible"] = int(confirmatory_eligible)
        row["confirmatory_policy_demoted"] = 0
        row["confirmatory_policy_reason"] = ""
        if p_pass and q_pass:
            tier_raw = "validated_candidate"
        elif str(row.get("status_discovery", "")) == "ok":
            tier_raw = "support_candidate"
        else:
            tier_raw = "exploratory"
        if not confirmatory_eligible:
            capped_tier = tier_raw
            if nonconfirmatory_max_tier == "exploratory":
                capped_tier = "exploratory"
            elif nonconfirmatory_max_tier == "support_candidate" and tier_raw == "validated_candidate":
                capped_tier = "support_candidate"
            if capped_tier != tier_raw:
                tier_raw = capped_tier
                row["confirmatory_policy_demoted"] = 1
                row["confirmatory_policy_reason"] = "nonconfirmatory_y_tier_capped"
                n_candidates_demoted_nonconfirmatory += 1
        tier_final = tier_raw
        demoted = 0
        if (
            int(row.get("track_consensus_enforced", 0) or 0) == 1
            and int(row.get("track_consensus_anchor_pass", 1) or 0) == 0
            and tier_raw == "validated_candidate"
        ):
            tier_final = "support_candidate"
            demoted = 1
        row["candidate_tier_raw"] = tier_raw
        row["candidate_tier"] = tier_final
        row["track_consensus_demoted"] = int(demoted)
        if tier_final == "validated_candidate":
            n_candidates_validated += 1

    tier_order = {"validated_candidate": 0, "support_candidate": 1, "exploratory": 2}
    rows_out.sort(
        key=lambda r: (
            tier_order.get(str(r.get("candidate_tier", "")), 9),
            _as_float(r.get("q_value_validation"), default=float("inf")),
            _as_float(r.get("p_boot_validation"), default=float("inf")),
            -abs(_as_float(r.get("beta_validation"), default=0.0)),
            str(r.get("candidate_id", "")),
        )
    )
    meta = {
        "mode": "median_p_validation_ok",
        "confirmatory_y_cols": sorted(confirmatory_y_set),
        "nonconfirmatory_max_tier": nonconfirmatory_max_tier,
        "n_candidates": int(len(rows_out)),
        "n_candidates_confirmatory_eligible": int(n_candidates_confirmatory_eligible),
        "n_candidates_demoted_nonconfirmatory": int(n_candidates_demoted_nonconfirmatory),
        "n_candidates_validation_ok": int(sum(1 for r in rows_out if str(r.get("status_validation", "")) == "ok")),
        "n_candidates_validated": int(n_candidates_validated),
        "n_candidates_q_nonnull": int(sum(1 for r in rows_out if r.get("q_value_validation") is not None)),
        "top5_candidate_ids": [str(r.get("candidate_id", "")) for r in rows_out[:5]],
    }
    return rows_out, meta


def _build_restart_stability_rows(
    *,
    top_rows: List[Dict[str, object]],
    n_restarts: int,
) -> tuple[List[Dict[str, object]], Dict[str, object]]:
    by_candidate: Dict[str, List[Dict[str, object]]] = {}
    for row in top_rows:
        cid = str(row.get("candidate_id", "")).strip()
        if not cid:
            continue
        by_candidate.setdefault(cid, []).append(row)

    rows_out: List[Dict[str, object]] = []
    for cid, rows in by_candidate.items():
        restart_ids = sorted({int(r.get("restart_id", 0) or 0) for r in rows if int(r.get("restart_id", 0) or 0) > 0})
        n_present = len(restart_ids)
        n_present = max(n_present, len(rows)) if n_restarts <= 1 else n_present
        denominator = max(int(n_restarts), 1)
        tiers = [str(r.get("candidate_tier", "")) for r in rows]
        tier_counts = Counter(tiers)
        modal_tier = ""
        modal_tier_count = 0
        if tier_counts:
            modal_tier, modal_tier_count = tier_counts.most_common(1)[0]
        n_validated = int(tier_counts.get("validated_candidate", 0))
        n_support_or_better = int(
            tier_counts.get("validated_candidate", 0) + tier_counts.get("support_candidate", 0)
        )
        sign_val = [_sign_token(r.get("beta_validation")) for r in rows if _sign_token(r.get("beta_validation")) != 0]
        sign_disc = [_sign_token(r.get("beta_discovery")) for r in rows if _sign_token(r.get("beta_discovery")) != 0]
        sign_val_consistency = (
            max(Counter(sign_val).values()) / float(len(sign_val)) if sign_val else 1.0
        )
        sign_disc_consistency = (
            max(Counter(sign_disc).values()) / float(len(sign_disc)) if sign_disc else 1.0
        )
        presence_rate = float(n_present) / float(denominator)
        support_rate = float(n_support_or_better) / float(denominator)
        validated_rate = float(n_validated) / float(denominator)
        modal_rate = float(modal_tier_count) / float(max(len(rows), 1))
        stability_score = (
            0.35 * presence_rate
            + 0.35 * support_rate
            + 0.20 * modal_rate
            + 0.10 * max(sign_val_consistency, sign_disc_consistency)
        )

        ref = rows[0]
        rows_out.append(
            {
                "candidate_id": cid,
                "track": ref.get("track", ""),
                "context_scope": ref.get("context_scope", ""),
                "y_col": ref.get("y_col", ""),
                "spec_id": ref.get("spec_id", ""),
                "key_factor": ref.get("key_factor", ""),
                "control_set": ref.get("control_set", ""),
                "n_restarts_total": int(denominator),
                "n_restarts_present": int(n_present),
                "presence_rate": round(presence_rate, 6),
                "n_validated": int(n_validated),
                "validated_rate": round(validated_rate, 6),
                "n_support_or_better": int(n_support_or_better),
                "support_or_better_rate": round(support_rate, 6),
                "modal_tier": modal_tier,
                "modal_tier_rate": round(modal_rate, 6),
                "beta_validation_sign_consistency": round(float(sign_val_consistency), 6),
                "beta_discovery_sign_consistency": round(float(sign_disc_consistency), 6),
                "stability_score": round(float(stability_score), 6),
            }
        )

    rows_out.sort(
        key=lambda r: (
            -_as_float(r.get("stability_score"), default=0.0),
            -_as_float(r.get("validated_rate"), default=0.0),
            -_as_float(r.get("support_or_better_rate"), default=0.0),
            str(r.get("candidate_id", "")),
        )
    )
    meta = {
        "n_candidates": int(len(rows_out)),
        "n_candidates_stability_ge_0_75": int(sum(1 for r in rows_out if _as_float(r.get("stability_score"), 0.0) >= 0.75)),
        "n_candidates_validated_rate_ge_0_50": int(sum(1 for r in rows_out if _as_float(r.get("validated_rate"), 0.0) >= 0.50)),
        "top5_candidate_ids": [str(r.get("candidate_id", "")) for r in rows_out[:5]],
    }
    return rows_out, meta


def _validate_split_integrity(data: pd.DataFrame, *, strict: bool) -> Dict[str, object]:
    doc_role = data[["policy_document_id", "split_role"]].drop_duplicates()
    role_counts = doc_role.groupby("policy_document_id", dropna=False)["split_role"].nunique()
    overlap_docs = int((role_counts > 1).sum())
    if overlap_docs > 0:
        raise ValueError(
            f"split integrity failure: {overlap_docs} policy_document_id values appear in both discovery and validation"
        )
    counts = doc_role["split_role"].value_counts()
    n_docs_discovery = int(counts.get("discovery", 0))
    n_docs_validation = int(counts.get("validation", 0))
    n_docs_total = int(n_docs_discovery + n_docs_validation)
    if strict and (n_docs_discovery == 0 or n_docs_validation == 0):
        raise ValueError(
            "strict split lock failed: discovery/validation must both be non-empty "
            f"(discovery={n_docs_discovery}, validation={n_docs_validation})"
        )
    return {
        "split_integrity_ok": True,
        "strict_lock_mode": int(strict),
        "n_policy_docs_total": n_docs_total,
        "n_policy_docs_discovery": n_docs_discovery,
        "n_policy_docs_validation": n_docs_validation,
        "n_overlap_policy_docs": overlap_docs,
        "discovery_share_by_docs": (
            float(n_docs_discovery) / float(n_docs_total) if n_docs_total > 0 else None
        ),
    }


def _validation_capacity_by_track(data: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    if "track" not in data.columns:
        return out
    val = data[data["split_role"] == "validation"].copy() if "split_role" in data.columns else data.copy()
    for track, g in val.groupby("track", dropna=False):
        key = str(track)
        out[key] = {
            "n_policy_docs_validation": int(g["policy_document_id"].dropna().astype(str).nunique())
            if "policy_document_id" in g.columns
            else 0,
            "n_events_validation": int(g["event_id"].dropna().astype(str).nunique()) if "event_id" in g.columns else 0,
        }
    return out


def _validation_capacity_totals(data: pd.DataFrame) -> Dict[str, int]:
    val = data[data["split_role"] == "validation"].copy() if "split_role" in data.columns else data.copy()
    return {
        "n_policy_docs_validation_total": int(val["policy_document_id"].dropna().astype(str).nunique())
        if "policy_document_id" in val.columns
        else 0,
        "n_events_validation_total": int(val["event_id"].dropna().astype(str).nunique())
        if "event_id" in val.columns
        else 0,
    }


def _track_capacity_stats(validation_track_capacity: Dict[str, Dict[str, int]]) -> Dict[str, int]:
    if not validation_track_capacity:
        return {
            "n_tracks": 0,
            "min_track_policy_docs_validation": 0,
            "max_track_policy_docs_validation": 0,
            "min_track_events_validation": 0,
            "max_track_events_validation": 0,
        }
    docs = [int(cap.get("n_policy_docs_validation", 0)) for cap in validation_track_capacity.values()]
    events = [int(cap.get("n_events_validation", 0)) for cap in validation_track_capacity.values()]
    return {
        "n_tracks": int(len(validation_track_capacity)),
        "min_track_policy_docs_validation": int(min(docs)),
        "max_track_policy_docs_validation": int(max(docs)),
        "min_track_events_validation": int(min(events)),
        "max_track_events_validation": int(max(events)),
    }


def _collect_track_gate_infeasibility(
    validation_track_capacity: Dict[str, Dict[str, int]],
    *,
    min_policy_docs_gate: int,
    min_events_gate: int,
) -> List[str]:
    infeasible: List[str] = []
    for track, cap in sorted(validation_track_capacity.items(), key=lambda kv: kv[0]):
        n_docs_track = int(cap.get("n_policy_docs_validation", 0))
        n_events_track = int(cap.get("n_events_validation", 0))
        if n_docs_track < int(min_policy_docs_gate):
            infeasible.append(f"{track}:policy_docs {n_docs_track} < gate {int(min_policy_docs_gate)}")
        if n_events_track < int(min_events_gate):
            infeasible.append(f"{track}:events {n_events_track} < gate {int(min_events_gate)}")
    return infeasible


def _build_feasibility_frontier_payload(
    *,
    run_id: str,
    timestamp: str,
    strict_lock_mode: bool,
    gate_profile: str,
    validation_share: float,
    validation_totals: Dict[str, int],
    validation_track_capacity: Dict[str, Dict[str, int]],
    min_policy_docs_requested: int,
    min_events_requested: int,
    min_policy_docs_effective: int,
    min_events_effective: int,
) -> Dict[str, object]:
    stats = _track_capacity_stats(validation_track_capacity)
    n_docs_total = int(validation_totals.get("n_policy_docs_validation_total", 0))
    n_events_total = int(validation_totals.get("n_events_validation_total", 0))
    base_doc_gates = {
        1,
        3,
        5,
        7,
        10,
        12,
        15,
        20,
        25,
        30,
        int(min_policy_docs_requested),
        int(min_policy_docs_effective),
        int(n_docs_total),
        int(stats.get("min_track_policy_docs_validation", 0)),
        int(stats.get("max_track_policy_docs_validation", 0)),
    }
    base_event_gates = {
        1,
        3,
        5,
        7,
        10,
        15,
        20,
        30,
        50,
        100,
        int(min_events_requested),
        int(min_events_effective),
        int(n_events_total),
        int(stats.get("min_track_events_validation", 0)),
        int(stats.get("max_track_events_validation", 0)),
    }
    doc_gates = sorted(int(v) for v in base_doc_gates if int(v) > 0)
    event_gates = sorted(int(v) for v in base_event_gates if int(v) > 0)
    frontier_rows: List[Dict[str, object]] = []
    n_track_feasible_pairs = 0
    n_global_feasible_pairs = 0
    for doc_gate in doc_gates:
        for event_gate in event_gates:
            infeasible = _collect_track_gate_infeasibility(
                validation_track_capacity,
                min_policy_docs_gate=doc_gate,
                min_events_gate=event_gate,
            )
            failing_tracks = sorted({x.split(":")[0] for x in infeasible})
            strict_track_feasible = int(len(infeasible) == 0)
            global_feasible = int(
                n_docs_total >= int(doc_gate) and n_events_total >= int(event_gate)
            )
            if strict_track_feasible == 1:
                n_track_feasible_pairs += 1
            if global_feasible == 1:
                n_global_feasible_pairs += 1
            frontier_rows.append(
                {
                    "min_policy_docs_gate": int(doc_gate),
                    "min_informative_events_gate": int(event_gate),
                    "strict_track_feasible": strict_track_feasible,
                    "global_feasible": global_feasible,
                    "n_failing_tracks": int(len(failing_tracks)),
                    "failing_tracks": failing_tracks,
                }
            )
    summary = {
        "n_frontier_rows": len(frontier_rows),
        "n_track_feasible_pairs": int(n_track_feasible_pairs),
        "n_global_feasible_pairs": int(n_global_feasible_pairs),
        "max_track_feasible_policy_docs_gate": int(stats.get("min_track_policy_docs_validation", 0)),
        "max_track_feasible_events_gate": int(stats.get("min_track_events_validation", 0)),
        "max_global_feasible_policy_docs_gate": n_docs_total,
        "max_global_feasible_events_gate": n_events_total,
        "strict_track_feasible_for_effective_gate": int(
            len(
                _collect_track_gate_infeasibility(
                    validation_track_capacity,
                    min_policy_docs_gate=min_policy_docs_effective,
                    min_events_gate=min_events_effective,
                )
            )
            == 0
        ),
    }
    return {
        "run_id": run_id,
        "timestamp": timestamp,
        "strict_lock_mode": int(strict_lock_mode),
        "gate_profile": gate_profile,
        "validation_share": validation_share,
        "requested_gate": {
            "min_policy_docs": int(min_policy_docs_requested),
            "min_informative_events": int(min_events_requested),
        },
        "effective_gate": {
            "min_policy_docs": int(min_policy_docs_effective),
            "min_informative_events": int(min_events_effective),
        },
        "validation_totals": validation_totals,
        "validation_track_capacity": validation_track_capacity,
        "validation_track_capacity_stats": stats,
        "frontier_rows": frontier_rows,
        "summary": summary,
    }


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _resolve_git_commit_for_lock(*, strict: bool) -> tuple[str, str]:
    root_commit = get_git_commit(ROOT)
    if root_commit:
        return root_commit, "root_repo"
    module03_repo = ROOT / "modules" / "03_regspec_machine"
    module03_commit = get_git_commit(module03_repo)
    if module03_commit:
        return module03_commit, "module03_repo"
    if strict:
        raise ValueError(
            "strict lock mode requires non-empty git commit metadata; no git repo detected "
            f"for root({ROOT}) or module03({module03_repo})"
        )
    return "", "missing"


def _resolve_restart_seeds(base_seed: int, n_restarts: int, step: int) -> List[int]:
    if n_restarts < 1:
        raise ValueError("n_restarts must be >= 1")
    return [int(base_seed + i * step) for i in range(n_restarts)]


def _estimate_candidate_pool_size(
    *,
    data: pd.DataFrame,
    registry: List[Dict[str, object]],
    include_base_controls_effective: bool,
    scan_max_features: int,
    n_contexts: int,
) -> Dict[str, object]:
    allowed_features = [
        str(r.get("feature_name", "")).strip()
        for r in registry
        if int(r.get("allowed_in_scan", 0)) == 1 and str(r.get("feature_name", "")).strip() in data.columns
    ]
    # Keep deterministic unique order.
    deduped: List[str] = []
    seen: set[str] = set()
    for feat in allowed_features:
        if feat and feat not in seen:
            seen.add(feat)
            deduped.append(feat)
    if scan_max_features > 0:
        deduped = deduped[:scan_max_features]
    n_tracks = int(data["track"].dropna().astype(str).nunique()) if "track" in data.columns else 0
    n_contexts_effective = max(int(n_contexts), 0)
    n_control_specs = 2 if include_base_controls_effective else 1
    est_pool = int(len(deduped) * n_tracks * n_contexts_effective * n_control_specs)
    return {
        "n_tracks": n_tracks,
        "n_contexts": n_contexts_effective,
        "n_control_specs": n_control_specs,
        "n_features_effective": len(deduped),
        "estimated_candidate_pool_size": est_pool,
    }


def _resolve_complexity_penalty(
    *,
    requested_penalty: float,
    auto_mode: bool,
    auto_min: float,
    auto_max: float,
    data: pd.DataFrame,
    candidate_pool_meta: Dict[str, object],
) -> tuple[float, Dict[str, object]]:
    discovery_df = data[data["split_role"] == "discovery"].copy() if "split_role" in data.columns else data
    n_events_discovery = int(discovery_df["event_id"].nunique()) if "event_id" in discovery_df.columns else 0
    est_pool = int(candidate_pool_meta.get("estimated_candidate_pool_size", 0))
    ratio = float(est_pool) / float(max(n_events_discovery, 1))
    if auto_mode:
        scale = 1.0 + min(3.0, math.log1p(max(ratio, 0.0)))
        effective = requested_penalty * scale
        effective = min(max(float(effective), float(auto_min)), float(auto_max))
    else:
        scale = 1.0
        effective = float(requested_penalty)
    meta = {
        "complexity_penalty_requested": float(requested_penalty),
        "complexity_penalty_effective": float(effective),
        "complexity_penalty_auto_mode": bool(auto_mode),
        "complexity_penalty_auto_min": float(auto_min),
        "complexity_penalty_auto_max": float(auto_max),
        "complexity_penalty_scale_factor": float(scale),
        "n_events_discovery": n_events_discovery,
        "estimated_candidate_pool_size": est_pool,
        "candidate_event_ratio": ratio,
    }
    return float(effective), meta


def _normalize_registry_feature_names(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for r in rows:
        row = dict(r)
        name = str(row.get("feature_name", "")).strip()
        if not name:
            continue
        data_source = str(row.get("data_source", "")).strip()
        if name.startswith(("ext__", "pa__")):
            normalized_name = name
        elif data_source == "metadata_extension":
            normalized_name = f"ext__{name}"
        elif data_source == "phase_a_model_input":
            normalized_name = f"pa__{name}"
        else:
            normalized_name = name

        allowed = row.get("allowed_in_scan_after_variation_filter_any_scope", row.get("allowed_in_scan", 0))
        try:
            allowed_int = int(allowed)
        except Exception:
            allowed_int = 0

        row["feature_name"] = normalized_name
        row["allowed_in_scan"] = allowed_int
        out.append(row)
    return out


def _filter_registry_by_data(
    rows: List[Dict[str, object]], data: pd.DataFrame
) -> tuple[List[Dict[str, object]], Dict[str, int]]:
    counts: Counter[str] = Counter()
    out: List[Dict[str, object]] = []
    for r in rows:
        row = dict(r)
        feature = str(row.get("feature_name", "")).strip()
        if not feature:
            counts["empty_feature_name"] += 1
            continue
        allowed = int(row.get("allowed_in_scan", 0))
        reasons = list(row.get("block_reasons", [])) if isinstance(row.get("block_reasons", []), list) else []
        if feature not in data.columns:
            allowed = 0
            reasons.append("feature_not_in_joined_data")
            counts["feature_not_in_joined_data"] += 1
        else:
            vec = pd.to_numeric(data[feature], errors="coerce")
            if int(vec.notna().sum()) < 20:
                allowed = 0
                reasons.append("feature_non_numeric_or_low_nonmissing")
                counts["feature_non_numeric_or_low_nonmissing"] += 1
        row["allowed_in_scan"] = int(allowed)
        row["block_reasons"] = sorted(set(reasons))
        out.append(row)
    counts["registry_rows_total"] = len(rows)
    counts["registry_rows_after_filter"] = len(out)
    counts["registry_allowed_after_filter"] = sum(1 for r in out if int(r.get("allowed_in_scan", 0)) == 1)
    return out, dict(counts)


def _resolve_expression_ops(mode: str) -> Dict[str, List[str]]:
    if mode == "signed_log1p":
        return {"unary": ["signed_log1p"], "binary": []}
    if mode == "signed_log1p_square":
        return {"unary": ["signed_log1p", "square"], "binary": []}
    if mode == "ms_benchmark_lite":
        return {
            "unary": ["signed_log1p", "square"],
            "binary": ["ratio", "diff", "interaction"],
        }
    return {"unary": [], "binary": []}


def _expression_feature_name(
    *,
    op: str,
    lhs_feature: str,
    rhs_feature: str | None = None,
) -> str:
    if op == "signed_log1p":
        return f"expr__slog1p__{lhs_feature}"
    if op == "square":
        return f"expr__sq__{lhs_feature}"
    if op == "ratio" and rhs_feature:
        return f"expr__ratio__{lhs_feature}__over__{rhs_feature}"
    if op == "diff" and rhs_feature:
        return f"expr__diff__{lhs_feature}__minus__{rhs_feature}"
    if op == "interaction" and rhs_feature:
        return f"expr__mul__{lhs_feature}__x__{rhs_feature}"
    if rhs_feature:
        return f"expr__{op}__{lhs_feature}__{rhs_feature}"
    return f"expr__{op}__{lhs_feature}"


def _augment_registry_with_expressions(
    *,
    data: pd.DataFrame,
    registry: List[Dict[str, object]],
    build_scope_df: pd.DataFrame,
    mode: str,
    max_new_features: int,
    max_base_features: int,
    max_pairs: int,
    min_nonmissing_count: int,
    min_variation_share: float,
    min_nonmissing_share: float,
) -> tuple[pd.DataFrame, List[Dict[str, object]], Dict[str, object]]:
    build_df = build_scope_df if isinstance(build_scope_df, pd.DataFrame) else data
    build_idx = build_df.index
    mode = str(mode or "").strip().lower()
    ops = _resolve_expression_ops(mode)
    unary_ops = list(ops.get("unary", []))
    binary_ops = list(ops.get("binary", []))
    flat_ops = [*unary_ops, *binary_ops]
    if not flat_ops:
        return data, registry, {
            "expression_registry_mode": mode,
            "expression_ops": flat_ops,
            "expression_ops_unary": unary_ops,
            "expression_ops_binary": binary_ops,
            "expression_max_new_features": int(max_new_features),
            "expression_max_base_features": int(max_base_features),
            "expression_max_pairs": int(max_pairs),
            "expression_min_nonmissing_count": int(min_nonmissing_count),
            "expression_min_variation_share": float(min_variation_share),
            "expression_min_nonmissing_share": float(min_nonmissing_share),
            "n_base_candidates_total": 0,
            "n_base_candidates_used": 0,
            "n_base_candidates_eligible": 0,
            "n_pair_candidates_total": 0,
            "n_pair_candidates_used": 0,
            "n_generated_features": 0,
            "n_registry_rows_after_augmentation": len(registry),
            "n_skipped_existing_expression_name": 0,
            "n_skipped_low_nonmissing_base": 0,
            "n_skipped_low_nonmissing_expression": 0,
            "n_skipped_degenerate_signature": 0,
            "n_skipped_low_within_event_variation": 0,
            "n_skipped_low_nonmissing_share": 0,
        }

    out_data = data.copy()
    out_registry = [dict(r) for r in registry]
    base_features: List[str] = []
    seen_base: set[str] = set()
    for row in out_registry:
        feat = str(row.get("feature_name", "")).strip()
        if not feat or feat in seen_base:
            continue
        if feat.startswith(("expr__", "cat__")):
            continue
        if int(row.get("allowed_in_scan", 0)) != 1:
            continue
        if feat not in out_data.columns:
            continue
        seen_base.add(feat)
        base_features.append(feat)

    generated_rows: List[Dict[str, object]] = []
    skipped_existing = 0
    skipped_low_nonmissing_base = 0
    skipped_low_nonmissing_expression = 0
    skipped_degenerate_signature = 0
    skipped_low_variation = 0
    skipped_low_nonmissing_share = 0
    generated_names: List[str] = []
    cap = int(max_new_features) if int(max_new_features) > 0 else 0
    base_total = len(base_features)
    if int(max_base_features) > 0:
        base_features = base_features[: int(max_base_features)]

    min_count = max(int(min_nonmissing_count), 1)
    min_var = float(min_variation_share)
    min_nonmiss = float(min_nonmissing_share)

    def _vector_signature(vec: pd.Series) -> str:
        arr = pd.to_numeric(vec, errors="coerce").to_numpy(dtype=np.float64, copy=True)
        arr[~np.isfinite(arr)] = np.nan
        nan_mask = np.isnan(arr)
        arr2 = arr.copy()
        arr2[nan_mask] = 0.0
        h = hashlib.sha256()
        h.update(nan_mask.tobytes())
        h.update(arr2.tobytes())
        return h.hexdigest()

    def _default_transform_for(vec_build: pd.Series) -> str:
        numeric = pd.to_numeric(vec_build, errors="coerce")
        uniq = int(numeric.dropna().nunique())
        return "none" if uniq <= 2 else "zscore_within_track"

    base_vectors: Dict[str, pd.Series] = {}
    base_vectors_build: Dict[str, pd.Series] = {}
    seen_signatures: set[str] = set()
    eligible_bases: List[str] = []
    for feat in base_features:
        base_vec = pd.to_numeric(out_data[feat], errors="coerce").replace([np.inf, -np.inf], np.nan)
        base_vec_build = pd.to_numeric(out_data.loc[build_idx, feat], errors="coerce").replace(
            [np.inf, -np.inf], np.nan
        )
        base_vectors[feat] = base_vec
        base_vectors_build[feat] = base_vec_build
        if int(base_vec_build.notna().sum()) < int(min_count):
            skipped_low_nonmissing_base += 1
            continue
        if int(base_vec_build.dropna().nunique()) <= 1:
            skipped_low_nonmissing_base += 1
            continue
        seen_signatures.add(_vector_signature(base_vec_build))
        eligible_bases.append(feat)

    def _append_expression(
        *,
        expr_name: str,
        expr_vec: pd.Series,
        op: str,
        formula: str,
        lhs: str,
        rhs: str | None = None,
    ) -> None:
        nonlocal skipped_existing
        nonlocal skipped_low_nonmissing_expression
        nonlocal skipped_degenerate_signature
        nonlocal skipped_low_variation
        nonlocal skipped_low_nonmissing_share

        if expr_name in out_data.columns:
            skipped_existing += 1
            return
        expr_num = pd.to_numeric(expr_vec, errors="coerce").replace([np.inf, -np.inf], np.nan)
        expr_num_build = pd.to_numeric(expr_num.reindex(build_idx), errors="coerce").replace(
            [np.inf, -np.inf], np.nan
        )
        if int(expr_num_build.notna().sum()) < int(min_count):
            skipped_low_nonmissing_expression += 1
            return
        if int(expr_num_build.dropna().nunique()) <= 1:
            skipped_low_nonmissing_expression += 1
            return
        signature = _vector_signature(expr_num_build)
        if signature in seen_signatures:
            skipped_degenerate_signature += 1
            return

        metric_df = build_df[["event_id"]].copy()
        metric_df[expr_name] = expr_num_build.astype(float)
        share_var, share_nonmiss, n_var_events, n_two_alt = within_event_variation_metrics(
            metric_df, feature_col=expr_name
        )
        if float(share_var) < float(min_var):
            skipped_low_variation += 1
            return
        if float(share_nonmiss) < float(min_nonmiss):
            skipped_low_nonmissing_share += 1
            return

        out_data[expr_name] = expr_num.astype(float)
        seen_signatures.add(signature)
        row: Dict[str, object] = {
            "feature_name": expr_name,
            "data_source": "derived_expression",
            "timing_label": "pre_treatment",
            "role": "key_factor_candidate",
            "allowed_in_scan": 1,
            "within_event_variation_expected": within_event_variation_label(float(share_var)),
            "share_events_with_variation": round(float(share_var), 6),
            "share_events_nonmissing": round(float(share_nonmiss), 6),
            "n_events_with_variation": int(n_var_events),
            "n_two_alt_events": int(n_two_alt),
            "transform": _default_transform_for(expr_num_build),
            "missing_policy": "drop",
            "block_reasons": [],
            "expression_op": op,
            "expression_input_feature": lhs,
            "expression_input_features": [lhs] if rhs is None else [lhs, rhs],
            "expression_formula": formula,
            "expression_generated": 1,
        }
        if rhs is not None:
            row["expression_input_feature_lhs"] = lhs
            row["expression_input_feature_rhs"] = rhs
        generated_rows.append(row)
        generated_names.append(expr_name)

    # Unary expression generation.
    for feat in eligible_bases:
        base_vec = base_vectors[feat]
        for op in unary_ops:
            if cap > 0 and len(generated_rows) >= cap:
                break
            expr_name = _expression_feature_name(op=op, lhs_feature=feat)
            if op == "signed_log1p":
                expr_vec = np.sign(base_vec) * np.log1p(np.abs(base_vec))
                formula = f"sign({feat})*log1p(abs({feat}))"
            elif op == "square":
                expr_vec = base_vec * base_vec
                formula = f"{feat}^2"
            else:
                continue
            _append_expression(
                expr_name=expr_name,
                expr_vec=expr_vec,
                op=op,
                formula=formula,
                lhs=feat,
            )
        if cap > 0 and len(generated_rows) >= cap:
            break

    # Binary expression generation (ms_benchmark_lite).
    pair_candidates: List[tuple[str, str]] = []
    pair_total = 0
    pair_used = 0
    if binary_ops and not (cap > 0 and len(generated_rows) >= cap):
        for i, lhs in enumerate(eligible_bases):
            for j in range(i + 1, len(eligible_bases)):
                rhs = eligible_bases[j]
                pair_candidates.append((lhs, rhs))
        pair_total = len(pair_candidates)
        if int(max_pairs) > 0:
            pair_candidates = pair_candidates[: int(max_pairs)]
        pair_used = len(pair_candidates)
        denom_eps = 1e-12
        for lhs, rhs in pair_candidates:
            if cap > 0 and len(generated_rows) >= cap:
                break
            lhs_vec = base_vectors[lhs]
            rhs_vec = base_vectors[rhs]
            for op in binary_ops:
                if cap > 0 and len(generated_rows) >= cap:
                    break
                expr_name = _expression_feature_name(op=op, lhs_feature=lhs, rhs_feature=rhs)
                if op == "ratio":
                    rhs_safe = rhs_vec.where(rhs_vec.abs() > denom_eps, np.nan)
                    expr_vec = lhs_vec / rhs_safe
                    formula = f"{lhs}/nullif({rhs},0)"
                elif op == "diff":
                    expr_vec = lhs_vec - rhs_vec
                    formula = f"{lhs}-{rhs}"
                elif op == "interaction":
                    expr_vec = lhs_vec * rhs_vec
                    formula = f"{lhs}*{rhs}"
                else:
                    continue
                _append_expression(
                    expr_name=expr_name,
                    expr_vec=expr_vec,
                    op=op,
                    formula=formula,
                    lhs=lhs,
                    rhs=rhs,
                )
            if cap > 0 and len(generated_rows) >= cap:
                break

    out_registry.extend(generated_rows)
    meta = {
        "expression_registry_mode": mode,
        "expression_ops": flat_ops,
        "expression_ops_unary": unary_ops,
        "expression_ops_binary": binary_ops,
        "expression_max_new_features": int(max_new_features),
        "expression_max_base_features": int(max_base_features),
        "expression_max_pairs": int(max_pairs),
        "expression_min_nonmissing_count": int(min_nonmissing_count),
        "expression_min_variation_share": float(min_var),
        "expression_min_nonmissing_share": float(min_nonmiss),
        "n_base_candidates_total": base_total,
        "n_base_candidates_used": len(base_features),
        "n_base_candidates_eligible": len(eligible_bases),
        "n_pair_candidates_total": pair_total,
        "n_pair_candidates_used": pair_used,
        "n_generated_features": len(generated_rows),
        "n_registry_rows_after_augmentation": len(out_registry),
        "n_skipped_existing_expression_name": int(skipped_existing),
        "n_skipped_low_nonmissing_base": int(skipped_low_nonmissing_base),
        "n_skipped_low_nonmissing_expression": int(skipped_low_nonmissing_expression),
        "n_skipped_degenerate_signature": int(skipped_degenerate_signature),
        "n_skipped_low_within_event_variation": int(skipped_low_variation),
        "n_skipped_low_nonmissing_share": int(skipped_low_nonmissing_share),
        "generated_feature_names": generated_names[:50],
    }
    return out_data, out_registry, meta


def _sanitize_level_token(value: object, *, max_len: int = 48) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        return "empty"
    if text[0].isdigit():
        text = f"v_{text}"
    max_len_int = max(int(max_len), 8)
    return text[:max_len_int]


def _augment_registry_with_categorical(
    *,
    data: pd.DataFrame,
    registry: List[Dict[str, object]],
    build_scope_df: pd.DataFrame,
    mode: str,
    max_levels_per_feature: int,
    min_level_count: int,
    max_new_features: int,
    min_variation_share: float,
    min_nonmissing_share: float,
) -> tuple[pd.DataFrame, List[Dict[str, object]], Dict[str, object]]:
    build_df = build_scope_df if isinstance(build_scope_df, pd.DataFrame) else data
    mode = str(mode or "").strip().lower()
    if mode != "onehot":
        return data, registry, {
            "categorical_encoding_mode": mode,
            "categorical_max_levels_per_feature": int(max_levels_per_feature),
            "categorical_min_level_count": int(min_level_count),
            "categorical_max_new_features": int(max_new_features),
            "categorical_min_variation_share": float(min_variation_share),
            "categorical_min_nonmissing_share": float(min_nonmissing_share),
            "n_categorical_source_features": 0,
            "n_generated_features": 0,
            "n_skipped_existing_name": 0,
            "n_skipped_low_level_count": 0,
            "n_skipped_numeric_like": 0,
            "n_skipped_not_categorical_dtype": 0,
            "n_skipped_not_pretreatment": 0,
            "n_skipped_identifier_or_outcome": 0,
        }

    out_data = data.copy()
    out_registry = [dict(r) for r in registry]
    cap = int(max_new_features) if int(max_new_features) > 0 else 0
    max_levels = int(max_levels_per_feature) if int(max_levels_per_feature) > 0 else 0
    min_count = max(int(min_level_count), 1)
    min_var = float(min_variation_share)
    min_nonmissing = float(min_nonmissing_share)

    source_features: List[str] = []
    skipped_not_categorical_dtype = 0
    skipped_not_pretreatment = 0
    skipped_identifier_or_outcome = 0
    for col in sorted(build_df.columns.astype(str).tolist()):
        feat = str(col).strip()
        if not feat:
            continue
        if feat.startswith(("expr__", "cat__")):
            continue
        if feat in IDENTIFIER_COLUMNS or is_outcome_like(feat):
            skipped_identifier_or_outcome += 1
            continue
        if feat not in out_data.columns:
            continue
        timing_label = str(classify_timing(feat)).strip()
        if timing_label != "pre_treatment":
            skipped_not_pretreatment += 1
            continue
        raw_build = build_df[feat]
        if pd.api.types.is_numeric_dtype(raw_build) or pd.api.types.is_bool_dtype(raw_build):
            skipped_not_categorical_dtype += 1
            continue
        source_features.append(feat)

    def _clean_values(raw: pd.Series) -> pd.Series:
        values = raw.astype(str).str.strip()
        missing_mask = values.eq("") | values.str.lower().isin({"na", "nan", "none", "null"})
        return values.where(~missing_mask, other=np.nan)

    generated_rows: List[Dict[str, object]] = []
    generated_names: List[str] = []
    skipped_existing = 0
    skipped_low_level = 0
    skipped_numeric_like = 0
    encoded_feature_levels: Dict[str, int] = {}

    for feat in source_features:
        if cap > 0 and len(generated_rows) >= cap:
            break
        raw_build = build_df[feat]
        numeric_probe = pd.to_numeric(raw_build, errors="coerce")
        numeric_like_ratio = float(numeric_probe.notna().sum()) / float(max(len(raw_build), 1))
        if numeric_like_ratio >= 0.98:
            skipped_numeric_like += 1
            continue

        values_build = _clean_values(raw_build)
        vc = values_build.value_counts(dropna=True)
        if vc.empty:
            continue

        level_items = [
            (str(level), int(cnt))
            for level, cnt in vc.items()
            if int(cnt) >= int(min_count)
        ]
        level_items.sort(key=lambda x: (-int(x[1]), str(x[0])))
        if max_levels > 0:
            level_items = level_items[:max_levels]
        if not level_items:
            skipped_low_level += 1
            continue

        values_full = _clean_values(out_data[feat])
        n_added_this_feature = 0
        for level, cnt in level_items:
            if cap > 0 and len(generated_rows) >= cap:
                break
            # Stable onehot token: sanitized + short hash (avoid collisions across similar labels).
            level_hash = hashlib.sha256(str(level).encode("utf-8")).hexdigest()[:8]
            suffix = f"_h{level_hash}"
            base = _sanitize_level_token(level, max_len=max(8, 48 - len(suffix)))
            token = f"{base}{suffix}"
            cat_name = f"cat__{feat}__{token}"
            if cat_name in out_data.columns:
                skipped_existing += 1
                continue

            vec = (values_full == level).astype(float)
            vec = vec.where(values_full.notna(), other=np.nan)
            nonmissing = int(vec.notna().sum())
            positives = int((vec == 1.0).sum())
            if nonmissing <= 0 or positives < min_count or positives >= nonmissing:
                skipped_low_level += 1
                continue

            # Build-scope within-event variation gates (align candidate pool with discovery-only registry build).
            vec_build = (values_build == level).astype(float)
            vec_build = vec_build.where(values_build.notna(), other=np.nan)
            metric_df = build_df[["event_id"]].copy()
            metric_df[cat_name] = vec_build.astype(float)
            share_var, share_nonmiss, n_var_events, n_two_alt = within_event_variation_metrics(
                metric_df, feature_col=cat_name
            )
            variation_expected = within_event_variation_label(float(share_var))
            block_reasons: List[str] = []
            timing_label = "pre_treatment"
            if float(share_var) < float(min_var):
                block_reasons.append("low_within_event_variation")
            if float(share_nonmiss) < float(min_nonmissing):
                block_reasons.append("low_nonmissing_share")
            allowed = 1 if not block_reasons else 0

            out_data[cat_name] = vec.astype(float)
            generated_rows.append(
                {
                    "feature_name": cat_name,
                    "data_source": "derived_categorical_onehot",
                    "timing_label": timing_label,
                    "role": "key_factor_candidate",
                    "allowed_in_scan": int(allowed),
                    "within_event_variation_expected": variation_expected,
                    "share_events_with_variation": round(float(share_var), 6),
                    "share_events_nonmissing": round(float(share_nonmiss), 6),
                    "n_events_with_variation": int(n_var_events),
                    "n_two_alt_events": int(n_two_alt),
                    "transform": "none",
                    "missing_policy": "drop",
                    "block_reasons": block_reasons,
                    "categorical_encoding_mode": "onehot",
                    "categorical_source_feature": feat,
                    "categorical_level_value": level,
                    "categorical_level_count": int(cnt),
                    "categorical_generated": 1,
                }
            )
            generated_names.append(cat_name)
            n_added_this_feature += 1
        if n_added_this_feature > 0:
            encoded_feature_levels[feat] = n_added_this_feature

    out_registry.extend(generated_rows)
    meta = {
        "categorical_encoding_mode": mode,
        "categorical_max_levels_per_feature": int(max_levels_per_feature),
        "categorical_min_level_count": int(min_level_count),
        "categorical_max_new_features": int(max_new_features),
        "categorical_min_variation_share": float(min_var),
        "categorical_min_nonmissing_share": float(min_nonmissing),
        "n_categorical_source_features": len(source_features),
        "n_generated_features": len(generated_rows),
        "n_skipped_existing_name": int(skipped_existing),
        "n_skipped_low_level_count": int(skipped_low_level),
        "n_skipped_numeric_like": int(skipped_numeric_like),
        "n_skipped_not_categorical_dtype": int(skipped_not_categorical_dtype),
        "n_skipped_not_pretreatment": int(skipped_not_pretreatment),
        "n_skipped_identifier_or_outcome": int(skipped_identifier_or_outcome),
        "encoded_feature_level_counts": encoded_feature_levels,
        "generated_feature_names": generated_names[:50],
    }
    return out_data, out_registry, meta


def _fmt_float(value: object, digits: int = 4) -> str:
    try:
        if value is None:
            return "NA"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "NA"


def _record_runtime_stage(
    *,
    stage_rows: List[Dict[str, object]],
    stage: str,
    started_at: float,
    **meta: object,
) -> int:
    elapsed_ms = int(round((perf_counter() - started_at) * 1000.0))
    row: Dict[str, object] = {"stage": str(stage), "elapsed_ms": int(elapsed_ms)}
    for key, value in meta.items():
        row[str(key)] = value
    stage_rows.append(row)
    return elapsed_ms


def _runtime_stage_log_row(
    *,
    run_id: str,
    timestamp: str,
    stage_row: Dict[str, object],
) -> Dict[str, object]:
    stage_name = str(stage_row.get("stage", "unknown"))
    elapsed_ms = stage_row.get("elapsed_ms")
    meta = {
        str(k): v
        for k, v in stage_row.items()
        if str(k) not in {"stage", "elapsed_ms"}
    }
    meta_json = json.dumps(meta, ensure_ascii=False, sort_keys=True) if meta else "{}"
    return {
        "run_id": run_id,
        "candidate_id": "",
        "track": "",
        "context_scope": "",
        "y_col": "",
        "split_role": "runtime",
        "spec_id": "",
        "key_factor": "",
        "status": "runtime_stage",
        "reason_stage": "runtime",
        "reason_code": f"runtime_stage::{stage_name}",
        "reason_detail": f"elapsed_ms={elapsed_ms}; meta={meta_json}",
        "bootstrap_success": None,
        "bootstrap_attempted": None,
        "candidate_elapsed_ms": elapsed_ms,
        "effective_n_bootstrap": None,
        "candidate_eval_order": None,
        "candidate_pool_size": None,
        "equivalence_hash": "",
        "validation_used_for_search": 0,
        "candidate_pool_locked_pre_validation": 1,
        "timestamp": timestamp,
    }


def _print_cli_summary(
    *,
    run_id: str,
    gate_meta: Dict[str, object],
    scan_rows: List[Dict[str, object]],
    top_rows: List[Dict[str, object]],
    out_scan_runs_csv: str,
    out_top_models_csv: str,
    out_top_models_inference_csv: str,
    out_run_summary_json: str,
    top_n: int,
    inference_top_rows: List[Dict[str, object]] | None = None,
) -> None:
    status_counts = _status_counts(scan_rows, "status")
    effective_top_rows = inference_top_rows if inference_top_rows else top_rows
    tier_counts = _status_counts(effective_top_rows, "candidate_tier")
    validated_rows = [r for r in effective_top_rows if str(r.get("candidate_tier", "")) == "validated_candidate"]
    validated_rows.sort(
        key=lambda r: (
            float(r.get("q_value_validation"))
            if r.get("q_value_validation") is not None
            else float("inf"),
            float(r.get("p_boot_validation"))
            if r.get("p_boot_validation") is not None
            else float("inf"),
        )
    )
    gate = gate_meta.get("validated_gate_effective", {}) if isinstance(gate_meta, dict) else {}
    print("[Phase-B KeyFactor Scan Summary]")
    print(f"run_id: {run_id}")
    print(f"scan_rows/top_rows: {len(scan_rows)}/{len(top_rows)}")
    if inference_top_rows is not None:
        print(f"inference_top_rows: {len(inference_top_rows)}")
    print(
        "candidate_tiers: "
        f"validated={tier_counts.get('validated_candidate', 0)}, "
        f"support={tier_counts.get('support_candidate', 0)}, "
        f"exploratory={tier_counts.get('exploratory', 0)}"
    )
    print(
        "validated_gate_effective: "
        f"docs={gate.get('min_policy_docs', 'NA')}, "
        f"events={gate.get('min_informative_events', 'NA')}, "
        f"feasible={gate.get('validation_policy_doc_gate_feasible', 'NA')}"
    )
    print(f"validated_gate_source: {gate_meta.get('validated_gate_source', 'NA')}")
    print(
        "status_counts: "
        + ", ".join(f"{k}={v}" for k, v in status_counts.items())
        if status_counts
        else "status_counts: none"
    )
    if validated_rows:
        limit = max(int(top_n), 1)
        print(f"top_validated_candidates (max {limit}):")
        for row in validated_rows[:limit]:
            print(
                "  "
                + f"{row.get('track', '')} | {row.get('context_scope', '')} | "
                + f"{row.get('y_col', '')} | "
                + f"{row.get('key_factor', '')} | "
                + f"beta_val={_fmt_float(row.get('beta_validation'))}, "
                + f"p_val={_fmt_float(row.get('p_boot_validation'))}, "
                + f"q_val={_fmt_float(row.get('q_value_validation'))}"
            )
    else:
        print("top_validated_candidates: none")
    print(f"output_scan_runs_csv: {out_scan_runs_csv}")
    print(f"output_top_models_csv: {out_top_models_csv}")
    if str(out_top_models_inference_csv).strip():
        print(f"output_top_models_inference_csv: {out_top_models_inference_csv}")
    print(f"output_run_summary_json: {out_run_summary_json}")


def main() -> int:
    run_started_at = perf_counter()
    runtime_stage_rows: List[Dict[str, object]] = []
    args = parse_args()
    ts = utc_timestamp()
    now_utc = datetime.now(timezone.utc)
    strict_lock_mode = bool(args.strict_lock_mode)
    confirmatory_y_cols_requested = _parse_csv_tokens(args.confirmatory_y_cols)
    y_time_window_years = _parse_positive_int_csv(
        args.y_time_window_years,
        field_name="y-time-window-years",
    )
    out_top_models_inference_csv = (
        str(args.out_top_models_inference_csv).strip()
        if str(args.out_top_models_inference_csv).strip()
        else _with_path_suffix(str(args.out_top_models_csv), "inference")
    )
    registry_max_features, scan_max_features, feature_cap_meta = _resolve_feature_caps(args)
    restart_seeds = _resolve_restart_seeds(args.bootstrap_seed, args.n_restarts, args.restart_seed_step)
    disk_space_checks: List[Dict[str, object]] = []
    base_output_paths = [
        args.out_scan_runs_csv,
        args.out_top_models_csv,
        out_top_models_inference_csv,
        args.out_search_log_jsonl,
        args.out_run_summary_json,
        args.out_feasibility_frontier_json,
        args.out_feature_registry_json,
        args.out_restart_stability_csv,
    ]
    stage_started_at = perf_counter()
    _ensure_output_dirs(
        base_output_paths
    )
    disk_space_checks.append(
        _check_disk_space_or_raise(
            probe_paths=base_output_paths,
            min_free_space_mb=int(args.min_free_space_mb),
            stage="startup_base_outputs",
        )
    )
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="startup_preflight",
        started_at=stage_started_at,
        n_output_paths=int(len(base_output_paths)),
        min_free_space_mb=int(args.min_free_space_mb),
    )

    dyad_path = Path(args.input_dyad_base_csv)
    ext_path = Path(args.input_extension_feature_csv) if args.input_extension_feature_csv else None
    pa_path = Path(args.input_phase_a_covariates_csv) if args.input_phase_a_covariates_csv else None

    stage_started_at = perf_counter()
    data, load_meta = load_and_prepare_data(
        dyad_base_csv=dyad_path,
        extension_feature_csv=ext_path if (ext_path and ext_path.exists()) else None,
        phase_a_covariates_csv=pa_path if (pa_path and pa_path.exists()) else None,
    )
    y_time_windows_meta: Dict[str, object] = {
        "enabled": bool(args.derive_y_time_windows),
        "years": y_time_window_years,
        "generated_cols": [],
        "generated": [],
    }
    if bool(args.derive_y_time_windows):
        data, y_time_windows_meta = _derive_y_time_window_outcomes(
            data,
            years=y_time_window_years,
        )
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="data_load_and_prepare",
        started_at=stage_started_at,
        n_rows=int(len(data)),
        n_columns=int(len(data.columns)),
        derive_y_time_windows=bool(args.derive_y_time_windows),
    )
    stage_started_at = perf_counter()
    split_csv_path = Path(args.input_policy_split_csv) if args.input_policy_split_csv else None
    if split_csv_path and split_csv_path.exists():
        data, split_meta = apply_policy_split_file(
            data,
            split_csv=split_csv_path,
            split_seed_fallback=args.split_seed,
            discovery_ratio_fallback=args.split_ratio,
            strict=strict_lock_mode,
        )
    else:
        if strict_lock_mode and args.input_policy_split_csv:
            raise ValueError(
                "strict lock mode requires an existing split CSV; missing: "
                f"{args.input_policy_split_csv}"
            )
        data, split_meta = assign_policy_document_holdout(
            data,
            seed=args.split_seed,
            discovery_ratio=args.split_ratio,
            method=args.split_method,
        )
    try:
        effective_split_seed = int(split_meta.get("split_seed", args.split_seed))
    except Exception:
        effective_split_seed = args.split_seed
    effective_split_method = str(split_meta.get("split_method", f"policy_document_holdout_{args.split_method}"))
    if split_meta.get("n_policy_docs_total", 0):
        try:
            effective_split_ratio = float(split_meta.get("n_policy_docs_discovery", 0)) / float(
                split_meta.get("n_policy_docs_total", 1)
            )
        except Exception:
            effective_split_ratio = float(args.split_ratio)
    else:
        effective_split_ratio = float(split_meta.get("discovery_ratio", args.split_ratio))
    split_integrity_meta = _validate_split_integrity(data, strict=strict_lock_mode)
    data, y_contexts, y_contexts_meta = _resolve_y_contexts(
        data=data,
        y_contexts_json=str(args.y_contexts_json),
        merge_mode=str(args.y_contexts_merge_mode),
    )
    validation_doc_capacity = int(split_integrity_meta.get("n_policy_docs_validation", 0))
    validation_share = float(1.0 - effective_split_ratio)
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="split_and_context_resolution",
        started_at=stage_started_at,
        split_method=effective_split_method,
        validation_doc_capacity=int(validation_doc_capacity),
        y_contexts_effective=int(len(y_contexts)),
    )

    stage_started_at = perf_counter()
    (
        min_informative_events_estimable,
        min_policy_docs_estimable,
        min_informative_events_validated_requested,
        min_policy_docs_validated_requested,
        gate_resolution_meta,
    ) = _resolve_stage_gate_thresholds(args)
    min_track_positive_events_precheck = int(args.time_series_min_track_positive_events)
    if min_track_positive_events_precheck <= 0:
        min_track_positive_events_precheck = int(min_informative_events_estimable)
    time_series_precheck_meta = _build_time_series_precheck(
        data=data,
        y_contexts=y_contexts,
        confirmatory_y_cols=confirmatory_y_cols_requested,
        min_positive_events=max(int(args.time_series_min_positive_events), 1),
        min_track_positive_events=max(int(min_track_positive_events_precheck), 1),
        min_positive_share=max(float(args.time_series_min_positive_share), 0.0),
    )
    time_series_precheck_policy = _apply_time_series_precheck_mode(
        mode=str(args.time_series_precheck_mode),
        precheck_meta=time_series_precheck_meta,
    )
    (
        confirmatory_y_cols_effective,
        confirmatory_policy_meta,
    ) = _resolve_effective_confirmatory_y_cols(
        requested_confirmatory_y_cols=confirmatory_y_cols_requested,
        y_contexts=y_contexts,
        precheck_meta=time_series_precheck_meta,
        auto_policy=str(args.time_series_auto_confirmatory_policy),
    )
    min_informative_events_validated = int(min_informative_events_validated_requested)
    min_policy_docs_validated = int(min_policy_docs_validated_requested)
    gate_meta: Dict[str, object] = {
        "gate_profile": args.gate_profile,
        "validated_gate_source": gate_resolution_meta["validated_gate_source"],
        "legacy_single_gate_sync_validation": gate_resolution_meta["legacy_single_gate_sync_validation"],
        "legacy_single_gate_overrides": gate_resolution_meta["legacy_single_gate_overrides"],
        "legacy_override_applied_to_estimable": gate_resolution_meta["legacy_override_applied_to_estimable"],
        "legacy_override_applied_to_validated": gate_resolution_meta["legacy_override_applied_to_validated"],
        "auto_scale_validated_gates": bool(args.auto_scale_validated_gates),
        "validation_share": validation_share,
        "validated_gate_original": {
            "min_informative_events": min_informative_events_validated_requested,
            "min_policy_docs": min_policy_docs_validated_requested,
        },
        "validated_gate_floor": {
            "min_informative_events": int(args.validated_gate_min_events_floor),
            "min_policy_docs": int(args.validated_gate_min_docs_floor),
        },
        "time_series_precheck_policy": time_series_precheck_policy,
        "time_series_auto_confirmatory_policy": str(args.time_series_auto_confirmatory_policy),
        "time_series_confirmatory_policy_meta": confirmatory_policy_meta,
    }
    if args.auto_scale_validated_gates:
        scaled_events = int(round(min_informative_events_validated * max(validation_share, 0.0)))
        scaled_docs = int(round(min_policy_docs_validated * max(validation_share, 0.0)))
        min_informative_events_validated = min(
            min_informative_events_validated,
            max(int(args.validated_gate_min_events_floor), scaled_events),
        )
        min_policy_docs_validated = min(
            min_policy_docs_validated,
            max(int(args.validated_gate_min_docs_floor), scaled_docs),
        )
    gate_meta["validated_gate_after_autoscale"] = {
        "min_informative_events": int(min_informative_events_validated),
        "min_policy_docs": int(min_policy_docs_validated),
    }
    validation_track_capacity = _validation_capacity_by_track(data)
    validation_totals = _validation_capacity_totals(data)
    validation_track_capacity_stats = _track_capacity_stats(validation_track_capacity)
    profile_meta: Dict[str, object] = {
        "gate_profile": args.gate_profile,
        "validated_gate_before_profile": {
            "min_informative_events": int(min_informative_events_validated),
            "min_policy_docs": int(min_policy_docs_validated),
        },
        "profile_adjustments": [],
    }
    if args.gate_profile == "feasibility_diagnostic":
        cap_docs = int(validation_track_capacity_stats.get("min_track_policy_docs_validation", 0))
        cap_events = int(validation_track_capacity_stats.get("min_track_events_validation", 0))
        if cap_docs > 0 and min_policy_docs_validated > cap_docs:
            profile_meta["profile_adjustments"].append(
                f"min_policy_docs_validated capped to track capacity {cap_docs}"
            )
            min_policy_docs_validated = cap_docs
        if cap_events > 0 and min_informative_events_validated > cap_events:
            profile_meta["profile_adjustments"].append(
                f"min_informative_events_validated capped to track capacity {cap_events}"
            )
            min_informative_events_validated = cap_events
    elif args.gate_profile == "adaptive_production":
        cap_docs = int(validation_track_capacity_stats.get("min_track_policy_docs_validation", 0))
        cap_events = int(validation_track_capacity_stats.get("min_track_events_validation", 0))
        doc_ratio = float(args.validated_gate_adaptive_doc_ratio)
        event_ratio = float(args.validated_gate_adaptive_event_ratio)
        doc_ratio = min(max(doc_ratio, 0.0), 1.0)
        event_ratio = min(max(event_ratio, 0.0), 1.0)
        rounding_mode = str(args.validated_gate_adaptive_rounding)
        profile_meta["adaptive_params"] = {
            "doc_ratio": doc_ratio,
            "event_ratio": event_ratio,
            "rounding_mode": rounding_mode,
        }
        if cap_docs > 0:
            candidate_docs = _round_by_mode(float(cap_docs) * doc_ratio, mode=rounding_mode)
            candidate_docs = max(int(args.validated_gate_min_docs_floor), candidate_docs)
            candidate_docs = min(int(min_policy_docs_validated), int(cap_docs), int(candidate_docs))
            if candidate_docs != int(min_policy_docs_validated):
                profile_meta["profile_adjustments"].append(
                    "adaptive docs gate "
                    f"{int(min_policy_docs_validated)} -> {int(candidate_docs)} "
                    f"(cap={cap_docs}, ratio={doc_ratio:.3f})"
                )
                min_policy_docs_validated = int(candidate_docs)
        if cap_events > 0:
            candidate_events = _round_by_mode(float(cap_events) * event_ratio, mode=rounding_mode)
            candidate_events = max(int(args.validated_gate_min_events_floor), candidate_events)
            candidate_events = min(int(min_informative_events_validated), int(cap_events), int(candidate_events))
            if candidate_events != int(min_informative_events_validated):
                profile_meta["profile_adjustments"].append(
                    "adaptive events gate "
                    f"{int(min_informative_events_validated)} -> {int(candidate_events)} "
                    f"(cap={cap_events}, ratio={event_ratio:.3f})"
                )
                min_informative_events_validated = int(candidate_events)
    profile_meta["validated_gate_after_profile"] = {
        "min_informative_events": int(min_informative_events_validated),
        "min_policy_docs": int(min_policy_docs_validated),
    }
    y_validated_events_by_col, y_validated_docs_by_col, y_validated_gate_meta = _build_y_validated_gate_map(
        data=data,
        y_contexts=y_contexts,
        min_events_requested=int(min_informative_events_validated),
        min_docs_requested=int(min_policy_docs_validated),
        auto_scale=bool(args.auto_scale_y_validated_gates),
        min_events_floor=int(args.y_validated_gate_min_events_floor),
        min_docs_floor=int(args.y_validated_gate_min_docs_floor),
        event_ratio=float(args.y_validated_gate_adaptive_event_ratio),
        doc_ratio=float(args.y_validated_gate_adaptive_doc_ratio),
        rounding_mode=str(args.y_validated_gate_adaptive_rounding),
    )
    gate_meta["profile_meta"] = profile_meta
    gate_meta["validated_gate_effective"] = {
        "min_informative_events": int(min_informative_events_validated),
        "min_policy_docs": int(min_policy_docs_validated),
        "validation_doc_capacity": validation_doc_capacity,
        "validation_event_capacity": int(validation_totals.get("n_events_validation_total", 0)),
        "validation_policy_doc_gate_feasible": validation_doc_capacity >= int(min_policy_docs_validated),
    }
    gate_meta["y_validated_gate_meta"] = y_validated_gate_meta
    gate_meta["y_validated_gate_effective_by_y"] = {
        y_col: {
            "min_informative_events": int(y_validated_events_by_col.get(y_col, min_informative_events_validated)),
            "min_policy_docs": int(y_validated_docs_by_col.get(y_col, min_policy_docs_validated)),
        }
        for _, y_col in y_contexts
    }
    y_feasibility_mode = str(args.y_feasibility_mode)
    gate_meta["y_feasibility_mode"] = y_feasibility_mode
    y_feasibility_out = _apply_y_feasibility_mode(
        mode=y_feasibility_mode,
        y_validated_gate_meta=y_validated_gate_meta,
    )
    gate_meta["y_unusable_y_cols"] = y_feasibility_out["y_unusable_y_cols"]
    gate_meta["y_below_floor_y_cols"] = y_feasibility_out["y_below_floor_y_cols"]
    gate_meta["validation_track_capacity"] = validation_track_capacity
    gate_meta["validation_totals"] = validation_totals
    gate_meta["validation_track_capacity_stats"] = validation_track_capacity_stats
    gate_meta["strict_lock_mode"] = int(strict_lock_mode)
    infeasible_tracks = _collect_track_gate_infeasibility(
        validation_track_capacity,
        min_policy_docs_gate=int(min_policy_docs_validated),
        min_events_gate=int(min_informative_events_validated),
    )
    gate_meta["strict_infeasible_tracks"] = infeasible_tracks
    frontier_payload = _build_feasibility_frontier_payload(
        run_id=args.run_id,
        timestamp=ts,
        strict_lock_mode=strict_lock_mode,
        gate_profile=str(args.gate_profile),
        validation_share=validation_share,
        validation_totals=validation_totals,
        validation_track_capacity=validation_track_capacity,
        min_policy_docs_requested=int(min_policy_docs_validated_requested),
        min_events_requested=int(min_informative_events_validated_requested),
        min_policy_docs_effective=int(min_policy_docs_validated),
        min_events_effective=int(min_informative_events_validated),
    )
    if str(args.out_feasibility_frontier_json).strip():
        write_json(Path(args.out_feasibility_frontier_json), frontier_payload)
    if strict_lock_mode:
        if infeasible_tracks:
            raise ValueError(
                "strict validation lock failed (track-level infeasible gates): " + "; ".join(infeasible_tracks)
            )
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="gate_resolution",
        started_at=stage_started_at,
        validated_gate_events=int(min_informative_events_validated),
        validated_gate_policy_docs=int(min_policy_docs_validated),
        infeasible_track_count=int(len(infeasible_tracks)),
    )

    stage_started_at = perf_counter()
    registry_input = Path(args.input_feature_registry_json) if args.input_feature_registry_json else None
    registry_build_scope = "discovery" if strict_lock_mode else str(args.registry_build_scope)
    if registry_build_scope not in {"discovery", "all"}:
        raise ValueError(f"unsupported registry_build_scope: {registry_build_scope}")
    registry_build_df = (
        data[data["split_role"] == "discovery"].copy()
        if registry_build_scope == "discovery" and "split_role" in data.columns
        else data
    )
    if registry_input and registry_input.exists():
        registry = _normalize_registry_feature_names(load_feature_registry(registry_input))
        registry_source = str(registry_input)
    else:
        registry = build_feature_registry(
            registry_build_df,
            min_variation_share=args.registry_min_variation_share,
            min_nonmissing_share=args.registry_min_nonmissing_share,
            max_features=registry_max_features,
        )
        registry = _normalize_registry_feature_names(registry)
        registry_source = "generated"
    data, registry, expression_meta = _augment_registry_with_expressions(
        data=data,
        registry=registry,
        build_scope_df=registry_build_df,
        mode=str(args.expression_registry_mode),
        max_new_features=int(args.expression_max_new_features),
        max_base_features=int(args.expression_max_base_features),
        max_pairs=int(args.expression_max_pairs),
        min_nonmissing_count=int(args.expression_min_nonmissing_count),
        min_variation_share=float(args.registry_min_variation_share),
        min_nonmissing_share=float(args.registry_min_nonmissing_share),
    )
    data, registry, categorical_meta = _augment_registry_with_categorical(
        data=data,
        registry=registry,
        build_scope_df=registry_build_df,
        mode=str(args.categorical_encoding_mode),
        max_levels_per_feature=int(args.categorical_max_levels_per_feature),
        min_level_count=int(args.categorical_min_level_count),
        max_new_features=int(args.categorical_max_new_features),
        min_variation_share=float(args.registry_min_variation_share),
        min_nonmissing_share=float(args.registry_min_nonmissing_share),
    )
    registry_filter_df = (
        data[data["split_role"] == "discovery"].copy()
        if registry_build_scope == "discovery" and "split_role" in data.columns
        else data
    )
    registry, registry_filter_summary = _filter_registry_by_data(registry, registry_filter_df)
    registry, family_dedupe_meta = _apply_scan_family_dedupe(
        registry,
        mode=str(args.scan_family_dedupe_mode),
    )
    registry_filter_summary["scan_family_dedupe_mode"] = str(args.scan_family_dedupe_mode)
    registry_filter_summary["n_allowed_after_family_dedupe"] = int(family_dedupe_meta.get("n_allowed_after", 0))
    registry_filter_summary["n_dropped_family_duplicates"] = int(
        family_dedupe_meta.get("n_dropped_family_duplicates", 0)
    )
    base_controls_used, controls_meta = _resolve_base_controls(
        data,
        include_base_controls=bool(args.include_base_controls),
        base_controls_override=str(args.base_controls),
        strict=bool(args.base_controls_strict),
    )
    include_base_controls_effective = bool(args.include_base_controls and len(base_controls_used) > 0)
    controls_meta["include_base_controls_requested"] = bool(args.include_base_controls)
    controls_meta["include_base_controls_effective"] = include_base_controls_effective
    control_spec_mode = str(args.control_spec_mode or "both").strip().lower()
    if control_spec_mode == "key_plus_base_controls" and not include_base_controls_effective:
        raise ValueError(
            "control-spec-mode=key_plus_base_controls requires base controls, but "
            f"include_base_controls_effective={include_base_controls_effective}; "
            f"missing={controls_meta.get('missing_base_controls')}"
        )
    candidate_pool_meta = _estimate_candidate_pool_size(
        data=data,
        registry=registry,
        include_base_controls_effective=include_base_controls_effective,
        scan_max_features=scan_max_features,
        n_contexts=len(y_contexts),
    )
    complexity_penalty_effective, complexity_penalty_meta = _resolve_complexity_penalty(
        requested_penalty=args.complexity_penalty,
        auto_mode=bool(args.complexity_penalty_auto),
        auto_min=float(args.complexity_penalty_auto_min),
        auto_max=float(args.complexity_penalty_auto_max),
        data=data,
        candidate_pool_meta=candidate_pool_meta,
    )

    out_registry_path = Path(args.out_feature_registry_json)
    registry_payload: Dict[str, object] = {
        "meta": {
            "run_id": args.run_id,
            "timestamp": ts,
            "source": registry_source,
            "strict_lock_mode": int(strict_lock_mode),
            "registry_build_scope": registry_build_scope,
            "registry_min_variation_share": args.registry_min_variation_share,
            "registry_min_nonmissing_share": args.registry_min_nonmissing_share,
            "registry_max_features_effective": registry_max_features,
            "registry_build_rows": int(len(registry_build_df)),
            "registry_rows_total": len(registry),
            "registry_filter_summary": registry_filter_summary,
            "family_dedupe_meta": family_dedupe_meta,
            "expression_meta": expression_meta,
            "categorical_meta": categorical_meta,
        },
        "feature_registry": registry,
    }
    write_json(out_registry_path, registry_payload)
    feature_registry_hash = sha256_json(registry_payload)
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="registry_build_and_write",
        started_at=stage_started_at,
        registry_source=registry_source,
        registry_rows_total=int(len(registry)),
    )

    stage_started_at = perf_counter()
    data_fingerprint: Dict[str, object] = {
        "inputs": load_meta.get("inputs", {}),
        "merge_report": load_meta.get("merge_report", {}),
        "split_meta": split_meta,
        "split_integrity": split_integrity_meta,
        "n_rows": int(len(data)),
        "n_columns": int(len(data.columns)),
        "columns_sorted": sorted(data.columns.astype(str).tolist()),
    }
    composite_data_hash = sha256_json(data_fingerprint)
    runner_script_path = Path(__file__).resolve()
    try:
        runner_script_ref = str(runner_script_path.relative_to(ROOT))
    except Exception:
        runner_script_ref = str(runner_script_path)
    runner_script_sha256 = _sha256_file(runner_script_path)

    config_payload: Dict[str, object] = {
        "run_id": args.run_id,
        "analysis_phase": "phase_b_bikard_machine_scientist_scan",
        "timezone": "UTC",
        "strict_lock_mode": int(strict_lock_mode),
        "gate_profile": args.gate_profile,
        "runner_script": runner_script_ref,
        "runner_script_sha256": runner_script_sha256,
        "min_free_space_mb": int(args.min_free_space_mb),
        "out_restart_stability_csv": str(args.out_restart_stability_csv),
        "out_top_models_inference_csv": str(out_top_models_inference_csv),
        "input_dyad_base_csv": str(dyad_path),
        "input_extension_feature_csv": str(ext_path) if ext_path else "",
        "input_phase_a_covariates_csv": str(pa_path) if pa_path else "",
        "input_feature_registry_json": str(registry_input) if registry_input else "",
        "input_policy_split_csv": str(split_csv_path) if (split_csv_path and split_csv_path.exists()) else "",
        "split_method": effective_split_method,
        "split_seed": effective_split_seed,
        "split_ratio": effective_split_ratio,
        "split_integrity_meta": split_integrity_meta,
        "bootstrap_seed": args.bootstrap_seed,
        "bootstrap_seed_base": args.bootstrap_seed,
        "bootstrap_seed_restart_seeds": restart_seeds,
        "n_restarts": args.n_restarts,
        "restart_seed_step": args.restart_seed_step,
        "n_bootstrap": args.n_bootstrap,
        "bootstrap_cluster_unit": args.bootstrap_cluster_unit,
        "optimizer_mode": str(args.optimizer_mode),
        "optimizer_adam_max_iter": int(args.optimizer_adam_max_iter),
        "optimizer_adam_learning_rate": float(args.optimizer_adam_learning_rate),
        "optimizer_adam_beta1": float(args.optimizer_adam_beta1),
        "optimizer_adam_beta2": float(args.optimizer_adam_beta2),
        "optimizer_adam_eps": float(args.optimizer_adam_eps),
        "optimizer_adam_l2": float(args.optimizer_adam_l2),
        "optimizer_adam_min_iter": int(args.optimizer_adam_min_iter),
        "optimizer_adam_tol": float(args.optimizer_adam_tol),
        "min_informative_events_estimable": min_informative_events_estimable,
        "min_policy_docs_informative_estimable": min_policy_docs_estimable,
        "min_informative_events_validated": min_informative_events_validated,
        "min_policy_docs_informative_validated": min_policy_docs_validated,
        "validated_gate_source": gate_resolution_meta["validated_gate_source"],
        "legacy_single_gate_sync_validation": bool(args.legacy_single_gate_sync_validation),
        "legacy_single_gate_overrides": gate_resolution_meta["legacy_single_gate_overrides"],
        "validated_gate_effective_by_y_events": y_validated_events_by_col,
        "validated_gate_effective_by_y_policy_docs": y_validated_docs_by_col,
        "y_validated_gate_meta": y_validated_gate_meta,
        "auto_scale_y_validated_gates": bool(args.auto_scale_y_validated_gates),
        "y_validated_gate_min_events_floor": int(args.y_validated_gate_min_events_floor),
        "y_validated_gate_min_docs_floor": int(args.y_validated_gate_min_docs_floor),
        "y_validated_gate_adaptive_event_ratio": float(args.y_validated_gate_adaptive_event_ratio),
        "y_validated_gate_adaptive_doc_ratio": float(args.y_validated_gate_adaptive_doc_ratio),
        "y_validated_gate_adaptive_rounding": str(args.y_validated_gate_adaptive_rounding),
        "gate_meta": gate_meta,
        "feasibility_frontier_summary": frontier_payload.get("summary", {}),
        "max_top1_policy_doc_share": args.max_top1_policy_doc_share,
        "bootstrap_success_min_ratio": args.bootstrap_success_min_ratio,
        "q_threshold": args.q_threshold,
        "p_threshold": args.p_threshold,
        "complexity_penalty_requested": args.complexity_penalty,
        "complexity_penalty_effective": complexity_penalty_effective,
        "complexity_penalty_meta": complexity_penalty_meta,
        "include_base_controls_requested": bool(args.include_base_controls),
        "include_base_controls_effective": include_base_controls_effective,
        "control_spec_mode": control_spec_mode,
        "controls_meta": controls_meta,
        "candidate_pool_meta": candidate_pool_meta,
        "y_contexts_json": str(args.y_contexts_json),
        "y_contexts_merge_mode": str(args.y_contexts_merge_mode),
        "derive_y_time_windows": bool(args.derive_y_time_windows),
        "y_time_window_years": y_time_window_years,
        "y_time_windows_meta": y_time_windows_meta,
        "time_series_precheck_mode": str(args.time_series_precheck_mode),
        "time_series_auto_confirmatory_policy": str(args.time_series_auto_confirmatory_policy),
        "time_series_min_positive_events": int(args.time_series_min_positive_events),
        "time_series_min_track_positive_events": int(min_track_positive_events_precheck),
        "time_series_min_positive_share": float(args.time_series_min_positive_share),
        "time_series_precheck_meta": time_series_precheck_meta,
        "time_series_precheck_policy": time_series_precheck_policy,
        "time_series_confirmatory_policy_meta": confirmatory_policy_meta,
        "confirmatory_y_cols_requested": confirmatory_y_cols_requested,
        "confirmatory_y_cols_effective": confirmatory_y_cols_effective,
        "y_feasibility_mode": str(args.y_feasibility_mode),
        "skip_discovery_infeasible_track_y": bool(args.skip_discovery_infeasible_track_y),
        "auto_disable_base_controls_low_capacity": bool(args.auto_disable_base_controls_low_capacity),
        "base_controls_min_events_per_exog": int(args.base_controls_min_events_per_exog),
        "base_controls_min_policy_docs_per_exog": int(args.base_controls_min_policy_docs_per_exog),
        "y_contexts_effective": [
            {"context_scope": context_scope, "y_col": y_col} for context_scope, y_col in y_contexts
        ],
        "y_contexts_meta": y_contexts_meta,
        "legacy_max_features": args.max_features,
        "registry_max_features_effective": registry_max_features,
        "scan_max_features_effective": scan_max_features,
        "scan_family_dedupe_mode": str(args.scan_family_dedupe_mode),
        "family_dedupe_meta": family_dedupe_meta,
        "feature_cap_meta": feature_cap_meta,
        "registry_source": registry_source,
        "registry_build_scope": registry_build_scope,
        "registry_build_rows": int(len(registry_build_df)),
        "registry_filter_rows": int(len(registry_filter_df)),
        "registry_min_variation_share": args.registry_min_variation_share,
        "registry_min_nonmissing_share": args.registry_min_nonmissing_share,
        "expression_registry_mode": args.expression_registry_mode,
        "expression_max_new_features": int(args.expression_max_new_features),
        "expression_max_base_features": int(args.expression_max_base_features),
        "expression_max_pairs": int(args.expression_max_pairs),
        "expression_min_nonmissing_count": int(args.expression_min_nonmissing_count),
        "expression_meta": expression_meta,
        "categorical_encoding_mode": args.categorical_encoding_mode,
        "categorical_max_levels_per_feature": int(args.categorical_max_levels_per_feature),
        "categorical_min_level_count": int(args.categorical_min_level_count),
        "categorical_max_new_features": int(args.categorical_max_new_features),
        "categorical_meta": categorical_meta,
        "refinement_options": {
            "auto_refine_shortlist": bool(args.auto_refine_shortlist),
            "refine_tier_mode": str(args.refine_tier_mode),
            "refine_max_features": int(args.refine_max_features),
            "refine_dedupe_mode": str(args.refine_dedupe_mode),
            "refine_n_bootstrap": int(args.refine_n_bootstrap),
            "refine_run_id_suffix": str(args.refine_run_id_suffix),
        },
        "bootstrap_escalation_options": {
            "auto_bootstrap_escalation": bool(args.auto_bootstrap_escalation),
            "escalation_n_bootstrap": int(args.escalation_n_bootstrap),
            "escalation_max_candidates": int(args.escalation_max_candidates),
            "escalation_p_margin": float(args.escalation_p_margin),
            "escalation_q_margin": float(args.escalation_q_margin),
            "escalation_dedupe_mode": str(args.escalation_dedupe_mode),
            "escalation_run_id_suffix": str(args.escalation_run_id_suffix),
        },
        "track_consensus_options": {
            "enforce_track_consensus": bool(args.enforce_track_consensus),
            "consensus_anchor_track": str(args.consensus_anchor_track),
            "consensus_min_anchor_tier": str(args.consensus_min_anchor_tier),
        },
        "restart_inference_options": {
            "aggregation_mode": "median_p_validation_ok",
            "confirmatory_y_cols_requested": confirmatory_y_cols_requested,
            "confirmatory_y_cols_effective": confirmatory_y_cols_effective,
            "nonconfirmatory_max_tier": str(args.nonconfirmatory_max_tier),
        },
        "registry_filter_summary": registry_filter_summary,
        "inputs": load_meta.get("inputs", {}),
        "merge_report": load_meta.get("merge_report", {}),
        "split_meta": split_meta,
        "data_fingerprint": data_fingerprint,
        "data_fingerprint_hash": composite_data_hash,
        "validation_used_for_search": False,
        "candidate_pool_locked_pre_validation": True,
        "search_governance": {
            "validation_used_for_search": False,
            "candidate_pool_locked_pre_validation": True,
            "validation_stage_policy": "post_search_on_fixed_candidate_pool",
        },
    }
    config_hash = sha256_json(config_payload)
    git_commit, git_commit_source = _resolve_git_commit_for_lock(strict=strict_lock_mode)
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="config_and_hash_resolution",
        started_at=stage_started_at,
        n_restart_seeds=int(len(restart_seeds)),
    )
    scan_rows: List[Dict[str, object]] = []
    top_rows: List[Dict[str, object]] = []
    search_log_rows: List[Dict[str, object]] = []
    restart_run_summaries: List[Dict[str, object]] = []

    stage_started_at = perf_counter()
    for restart_id, restart_seed in enumerate(restart_seeds, start=1):
        scan_config = ScanConfig(
            run_id=args.run_id,
            bootstrap_seed=restart_seed,
            n_bootstrap=args.n_bootstrap,
            bootstrap_cluster_unit=args.bootstrap_cluster_unit,
            optimizer_mode=str(args.optimizer_mode),
            optimizer_adam_max_iter=int(args.optimizer_adam_max_iter),
            optimizer_adam_learning_rate=float(args.optimizer_adam_learning_rate),
            optimizer_adam_beta1=float(args.optimizer_adam_beta1),
            optimizer_adam_beta2=float(args.optimizer_adam_beta2),
            optimizer_adam_eps=float(args.optimizer_adam_eps),
            optimizer_adam_l2=float(args.optimizer_adam_l2),
            optimizer_adam_min_iter=int(args.optimizer_adam_min_iter),
            optimizer_adam_tol=float(args.optimizer_adam_tol),
            min_informative_events_estimable=min_informative_events_estimable,
            min_policy_docs_informative_estimable=min_policy_docs_estimable,
            min_informative_events_validated=min_informative_events_validated,
            min_policy_docs_informative_validated=min_policy_docs_validated,
            max_top1_policy_doc_share=args.max_top1_policy_doc_share,
            bootstrap_success_min_ratio=args.bootstrap_success_min_ratio,
            q_threshold=args.q_threshold,
            p_threshold=args.p_threshold,
            complexity_penalty=complexity_penalty_effective,
            include_base_controls=include_base_controls_effective,
            base_controls=tuple(base_controls_used),
            control_spec_mode=control_spec_mode,
            contexts=tuple(y_contexts),
            validated_min_informative_events_by_y=dict(y_validated_events_by_col),
            validated_min_policy_docs_by_y=dict(y_validated_docs_by_col),
            max_features=scan_max_features,
            data_hash=composite_data_hash,
            config_hash=config_hash,
            feature_registry_hash=feature_registry_hash,
            git_commit=git_commit,
            timestamp=ts,
            validation_used_for_search=False,
            candidate_pool_locked_pre_validation=True,
            skip_discovery_infeasible_track_y=bool(args.skip_discovery_infeasible_track_y),
            auto_disable_base_controls_low_capacity=bool(args.auto_disable_base_controls_low_capacity),
            base_controls_min_events_per_exog=int(args.base_controls_min_events_per_exog),
            base_controls_min_policy_docs_per_exog=int(args.base_controls_min_policy_docs_per_exog),
        )
        rows_r, top_r, log_r = run_key_factor_scan(
            df=data,
            feature_registry=registry,
            config=scan_config,
        )
        for row in rows_r:
            row["restart_id"] = restart_id
            row["restart_bootstrap_seed"] = restart_seed
        for row in top_r:
            row["restart_id"] = restart_id
            row["restart_bootstrap_seed"] = restart_seed
        for row in log_r:
            row["restart_id"] = restart_id
            row["restart_bootstrap_seed"] = restart_seed
        scan_rows.extend(rows_r)
        top_rows.extend(top_r)
        search_log_rows.extend(log_r)
        restart_run_summaries.append(
            {
                "restart_id": restart_id,
                "restart_bootstrap_seed": restart_seed,
                "scan_rows": len(rows_r),
                "top_rows": len(top_r),
                "status_scan_rows": _status_counts(rows_r, "status"),
                "candidate_tier_top_rows": _status_counts(top_r, "candidate_tier"),
            }
        )
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="scan_execution_main",
        started_at=stage_started_at,
        scan_rows=int(len(scan_rows)),
        top_rows=int(len(top_rows)),
        search_log_rows=int(len(search_log_rows)),
    )

    scan_fields = [
        "run_id",
        "restart_id",
        "restart_bootstrap_seed",
        "track",
        "context_scope",
        "y_col",
        "split_id",
        "split_role",
        "spec_id",
        "key_factor",
        "control_set",
        "fdr_family_id",
        "candidate_id",
        "n_choice_situations_total",
        "n_single_choice_informative",
        "n_policy_docs_informative",
        "n_twin_sets",
        "beta",
        "ci95_lower",
        "ci95_upper",
        "se_boot",
        "p_boot",
        "q_value",
        "score",
        "bootstrap_cluster_unit",
        "bootstrap_success",
        "bootstrap_attempted",
        "candidate_elapsed_ms",
        "effective_n_bootstrap",
        "status",
        "reason_code",
        "reason_detail",
        "reason_stage",
        "candidate_eval_order",
        "candidate_pool_size",
        "equivalence_hash",
        "top1_policy_doc_event_share",
        "candidate_tier",
        "candidate_tier_raw",
        "track_consensus_signature",
        "track_consensus_anchor_track",
        "track_consensus_anchor_tier",
        "track_consensus_min_anchor_tier",
        "track_consensus_anchor_pass",
        "track_consensus_status",
        "track_consensus_enforced",
        "track_consensus_demoted",
        "support_only",
        "not_replacing_confirmatory_claim",
        "validation_used_for_search",
        "candidate_pool_locked_pre_validation",
        "data_hash",
        "config_hash",
        "feature_registry_hash",
        "git_commit",
        "timestamp",
    ]
    top_fields = [
        "run_id",
        "restart_id",
        "restart_bootstrap_seed",
        "candidate_id",
        "track",
        "context_scope",
        "y_col",
        "spec_id",
        "key_factor",
        "control_set",
        "fdr_family_id",
        "status_discovery",
        "status_validation",
        "p_boot_discovery",
        "p_boot_validation",
        "q_value_validation",
        "candidate_elapsed_ms_discovery",
        "candidate_elapsed_ms_validation",
        "effective_n_bootstrap_discovery",
        "effective_n_bootstrap_validation",
        "beta_discovery",
        "beta_validation",
        "score_discovery",
        "score_validation",
        "validation_pass_p_raw",
        "validation_pass_q_raw",
        "validation_pass_p",
        "validation_pass_q",
        "candidate_tier",
        "candidate_tier_raw",
        "confirmatory_eligible",
        "confirmatory_policy_demoted",
        "confirmatory_policy_reason",
        "track_consensus_signature",
        "track_consensus_anchor_track",
        "track_consensus_anchor_tier",
        "track_consensus_min_anchor_tier",
        "track_consensus_anchor_pass",
        "track_consensus_status",
        "track_consensus_enforced",
        "track_consensus_demoted",
        "candidate_pool_size",
        "equivalence_hash",
        "candidate_eval_order_discovery",
        "candidate_eval_order_validation",
        "support_only",
        "not_replacing_confirmatory_claim",
        "validation_used_for_search",
        "candidate_pool_locked_pre_validation",
        "data_hash",
        "config_hash",
        "feature_registry_hash",
        "git_commit",
        "timestamp",
    ]
    top_inference_fields = [
        "run_id",
        "candidate_id",
        "track",
        "context_scope",
        "y_col",
        "spec_id",
        "key_factor",
        "control_set",
        "fdr_family_id",
        "status_discovery",
        "status_validation",
        "p_boot_discovery",
        "p_boot_validation",
        "q_value_validation",
        "candidate_elapsed_ms_discovery",
        "candidate_elapsed_ms_validation",
        "effective_n_bootstrap_discovery",
        "effective_n_bootstrap_validation",
        "beta_discovery",
        "beta_validation",
        "score_discovery",
        "score_validation",
        "validation_pass_p_raw",
        "validation_pass_q_raw",
        "validation_pass_p",
        "validation_pass_q",
        "candidate_tier",
        "candidate_tier_raw",
        "confirmatory_eligible",
        "confirmatory_policy_demoted",
        "confirmatory_policy_reason",
        "track_consensus_signature",
        "track_consensus_anchor_track",
        "track_consensus_anchor_tier",
        "track_consensus_min_anchor_tier",
        "track_consensus_anchor_pass",
        "track_consensus_status",
        "track_consensus_enforced",
        "track_consensus_demoted",
        "candidate_pool_size",
        "equivalence_hash",
        "support_only",
        "not_replacing_confirmatory_claim",
        "validation_used_for_search",
        "candidate_pool_locked_pre_validation",
        "data_hash",
        "config_hash",
        "feature_registry_hash",
        "git_commit",
        "timestamp",
        "restart_aggregation_mode",
        "n_restarts_total",
        "n_restarts_present",
        "presence_rate",
        "n_validation_ok_restarts",
        "p_boot_validation_min",
        "p_boot_validation_median",
        "p_boot_validation_max",
        "p_boot_validation_mean",
        "candidate_tier_modal_restart",
        "candidate_tier_modal_rate_restart",
        "validated_rate_restart",
        "support_or_better_rate_restart",
    ]

    stage_started_at = perf_counter()
    track_consensus_meta = _apply_track_consensus_gate(
        top_rows=top_rows,
        scan_rows=scan_rows,
        enforce=bool(args.enforce_track_consensus),
        anchor_track=str(args.consensus_anchor_track),
        min_anchor_tier=str(args.consensus_min_anchor_tier),
    )
    top_rows_inference, top_rows_inference_meta = _build_restart_inference_rows(
        top_rows=top_rows,
        n_restarts=len(restart_seeds),
        p_threshold=float(args.p_threshold),
        q_threshold=float(args.q_threshold),
        confirmatory_y_cols=confirmatory_y_cols_effective,
        nonconfirmatory_max_tier=str(args.nonconfirmatory_max_tier),
    )
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="postprocess_consensus_and_inference",
        started_at=stage_started_at,
        top_rows_inference=int(len(top_rows_inference)),
    )

    stage_started_at = perf_counter()
    disk_space_checks.append(
        _check_disk_space_or_raise(
            probe_paths=[
                args.out_scan_runs_csv,
                args.out_top_models_csv,
                out_top_models_inference_csv,
                args.out_search_log_jsonl,
                args.out_run_summary_json,
            ],
            min_free_space_mb=int(args.min_free_space_mb),
            stage="write_main_outputs",
        )
    )
    write_csv(Path(args.out_scan_runs_csv), fieldnames=scan_fields, rows=scan_rows)
    write_csv(Path(args.out_top_models_csv), fieldnames=top_fields, rows=top_rows)
    write_csv(Path(out_top_models_inference_csv), fieldnames=top_inference_fields, rows=top_rows_inference)
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="write_main_output_tables",
        started_at=stage_started_at,
    )

    n_restarts_effective = len(restart_seeds)
    skipped_equivalent_specs = sum(1 for r in search_log_rows if r.get("status") == "skipped_equivalent_spec")
    candidate_stability: Dict[str, Dict[str, object]] = {}
    for row in top_rows:
        cid = str(row.get("candidate_id", ""))
        if not cid:
            continue
        if cid not in candidate_stability:
            candidate_stability[cid] = {
                "candidate_id": cid,
                "restart_ids": set(),
                "tiers": [],
                "n_validated": 0,
                "n_support_or_better": 0,
            }
        stats = candidate_stability[cid]
        tier = str(row.get("candidate_tier", ""))
        rid = int(row.get("restart_id", 0) or 0)
        stats["tiers"].append(tier)
        if rid > 0:
            stats["restart_ids"].add(rid)
        if tier == "validated_candidate":
            stats["n_validated"] = int(stats["n_validated"]) + 1
        if tier in {"validated_candidate", "support_candidate"}:
            stats["n_support_or_better"] = int(stats["n_support_or_better"]) + 1
    n_candidates_tier_consistent_all_restarts = 0
    n_candidates_support_or_better_all_restarts = 0
    n_candidates_validated_any_restart = 0
    for stats in candidate_stability.values():
        tiers = [str(t) for t in stats["tiers"]]
        restart_ids = stats["restart_ids"]
        present_all = len(restart_ids) == n_restarts_effective
        tier_consistent = len(set(tiers)) == 1 and present_all
        support_all = int(stats["n_support_or_better"]) == n_restarts_effective and present_all
        validated_any = int(stats["n_validated"]) > 0
        if tier_consistent:
            n_candidates_tier_consistent_all_restarts += 1
        if support_all:
            n_candidates_support_or_better_all_restarts += 1
        if validated_any:
            n_candidates_validated_any_restart += 1

    restart_stability_rows, restart_stability_meta = _build_restart_stability_rows(
        top_rows=top_rows,
        n_restarts=n_restarts_effective,
    )
    restart_stability_fields = [
        "candidate_id",
        "track",
        "context_scope",
        "y_col",
        "spec_id",
        "key_factor",
        "control_set",
        "n_restarts_total",
        "n_restarts_present",
        "presence_rate",
        "n_validated",
        "validated_rate",
        "n_support_or_better",
        "support_or_better_rate",
        "modal_tier",
        "modal_tier_rate",
        "beta_validation_sign_consistency",
        "beta_discovery_sign_consistency",
        "stability_score",
    ]
    stage_started_at = perf_counter()
    restart_stability_written = False
    if str(args.out_restart_stability_csv).strip():
        write_csv(
            Path(args.out_restart_stability_csv),
            fieldnames=restart_stability_fields,
            rows=restart_stability_rows,
        )
        restart_stability_written = True
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="write_restart_stability",
        started_at=stage_started_at,
        written=bool(restart_stability_written),
        rows=int(len(restart_stability_rows)),
    )

    stage_started_at = perf_counter()
    bootstrap_escalation_summary: Dict[str, object] = {
        "enabled": bool(args.auto_bootstrap_escalation),
        "executed": False,
    }
    if bool(args.auto_bootstrap_escalation):
        escalation_shortlist_source_rows = top_rows_inference if top_rows_inference else top_rows
        escalation_shortlist_source_label = (
            "top_models_inference_aggregated"
            if escalation_shortlist_source_rows is top_rows_inference
            else "top_models_restart_level"
        )
        escalation_shortlist_registry, escalation_selection_meta = _select_bootstrap_escalation_shortlist(
            top_rows=escalation_shortlist_source_rows,
            p_threshold=float(args.p_threshold),
            q_threshold=float(args.q_threshold),
            p_margin=float(args.escalation_p_margin),
            q_margin=float(args.escalation_q_margin),
            max_candidates=int(args.escalation_max_candidates),
            dedupe_mode=str(args.escalation_dedupe_mode),
        )
        escalation_selection_meta["shortlist_source"] = escalation_shortlist_source_label
        bootstrap_escalation_summary["selection_meta"] = escalation_selection_meta
        bootstrap_escalation_summary["shortlist_source"] = escalation_shortlist_source_label
        if int(args.escalation_n_bootstrap) <= int(args.n_bootstrap):
            bootstrap_escalation_summary["skip_reason"] = "escalation_n_bootstrap_must_exceed_base_n_bootstrap"
        elif int(args.escalation_n_bootstrap) <= 0:
            bootstrap_escalation_summary["skip_reason"] = "escalation_n_bootstrap_must_be_positive"
        elif not escalation_shortlist_registry:
            bootstrap_escalation_summary["skip_reason"] = "no_borderline_candidates_selected"
        else:
            escalation_suffix = str(args.escalation_run_id_suffix or "").strip() or "escalate"
            escalation_run_id = f"{args.run_id}__{escalation_suffix}"
            escalation_out_scan_runs_csv = _with_path_suffix(args.out_scan_runs_csv, escalation_suffix)
            escalation_out_top_models_csv = _with_path_suffix(args.out_top_models_csv, escalation_suffix)
            escalation_out_top_models_inference_csv = _with_path_suffix(escalation_out_top_models_csv, "inference")
            escalation_out_search_log_jsonl = _with_path_suffix(args.out_search_log_jsonl, escalation_suffix)
            escalation_out_run_summary_json = _with_path_suffix(args.out_run_summary_json, escalation_suffix)
            escalation_out_feature_registry_json = _with_path_suffix(args.out_feature_registry_json, escalation_suffix)
            _ensure_output_dirs(
                [
                    escalation_out_scan_runs_csv,
                    escalation_out_top_models_csv,
                    escalation_out_top_models_inference_csv,
                    escalation_out_search_log_jsonl,
                    escalation_out_run_summary_json,
                    escalation_out_feature_registry_json,
                ]
            )
            disk_space_checks.append(
                _check_disk_space_or_raise(
                    probe_paths=[
                        escalation_out_scan_runs_csv,
                        escalation_out_top_models_csv,
                        escalation_out_top_models_inference_csv,
                        escalation_out_search_log_jsonl,
                        escalation_out_run_summary_json,
                        escalation_out_feature_registry_json,
                    ],
                    min_free_space_mb=int(args.min_free_space_mb),
                    stage="bootstrap_escalation_stage_setup",
                )
            )

            escalation_registry_payload: Dict[str, object] = {
                "meta": {
                    "run_id": escalation_run_id,
                    "timestamp": ts,
                    "parent_run_id": args.run_id,
                    "source": "borderline_candidate_escalation_shortlist",
                    "selection_meta": escalation_selection_meta,
                    "shortlist_source": escalation_shortlist_source_label,
                },
                "feature_registry": escalation_shortlist_registry,
            }
            write_json(Path(escalation_out_feature_registry_json), escalation_registry_payload)
            escalation_feature_registry_hash = sha256_json(escalation_registry_payload)

            escalation_config_payload = dict(config_payload)
            escalation_config_payload.update(
                {
                    "run_id": escalation_run_id,
                    "n_bootstrap": int(args.escalation_n_bootstrap),
                    "bootstrap_escalation_parent_run_id": args.run_id,
                    "bootstrap_escalation_selection_meta": escalation_selection_meta,
                    "bootstrap_escalation_n_shortlist_features": int(len(escalation_shortlist_registry)),
                }
            )
            escalation_config_hash = sha256_json(escalation_config_payload)

            escalation_scan_rows: List[Dict[str, object]] = []
            escalation_top_rows: List[Dict[str, object]] = []
            escalation_search_log_rows: List[Dict[str, object]] = []
            escalation_restart_run_summaries: List[Dict[str, object]] = []
            for restart_id, restart_seed in enumerate(restart_seeds, start=1):
                escalation_scan_config = ScanConfig(
                    run_id=escalation_run_id,
                    bootstrap_seed=restart_seed,
                    n_bootstrap=int(args.escalation_n_bootstrap),
                    bootstrap_cluster_unit=args.bootstrap_cluster_unit,
                    optimizer_mode=str(args.optimizer_mode),
                    optimizer_adam_max_iter=int(args.optimizer_adam_max_iter),
                    optimizer_adam_learning_rate=float(args.optimizer_adam_learning_rate),
                    optimizer_adam_beta1=float(args.optimizer_adam_beta1),
                    optimizer_adam_beta2=float(args.optimizer_adam_beta2),
                    optimizer_adam_eps=float(args.optimizer_adam_eps),
                    optimizer_adam_l2=float(args.optimizer_adam_l2),
                    optimizer_adam_min_iter=int(args.optimizer_adam_min_iter),
                    optimizer_adam_tol=float(args.optimizer_adam_tol),
                    min_informative_events_estimable=min_informative_events_estimable,
                    min_policy_docs_informative_estimable=min_policy_docs_estimable,
                    min_informative_events_validated=min_informative_events_validated,
                    min_policy_docs_informative_validated=min_policy_docs_validated,
                    max_top1_policy_doc_share=args.max_top1_policy_doc_share,
                    bootstrap_success_min_ratio=args.bootstrap_success_min_ratio,
                    q_threshold=args.q_threshold,
                    p_threshold=args.p_threshold,
                    complexity_penalty=complexity_penalty_effective,
                    include_base_controls=include_base_controls_effective,
                    base_controls=tuple(base_controls_used),
                    control_spec_mode=control_spec_mode,
                    contexts=tuple(y_contexts),
                    validated_min_informative_events_by_y=dict(y_validated_events_by_col),
                    validated_min_policy_docs_by_y=dict(y_validated_docs_by_col),
                    max_features=0,
                    data_hash=composite_data_hash,
                    config_hash=escalation_config_hash,
                    feature_registry_hash=escalation_feature_registry_hash,
                    git_commit=git_commit,
                    timestamp=ts,
                    validation_used_for_search=False,
                    candidate_pool_locked_pre_validation=True,
                )
                rows_r, top_r, log_r = run_key_factor_scan(
                    df=data,
                    feature_registry=escalation_shortlist_registry,
                    config=escalation_scan_config,
                )
                for row in rows_r:
                    row["restart_id"] = restart_id
                    row["restart_bootstrap_seed"] = restart_seed
                for row in top_r:
                    row["restart_id"] = restart_id
                    row["restart_bootstrap_seed"] = restart_seed
                for row in log_r:
                    row["restart_id"] = restart_id
                    row["restart_bootstrap_seed"] = restart_seed
                escalation_scan_rows.extend(rows_r)
                escalation_top_rows.extend(top_r)
                escalation_search_log_rows.extend(log_r)
                escalation_restart_run_summaries.append(
                    {
                        "restart_id": restart_id,
                        "restart_bootstrap_seed": restart_seed,
                        "scan_rows": len(rows_r),
                        "top_rows": len(top_r),
                        "status_scan_rows": _status_counts(rows_r, "status"),
                        "candidate_tier_top_rows": _status_counts(top_r, "candidate_tier"),
                    }
                )

            escalation_track_consensus_meta = _apply_track_consensus_gate(
                top_rows=escalation_top_rows,
                scan_rows=escalation_scan_rows,
                enforce=bool(args.enforce_track_consensus),
                anchor_track=str(args.consensus_anchor_track),
                min_anchor_tier=str(args.consensus_min_anchor_tier),
            )
            escalation_top_rows_inference, escalation_top_rows_inference_meta = _build_restart_inference_rows(
                top_rows=escalation_top_rows,
                n_restarts=len(restart_seeds),
                p_threshold=float(args.p_threshold),
                q_threshold=float(args.q_threshold),
                confirmatory_y_cols=confirmatory_y_cols_effective,
                nonconfirmatory_max_tier=str(args.nonconfirmatory_max_tier),
            )
            disk_space_checks.append(
                _check_disk_space_or_raise(
                    probe_paths=[
                        escalation_out_scan_runs_csv,
                        escalation_out_top_models_csv,
                        escalation_out_top_models_inference_csv,
                        escalation_out_search_log_jsonl,
                        escalation_out_run_summary_json,
                    ],
                    min_free_space_mb=int(args.min_free_space_mb),
                    stage="write_bootstrap_escalation_outputs",
                )
            )
            write_csv(Path(escalation_out_scan_runs_csv), fieldnames=scan_fields, rows=escalation_scan_rows)
            write_csv(Path(escalation_out_top_models_csv), fieldnames=top_fields, rows=escalation_top_rows)
            write_csv(
                Path(escalation_out_top_models_inference_csv),
                fieldnames=top_inference_fields,
                rows=escalation_top_rows_inference,
            )
            write_jsonl(Path(escalation_out_search_log_jsonl), escalation_search_log_rows)

            escalation_run_summary = {
                "analysis_phase": "phase_b_bikard_machine_scientist_scan_bootstrap_escalation",
                "run_id": escalation_run_id,
                "parent_run_id": args.run_id,
                "timestamp": ts,
                "config": escalation_config_payload,
                "selection_meta": escalation_selection_meta,
                "track_consensus_meta": escalation_track_consensus_meta,
                "registry": {
                    "source": "borderline_candidate_escalation_shortlist",
                    "out_path": escalation_out_feature_registry_json,
                    "n_rows": len(escalation_shortlist_registry),
                    "feature_registry_hash": escalation_feature_registry_hash,
                },
                "outputs": {
                    "scan_runs_csv": escalation_out_scan_runs_csv,
                    "top_models_csv": escalation_out_top_models_csv,
                    "top_models_inference_csv": escalation_out_top_models_inference_csv,
                    "search_log_jsonl": escalation_out_search_log_jsonl,
                    "run_summary_json": escalation_out_run_summary_json,
                },
                "counts": {
                    "scan_rows": len(escalation_scan_rows),
                    "top_rows": len(escalation_top_rows),
                    "top_rows_inference": len(escalation_top_rows_inference),
                    "status_scan_rows": _status_counts(escalation_scan_rows, "status"),
                    "candidate_tier_top_rows": _status_counts(escalation_top_rows, "candidate_tier"),
                    "candidate_tier_top_rows_inference": _status_counts(
                        escalation_top_rows_inference, "candidate_tier"
                    ),
                    "search_log_rows": len(escalation_search_log_rows),
                },
                "restart": {
                    "n_restarts": n_restarts_effective,
                    "restart_bootstrap_seeds": restart_seeds,
                    "restart_run_summaries": escalation_restart_run_summaries,
                    "inference_aggregation_summary": escalation_top_rows_inference_meta,
                },
                "audit_hashes": {
                    "data_hash": composite_data_hash,
                    "config_hash": escalation_config_hash,
                    "feature_registry_hash": escalation_feature_registry_hash,
                    "git_commit": git_commit,
                    "git_commit_source": git_commit_source,
                },
            }
            write_json(Path(escalation_out_run_summary_json), escalation_run_summary)
            bootstrap_escalation_summary.update(
                {
                    "executed": True,
                    "run_id": escalation_run_id,
                    "config_hash": escalation_config_hash,
                    "feature_registry_hash": escalation_feature_registry_hash,
                    "outputs": escalation_run_summary["outputs"],
                    "counts": escalation_run_summary["counts"],
                    "restart": escalation_run_summary["restart"],
                }
            )
            if args.print_cli_summary:
                print("[Bootstrap Escalation Stage]")
                _print_cli_summary(
                    run_id=escalation_run_id,
                    gate_meta=gate_meta,
                    scan_rows=escalation_scan_rows,
                    top_rows=escalation_top_rows,
                    out_scan_runs_csv=escalation_out_scan_runs_csv,
                    out_top_models_csv=escalation_out_top_models_csv,
                    out_top_models_inference_csv=escalation_out_top_models_inference_csv,
                    out_run_summary_json=escalation_out_run_summary_json,
                    top_n=int(args.cli_summary_top_n),
                    inference_top_rows=escalation_top_rows_inference,
                )
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="bootstrap_escalation",
        started_at=stage_started_at,
        enabled=bool(args.auto_bootstrap_escalation),
        executed=bool(bootstrap_escalation_summary.get("executed", False)),
    )

    stage_started_at = perf_counter()
    refinement_summary: Dict[str, object] = {
        "enabled": bool(args.auto_refine_shortlist),
        "executed": False,
    }
    if bool(args.auto_refine_shortlist):
        shortlist_source_rows = top_rows_inference if top_rows_inference else top_rows
        shortlist_source_label = (
            "top_models_inference_aggregated" if shortlist_source_rows is top_rows_inference else "top_models_restart_level"
        )
        shortlist_registry, shortlist_meta = select_shortlist_features_from_top_models(
            shortlist_source_rows,
            tier_mode=str(args.refine_tier_mode),
            max_features=int(args.refine_max_features),
            dedupe_mode=str(args.refine_dedupe_mode),
        )
        refinement_summary["shortlist_meta"] = shortlist_meta
        refinement_summary["shortlist_source"] = shortlist_source_label
        if int(args.refine_n_bootstrap) <= 0:
            refinement_summary["skip_reason"] = "refine_n_bootstrap_must_be_positive"
        elif not shortlist_registry:
            refinement_summary["skip_reason"] = "no_shortlist_features_selected"
        else:
            refine_suffix = str(args.refine_run_id_suffix or "").strip() or "refine"
            refine_run_id = f"{args.run_id}__{refine_suffix}"
            refine_out_scan_runs_csv = _with_path_suffix(args.out_scan_runs_csv, refine_suffix)
            refine_out_top_models_csv = _with_path_suffix(args.out_top_models_csv, refine_suffix)
            refine_out_top_models_inference_csv = _with_path_suffix(refine_out_top_models_csv, "inference")
            refine_out_search_log_jsonl = _with_path_suffix(args.out_search_log_jsonl, refine_suffix)
            refine_out_run_summary_json = _with_path_suffix(args.out_run_summary_json, refine_suffix)
            refine_out_feature_registry_json = _with_path_suffix(args.out_feature_registry_json, refine_suffix)
            _ensure_output_dirs(
                [
                    refine_out_scan_runs_csv,
                    refine_out_top_models_csv,
                    refine_out_top_models_inference_csv,
                    refine_out_search_log_jsonl,
                    refine_out_run_summary_json,
                    refine_out_feature_registry_json,
                ]
            )
            disk_space_checks.append(
                _check_disk_space_or_raise(
                    probe_paths=[
                        refine_out_scan_runs_csv,
                        refine_out_top_models_csv,
                        refine_out_top_models_inference_csv,
                        refine_out_search_log_jsonl,
                        refine_out_run_summary_json,
                        refine_out_feature_registry_json,
                    ],
                    min_free_space_mb=int(args.min_free_space_mb),
                    stage="refinement_stage_setup",
                )
            )

            refine_registry_payload: Dict[str, object] = {
                "meta": {
                    "run_id": refine_run_id,
                    "timestamp": ts,
                    "parent_run_id": args.run_id,
                    "source": "top_models_shortlist",
                    "shortlist_meta": shortlist_meta,
                },
                "feature_registry": shortlist_registry,
            }
            write_json(Path(refine_out_feature_registry_json), refine_registry_payload)
            refine_feature_registry_hash = sha256_json(refine_registry_payload)

            refine_config_payload = dict(config_payload)
            shortlist_source_csv = (
                out_top_models_inference_csv if shortlist_source_rows is top_rows_inference else args.out_top_models_csv
            )
            refine_config_payload.update(
                {
                    "run_id": refine_run_id,
                    "n_bootstrap": int(args.refine_n_bootstrap),
                    "refinement_parent_run_id": args.run_id,
                    "refinement_source_top_models_csv": shortlist_source_csv,
                    "refinement_source_top_models_level": shortlist_source_label,
                    "refinement_tier_mode": str(args.refine_tier_mode),
                    "refinement_max_features": int(args.refine_max_features),
                    "refinement_dedupe_mode": str(args.refine_dedupe_mode),
                    "refinement_n_shortlist_features": int(len(shortlist_registry)),
                }
            )
            refine_config_hash = sha256_json(refine_config_payload)

            refine_scan_rows: List[Dict[str, object]] = []
            refine_top_rows: List[Dict[str, object]] = []
            refine_search_log_rows: List[Dict[str, object]] = []
            refine_restart_run_summaries: List[Dict[str, object]] = []
            for restart_id, restart_seed in enumerate(restart_seeds, start=1):
                refine_scan_config = ScanConfig(
                    run_id=refine_run_id,
                    bootstrap_seed=restart_seed,
                    n_bootstrap=int(args.refine_n_bootstrap),
                    bootstrap_cluster_unit=args.bootstrap_cluster_unit,
                    optimizer_mode=str(args.optimizer_mode),
                    optimizer_adam_max_iter=int(args.optimizer_adam_max_iter),
                    optimizer_adam_learning_rate=float(args.optimizer_adam_learning_rate),
                    optimizer_adam_beta1=float(args.optimizer_adam_beta1),
                    optimizer_adam_beta2=float(args.optimizer_adam_beta2),
                    optimizer_adam_eps=float(args.optimizer_adam_eps),
                    optimizer_adam_l2=float(args.optimizer_adam_l2),
                    optimizer_adam_min_iter=int(args.optimizer_adam_min_iter),
                    optimizer_adam_tol=float(args.optimizer_adam_tol),
                    min_informative_events_estimable=min_informative_events_estimable,
                    min_policy_docs_informative_estimable=min_policy_docs_estimable,
                    min_informative_events_validated=min_informative_events_validated,
                    min_policy_docs_informative_validated=min_policy_docs_validated,
                    max_top1_policy_doc_share=args.max_top1_policy_doc_share,
                    bootstrap_success_min_ratio=args.bootstrap_success_min_ratio,
                    q_threshold=args.q_threshold,
                    p_threshold=args.p_threshold,
                    complexity_penalty=complexity_penalty_effective,
                    include_base_controls=include_base_controls_effective,
                    base_controls=tuple(base_controls_used),
                    control_spec_mode=control_spec_mode,
                    contexts=tuple(y_contexts),
                    validated_min_informative_events_by_y=dict(y_validated_events_by_col),
                    validated_min_policy_docs_by_y=dict(y_validated_docs_by_col),
                    max_features=0,
                    data_hash=composite_data_hash,
                    config_hash=refine_config_hash,
                    feature_registry_hash=refine_feature_registry_hash,
                    git_commit=git_commit,
                    timestamp=ts,
                    validation_used_for_search=False,
                    candidate_pool_locked_pre_validation=True,
                )
                rows_r, top_r, log_r = run_key_factor_scan(
                    df=data,
                    feature_registry=shortlist_registry,
                    config=refine_scan_config,
                )
                for row in rows_r:
                    row["restart_id"] = restart_id
                    row["restart_bootstrap_seed"] = restart_seed
                for row in top_r:
                    row["restart_id"] = restart_id
                    row["restart_bootstrap_seed"] = restart_seed
                for row in log_r:
                    row["restart_id"] = restart_id
                    row["restart_bootstrap_seed"] = restart_seed
                refine_scan_rows.extend(rows_r)
                refine_top_rows.extend(top_r)
                refine_search_log_rows.extend(log_r)
                refine_restart_run_summaries.append(
                    {
                        "restart_id": restart_id,
                        "restart_bootstrap_seed": restart_seed,
                        "scan_rows": len(rows_r),
                        "top_rows": len(top_r),
                        "status_scan_rows": _status_counts(rows_r, "status"),
                        "candidate_tier_top_rows": _status_counts(top_r, "candidate_tier"),
                    }
                )

            refine_track_consensus_meta = _apply_track_consensus_gate(
                top_rows=refine_top_rows,
                scan_rows=refine_scan_rows,
                enforce=bool(args.enforce_track_consensus),
                anchor_track=str(args.consensus_anchor_track),
                min_anchor_tier=str(args.consensus_min_anchor_tier),
            )
            refine_top_rows_inference, refine_top_rows_inference_meta = _build_restart_inference_rows(
                top_rows=refine_top_rows,
                n_restarts=len(restart_seeds),
                p_threshold=float(args.p_threshold),
                q_threshold=float(args.q_threshold),
                confirmatory_y_cols=confirmatory_y_cols_effective,
                nonconfirmatory_max_tier=str(args.nonconfirmatory_max_tier),
            )
            disk_space_checks.append(
                _check_disk_space_or_raise(
                    probe_paths=[
                        refine_out_scan_runs_csv,
                        refine_out_top_models_csv,
                        refine_out_top_models_inference_csv,
                        refine_out_search_log_jsonl,
                        refine_out_run_summary_json,
                    ],
                    min_free_space_mb=int(args.min_free_space_mb),
                    stage="write_refinement_outputs",
                )
            )
            write_csv(Path(refine_out_scan_runs_csv), fieldnames=scan_fields, rows=refine_scan_rows)
            write_csv(Path(refine_out_top_models_csv), fieldnames=top_fields, rows=refine_top_rows)
            write_csv(
                Path(refine_out_top_models_inference_csv),
                fieldnames=top_inference_fields,
                rows=refine_top_rows_inference,
            )
            write_jsonl(Path(refine_out_search_log_jsonl), refine_search_log_rows)

            refine_run_summary = {
                "analysis_phase": "phase_b_bikard_machine_scientist_scan_refinement",
                "run_id": refine_run_id,
                "parent_run_id": args.run_id,
                "timestamp": ts,
                "config": refine_config_payload,
                "shortlist_meta": shortlist_meta,
                "track_consensus_meta": refine_track_consensus_meta,
                "registry": {
                    "source": "top_models_shortlist",
                    "out_path": refine_out_feature_registry_json,
                    "n_rows": len(shortlist_registry),
                    "feature_registry_hash": refine_feature_registry_hash,
                },
                "outputs": {
                    "scan_runs_csv": refine_out_scan_runs_csv,
                    "top_models_csv": refine_out_top_models_csv,
                    "top_models_inference_csv": refine_out_top_models_inference_csv,
                    "search_log_jsonl": refine_out_search_log_jsonl,
                    "run_summary_json": refine_out_run_summary_json,
                },
                "counts": {
                    "scan_rows": len(refine_scan_rows),
                    "top_rows": len(refine_top_rows),
                    "top_rows_inference": len(refine_top_rows_inference),
                    "status_scan_rows": _status_counts(refine_scan_rows, "status"),
                    "candidate_tier_top_rows": _status_counts(refine_top_rows, "candidate_tier"),
                    "candidate_tier_top_rows_inference": _status_counts(refine_top_rows_inference, "candidate_tier"),
                    "search_log_rows": len(refine_search_log_rows),
                },
                "restart": {
                    "n_restarts": n_restarts_effective,
                    "restart_bootstrap_seeds": restart_seeds,
                    "restart_run_summaries": refine_restart_run_summaries,
                    "inference_aggregation_summary": refine_top_rows_inference_meta,
                },
                "audit_hashes": {
                    "data_hash": composite_data_hash,
                    "config_hash": refine_config_hash,
                    "feature_registry_hash": refine_feature_registry_hash,
                    "git_commit": git_commit,
                    "git_commit_source": git_commit_source,
                },
            }
            write_json(Path(refine_out_run_summary_json), refine_run_summary)
            refinement_summary.update(
                {
                    "executed": True,
                    "run_id": refine_run_id,
                    "config_hash": refine_config_hash,
                    "feature_registry_hash": refine_feature_registry_hash,
                    "outputs": refine_run_summary["outputs"],
                    "counts": refine_run_summary["counts"],
                    "restart": refine_run_summary["restart"],
                }
            )
            if args.print_cli_summary:
                print("[Refinement Stage]")
                _print_cli_summary(
                    run_id=refine_run_id,
                    gate_meta=gate_meta,
                    scan_rows=refine_scan_rows,
                    top_rows=refine_top_rows,
                    out_scan_runs_csv=refine_out_scan_runs_csv,
                    out_top_models_csv=refine_out_top_models_csv,
                    out_top_models_inference_csv=refine_out_top_models_inference_csv,
                    out_run_summary_json=refine_out_run_summary_json,
                    top_n=int(args.cli_summary_top_n),
                    inference_top_rows=refine_top_rows_inference,
                )
    _record_runtime_stage(
        stage_rows=runtime_stage_rows,
        stage="refinement_stage",
        started_at=stage_started_at,
        enabled=bool(args.auto_refine_shortlist),
        executed=bool(refinement_summary.get("executed", False)),
    )

    runtime_total_elapsed_ms = int(round((perf_counter() - run_started_at) * 1000.0))
    runtime_stage_elapsed_ms_total = int(
        sum(int(stage.get("elapsed_ms", 0) or 0) for stage in runtime_stage_rows)
    )
    runtime_profile = {
        "total_elapsed_ms": int(runtime_total_elapsed_ms),
        "stage_elapsed_ms_total": int(runtime_stage_elapsed_ms_total),
        "n_stages": int(len(runtime_stage_rows)),
        "stages": runtime_stage_rows,
    }
    for stage_row in runtime_stage_rows:
        search_log_rows.append(
            _runtime_stage_log_row(
                run_id=args.run_id,
                timestamp=ts,
                stage_row=stage_row,
            )
        )
    write_jsonl(Path(args.out_search_log_jsonl), search_log_rows)

    run_summary = {
        "analysis_phase": "phase_b_bikard_machine_scientist_scan",
        "run_id": args.run_id,
        "as_of_date": str(now_utc.date()),
        "timezone": "UTC",
        "timestamp": ts,
        "git_commit_source": git_commit_source,
        "support_only": True,
        "not_replacing_confirmatory_claim": True,
        "validation_used_for_search": False,
        "candidate_pool_locked_pre_validation": True,
        "search_governance": {
            "validation_used_for_search": False,
            "candidate_pool_locked_pre_validation": True,
            "validation_stage_policy": "post_search_on_fixed_candidate_pool",
        },
        "config": config_payload,
        "split_meta": split_meta,
        "split_integrity": split_integrity_meta,
        "gate_meta": gate_meta,
        "controls_meta": controls_meta,
        "expression_meta": expression_meta,
        "categorical_meta": categorical_meta,
        "family_dedupe_meta": family_dedupe_meta,
        "y_contexts_meta": y_contexts_meta,
        "y_time_windows_meta": y_time_windows_meta,
        "time_series_precheck_meta": time_series_precheck_meta,
        "time_series_precheck_policy": time_series_precheck_policy,
        "time_series_confirmatory_policy_meta": confirmatory_policy_meta,
        "y_validated_gate_meta": y_validated_gate_meta,
        "track_consensus_meta": track_consensus_meta,
        "runtime_profile": runtime_profile,
        "runtime_preflight": {
            "min_free_space_mb": int(args.min_free_space_mb),
            "disk_space_checks": disk_space_checks,
        },
        "inputs": load_meta.get("inputs", {}),
        "merge_report": load_meta.get("merge_report", {}),
        "data_fingerprint": data_fingerprint,
        "registry": {
            "source": registry_source,
            "out_path": str(out_registry_path),
            "n_rows": len(registry),
            "feature_registry_hash": feature_registry_hash,
            "filter_summary": registry_filter_summary,
        },
        "restart": {
            "n_restarts": n_restarts_effective,
            "restart_seed_step": args.restart_seed_step,
            "restart_bootstrap_seeds": restart_seeds,
            "restart_run_summaries": restart_run_summaries,
            "candidate_stability_summary": {
                "n_candidates": len(candidate_stability),
                "n_candidates_tier_consistent_all_restarts": n_candidates_tier_consistent_all_restarts,
                "n_candidates_support_or_better_all_restarts": n_candidates_support_or_better_all_restarts,
                "n_candidates_validated_any_restart": n_candidates_validated_any_restart,
            },
            "restart_stability_summary": restart_stability_meta,
            "inference_aggregation_summary": top_rows_inference_meta,
            "output_csv": args.out_restart_stability_csv,
            "out_restart_stability_csv": args.out_restart_stability_csv,
            "restart_stability_csv": args.out_restart_stability_csv,
        },
        "bootstrap_escalation": bootstrap_escalation_summary,
        "refinement": refinement_summary,
        "outputs": {
            "scan_runs_csv": args.out_scan_runs_csv,
            "top_models_csv": args.out_top_models_csv,
            "top_models_inference_csv": out_top_models_inference_csv,
            "search_log_jsonl": args.out_search_log_jsonl,
            "run_summary_json": args.out_run_summary_json,
            "feasibility_frontier_json": args.out_feasibility_frontier_json,
            "restart_stability_csv": args.out_restart_stability_csv,
        },
        "counts": {
            "scan_rows": len(scan_rows),
            "top_rows": len(top_rows),
            "top_rows_inference": len(top_rows_inference),
            "status_scan_rows": _status_counts(scan_rows, "status"),
            "candidate_tier_top_rows": _status_counts(top_rows, "candidate_tier"),
            "candidate_tier_top_rows_inference": _status_counts(top_rows_inference, "candidate_tier"),
            "search_log_rows": len(search_log_rows),
            "skipped_equivalent_specs": skipped_equivalent_specs,
        },
        "audit_hashes": {
            "data_hash": composite_data_hash,
            "dyad_base_hash": load_meta.get("inputs", {}).get("dyad_base_sha256", ""),
            "config_hash": config_hash,
            "feature_registry_hash": feature_registry_hash,
            "git_commit": git_commit,
            "git_commit_source": git_commit_source,
            "runner_script": runner_script_ref,
            "runner_script_sha256": runner_script_sha256,
        },
    }
    write_json(Path(args.out_run_summary_json), run_summary)
    if args.print_cli_summary:
        _print_cli_summary(
            run_id=args.run_id,
            gate_meta=gate_meta,
            scan_rows=scan_rows,
            top_rows=top_rows,
            out_scan_runs_csv=args.out_scan_runs_csv,
            out_top_models_csv=args.out_top_models_csv,
            out_top_models_inference_csv=out_top_models_inference_csv,
            out_run_summary_json=args.out_run_summary_json,
            top_n=int(args.cli_summary_top_n),
            inference_top_rows=top_rows_inference,
        )

    result_payload: Dict[str, object] = {
        "scan_rows": len(scan_rows),
        "top_rows": len(top_rows),
        "top_rows_inference": len(top_rows_inference),
        "min_free_space_mb": int(args.min_free_space_mb),
        "validated_gate_source": gate_resolution_meta["validated_gate_source"],
        "data_hash": composite_data_hash,
        "config_hash": config_hash,
        "feature_registry_hash": feature_registry_hash,
        "out_scan_runs_csv": args.out_scan_runs_csv,
        "out_top_models_csv": args.out_top_models_csv,
        "out_top_models_inference_csv": out_top_models_inference_csv,
        "out_search_log_jsonl": args.out_search_log_jsonl,
        "out_run_summary_json": args.out_run_summary_json,
        "out_feasibility_frontier_json": args.out_feasibility_frontier_json,
        "out_restart_stability_csv": args.out_restart_stability_csv,
    }
    if bool(refinement_summary.get("executed", False)):
        result_payload["refinement"] = {
            "run_id": refinement_summary.get("run_id", ""),
            "outputs": refinement_summary.get("outputs", {}),
            "counts": refinement_summary.get("counts", {}),
        }
    if bool(bootstrap_escalation_summary.get("executed", False)):
        result_payload["bootstrap_escalation"] = {
            "run_id": bootstrap_escalation_summary.get("run_id", ""),
            "outputs": bootstrap_escalation_summary.get("outputs", {}),
            "counts": bootstrap_escalation_summary.get("counts", {}),
        }
    print(result_payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
