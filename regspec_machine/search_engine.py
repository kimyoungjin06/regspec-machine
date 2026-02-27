from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from math import inf
from time import perf_counter
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

from .bootstrap import bootstrap_clogit, summarize_bootstrap
from .estimators import fit_clogit, prepare_informative_df, standardize_inplace
from .fdr import attach_bh_qvalues


@dataclass
class ScanConfig:
    run_id: str
    bootstrap_seed: int = 20260219
    n_bootstrap: int = 499
    bootstrap_cluster_unit: str = "policy_document_id"
    min_informative_events_estimable: int = 20
    min_policy_docs_informative_estimable: int = 10
    min_informative_events_validated: int = 100
    min_policy_docs_informative_validated: int = 30
    max_top1_policy_doc_share: float = 0.20
    bootstrap_success_min_ratio: float = 0.80
    q_threshold: float = 0.10
    p_threshold: float = 0.05
    complexity_penalty: float = 0.01
    include_base_controls: bool = True
    base_controls: Tuple[str, ...] = ("pub_year_alt", "recency_years_alt")
    control_spec_mode: str = "both"
    contexts: Tuple[Tuple[str, str], ...] = (
        ("all_contexts", "y_all"),
        ("evidence_use_only", "y_evidence"),
    )
    validated_min_informative_events_by_y: Dict[str, int] = field(default_factory=dict)
    validated_min_policy_docs_by_y: Dict[str, int] = field(default_factory=dict)
    max_features: int = 0
    data_hash: str = ""
    config_hash: str = ""
    feature_registry_hash: str = ""
    git_commit: str = ""
    timestamp: str = ""
    validation_used_for_search: bool = False
    candidate_pool_locked_pre_validation: bool = True
    skip_discovery_infeasible_track_y: bool = False
    auto_disable_base_controls_low_capacity: bool = False
    base_controls_min_events_per_exog: int = 10
    base_controls_min_policy_docs_per_exog: int = 5
    optimizer_mode: str = "none"
    optimizer_adam_max_iter: int = 300
    optimizer_adam_learning_rate: float = 0.05
    optimizer_adam_beta1: float = 0.9
    optimizer_adam_beta2: float = 0.999
    optimizer_adam_eps: float = 1e-8
    optimizer_adam_l2: float = 1e-4
    optimizer_adam_min_iter: int = 25
    optimizer_adam_tol: float = 1e-6


def _stable_seed(base_seed: int, token: str) -> int:
    hx = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return int(base_seed + (int(hx[:8], 16) % 1_000_000))


def _equivalence_hash(
    *, track: str, context_scope: str, y_col: str, exog_cols: Sequence[str]
) -> str:
    normalized_exog = sorted({str(c).strip() for c in exog_cols if str(c).strip()})
    token = f"{track}|{context_scope}|{y_col}|{','.join(normalized_exog)}"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _count_two_alt_events(df: pd.DataFrame) -> int:
    return int((df.groupby("event_id", dropna=False).size() == 2).sum())


def _variation_filtered_df(df: pd.DataFrame, *, feature_col: str) -> pd.DataFrame:
    keep_events: List[str] = []
    for eid, g in df.groupby("event_id", dropna=False):
        if len(g) != 2:
            continue
        vals = pd.to_numeric(g[feature_col], errors="coerce")
        if vals.isna().any():
            continue
        if float(vals.iloc[0]) != float(vals.iloc[1]):
            keep_events.append(str(eid))
    return df[df["event_id"].astype(str).isin(keep_events)].copy()


def _informative_capacity_for_y(df: pd.DataFrame, *, y_col: str) -> Dict[str, int]:
    if y_col not in df.columns:
        return {
            "n_two_alt_events": 0,
            "n_informative_events": 0,
            "n_policy_docs_informative": 0,
        }
    use = df[["event_id", "policy_document_id", y_col]].copy()
    use[y_col] = pd.to_numeric(use[y_col], errors="coerce").fillna(0.0)
    n_two_alt_events = 0
    informative_events = 0
    informative_docs: List[str] = []
    for _, g in use.groupby("event_id", dropna=False):
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


def _build_row(
    *,
    run_id: str,
    track: str,
    context_scope: str,
    split_id: str,
    split_role: str,
    spec_id: str,
    key_factor: str,
    control_set: str,
    fdr_family_id: str,
    candidate_id: str,
    n_choice_situations_total: int,
    n_single_choice_informative: int,
    n_policy_docs_informative: int,
    n_twin_sets: int,
    beta: object,
    ci95_lower: object,
    ci95_upper: object,
    se_boot: object,
    p_boot: object,
    score: object,
    bootstrap_cluster_unit: str,
    bootstrap_success: int,
    bootstrap_attempted: int,
    status: str,
    reason_code: str,
    reason_detail: str,
    reason_stage: str,
    top1_policy_doc_event_share: object,
    config: ScanConfig,
) -> Dict[str, object]:
    return {
        "run_id": run_id,
        "track": track,
        "context_scope": context_scope,
        "split_id": split_id,
        "split_role": split_role,
        "spec_id": spec_id,
        "key_factor": key_factor,
        "control_set": control_set,
        "fdr_family_id": fdr_family_id,
        "candidate_id": candidate_id,
        "n_choice_situations_total": n_choice_situations_total,
        "n_single_choice_informative": n_single_choice_informative,
        "n_policy_docs_informative": n_policy_docs_informative,
        "n_twin_sets": n_twin_sets,
        "beta": beta,
        "ci95_lower": ci95_lower,
        "ci95_upper": ci95_upper,
        "se_boot": se_boot,
        "p_boot": p_boot,
        "q_value": None,
        "score": score,
        "bootstrap_cluster_unit": bootstrap_cluster_unit,
        "bootstrap_success": bootstrap_success,
        "bootstrap_attempted": bootstrap_attempted,
        "status": status,
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "reason_stage": reason_stage,
        "top1_policy_doc_event_share": top1_policy_doc_event_share,
        "support_only": 1,
        "not_replacing_confirmatory_claim": 1,
        "data_hash": config.data_hash,
        "config_hash": config.config_hash,
        "feature_registry_hash": config.feature_registry_hash,
        "git_commit": config.git_commit,
        "timestamp": config.timestamp,
        "candidate_tier": "exploratory",
        "validation_used_for_search": int(bool(config.validation_used_for_search)),
        "candidate_pool_locked_pre_validation": int(bool(config.candidate_pool_locked_pre_validation)),
        "candidate_eval_order": None,
        "candidate_pool_size": None,
        "equivalence_hash": "",
        "candidate_elapsed_ms": None,
        "effective_n_bootstrap": int(bootstrap_success),
    }


def _scan_one_split(
    *,
    df: pd.DataFrame,
    y_col: str,
    track: str,
    context_scope: str,
    split_id: str,
    split_role: str,
    spec_id: str,
    key_factor: str,
    control_set_name: str,
    exog_cols: Sequence[str],
    fdr_family_id: str,
    candidate_id: str,
    config: ScanConfig,
) -> Dict[str, object]:
    n_choice_situations_total = _count_two_alt_events(df)
    n_twin_sets = int(df["pair_id"].nunique()) if "pair_id" in df.columns else 0

    if y_col not in df.columns:
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=0,
            n_policy_docs_informative=0,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=0,
            bootstrap_attempted=0,
            status="failed_rule_checks",
            reason_code="missing_y_column",
            reason_detail=str(y_col),
            reason_stage="precheck",
            top1_policy_doc_event_share=None,
            config=config,
        )

    missing_exogs = [c for c in exog_cols if c not in df.columns]
    if missing_exogs:
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=0,
            n_policy_docs_informative=0,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=0,
            bootstrap_attempted=0,
            status="failed_rule_checks",
            reason_code="missing_feature_columns",
            reason_detail="|".join(missing_exogs),
            reason_stage="precheck",
            top1_policy_doc_event_share=None,
            config=config,
        )

    role_is_validation = split_role == "validation"
    if role_is_validation:
        min_informative_events = int(
            config.validated_min_informative_events_by_y.get(y_col, config.min_informative_events_validated)
        )
        min_policy_docs_informative = int(
            config.validated_min_policy_docs_by_y.get(y_col, config.min_policy_docs_informative_validated)
        )
        low_events_status = "not_validated_out_of_sample"
        low_policy_docs_status = "not_validated_out_of_sample"
        high_concentration_status = "not_validated_out_of_sample"
        no_variation_status = "not_validated_out_of_sample"
        low_events_code = "validation_low_informative_events"
        low_policy_docs_code = "validation_low_policy_doc_clusters"
        high_concentration_code = "validation_high_policy_doc_concentration"
        no_variation_code = "validation_no_variation_within_event"
    else:
        min_informative_events = config.min_informative_events_estimable
        min_policy_docs_informative = config.min_policy_docs_informative_estimable
        low_events_status = "not_estimable_low_events"
        low_policy_docs_status = "not_estimable_low_policy_doc_clusters"
        high_concentration_status = "not_estimable_high_policy_doc_concentration"
        no_variation_status = "not_estimable_low_within_event_variation"
        low_events_code = "not_estimable_low_events"
        low_policy_docs_code = "not_estimable_low_policy_doc_clusters"
        high_concentration_code = "not_estimable_high_policy_doc_concentration"
        no_variation_code = "not_estimable_low_within_event_variation"

    max_docs_possible = int(df["policy_document_id"].astype(str).nunique())
    if role_is_validation and max_docs_possible < min_policy_docs_informative:
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=0,
            n_policy_docs_informative=max_docs_possible,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=0,
            bootstrap_attempted=0,
            status="not_validated_out_of_sample",
            reason_code="validation_gate_infeasible_policy_docs",
            reason_detail=f"max_docs_possible={max_docs_possible} < gate={min_policy_docs_informative}",
            reason_stage="precheck",
            top1_policy_doc_event_share=None,
            config=config,
        )

    informative = prepare_informative_df(df, y_col=y_col, exog_cols=exog_cols)
    informative = _variation_filtered_df(informative, feature_col=key_factor)
    n_informative = int(informative["event_id"].nunique())
    if n_informative == 0:
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=0,
            n_policy_docs_informative=0,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=0,
            bootstrap_attempted=0,
            status=no_variation_status,
            reason_code=no_variation_code,
            reason_detail="informative variation events = 0",
            reason_stage="precheck",
            top1_policy_doc_event_share=None,
            config=config,
        )

    event_doc = informative[["event_id", "policy_document_id"]].drop_duplicates("event_id")
    n_policy_docs_informative = int(event_doc["policy_document_id"].astype(str).nunique())
    doc_counts = event_doc["policy_document_id"].astype(str).value_counts()
    top1_share = float(doc_counts.iloc[0] / n_informative) if not doc_counts.empty else 1.0

    if n_informative < min_informative_events:
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=n_informative,
            n_policy_docs_informative=n_policy_docs_informative,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=0,
            bootstrap_attempted=0,
            status=low_events_status,
            reason_code=low_events_code,
            reason_detail=f"{n_informative} < {min_informative_events}",
            reason_stage="precheck",
            top1_policy_doc_event_share=top1_share,
            config=config,
        )

    if n_policy_docs_informative < min_policy_docs_informative:
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=n_informative,
            n_policy_docs_informative=n_policy_docs_informative,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=0,
            bootstrap_attempted=0,
            status=low_policy_docs_status,
            reason_code=low_policy_docs_code,
            reason_detail=f"{n_policy_docs_informative} < {min_policy_docs_informative}",
            reason_stage="precheck",
            top1_policy_doc_event_share=top1_share,
            config=config,
        )

    if top1_share > config.max_top1_policy_doc_share:
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=n_informative,
            n_policy_docs_informative=n_policy_docs_informative,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=0,
            bootstrap_attempted=0,
            status=high_concentration_status,
            reason_code=high_concentration_code,
            reason_detail=f"{top1_share:.4f} > {config.max_top1_policy_doc_share:.4f}",
            reason_stage="precheck",
            top1_policy_doc_event_share=top1_share,
            config=config,
        )

    fit_df = informative.copy()
    standardize_inplace(fit_df, exog_cols)
    x_mat = fit_df[list(exog_cols)].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
    if x_mat.size == 0:
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=n_informative,
            n_policy_docs_informative=n_policy_docs_informative,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=0,
            bootstrap_attempted=0,
            status="fit_failed",
            reason_code="fit_failed_empty_design",
            reason_detail="design matrix empty after preprocessing",
            reason_stage="fit",
            top1_policy_doc_event_share=top1_share,
            config=config,
        )
    try:
        design_rank = int(np.linalg.matrix_rank(np.nan_to_num(x_mat, copy=False)))
    except Exception:
        design_rank = -1
    if design_rank >= 0 and design_rank < len(exog_cols):
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=n_informative,
            n_policy_docs_informative=n_policy_docs_informative,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=0,
            bootstrap_attempted=0,
            status="not_estimable_degenerate_design_matrix",
            reason_code="not_estimable_degenerate_design_matrix",
            reason_detail=f"matrix_rank={design_rank} < n_exog={len(exog_cols)}",
            reason_stage="precheck",
            top1_policy_doc_event_share=top1_share,
            config=config,
        )
    beta_hat, llf, fit_err = fit_clogit(
        fit_df,
        exog_cols=exog_cols,
        key_factor_col=key_factor,
        optimizer_mode=config.optimizer_mode,
        adam_max_iter=config.optimizer_adam_max_iter,
        adam_learning_rate=config.optimizer_adam_learning_rate,
        adam_beta1=config.optimizer_adam_beta1,
        adam_beta2=config.optimizer_adam_beta2,
        adam_eps=config.optimizer_adam_eps,
        adam_l2=config.optimizer_adam_l2,
        adam_min_iter=config.optimizer_adam_min_iter,
        adam_tol=config.optimizer_adam_tol,
    )
    if beta_hat is None or fit_err is not None:
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=n_informative,
            n_policy_docs_informative=n_policy_docs_informative,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=0,
            bootstrap_attempted=0,
            status="fit_failed",
            reason_code="fit_failed",
            reason_detail=str(fit_err or "fit_failed_unknown"),
            reason_stage="fit",
            top1_policy_doc_event_share=top1_share,
            config=config,
        )

    bs_seed = _stable_seed(config.bootstrap_seed, f"{candidate_id}|{split_role}")
    betas, _, boot_attempted = bootstrap_clogit(
        fit_df,
        exog_cols=exog_cols,
        key_factor_col=key_factor,
        cluster_unit=config.bootstrap_cluster_unit,
        n_bootstrap=config.n_bootstrap,
        seed=bs_seed,
        optimizer_mode=config.optimizer_mode,
        adam_max_iter=config.optimizer_adam_max_iter,
        adam_learning_rate=config.optimizer_adam_learning_rate,
        adam_beta1=config.optimizer_adam_beta1,
        adam_beta2=config.optimizer_adam_beta2,
        adam_eps=config.optimizer_adam_eps,
        adam_l2=config.optimizer_adam_l2,
        adam_min_iter=config.optimizer_adam_min_iter,
        adam_tol=config.optimizer_adam_tol,
    )
    boot_success = len(betas)
    success_ratio = float(boot_success) / float(boot_attempted) if boot_attempted > 0 else 0.0
    if success_ratio < config.bootstrap_success_min_ratio:
        return _build_row(
            run_id=config.run_id,
            track=track,
            context_scope=context_scope,
            split_id=split_id,
            split_role=split_role,
            spec_id=spec_id,
            key_factor=key_factor,
            control_set=control_set_name,
            fdr_family_id=fdr_family_id,
            candidate_id=candidate_id,
            n_choice_situations_total=n_choice_situations_total,
            n_single_choice_informative=n_informative,
            n_policy_docs_informative=n_policy_docs_informative,
            n_twin_sets=n_twin_sets,
            beta=None,
            ci95_lower=None,
            ci95_upper=None,
            se_boot=None,
            p_boot=None,
            score=None,
            bootstrap_cluster_unit=config.bootstrap_cluster_unit,
            bootstrap_success=boot_success,
            bootstrap_attempted=boot_attempted,
            status="not_estimable_low_bootstrap_success",
            reason_code="not_estimable_low_bootstrap_success",
            reason_detail=f"{boot_success}/{boot_attempted} < {config.bootstrap_success_min_ratio:.3f}",
            reason_stage="bootstrap",
            top1_policy_doc_event_share=top1_share,
            config=config,
        )

    _, ci_lo, ci_hi, se_boot, p_boot = summarize_bootstrap(betas)
    score = (llf if llf is not None else 0.0) - config.complexity_penalty * float(len(exog_cols))
    return _build_row(
        run_id=config.run_id,
        track=track,
        context_scope=context_scope,
        split_id=split_id,
        split_role=split_role,
        spec_id=spec_id,
        key_factor=key_factor,
        control_set=control_set_name,
        fdr_family_id=fdr_family_id,
        candidate_id=candidate_id,
        n_choice_situations_total=n_choice_situations_total,
        n_single_choice_informative=n_informative,
        n_policy_docs_informative=n_policy_docs_informative,
        n_twin_sets=n_twin_sets,
        beta=beta_hat,
        ci95_lower=ci_lo,
        ci95_upper=ci_hi,
        se_boot=se_boot,
        p_boot=p_boot,
        score=score,
        bootstrap_cluster_unit=config.bootstrap_cluster_unit,
        bootstrap_success=boot_success,
        bootstrap_attempted=boot_attempted,
        status="ok",
        reason_code="ok",
        reason_detail="",
        reason_stage="completed",
        top1_policy_doc_event_share=top1_share,
        config=config,
    )


def run_key_factor_scan(
    *,
    df: pd.DataFrame,
    feature_registry: Sequence[Dict[str, object]],
    config: ScanConfig,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]], List[Dict[str, object]]]:
    allowed_features: List[str] = []
    seen: set[str] = set()
    for row in feature_registry:
        if int(row.get("allowed_in_scan", 0)) != 1:
            continue
        feat = str(row.get("feature_name", "")).strip()
        if not feat:
            continue
        if feat not in df.columns:
            continue
        if feat in seen:
            continue
        seen.add(feat)
        allowed_features.append(feat)
    if config.max_features > 0:
        allowed_features = allowed_features[: config.max_features]

    base_controls: List[str] = []
    if config.include_base_controls:
        base_controls = [c for c in config.base_controls if c in df.columns]
    base_control_spec_enabled = bool(config.include_base_controls and len(base_controls) > 0)
    spec_mode = str(getattr(config, "control_spec_mode", "both") or "both").strip().lower()
    if spec_mode not in {"both", "key_only", "key_plus_base_controls"}:
        raise ValueError(f"unsupported control_spec_mode: {spec_mode}")
    if spec_mode == "key_plus_base_controls" and not base_control_spec_enabled:
        raise ValueError(
            "control_spec_mode=key_plus_base_controls requires include_base_controls=true and "
            "at least one base control column present in data"
        )
    base_controls_min_events_per_exog = max(int(config.base_controls_min_events_per_exog), 1)
    base_controls_min_policy_docs_per_exog = max(int(config.base_controls_min_policy_docs_per_exog), 1)
    n_exog_plus_base = int(1 + len(base_controls)) if base_control_spec_enabled else 0
    min_events_for_plus_base = (
        max(
            int(config.min_informative_events_estimable),
            int(n_exog_plus_base * base_controls_min_events_per_exog),
        )
        if base_control_spec_enabled
        else 0
    )
    min_docs_for_plus_base = (
        max(
            int(config.min_policy_docs_informative_estimable),
            int(n_exog_plus_base * base_controls_min_policy_docs_per_exog),
        )
        if base_control_spec_enabled
        else 0
    )

    raw_contexts = list(config.contexts) if config.contexts else []
    contexts: List[Tuple[str, str]] = []
    skipped_contexts: List[Dict[str, str]] = []
    seen_contexts: set[Tuple[str, str]] = set()
    for row in raw_contexts:
        if not isinstance(row, (tuple, list)) or len(row) != 2:
            skipped_contexts.append(
                {
                    "context_scope": "",
                    "y_col": "",
                    "reason_code": "invalid_context_definition",
                    "reason_detail": str(row),
                }
            )
            continue
        context_scope = str(row[0]).strip()
        y_col = str(row[1]).strip()
        if not context_scope or not y_col:
            skipped_contexts.append(
                {
                    "context_scope": context_scope,
                    "y_col": y_col,
                    "reason_code": "invalid_context_definition",
                    "reason_detail": "context_scope_or_y_col_empty",
                }
            )
            continue
        key = (context_scope, y_col)
        if key in seen_contexts:
            skipped_contexts.append(
                {
                    "context_scope": context_scope,
                    "y_col": y_col,
                    "reason_code": "duplicate_context_definition",
                    "reason_detail": "duplicate context_scope+y_col pair",
                }
            )
            continue
        seen_contexts.add(key)
        if y_col not in df.columns:
            skipped_contexts.append(
                {
                    "context_scope": context_scope,
                    "y_col": y_col,
                    "reason_code": "missing_context_y_column",
                    "reason_detail": f"y column not found in data: {y_col}",
                }
            )
            continue
        contexts.append(key)
    if not contexts:
        raise ValueError("no valid contexts available for scan")

    tracks = sorted(df["track"].dropna().astype(str).unique())
    split_id = str(df["split_id"].dropna().astype(str).iloc[0]) if "split_id" in df.columns else ""

    candidate_plan: List[Dict[str, object]] = []
    skipped_candidates: List[Dict[str, object]] = []
    skipped_track_contexts: List[Dict[str, object]] = []
    skipped_control_specs: List[Dict[str, object]] = []
    eq_seen: Dict[str, str] = {}
    track_split_cache: Dict[Tuple[str, str], pd.DataFrame] = {}
    for track in tracks:
        track_df = df[df["track"].astype(str) == track].copy()
        for split_role in ("discovery", "validation"):
            track_split_cache[(track, split_role)] = track_df[track_df["split_role"] == split_role].copy()
    discovery_capacity_by_track_y: Dict[Tuple[str, str], Dict[str, int]] = {}
    if config.skip_discovery_infeasible_track_y:
        for track in tracks:
            discovery_df = track_split_cache[(track, "discovery")]
            for _, y_col in contexts:
                key = (track, y_col)
                if key in discovery_capacity_by_track_y:
                    continue
                discovery_capacity_by_track_y[key] = _informative_capacity_for_y(
                    discovery_df,
                    y_col=y_col,
                )
    for track in tracks:
        for context_scope, y_col in contexts:
            if config.skip_discovery_infeasible_track_y:
                cap = discovery_capacity_by_track_y.get((track, y_col), {})
                cap_events = int(cap.get("n_informative_events", 0))
                cap_docs = int(cap.get("n_policy_docs_informative", 0))
                low_events = cap_events < int(config.min_informative_events_estimable)
                low_docs = cap_docs < int(config.min_policy_docs_informative_estimable)
                if low_events or low_docs:
                    fail_parts: List[str] = []
                    if low_events:
                        fail_parts.append(
                            f"events {cap_events} < gate {int(config.min_informative_events_estimable)}"
                        )
                    if low_docs:
                        fail_parts.append(
                            f"policy_docs {cap_docs} < gate {int(config.min_policy_docs_informative_estimable)}"
                        )
                    skipped_track_contexts.append(
                        {
                            "track": track,
                            "context_scope": context_scope,
                            "y_col": y_col,
                            "reason_code": "discovery_gate_infeasible_for_track_y",
                            "reason_detail": "; ".join(fail_parts),
                        }
                    )
                    continue
            context_control_specs: List[Tuple[str, List[str]]] = []
            if spec_mode in {"both", "key_only"}:
                context_control_specs.append(("clogit_key_only", []))
            if spec_mode in {"both", "key_plus_base_controls"} and base_control_spec_enabled:
                allow_plus_base = True
                cap = discovery_capacity_by_track_y.get((track, y_col))
                if cap is None:
                    cap = _informative_capacity_for_y(
                        track_split_cache[(track, "discovery")],
                        y_col=y_col,
                    )
                    discovery_capacity_by_track_y[(track, y_col)] = cap
                cap_events = int(cap.get("n_informative_events", 0))
                cap_docs = int(cap.get("n_policy_docs_informative", 0))
                if config.auto_disable_base_controls_low_capacity and spec_mode != "key_plus_base_controls":
                    allow_plus_base = (
                        cap_events >= int(min_events_for_plus_base)
                        and cap_docs >= int(min_docs_for_plus_base)
                    )
                    if not allow_plus_base:
                        skipped_control_specs.append(
                            {
                                "track": track,
                                "context_scope": context_scope,
                                "y_col": y_col,
                                "spec_id": "clogit_key_plus_base_controls",
                                "reason_code": "disabled_base_controls_low_discovery_capacity",
                                "reason_detail": (
                                    f"events {cap_events} < {int(min_events_for_plus_base)} or "
                                    f"policy_docs {cap_docs} < {int(min_docs_for_plus_base)}"
                                ),
                            }
                        )
                if allow_plus_base:
                    context_control_specs.append(("clogit_key_plus_base_controls", base_controls))
            for spec_id, controls in context_control_specs:
                fdr_family_id = f"{track}|{context_scope}|{y_col}|{spec_id}"
                for feature in allowed_features:
                    exog_cols = [feature, *[c for c in controls if c != feature]]
                    candidate_id = f"{track}|{context_scope}|{y_col}|{spec_id}|{feature}"
                    equivalence_hash = _equivalence_hash(
                        track=track,
                        context_scope=context_scope,
                        y_col=y_col,
                        exog_cols=exog_cols,
                    )
                    if equivalence_hash in eq_seen:
                        skipped_candidates.append(
                            {
                                "candidate_id": candidate_id,
                                "duplicate_of_candidate_id": eq_seen[equivalence_hash],
                                "equivalence_hash": equivalence_hash,
                                "track": track,
                                "context_scope": context_scope,
                                "y_col": y_col,
                                "spec_id": spec_id,
                                "key_factor": feature,
                            }
                        )
                        continue
                    eq_seen[equivalence_hash] = candidate_id
                    candidate_plan.append(
                        {
                            "track": track,
                            "context_scope": context_scope,
                            "y_col": y_col,
                            "spec_id": spec_id,
                            "feature": feature,
                            "exog_cols": exog_cols,
                            "fdr_family_id": fdr_family_id,
                            "candidate_id": candidate_id,
                            "equivalence_hash": equivalence_hash,
                        }
                    )
    candidate_pool_size = len(candidate_plan)
    if bool(config.validation_used_for_search):
        raise ValueError("validation_used_for_search must remain false in run_key_factor_scan")
    if not bool(config.candidate_pool_locked_pre_validation):
        raise ValueError(
            "candidate_pool_locked_pre_validation must remain true to preserve holdout integrity"
        )

    # Holdout-leakage guard: candidate pool is fixed before any split evaluation.
    search_log_rows: List[Dict[str, object]] = [
        {
            "run_id": config.run_id,
            "candidate_id": "",
            "track": "",
            "context_scope": "",
            "y_col": "",
            "split_role": "plan",
            "spec_id": "",
            "key_factor": "",
            "status": "search_governance_contract",
            "reason_stage": "plan",
            "reason_code": "validation_not_used_for_search",
            "reason_detail": (
                f"candidate_pool_locked_pre_validation=true; "
                f"candidate_pool_size={candidate_pool_size}"
            ),
            "bootstrap_success": None,
            "bootstrap_attempted": None,
            "candidate_elapsed_ms": None,
            "effective_n_bootstrap": None,
            "candidate_eval_order": None,
            "candidate_pool_size": candidate_pool_size,
            "equivalence_hash": "",
            "validation_used_for_search": 0,
            "candidate_pool_locked_pre_validation": 1,
            "timestamp": config.timestamp,
        }
    ]

    # Discovery pass runs first on the fixed candidate pool.
    scan_rows: List[Dict[str, object]] = []
    candidate_eval_order = 0
    for spec in candidate_plan:
        split_role = "discovery"
        split_df = track_split_cache[(str(spec["track"]), split_role)]
        candidate_eval_order += 1
        candidate_started_at = perf_counter()
        row = _scan_one_split(
            df=split_df,
            y_col=str(spec["y_col"]),
            track=str(spec["track"]),
            context_scope=str(spec["context_scope"]),
            split_id=split_id,
            split_role=split_role,
            spec_id=str(spec["spec_id"]),
            key_factor=str(spec["feature"]),
            control_set_name="+".join(list(spec["exog_cols"])),
            exog_cols=list(spec["exog_cols"]),
            fdr_family_id=str(spec["fdr_family_id"]),
            candidate_id=str(spec["candidate_id"]),
            config=config,
        )
        row["candidate_elapsed_ms"] = int(round((perf_counter() - candidate_started_at) * 1000.0))
        row["effective_n_bootstrap"] = int(row.get("bootstrap_success") or 0)
        row["candidate_eval_order"] = candidate_eval_order
        row["candidate_pool_size"] = candidate_pool_size
        row["equivalence_hash"] = str(spec["equivalence_hash"])
        row["y_col"] = str(spec["y_col"])
        scan_rows.append(row)
    search_log_rows.append(
        {
            "run_id": config.run_id,
            "candidate_id": "",
            "track": "",
            "context_scope": "",
            "y_col": "",
            "split_role": "plan",
            "spec_id": "",
            "key_factor": "",
            "status": "search_governance_contract",
            "reason_stage": "schedule",
            "reason_code": "validation_runs_after_discovery_on_locked_pool",
            "reason_detail": f"discovery_candidates_evaluated={candidate_eval_order}",
            "bootstrap_success": None,
            "bootstrap_attempted": None,
            "candidate_elapsed_ms": None,
            "effective_n_bootstrap": None,
            "candidate_eval_order": None,
            "candidate_pool_size": candidate_pool_size,
            "equivalence_hash": "",
            "validation_used_for_search": 0,
            "candidate_pool_locked_pre_validation": 1,
            "timestamp": config.timestamp,
        }
    )
    # Validation pass runs after discovery and never mutates candidate_plan.
    for spec in candidate_plan:
        split_role = "validation"
        split_df = track_split_cache[(str(spec["track"]), split_role)]
        candidate_eval_order += 1
        candidate_started_at = perf_counter()
        row = _scan_one_split(
            df=split_df,
            y_col=str(spec["y_col"]),
            track=str(spec["track"]),
            context_scope=str(spec["context_scope"]),
            split_id=split_id,
            split_role=split_role,
            spec_id=str(spec["spec_id"]),
            key_factor=str(spec["feature"]),
            control_set_name="+".join(list(spec["exog_cols"])),
            exog_cols=list(spec["exog_cols"]),
            fdr_family_id=str(spec["fdr_family_id"]),
            candidate_id=str(spec["candidate_id"]),
            config=config,
        )
        row["candidate_elapsed_ms"] = int(round((perf_counter() - candidate_started_at) * 1000.0))
        row["effective_n_bootstrap"] = int(row.get("bootstrap_success") or 0)
        row["candidate_eval_order"] = candidate_eval_order
        row["candidate_pool_size"] = candidate_pool_size
        row["equivalence_hash"] = str(spec["equivalence_hash"])
        row["y_col"] = str(spec["y_col"])
        scan_rows.append(row)

    validation_rows = [
        row
        for row in scan_rows
        if row.get("split_role") == "validation" and row.get("status") == "ok"
    ]
    attach_bh_qvalues(validation_rows, p_col="p_boot", family_col="fdr_family_id")
    q_lookup = {
        (str(r["candidate_id"]), str(r["split_role"])): r.get("q_value")
        for r in validation_rows
    }
    for row in scan_rows:
        key = (str(row["candidate_id"]), str(row["split_role"]))
        if key in q_lookup:
            row["q_value"] = q_lookup[key]

    by_candidate: Dict[str, Dict[str, Dict[str, object]]] = {}
    for row in scan_rows:
        cid = str(row["candidate_id"])
        role = str(row["split_role"])
        by_candidate.setdefault(cid, {})[role] = row

    top_rows: List[Dict[str, object]] = []
    for cid, blocks in by_candidate.items():
        disc = blocks.get("discovery")
        val = blocks.get("validation")
        disc_ok = bool(disc is not None and disc.get("status") == "ok")
        val_ok = bool(val is not None and val.get("status") == "ok")
        q_val = val.get("q_value") if val else None
        q_pass = bool(q_val is not None and float(q_val) <= config.q_threshold) if val_ok else False
        p_val = val.get("p_boot") if val else None
        p_pass = bool(p_val is not None and float(p_val) <= config.p_threshold) if val_ok else False
        validated = bool(val_ok and q_pass and p_pass)

        if validated:
            tier = "validated_candidate"
        elif disc_ok:
            tier = "support_candidate"
        else:
            tier = "exploratory"

        for role, row in blocks.items():
            row["candidate_tier"] = tier

        ref = val if val is not None else disc
        if ref is None:
            continue
        top_rows.append(
            {
                "run_id": config.run_id,
                "candidate_id": cid,
                "track": ref.get("track"),
                "context_scope": ref.get("context_scope"),
                "y_col": ref.get("y_col"),
                "spec_id": ref.get("spec_id"),
                "key_factor": ref.get("key_factor"),
                "control_set": ref.get("control_set"),
                "fdr_family_id": ref.get("fdr_family_id"),
                "status_discovery": disc.get("status") if disc else "",
                "status_validation": val.get("status") if val else "",
                "p_boot_discovery": disc.get("p_boot") if disc else None,
                "p_boot_validation": val.get("p_boot") if val else None,
                "q_value_validation": q_val,
                "candidate_elapsed_ms_discovery": disc.get("candidate_elapsed_ms") if disc else None,
                "candidate_elapsed_ms_validation": val.get("candidate_elapsed_ms") if val else None,
                "effective_n_bootstrap_discovery": disc.get("effective_n_bootstrap") if disc else None,
                "effective_n_bootstrap_validation": val.get("effective_n_bootstrap") if val else None,
                "beta_discovery": disc.get("beta") if disc else None,
                "beta_validation": val.get("beta") if val else None,
                "score_discovery": disc.get("score") if disc else None,
                "score_validation": val.get("score") if val else None,
                "validation_pass_p": p_pass,
                "validation_pass_q": q_pass,
                "candidate_tier": tier,
                "support_only": 1,
                "not_replacing_confirmatory_claim": 1,
                "validation_used_for_search": int(bool(config.validation_used_for_search)),
                "candidate_pool_locked_pre_validation": int(bool(config.candidate_pool_locked_pre_validation)),
                "candidate_pool_size": ref.get("candidate_pool_size"),
                "equivalence_hash": ref.get("equivalence_hash"),
                "candidate_eval_order_discovery": disc.get("candidate_eval_order") if disc else None,
                "candidate_eval_order_validation": val.get("candidate_eval_order") if val else None,
                "data_hash": config.data_hash,
                "config_hash": config.config_hash,
                "feature_registry_hash": config.feature_registry_hash,
                "git_commit": config.git_commit,
                "timestamp": config.timestamp,
            }
        )

    tier_order = {"validated_candidate": 0, "support_candidate": 1, "exploratory": 2}
    top_rows.sort(
        key=lambda r: (
            tier_order.get(str(r.get("candidate_tier")), 9),
            float(r.get("q_value_validation")) if r.get("q_value_validation") is not None else inf,
            -(float(r.get("score_validation")) if r.get("score_validation") is not None else -inf),
        )
    )

    for row in scan_rows:
        search_log_rows.append(
            {
                "run_id": row.get("run_id"),
                "candidate_id": row.get("candidate_id"),
                "track": row.get("track"),
                "context_scope": row.get("context_scope"),
                "y_col": row.get("y_col"),
                "split_role": row.get("split_role"),
                "spec_id": row.get("spec_id"),
                "key_factor": row.get("key_factor"),
                "status": row.get("status"),
                "reason_stage": row.get("reason_stage"),
                "reason_code": row.get("reason_code"),
                "reason_detail": row.get("reason_detail"),
                "bootstrap_success": row.get("bootstrap_success"),
                "bootstrap_attempted": row.get("bootstrap_attempted"),
                "candidate_elapsed_ms": row.get("candidate_elapsed_ms"),
                "effective_n_bootstrap": row.get("effective_n_bootstrap"),
                "candidate_eval_order": row.get("candidate_eval_order"),
                "candidate_pool_size": row.get("candidate_pool_size"),
                "equivalence_hash": row.get("equivalence_hash"),
                "validation_used_for_search": row.get("validation_used_for_search"),
                "candidate_pool_locked_pre_validation": row.get("candidate_pool_locked_pre_validation"),
                "timestamp": row.get("timestamp"),
            }
        )
    for skipped in skipped_candidates:
        search_log_rows.append(
            {
                "run_id": config.run_id,
                "candidate_id": skipped.get("candidate_id"),
                "track": skipped.get("track"),
                "context_scope": skipped.get("context_scope"),
                "y_col": skipped.get("y_col"),
                "split_role": "plan",
                "spec_id": skipped.get("spec_id"),
                "key_factor": skipped.get("key_factor"),
                "status": "skipped_equivalent_spec",
                "reason_stage": "plan",
                "reason_code": "skipped_equivalent_spec",
                "reason_detail": f"duplicate_of={skipped.get('duplicate_of_candidate_id')}",
                "bootstrap_success": None,
                "bootstrap_attempted": None,
                "candidate_elapsed_ms": None,
                "effective_n_bootstrap": None,
                "candidate_eval_order": None,
                "candidate_pool_size": candidate_pool_size,
                "equivalence_hash": skipped.get("equivalence_hash"),
                "validation_used_for_search": int(bool(config.validation_used_for_search)),
                "candidate_pool_locked_pre_validation": int(bool(config.candidate_pool_locked_pre_validation)),
                "timestamp": config.timestamp,
            }
        )
    for skipped in skipped_contexts:
        search_log_rows.append(
            {
                "run_id": config.run_id,
                "candidate_id": "",
                "track": "",
                "context_scope": skipped.get("context_scope"),
                "y_col": skipped.get("y_col"),
                "split_role": "plan",
                "spec_id": "",
                "key_factor": "",
                "status": "skipped_context",
                "reason_stage": "plan",
                "reason_code": skipped.get("reason_code"),
                "reason_detail": skipped.get("reason_detail"),
                "bootstrap_success": None,
                "bootstrap_attempted": None,
                "candidate_elapsed_ms": None,
                "effective_n_bootstrap": None,
                "candidate_eval_order": None,
                "candidate_pool_size": candidate_pool_size,
                "equivalence_hash": "",
                "validation_used_for_search": int(bool(config.validation_used_for_search)),
                "candidate_pool_locked_pre_validation": int(bool(config.candidate_pool_locked_pre_validation)),
                "timestamp": config.timestamp,
            }
        )
    for skipped in skipped_track_contexts:
        search_log_rows.append(
            {
                "run_id": config.run_id,
                "candidate_id": "",
                "track": skipped.get("track"),
                "context_scope": skipped.get("context_scope"),
                "y_col": skipped.get("y_col"),
                "split_role": "plan",
                "spec_id": "",
                "key_factor": "",
                "status": "skipped_track_context",
                "reason_stage": "plan",
                "reason_code": skipped.get("reason_code"),
                "reason_detail": skipped.get("reason_detail"),
                "bootstrap_success": None,
                "bootstrap_attempted": None,
                "candidate_elapsed_ms": None,
                "effective_n_bootstrap": None,
                "candidate_eval_order": None,
                "candidate_pool_size": candidate_pool_size,
                "equivalence_hash": "",
                "validation_used_for_search": int(bool(config.validation_used_for_search)),
                "candidate_pool_locked_pre_validation": int(bool(config.candidate_pool_locked_pre_validation)),
                "timestamp": config.timestamp,
            }
        )
    for skipped in skipped_control_specs:
        search_log_rows.append(
            {
                "run_id": config.run_id,
                "candidate_id": "",
                "track": skipped.get("track"),
                "context_scope": skipped.get("context_scope"),
                "y_col": skipped.get("y_col"),
                "split_role": "plan",
                "spec_id": skipped.get("spec_id"),
                "key_factor": "",
                "status": "skipped_control_spec",
                "reason_stage": "plan",
                "reason_code": skipped.get("reason_code"),
                "reason_detail": skipped.get("reason_detail"),
                "bootstrap_success": None,
                "bootstrap_attempted": None,
                "candidate_elapsed_ms": None,
                "effective_n_bootstrap": None,
                "candidate_eval_order": None,
                "candidate_pool_size": candidate_pool_size,
                "equivalence_hash": "",
                "validation_used_for_search": int(bool(config.validation_used_for_search)),
                "candidate_pool_locked_pre_validation": int(bool(config.candidate_pool_locked_pre_validation)),
                "timestamp": config.timestamp,
            }
        )
    return scan_rows, top_rows, search_log_rows
