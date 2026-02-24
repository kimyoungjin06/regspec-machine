from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path

import pandas as pd
import pytest


def _load_phase_b_runner_module():
    root = Path(__file__).resolve().parents[4]
    runner_path = root / "scripts" / "modeling" / "run_phase_b_bikard_machine_scientist_scan.py"
    if not runner_path.exists():
        pytest.skip("phase-b runner script is not available in this environment")
    spec = importlib.util.spec_from_file_location("phase_b_runner_module", runner_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load module spec: {runner_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_y_feasibility_mode_fail_policies() -> None:
    mod = _load_phase_b_runner_module()
    meta = {
        "unusable_y_cols": ["y_unusable"],
        "below_floor_y_cols": ["y_sparse"],
    }

    out = mod._apply_y_feasibility_mode(mode="warn", y_validated_gate_meta=meta)
    assert out["y_unusable_y_cols"] == ["y_unusable"]
    assert out["y_below_floor_y_cols"] == ["y_sparse"]

    with pytest.raises(ValueError, match="unusable y contexts"):
        mod._apply_y_feasibility_mode(mode="fail_unusable", y_validated_gate_meta=meta)

    with pytest.raises(ValueError, match="below floor or unusable"):
        mod._apply_y_feasibility_mode(mode="fail_below_floor", y_validated_gate_meta=meta)


def test_restart_stability_rows_rates_and_meta() -> None:
    mod = _load_phase_b_runner_module()
    top_rows = [
        {
            "candidate_id": "cand_a",
            "restart_id": 1,
            "candidate_tier": "validated_candidate",
            "track": "primary_strict",
            "context_scope": "all_contexts",
            "y_col": "y_all",
            "spec_id": "clogit_key_only",
            "key_factor": "x_a",
            "control_set": "x_a",
            "beta_validation": 0.5,
            "beta_discovery": 0.4,
        },
        {
            "candidate_id": "cand_a",
            "restart_id": 2,
            "candidate_tier": "validated_candidate",
            "track": "primary_strict",
            "context_scope": "all_contexts",
            "y_col": "y_all",
            "spec_id": "clogit_key_only",
            "key_factor": "x_a",
            "control_set": "x_a",
            "beta_validation": 0.7,
            "beta_discovery": 0.6,
        },
        {
            "candidate_id": "cand_b",
            "restart_id": 1,
            "candidate_tier": "exploratory",
            "track": "primary_strict",
            "context_scope": "all_contexts",
            "y_col": "y_all",
            "spec_id": "clogit_key_only",
            "key_factor": "x_b",
            "control_set": "x_b",
            "beta_validation": None,
            "beta_discovery": None,
        },
    ]

    rows, meta = mod._build_restart_stability_rows(top_rows=top_rows, n_restarts=2)
    by_id = {str(r.get("candidate_id", "")): r for r in rows}
    assert set(by_id.keys()) == {"cand_a", "cand_b"}

    cand_a = by_id["cand_a"]
    assert cand_a["n_restarts_total"] == 2
    assert cand_a["n_restarts_present"] == 2
    assert cand_a["presence_rate"] == 1.0
    assert cand_a["n_validated"] == 2
    assert cand_a["validated_rate"] == 1.0
    assert cand_a["support_or_better_rate"] == 1.0
    assert cand_a["stability_score"] == 1.0

    cand_b = by_id["cand_b"]
    assert cand_b["n_restarts_total"] == 2
    assert cand_b["n_restarts_present"] == 1
    assert cand_b["presence_rate"] == 0.5
    assert cand_b["n_validated"] == 0
    assert cand_b["validated_rate"] == 0.0
    assert cand_b["n_support_or_better"] == 0
    assert cand_b["support_or_better_rate"] == 0.0
    assert cand_b["stability_score"] == 0.475

    assert meta["n_candidates"] == 2
    assert meta["n_candidates_stability_ge_0_75"] == 1
    assert meta["n_candidates_validated_rate_ge_0_50"] == 1


def test_legacy_single_gate_sync_behavior() -> None:
    mod = _load_phase_b_runner_module()
    args = argparse.Namespace(
        min_informative_events_estimable=20,
        min_policy_docs_informative_estimable=10,
        min_informative_events_validated=100,
        min_policy_docs_informative_validated=30,
        min_informative_events=40,
        min_policy_docs_informative=15,
        legacy_single_gate_sync_validation=False,
    )

    min_e_est, min_d_est, min_e_val_req, min_d_val_req, meta = mod._resolve_stage_gate_thresholds(args)
    assert min_e_est == 40
    assert min_d_est == 15
    assert min_e_val_req == 100
    assert min_d_val_req == 30
    assert meta["validated_gate_source"] == "split_gate_with_legacy_estimable_override"
    assert meta["legacy_override_applied_to_validated"] == 0

    args.legacy_single_gate_sync_validation = True
    min_e_est2, min_d_est2, min_e_val_req2, min_d_val_req2, meta2 = mod._resolve_stage_gate_thresholds(args)
    assert min_e_est2 == 40
    assert min_d_est2 == 15
    assert min_e_val_req2 == 40
    assert min_d_val_req2 == 15
    assert meta2["validated_gate_source"] == "legacy_single_gate_synced"
    assert meta2["legacy_override_applied_to_validated"] == 1


def test_time_series_precheck_detects_redundancy_and_low_track_support() -> None:
    mod = _load_phase_b_runner_module()
    df = pd.DataFrame(
        {
            "track": ["A", "A", "A", "B", "B", "B"],
            "pub_year_alt": [2001, 2002, 2003, 2001, 2002, 2003],
            "recency_years_alt": [2.1, 3.2, 4.4, 1.9, 2.7, 3.8],
            "y_3y": [1, 0, 0, 1, 1, 1],
            "y_5y": [1, 1, 1, 1, 1, 1],
            "y_10y": [1, 1, 1, 1, 1, 1],
        }
    )
    y_contexts = [
        ("all_contexts_3y", "y_3y"),
        ("all_contexts_5y", "y_5y"),
        ("all_contexts_10y", "y_10y"),
    ]
    meta = mod._build_time_series_precheck(
        data=df,
        y_contexts=y_contexts,
        confirmatory_y_cols=["y_3y", "y_5y", "y_10y"],
        min_positive_events=2,
        min_track_positive_events=3,
        min_positive_share=0.10,
    )
    assert meta["summary"]["n_y_cols_considered"] == 3
    assert meta["summary"]["n_redundant_groups"] == 1
    assert meta["summary"]["n_confirmatory_redundant_groups"] == 1
    redundant = meta["confirmatory_redundant_groups"][0]
    assert set(redundant["y_cols"]) == {"y_5y", "y_10y"}
    assert redundant["recommended_keep"] == "y_5y"
    assert meta["recommended_confirmatory_y_cols"] == ["y_3y", "y_5y"]
    assert meta["low_support_y_cols"] == []
    assert {"y_col": "y_3y", "track": "A"} in meta["low_support_track_pairs"]
    assert "pub_year_alt" in meta["time_columns_detected"]["by_name"]
    assert "recency_years_alt" in meta["time_columns_detected"]["by_name"]


def test_time_series_precheck_mode_fail_policies() -> None:
    mod = _load_phase_b_runner_module()
    precheck_meta = {
        "confirmatory_redundant_groups": [{"y_cols": ["y_5y", "y_10y"]}],
        "low_support_y_cols": ["y_3y"],
        "low_support_track_pairs": [{"y_col": "y_3y", "track": "primary_strict"}],
    }
    warn_out = mod._apply_time_series_precheck_mode(
        mode="warn",
        precheck_meta=precheck_meta,
    )
    assert warn_out["triggered_redundant_confirmatory"] is True
    assert warn_out["triggered_low_support"] is True

    off_out = mod._apply_time_series_precheck_mode(
        mode="off",
        precheck_meta=precheck_meta,
    )
    assert off_out["triggered_redundant_confirmatory"] is False
    assert off_out["triggered_low_support"] is False

    with pytest.raises(ValueError, match="time-series precheck failed"):
        mod._apply_time_series_precheck_mode(
            mode="fail_redundant_confirmatory",
            precheck_meta=precheck_meta,
        )

    with pytest.raises(ValueError, match="time-series precheck failed"):
        mod._apply_time_series_precheck_mode(
            mode="fail_low_support",
            precheck_meta=precheck_meta,
        )

    with pytest.raises(ValueError, match="time-series precheck failed"):
        mod._apply_time_series_precheck_mode(
            mode="fail_any",
            precheck_meta=precheck_meta,
        )


def test_resolve_effective_confirmatory_y_cols_auto_policy() -> None:
    mod = _load_phase_b_runner_module()
    y_contexts = [
        ("all_contexts_3y", "y_3y"),
        ("all_contexts_5y", "y_5y"),
        ("all_contexts_10y", "y_10y"),
    ]
    precheck_meta = {
        "recommended_confirmatory_y_cols": ["y_3y", "y_5y"],
        "low_support_y_cols": [],
        "low_support_track_y_cols": ["y_3y"],
    }

    effective, meta = mod._resolve_effective_confirmatory_y_cols(
        requested_confirmatory_y_cols=["y_3y", "y_5y"],
        y_contexts=y_contexts,
        precheck_meta=precheck_meta,
        auto_policy="drop_redundant_and_low_support",
    )
    assert effective == ["y_5y"]
    assert meta["dropped_low_support_y_cols"] == ["y_3y"]
    assert meta["fallback_applied"] is False

    effective_off, meta_off = mod._resolve_effective_confirmatory_y_cols(
        requested_confirmatory_y_cols=["y_3y", "y_5y"],
        y_contexts=y_contexts,
        precheck_meta=precheck_meta,
        auto_policy="off",
    )
    assert effective_off == ["y_3y", "y_5y"]
    assert meta_off["dropped_low_support_y_cols"] == []


def test_nonconfirmatory_tier_cap_enforced_for_support_candidate() -> None:
    mod = _load_phase_b_runner_module()
    top_rows = [
        {
            "run_id": "ut",
            "candidate_id": "cand_confirm",
            "restart_id": 1,
            "track": "primary_strict",
            "context_scope": "all_contexts_5y",
            "y_col": "y_5y",
            "spec_id": "clogit_key_only",
            "key_factor": "is_academia_origin",
            "control_set": "is_academia_origin",
            "fdr_family_id": "fam_confirm",
            "status_discovery": "ok",
            "status_validation": "ok",
            "p_boot_discovery": 0.2,
            "p_boot_validation": 0.2,
            "beta_discovery": 0.1,
            "beta_validation": 0.1,
            "score_discovery": -1.0,
            "score_validation": -1.0,
        },
        {
            "run_id": "ut",
            "candidate_id": "cand_nonconfirm",
            "restart_id": 1,
            "track": "primary_strict",
            "context_scope": "all_contexts_10y",
            "y_col": "y_10y",
            "spec_id": "clogit_key_only",
            "key_factor": "is_academia_origin",
            "control_set": "is_academia_origin",
            "fdr_family_id": "fam_nonconfirm",
            "status_discovery": "ok",
            "status_validation": "ok",
            "p_boot_discovery": 0.2,
            "p_boot_validation": 0.2,
            "beta_discovery": 0.1,
            "beta_validation": 0.1,
            "score_discovery": -1.0,
            "score_validation": -1.0,
        },
    ]
    out, meta = mod._build_restart_inference_rows(
        top_rows=top_rows,
        n_restarts=1,
        p_threshold=0.05,
        q_threshold=0.10,
        confirmatory_y_cols=["y_5y"],
        nonconfirmatory_max_tier="exploratory",
    )
    by_id = {str(r.get("candidate_id", "")): r for r in out}
    assert set(by_id.keys()) == {"cand_confirm", "cand_nonconfirm"}
    confirm = by_id["cand_confirm"]
    nonconfirm = by_id["cand_nonconfirm"]

    assert int(confirm["confirmatory_eligible"]) == 1
    assert str(confirm["candidate_tier"]) == "support_candidate"
    assert int(confirm["confirmatory_policy_demoted"]) == 0

    assert int(nonconfirm["confirmatory_eligible"]) == 0
    assert str(nonconfirm["candidate_tier_raw"]) == "exploratory"
    assert str(nonconfirm["candidate_tier"]) == "exploratory"
    assert int(nonconfirm["confirmatory_policy_demoted"]) == 1
    assert str(nonconfirm["confirmatory_policy_reason"]) == "nonconfirmatory_y_tier_capped"
    assert int(meta["n_candidates_demoted_nonconfirmatory"]) >= 1


def test_runtime_stage_helpers_emit_expected_shape() -> None:
    mod = _load_phase_b_runner_module()
    stage_rows = []
    started_at = mod.perf_counter()
    elapsed_ms = mod._record_runtime_stage(
        stage_rows=stage_rows,
        stage="unit_stage",
        started_at=started_at,
        unit_flag=True,
    )
    assert isinstance(elapsed_ms, int)
    assert elapsed_ms >= 0
    assert len(stage_rows) == 1
    assert stage_rows[0]["stage"] == "unit_stage"
    assert stage_rows[0]["unit_flag"] is True

    log_row = mod._runtime_stage_log_row(
        run_id="unit_run",
        timestamp="2026-02-24T00:00:00Z",
        stage_row=stage_rows[0],
    )
    assert log_row["run_id"] == "unit_run"
    assert log_row["status"] == "runtime_stage"
    assert log_row["reason_code"] == "runtime_stage::unit_stage"
    assert log_row["candidate_elapsed_ms"] == stage_rows[0]["elapsed_ms"]
