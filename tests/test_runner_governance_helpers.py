from __future__ import annotations

import importlib.util
from pathlib import Path

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
