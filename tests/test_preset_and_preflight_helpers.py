from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


def _load_module(module_path: Path, name: str):
    if not module_path.exists():
        pytest.skip(f"module not found: {module_path}")
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load spec for {module_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _root() -> Path:
    return Path(__file__).resolve().parents[4]


def test_disk_preflight_helper_pass_and_fail() -> None:
    root = _root()
    runner = _load_module(
        root / "scripts" / "modeling" / "run_phase_b_bikard_machine_scientist_scan.py",
        "phase_b_runner_module_preflight",
    )

    ok = runner._check_disk_space_or_raise(
        probe_paths=[str(root / "data")],
        min_free_space_mb=1,
        stage="unit_test_ok",
    )
    assert ok["stage"] == "unit_test_ok"
    assert int(ok["min_free_space_mb_observed"]) >= 1
    assert isinstance(ok["checks"], list) and len(ok["checks"]) >= 1

    with pytest.raises(RuntimeError, match="insufficient free disk space"):
        runner._check_disk_space_or_raise(
            probe_paths=[str(root / "data")],
            min_free_space_mb=10**12,
            stage="unit_test_fail",
        )


def test_paired_summary_helpers(tmp_path: Path) -> None:
    root = _root()
    preset = _load_module(
        root / "scripts" / "modeling" / "run_phase_b_regspec_preset.py",
        "phase_b_preset_module_summary",
    )

    path = preset._resolve_paired_summary_path("example run id", "")
    assert path.name.startswith("phase_b_bikard_machine_scientist_paired_preset_summary_")
    assert path.suffix == ".json"

    explicit = tmp_path / "paired_summary_unit_test.json"
    payload = {"mode": "paired_nooption_singlex", "run_id": "ut", "status": "ok"}
    preset._write_paired_summary(explicit, payload)
    assert explicit.exists()
    text = explicit.read_text(encoding="utf-8")
    assert '"status": "ok"' in text


def test_paired_child_outputs_contract() -> None:
    root = _root()
    preset = _load_module(
        root / "scripts" / "modeling" / "run_phase_b_regspec_preset.py",
        "phase_b_preset_module_outputs_contract",
    )

    outputs = preset._build_paired_child_outputs("unit test run")
    assert outputs["run_summary_json"].endswith(
        "phase_b_bikard_machine_scientist_run_summary_unit_test_run.json"
    )
    assert "scan_runs_csv" in outputs
    assert "top_models_csv" in outputs
    assert "search_log_jsonl" in outputs


def test_paired_common_tail_forwards_legacy_sync_flag() -> None:
    root = _root()
    preset = _load_module(
        root / "scripts" / "modeling" / "run_phase_b_regspec_preset.py",
        "phase_b_preset_module_common_tail",
    )

    args = SimpleNamespace(
        runner_python=".venv/bin/python",
        cli_summary_top_n=3,
        scan_n_bootstrap=0,
        scan_max_features=0,
        paired_legacy_sync_validation=True,
        extra_arg=[],
    )
    tail = preset._build_paired_common_tail(args)
    assert "--extra-arg=--legacy-single-gate-sync-validation" in tail
    args.paired_legacy_sync_validation = False
    tail_no_sync = preset._build_paired_common_tail(args)
    assert "--extra-arg=--legacy-single-gate-sync-validation" not in tail_no_sync


def test_resolve_hypothesis_confirmatory_years_subset_contract() -> None:
    root = _root()
    preset = _load_module(
        root / "scripts" / "modeling" / "run_phase_b_regspec_preset.py",
        "phase_b_preset_module_hypothesis_windows",
    )

    years = [3, 5, 10]
    out = preset._resolve_hypothesis_confirmatory_years(
        hypothesis_window_years=years,
        confirmatory_window_years_raw="3,5",
    )
    assert out == [3, 5]

    out_all = preset._resolve_hypothesis_confirmatory_years(
        hypothesis_window_years=years,
        confirmatory_window_years_raw="",
    )
    assert out_all == [3, 5, 10]

    with pytest.raises(ValueError, match="subset of --hypothesis-window-years"):
        preset._resolve_hypothesis_confirmatory_years(
            hypothesis_window_years=years,
            confirmatory_window_years_raw="3,7",
        )


def test_direction_review_payload_extracts_pq_restart_and_consensus(tmp_path: Path) -> None:
    root = _root()
    preset = _load_module(
        root / "scripts" / "modeling" / "run_phase_b_regspec_preset.py",
        "phase_b_preset_module_direction_review",
    )

    top_inf_nooption = tmp_path / "top_inf_nooption.csv"
    top_inf_nooption.write_text(
        "\n".join(
            [
                "run_id,candidate_id,track,y_col,status_validation,p_boot_validation,q_value_validation,candidate_tier",
                "ut,noopt_cand_1,primary_strict,y_all,ok,0.040,0.120,exploratory",
                "ut,noopt_cand_2,primary_strict,y_all,ok,0.080,0.220,support_candidate",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    top_inf_singlex = tmp_path / "top_inf_singlex.csv"
    top_inf_singlex.write_text(
        "\n".join(
            [
                "run_id,candidate_id,track,y_col,status_validation,p_boot_validation,q_value_validation,candidate_tier",
                "ut,singlex_cand_1,primary_strict,y_all,ok,0.150,0.280,support_candidate",
                "ut,singlex_cand_2,sensitivity_broad_company_no_edu,y_all,ok,0.310,0.680,exploratory",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    rst_nooption = tmp_path / "rst_nooption.csv"
    rst_nooption.write_text(
        "\n".join(
            [
                "candidate_id,validated_rate,support_or_better_rate",
                "noopt_cand_1,0.0,1.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    rst_singlex = tmp_path / "rst_singlex.csv"
    rst_singlex.write_text(
        "\n".join(
            [
                "candidate_id,validated_rate,support_or_better_rate",
                "singlex_cand_1,0.0,1.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    run_summary_nooption = tmp_path / "run_summary_nooption.json"
    run_summary_nooption.write_text(
        json.dumps(
            {
                "counts": {
                    "scan_rows": 50,
                    "top_rows": 20,
                    "top_rows_inference": 2,
                    "candidate_tier_top_rows_inference": {
                        "validated_candidate": 0,
                        "support_candidate": 1,
                        "exploratory": 1,
                    },
                },
                "outputs": {
                    "top_models_inference_csv": str(top_inf_nooption),
                    "restart_stability_csv": str(rst_nooption),
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    run_summary_singlex = tmp_path / "run_summary_singlex.json"
    run_summary_singlex.write_text(
        json.dumps(
            {
                "counts": {
                    "scan_rows": 16,
                    "top_rows": 8,
                    "top_rows_inference": 2,
                    "candidate_tier_top_rows_inference": {
                        "validated_candidate": 0,
                        "support_candidate": 1,
                        "exploratory": 1,
                    },
                },
                "outputs": {
                    "top_models_inference_csv": str(top_inf_singlex),
                    "restart_stability_csv": str(rst_singlex),
                },
                "track_consensus_meta": {
                    "enforce_track_consensus": True,
                    "n_rows_demoted_from_validated": 0,
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    pair_payload = {
        "mode": "paired_nooption_singlex",
        "run_id": "ut_pair_20260223",
        "status": "ok",
        "children": [
            {
                "mode": "nooption_baseline",
                "run_id": "ut_pair_20260223__nooption_baseline",
                "status": "ok",
                "returncode": 0,
                "outputs": {"run_summary_json": str(run_summary_nooption)},
            },
            {
                "mode": "singlex_baseline",
                "run_id": "ut_pair_20260223__singlex",
                "status": "ok",
                "returncode": 0,
                "outputs": {"run_summary_json": str(run_summary_singlex)},
            },
        ],
    }

    review = preset._build_direction_review_payload(pair_payload)
    assert review["paired"]["mode"] == "paired_nooption_singlex"
    assert review["checks"]["all_children_ok"] is True
    assert review["checks"]["required_fields_present"] is True
    assert review["checks"]["singlex_track_consensus_check_pass"] is True
    assert review["comparison"]["nooption_best_p_validation"] == pytest.approx(0.04)
    assert review["comparison"]["singlex_best_q_validation"] == pytest.approx(0.28)
    assert review["comparison"]["singlex_restart_max_validated_rate"] == pytest.approx(0.0)
    assert len(review["branches"]) == 2


def test_direction_review_payload_marks_required_fields_false_when_no_ok_children() -> None:
    root = _root()
    preset = _load_module(
        root / "scripts" / "modeling" / "run_phase_b_regspec_preset.py",
        "phase_b_preset_module_direction_review_fail_case",
    )

    pair_payload = {
        "mode": "paired_nooption_singlex",
        "run_id": "ut_pair_failed_20260223",
        "status": "partial_failure",
        "children": [
            {
                "mode": "nooption_baseline",
                "run_id": "ut_pair_failed_20260223__nooption_baseline",
                "status": "failed",
                "returncode": 2,
            },
            {
                "mode": "singlex_baseline",
                "run_id": "ut_pair_failed_20260223__singlex",
                "status": "skipped_due_to_nooption_failure",
                "returncode": 0,
            },
        ],
    }

    review = preset._build_direction_review_payload(pair_payload)
    assert review["checks"]["all_children_ok"] is False
    assert review["checks"]["required_fields_present"] is False
    assert review["checks"]["singlex_track_consensus_check_pass"] is False


def test_dashboard_prefers_child_declared_run_summary_path(tmp_path: Path) -> None:
    root = _root()
    dashboard = _load_module(
        root / "scripts" / "reporting" / "build_phase_b_regspec_dashboard.py",
        "phase_b_dashboard_module_declared_path",
    )

    run_summary_path = tmp_path / "declared_run_summary.json"
    run_summary_path.write_text(
        json.dumps(
            {
                "counts": {
                    "scan_rows": 7,
                    "top_rows": 3,
                    "top_rows_inference": 2,
                    "candidate_tier_top_rows_inference": {
                        "validated_candidate": 1,
                        "support_candidate": 1,
                        "exploratory": 0,
                    },
                },
                "restart": {"inference_aggregation_summary": {"n_candidates_validation_ok": 1, "n_candidates_q_nonnull": 1}},
                "gate_meta": {
                    "validated_gate_source": "split_gate_with_legacy_estimable_override",
                    "legacy_single_gate_sync_validation": 0,
                },
                "search_governance": {
                    "validation_stage_policy": "post_search_on_fixed_candidate_pool",
                    "validation_used_for_search": False,
                    "candidate_pool_locked_pre_validation": True,
                },
                "audit_hashes": {
                    "data_hash": "datahash_ut",
                    "config_hash": "confighash_ut",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    paired_summary = {
        "run_id": "paired_unit_test",
        "status": "ok",
        "children": [
            {
                "mode": "singlex_baseline",
                "run_id": "child_run_unused_when_declared",
                "status": "ok",
                "returncode": 0,
                "outputs": {"run_summary_json": str(run_summary_path)},
            }
        ],
    }
    payload = dashboard._build_payload(tmp_path / "paired_summary.json", paired_summary)
    branch = payload["branches"][0]
    assert branch["run_summary_exists"] is True
    assert branch["run_summary_path_source"] == "child_outputs:run_summary_json"
    assert branch["run_summary_path"] == str(run_summary_path)
    assert branch["metrics"]["scan_rows"] == 7
    assert branch["metrics"]["validated_gate_source"] == "split_gate_with_legacy_estimable_override"
    assert branch["metrics"]["validation_stage_policy"] == "post_search_on_fixed_candidate_pool"
    assert branch["metrics"]["validation_used_for_search"] == 0
    assert branch["metrics"]["candidate_pool_locked_pre_validation"] == 1
    assert branch["metrics"]["data_hash"] == "datahash_ut"
    assert branch["metrics"]["config_hash"] == "confighash_ut"
    assert payload["governance"]["all_branches_validation_used_for_search_false"] == 1
    assert payload["governance"]["all_branches_candidate_pool_locked_pre_validation"] == 1


def test_dashboard_falls_back_to_run_id_inference() -> None:
    root = _root()
    dashboard = _load_module(
        root / "scripts" / "reporting" / "build_phase_b_regspec_dashboard.py",
        "phase_b_dashboard_module_run_id_fallback",
    )

    missing_run_id = "unit_test_missing_run_summary_fallback_20260223"
    paired_summary = {
        "run_id": "paired_unit_test_fallback",
        "status": "partial_failure",
        "children": [
            {
                "mode": "nooption_baseline",
                "run_id": missing_run_id,
                "status": "failed",
                "returncode": 2,
            }
        ],
    }
    payload = dashboard._build_payload(Path("ignored.json"), paired_summary)
    branch = payload["branches"][0]
    assert branch["run_summary_path_source"] == "run_id_inferred"
    assert branch["run_summary_path"].endswith(
        "phase_b_bikard_machine_scientist_run_summary_unit_test_missing_run_summary_fallback_20260223.json"
    )
