from __future__ import annotations

import pytest

from regspec_machine.contracts import (
    RUN_MODES,
    RunArtifactsContract,
    RunErrorContract,
    RunRequestContract,
    RunResultContract,
    RunStatusContract,
)


def test_request_contract_from_payload_ok() -> None:
    payload = {
        "mode": "paired_nooption_singlex",
        "run_id": "phase_b_pair_ut_20260224",
        "scan_n_bootstrap": 49,
        "scan_max_features": 120,
        "cli_summary_top_n": 3,
        "extra_args": ["--foo", "--bar=1"],
        "idempotency_key": "idem-ut-001",
    }
    req = RunRequestContract.from_payload(payload)
    assert req.mode == "paired_nooption_singlex"
    assert req.run_id == "phase_b_pair_ut_20260224"
    assert req.scan_n_bootstrap == 49
    assert req.scan_max_features == 120
    assert req.cli_summary_top_n == 3
    assert list(req.extra_args) == ["--foo", "--bar=1"]
    assert req.idempotency_key == "idem-ut-001"


def test_request_contract_rejects_invalid_mode() -> None:
    with pytest.raises(ValueError, match="mode must be one of"):
        RunRequestContract.from_payload({"mode": "bad_mode", "run_id": "ut"})


def test_request_contract_rejects_negative_int() -> None:
    with pytest.raises(ValueError, match="scan_n_bootstrap must be an integer >= 0"):
        RunRequestContract.from_payload(
            {"mode": "singlex_baseline", "run_id": "ut", "scan_n_bootstrap": -1}
        )


def test_status_contract_and_error_payload() -> None:
    err = RunErrorContract.create(
        code="RUNTIME_ERROR",
        message="subprocess failed",
        retryable=True,
        details={"returncode": 2},
    )
    status = RunStatusContract.create(
        run_id="ut_status_20260224",
        mode="singlex_baseline",
        state="failed",
        progress_stage="execution",
        progress_message="child process failed",
        progress_fraction=0.5,
        error=err,
    )
    payload = status.as_dict()
    assert payload["run_id"] == "ut_status_20260224"
    assert payload["state"] == "failed"
    assert payload["error"]["code"] == "RUNTIME_ERROR"
    assert payload["error"]["retryable"] is True


def test_status_contract_progress_bounds() -> None:
    with pytest.raises(ValueError, match="progress_fraction must be in \\[0.0, 1.0\\]"):
        RunStatusContract.create(
            run_id="ut_status_bad",
            mode="singlex_baseline",
            state="running",
            progress_fraction=1.2,
        )


def test_result_contract_serialization() -> None:
    artifacts = RunArtifactsContract(
        run_summary_json="data/metadata/run_summary_ut.json",
        top_models_inference_csv="outputs/tables/top_models_ut_inference.csv",
        direction_review_json="data/metadata/direction_review_ut.json",
    )
    result = RunResultContract.create(
        run_id="ut_result_20260224",
        mode="paired_nooption_singlex",
        state="succeeded",
        artifacts=artifacts,
        counts={"top_rows_inference": 6, "scan_rows": 120},
        governance_checks={"all_children_ok": True},
        audit_hashes={"data_hash": "abc", "config_hash": "def"},
    )
    payload = result.as_dict()
    assert payload["run_id"] == "ut_result_20260224"
    assert payload["artifacts"]["direction_review_json"].endswith("direction_review_ut.json")
    assert payload["counts"]["top_rows_inference"] == 6
    assert payload["governance_checks"]["all_children_ok"] is True
    assert payload["audit_hashes"]["data_hash"] == "abc"


def test_run_modes_contract_is_not_empty() -> None:
    assert len(RUN_MODES) >= 5

