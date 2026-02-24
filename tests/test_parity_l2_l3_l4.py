from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from regspec_machine.api import create_app
from regspec_machine.contracts import RunRequestContract
from regspec_machine.engine import CommandResult, PresetEngine
from regspec_machine.orchestrator import RunOrchestrator


def _mk_workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    preset = root / "scripts" / "modeling" / "run_phase_b_regspec_preset.py"
    preset.parent.mkdir(parents=True, exist_ok=True)
    preset.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
    return root


def _write_scan_run_summary(root: Path, run_id: str) -> Path:
    summary_rel = f"data/metadata/phase_b_bikard_machine_scientist_run_summary_{run_id}.json"
    summary_path = root / summary_rel
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(
            {
                "counts": {
                    "scan_rows": 24,
                    "top_rows": 8,
                    "top_rows_inference": 3,
                },
                "search_governance": {
                    "validation_used_for_search": False,
                    "candidate_pool_locked_pre_validation": True,
                },
                "gate_meta": {
                    "min_informative_events_estimable": 20,
                    "min_policy_docs_informative_estimable": 10,
                },
                "track_consensus_meta": {
                    "enforce_track_consensus": True,
                    "n_rows_demoted_from_validated": 0,
                },
                "audit_hashes": {
                    "data_hash": "datahash_parity",
                    "config_hash": "confighash_parity",
                    "feature_registry_hash": "featurehash_parity",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return summary_path


def _minimal_result_view(payload: dict) -> dict:
    return {
        "run_id": payload["run_id"],
        "mode": payload["mode"],
        "state": payload["state"],
        "artifacts": payload["artifacts"],
        "counts": payload["counts"],
        "governance_checks": payload["governance_checks"],
        "audit_hashes": payload["audit_hashes"],
    }


def _wait_api_result(client: TestClient, run_id: str, timeout_sec: float = 3.0) -> dict:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = client.get(f"/runs/{run_id}/result")
        assert resp.status_code in {200, 202}
        payload = resp.json()
        if resp.status_code == 200 and payload.get("result") is not None:
            return payload
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for API result: {run_id}")


def test_l2_l3_l4_parity_on_success_payload(tmp_path: Path) -> None:
    root = _mk_workspace(tmp_path)
    run_id = "ut_parity_success"
    _write_scan_run_summary(root, run_id)
    req = RunRequestContract.from_payload({"mode": "singlex_baseline", "run_id": run_id})

    def ok_exec(_cmd, _cwd):
        return CommandResult(returncode=0, stdout="ok")

    l2 = PresetEngine(workspace_root=root, command_executor=ok_exec).execute(req)
    l3 = RunOrchestrator(engine=PresetEngine(workspace_root=root, command_executor=ok_exec)).run(req)

    l4_orch = RunOrchestrator(engine=PresetEngine(workspace_root=root, command_executor=ok_exec))
    client = TestClient(create_app(orchestrator=l4_orch))
    created = client.post("/runs", json=req.as_dict())
    assert created.status_code == 202
    api_payload = _wait_api_result(client, run_id)
    l4_result = api_payload["result"]
    l4_status = api_payload["status"]

    l2_view = _minimal_result_view(l2.result.as_dict())
    l3_view = _minimal_result_view(l3.result.as_dict())
    l4_view = _minimal_result_view(l4_result)

    assert l2.status.state == "succeeded"
    assert l3.status.state == "succeeded"
    assert l4_status["state"] == "succeeded"
    assert l2_view == l3_view == l4_view
    assert l4_view["governance_checks"]["search_governance"]["validation_used_for_search"] is False
    assert (
        l4_view["governance_checks"]["search_governance"]["candidate_pool_locked_pre_validation"] is True
    )


def test_l2_l3_l4_parity_on_failure_state(tmp_path: Path) -> None:
    root = _mk_workspace(tmp_path)
    run_id = "ut_parity_failure"
    req = RunRequestContract.from_payload({"mode": "singlex_baseline", "run_id": run_id})

    def fail_exec(_cmd, _cwd):
        return CommandResult(returncode=2, stderr="boom")

    l2 = PresetEngine(workspace_root=root, command_executor=fail_exec).execute(req)
    l3 = RunOrchestrator(engine=PresetEngine(workspace_root=root, command_executor=fail_exec)).run(req)

    l4_orch = RunOrchestrator(engine=PresetEngine(workspace_root=root, command_executor=fail_exec))
    client = TestClient(create_app(orchestrator=l4_orch))
    created = client.post("/runs", json=req.as_dict())
    assert created.status_code == 202
    api_payload = _wait_api_result(client, run_id)
    l4_status = api_payload["status"]
    l4_result = api_payload["result"]

    assert l2.status.state == "failed"
    assert l3.status.state == "failed"
    assert l4_status["state"] == "failed"
    assert l2.status.error is not None and l2.status.error.code == "PRESET_RUNNER_FAILED"
    assert l3.status.error is not None and l3.status.error.code == "PRESET_RUNNER_FAILED"
    assert l4_status["error"]["code"] == "PRESET_RUNNER_FAILED"
    assert l2.result.state == "failed"
    assert l3.result.state == "failed"
    assert l4_result["state"] == "failed"

