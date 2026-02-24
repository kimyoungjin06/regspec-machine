from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time
from typing import List

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from regspec_machine.api import create_app
from regspec_machine.contracts import (
    RunArtifactsContract,
    RunErrorContract,
    RunRequestContract,
    RunResultContract,
    RunStatusContract,
)
from regspec_machine.engine import EngineExecution
from regspec_machine.orchestrator import RunOrchestrator


@dataclass
class _FakeEngine:
    states: List[str]

    def execute(self, request: RunRequestContract, *, dry_run: bool = False) -> EngineExecution:
        state = self.states.pop(0) if self.states else "succeeded"
        rc = 0
        error = None
        if state != "succeeded":
            rc = 2
            error = RunErrorContract.create(
                code="FAKE_ENGINE_FAILED",
                message=f"fake state={state}",
                retryable=True,
            )
        status = RunStatusContract.create(
            run_id=request.run_id,
            mode=request.mode,
            state=state,
            progress_stage="completed",
            progress_message="fake done",
            progress_fraction=1.0,
            error=error,
        )
        result = RunResultContract.create(
            run_id=request.run_id,
            mode=request.mode,
            state=state,
            artifacts=RunArtifactsContract(
                run_summary_json=f"data/metadata/{request.run_id}_summary.json",
                top_models_csv=f"outputs/tables/{request.run_id}_top.csv",
            ),
            counts={"top_rows_inference": 1},
            governance_checks={"dry_run": bool(dry_run)},
        )
        return EngineExecution(
            request=request,
            command=("fake", request.mode),
            returncode=rc,
            status=status,
            result=result,
            stdout="fake_stdout",
            stderr="" if rc == 0 else "fake_stderr",
        )


@dataclass
class _ReviewEngine:
    workspace_root: Path

    def execute(self, request: RunRequestContract, *, dry_run: bool = False) -> EngineExecution:
        top_rel = f"outputs/tables/{request.run_id}_top_inference.csv"
        rst_rel = f"data/metadata/{request.run_id}_restart.csv"
        top_path = self.workspace_root / top_rel
        rst_path = self.workspace_root / rst_rel
        top_path.parent.mkdir(parents=True, exist_ok=True)
        rst_path.parent.mkdir(parents=True, exist_ok=True)
        top_path.write_text(
            "\n".join(
                [
                    "candidate_id,candidate_tier,p_boot_validation,q_value_validation,status_validation,track",
                    "c1,validated_candidate,0.03,0.08,ok,primary_strict",
                    "c2,support_candidate,0.09,0.18,ok,sensitivity_broad_company_no_edu",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        rst_path.write_text(
            "\n".join(
                [
                    "candidate_id,validated_rate,support_or_better_rate",
                    "c1,0.8,1.0",
                    "c2,0.2,0.9",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        status = RunStatusContract.create(
            run_id=request.run_id,
            mode=request.mode,
            state="succeeded",
            progress_stage="completed",
            progress_message="review done",
            progress_fraction=1.0,
        )
        result = RunResultContract.create(
            run_id=request.run_id,
            mode=request.mode,
            state="succeeded",
            artifacts=RunArtifactsContract(
                top_models_inference_csv=top_rel,
                restart_stability_csv=rst_rel,
            ),
            counts={"top_rows_inference": 2},
            governance_checks={
                "search_governance": {
                    "validation_used_for_search": False,
                    "candidate_pool_locked_pre_validation": True,
                },
                "track_consensus_meta": {
                    "enforce_track_consensus": True,
                    "n_rows_demoted_from_validated": 1,
                },
            },
        )
        return EngineExecution(
            request=request,
            command=("fake", request.mode),
            returncode=0,
            status=status,
            result=result,
            stdout="ok",
            stderr="",
        )


def _wait_terminal_state(client: TestClient, run_id: str, timeout_sec: float = 2.0) -> str:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = client.get(f"/runs/{run_id}")
        assert resp.status_code == 200
        state = str(resp.json()["status"]["state"])
        if state in {"succeeded", "failed", "cancelled"}:
            return state
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for terminal state: {run_id}")


def test_api_create_and_cancel_queue_run() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=["succeeded"]))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    create = client.post(
        "/runs?execute=false",
        json={"mode": "singlex_baseline", "run_id": "ut_api_cancel"},
    )
    assert create.status_code == 202
    assert create.json()["status"]["state"] == "queued"

    cancel = client.post("/runs/ut_api_cancel/cancel?reason=user_request")
    assert cancel.status_code == 200
    assert cancel.json()["status"]["state"] == "cancelled"

    result = client.get("/runs/ut_api_cancel/result")
    assert result.status_code == 202
    assert result.json()["result"] is None


def test_api_idempotency_reuses_run() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=[]))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    first = client.post(
        "/runs?execute=false",
        json={
            "mode": "singlex_baseline",
            "run_id": "ut_api_idem_a",
            "idempotency_key": "idem-api-001",
        },
    )
    second = client.post(
        "/runs?execute=false",
        json={
            "mode": "singlex_baseline",
            "run_id": "ut_api_idem_b",
            "idempotency_key": "idem-api-001",
        },
    )

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["status"]["run_id"] == "ut_api_idem_a"
    assert second.json()["idempotent_reused"] is True


def test_api_autodispatch_result_summary_and_artifacts() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=["succeeded"]))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    create = client.post(
        "/runs",
        json={"mode": "singlex_baseline", "run_id": "ut_api_success"},
    )
    assert create.status_code == 202

    final_state = _wait_terminal_state(client, "ut_api_success")
    assert final_state == "succeeded"

    result = client.get("/runs/ut_api_success/result")
    assert result.status_code == 200
    assert result.json()["result"]["state"] == "succeeded"

    summary = client.get("/runs/ut_api_success/summary")
    assert summary.status_code == 200
    assert summary.json()["summary"]["counts"]["top_rows_inference"] == 1

    artifacts = client.get("/runs/ut_api_success/artifacts")
    assert artifacts.status_code == 200
    assert "top_models_csv" in artifacts.json()["artifacts"]
    assert "top_models_csv" in artifacts.json()["artifact_exists"]


def test_api_retry_after_failure() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=["failed", "succeeded"]), max_attempts=2)
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    create = client.post(
        "/runs",
        json={"mode": "singlex_baseline", "run_id": "ut_api_retry"},
    )
    assert create.status_code == 202
    assert _wait_terminal_state(client, "ut_api_retry") == "failed"

    retry = client.post("/runs/ut_api_retry/retry")
    assert retry.status_code == 200
    assert retry.json()["status"]["state"] == "succeeded"
    assert int(retry.json()["status"]["attempt"]) == 2


def test_api_rejects_invalid_payload() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=[]))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    bad = client.post("/runs", json={"mode": "bad_mode", "run_id": "ut"})
    assert bad.status_code == 422
    assert "mode must be one of" in str(bad.json()["detail"])


def test_api_list_runs_endpoint_returns_rows() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=[]))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    client.post(
        "/runs?execute=false",
        json={"mode": "singlex_baseline", "run_id": "ut_api_list_1"},
    )
    client.post(
        "/runs?execute=false",
        json={"mode": "nooption_baseline", "run_id": "ut_api_list_2"},
    )
    listed = client.get("/runs")
    assert listed.status_code == 200
    payload = listed.json()
    assert "rows" in payload
    assert payload["total_rows"] >= 2
    run_ids = {row["run_id"] for row in payload["rows"]}
    assert "ut_api_list_1" in run_ids
    assert "ut_api_list_2" in run_ids

    queued_only = client.get("/runs?state=queued")
    assert queued_only.status_code == 200
    for row in queued_only.json()["rows"]:
        assert row["state"] == "queued"


def test_api_ui_endpoint_serves_html_console() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=[]))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    resp = client.get("/ui")
    assert resp.status_code == 200
    assert "RegSpec-Machine Console" in resp.text
    assert "/runs" in resp.text
    assert "/review" in resp.text


def test_api_review_endpoint_returns_pending_when_no_result() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=[]))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    create = client.post(
        "/runs?execute=false",
        json={"mode": "singlex_baseline", "run_id": "ut_api_review_pending"},
    )
    assert create.status_code == 202

    review = client.get("/runs/ut_api_review_pending/review")
    assert review.status_code == 202
    assert review.json()["review"] is None


def test_api_review_endpoint_extracts_core_metrics(tmp_path: Path) -> None:
    orch = RunOrchestrator(engine=_ReviewEngine(workspace_root=tmp_path))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    create = client.post(
        "/runs?execute=false",
        json={"mode": "singlex_baseline", "run_id": "ut_api_review"},
    )
    assert create.status_code == 202
    orch.execute("ut_api_review")

    review = client.get("/runs/ut_api_review/review")
    assert review.status_code == 200
    payload = review.json()["review"]
    metrics = payload["metrics"]
    governance = payload["governance"]

    assert metrics["validated_candidate_count"] == 1
    assert metrics["support_candidate_count"] == 1
    assert metrics["best_p_validation"] == 0.03
    assert metrics["best_q_validation"] == 0.08
    assert metrics["restart_validated_rate_max"] == 0.8
    assert metrics["restart_validated_rate_mean"] == 0.5
    assert governance["validation_used_for_search_false"] is True
    assert governance["candidate_pool_locked_pre_validation_true"] is True
    assert governance["track_consensus_enforced"] is True
    assert governance["track_consensus_demoted_rows"] == 1
