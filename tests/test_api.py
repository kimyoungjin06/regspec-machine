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
    assert "review_cards" in resp.text
    assert "leakage guard" in resp.text
    assert "Quick Start" in resp.text
    assert "detail_overview_tbody" in resp.text
    assert "preset_pair_btn" in resp.text
    assert "run_pair_now_btn" in resp.text
    assert "Baseline Compare (nooption vs singlex)" in resp.text
    assert "compare_btn" in resp.text
    assert "compare_tbody" in resp.text
    assert "Direction Review Hints" in resp.text
    assert "compare_interp_list" in resp.text
    assert "save_compare_outputs_btn" in resp.text
    assert "export_compare_md_btn" in resp.text
    assert "export_compare_json_btn" in resp.text
    assert "list_mode_filter" in resp.text
    assert "list_run_id_filter" in resp.text
    assert "runs_notice" in resp.text
    assert "Dataset Explorer (Question Seeder)" in resp.text
    assert "dataset_run_id" in resp.text
    assert "dataset_path" in resp.text
    assert "profile_btn" in resp.text
    assert "profile_seed_tbody" in resp.text
    assert "dataset_research_mode" in resp.text
    assert "dataset_fixed_y" in resp.text
    assert "dataset_exclude_x_cols" in resp.text


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


def test_api_review_endpoint_ignores_non_ok_validation_rows(tmp_path: Path) -> None:
    class _ReviewStatusEngine:
        def __init__(self, workspace_root: Path) -> None:
            self.workspace_root = workspace_root

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
                        "bad_row,validated_candidate,0.0001,0.0002,failed,primary_strict",
                        "good_row,support_candidate,0.05,0.10,ok,primary_strict",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            rst_path.write_text(
                "\n".join(
                    [
                        "candidate_id,validated_rate,support_or_better_rate",
                        "good_row,0.4,0.9",
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
                progress_message="done",
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
                governance_checks={
                    "search_governance": {
                        "validation_used_for_search": False,
                        "candidate_pool_locked_pre_validation": True,
                    }
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

    orch = RunOrchestrator(engine=_ReviewStatusEngine(workspace_root=tmp_path))
    app = create_app(orchestrator=orch)
    client = TestClient(app)
    client.post(
        "/runs?execute=false",
        json={"mode": "singlex_baseline", "run_id": "ut_api_review_status_filter"},
    )
    orch.execute("ut_api_review_status_filter")
    review = client.get("/runs/ut_api_review_status_filter/review")
    assert review.status_code == 200
    metrics = review.json()["review"]["metrics"]
    assert metrics["validated_candidate_count"] == 0
    assert metrics["support_candidate_count"] == 1
    assert metrics["best_p_validation"] == 0.05
    assert metrics["best_q_validation"] == 0.10


def test_api_compare_export_blocks_promotion_when_any_governance_fails(tmp_path: Path) -> None:
    class _CompareExportEngine:
        def __init__(self, workspace_root: Path) -> None:
            self.workspace_root = workspace_root

        def execute(self, request: RunRequestContract, *, dry_run: bool = False) -> EngineExecution:
            top_rel = f"outputs/tables/{request.run_id}_top_inference.csv"
            rst_rel = f"data/metadata/{request.run_id}_restart.csv"
            top_path = self.workspace_root / top_rel
            rst_path = self.workspace_root / rst_rel
            top_path.parent.mkdir(parents=True, exist_ok=True)
            rst_path.parent.mkdir(parents=True, exist_ok=True)

            if "nooption" in request.run_id:
                top_path.write_text(
                    "\n".join(
                        [
                            "candidate_id,candidate_tier,p_boot_validation,q_value_validation,status_validation,track",
                            "no_c1,validated_candidate,0.01,0.03,ok,primary_strict",
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
                rst_path.write_text(
                    "\n".join(
                        [
                            "candidate_id,validated_rate,support_or_better_rate",
                            "no_c1,0.90,1.0",
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
                governance_checks = {
                    "search_governance": {
                        "validation_used_for_search": False,
                        "candidate_pool_locked_pre_validation": True,
                    },
                    "track_consensus_meta": {
                        "enforce_track_consensus": False,
                        "n_rows_demoted_from_validated": 0,
                    },
                }
            else:
                top_path.write_text(
                    "\n".join(
                        [
                            "candidate_id,candidate_tier,p_boot_validation,q_value_validation,status_validation,track",
                            "sx_c1,support_candidate,0.20,0.20,ok,primary_strict",
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
                rst_path.write_text(
                    "\n".join(
                        [
                            "candidate_id,validated_rate,support_or_better_rate",
                            "sx_c1,0.00,1.0",
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
                governance_checks = {
                    "search_governance": {
                        "validation_used_for_search": False,
                        "candidate_pool_locked_pre_validation": False,
                    },
                    "track_consensus_meta": {
                        "enforce_track_consensus": True,
                        "n_rows_demoted_from_validated": 0,
                    },
                }

            status = RunStatusContract.create(
                run_id=request.run_id,
                mode=request.mode,
                state="succeeded",
                progress_stage="completed",
                progress_message="done",
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
                governance_checks=governance_checks,
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

    orch = RunOrchestrator(engine=_CompareExportEngine(workspace_root=tmp_path))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    client.post(
        "/runs?execute=false",
        json={"mode": "nooption_baseline", "run_id": "ut_compare_nooption"},
    )
    client.post(
        "/runs?execute=false",
        json={"mode": "singlex_baseline", "run_id": "ut_compare_singlex"},
    )
    orch.execute("ut_compare_nooption")
    orch.execute("ut_compare_singlex")

    resp = client.post(
        "/compare/export",
        json={
            "nooption_run_id": "ut_compare_nooption",
            "singlex_run_id": "ut_compare_singlex",
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    checks = payload["comparison"]["checks"]

    assert checks["both_succeeded"] is True
    assert checks["nooption_governance_pass"] is True
    assert checks["singlex_governance_pass"] is False
    assert checks["all_governance_pass"] is False
    assert checks["nooption_primary_validated_gate_pass"] is True
    assert checks["nooption_q_gate_pass"] is True
    assert checks["nooption_restart_validated_rate_gate_pass"] is True
    assert checks["nooption_promotion_gate_pass"] is False
    assert checks["primary_objective_gate_pass"] is False

    out_json = Path(payload["outputs"]["json"])
    out_md = Path(payload["outputs"]["markdown"])
    assert out_json.is_file()
    assert out_md.is_file()


def test_api_compare_export_rejects_mode_mismatch() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=[]))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    create_nooption = client.post(
        "/runs?execute=false",
        json={"mode": "singlex_baseline", "run_id": "ut_mode_mismatch_nooption"},
    )
    create_singlex = client.post(
        "/runs?execute=false",
        json={"mode": "nooption_baseline", "run_id": "ut_mode_mismatch_singlex"},
    )
    assert create_nooption.status_code == 202
    assert create_singlex.status_code == 202

    resp = client.post(
        "/compare/export",
        json={
            "nooption_run_id": "ut_mode_mismatch_nooption",
            "singlex_run_id": "ut_mode_mismatch_singlex",
        },
    )
    assert resp.status_code == 422
    assert "mode mismatch" in str(resp.json()["detail"])


def test_api_history_scan_supports_listing_and_detail(tmp_path: Path) -> None:
    class _HistoryEngine:
        def __init__(self, workspace_root: Path) -> None:
            self.workspace_root = workspace_root

        def execute(self, request: RunRequestContract, *, dry_run: bool = False) -> EngineExecution:
            raise RuntimeError("not used in history-only test")

    run_id = "ut_history_singlex_20260224"
    top_rel = f"outputs/tables/{run_id}_top_inference.csv"
    rst_rel = f"data/metadata/{run_id}_restart.csv"
    top_path = tmp_path / top_rel
    rst_path = tmp_path / rst_rel
    top_path.parent.mkdir(parents=True, exist_ok=True)
    rst_path.parent.mkdir(parents=True, exist_ok=True)
    top_path.write_text(
        "\n".join(
            [
                "candidate_id,candidate_tier,p_boot_validation,q_value_validation,status_validation,track",
                "c1,validated_candidate,0.02,0.07,ok,primary_strict",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    rst_path.write_text(
        "\n".join(
            [
                "candidate_id,validated_rate,support_or_better_rate",
                "c1,0.75,1.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    summary_path = (
        tmp_path
        / "data"
        / "metadata"
        / "phase_b_bikard_machine_scientist_run_summary_ut_history_singlex_20260224.json"
    )
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        "\n".join(
            [
                "{",
                f'  "run_id": "{run_id}",',
                '  "timestamp": "2026-02-24T03:00:00Z",',
                '  "counts": {"top_rows_inference": 1, "scan_rows": 10},',
                '  "search_governance": {"validation_used_for_search": false, "candidate_pool_locked_pre_validation": true},',
                '  "track_consensus_meta": {"enforce_track_consensus": true, "n_rows_demoted_from_validated": 0},',
                '  "outputs": {',
                f'    "top_models_inference_csv": "{top_rel}",',
                f'    "restart_stability_csv": "{rst_rel}",',
                f'    "run_summary_json": "{summary_path}"',
                "  }",
                "}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    orch = RunOrchestrator(engine=_HistoryEngine(workspace_root=tmp_path))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    listed = client.get("/runs?include_history=true&mode=singlex_baseline&run_id_contains=ut_history")
    assert listed.status_code == 200
    rows = listed.json()["rows"]
    assert any(str(row.get("run_id")) == run_id for row in rows)

    status_resp = client.get(f"/runs/{run_id}")
    assert status_resp.status_code == 200
    assert status_resp.json()["status"]["state"] == "succeeded"
    assert status_resp.json()["source"] == "run_summary"

    review_resp = client.get(f"/runs/{run_id}/review")
    assert review_resp.status_code == 200
    review = review_resp.json()["review"]
    assert review["metrics"]["validated_candidate_count"] == 1
    assert review["metrics"]["best_p_validation"] == 0.02


def test_api_dataset_profile_from_direct_path(tmp_path: Path) -> None:
    class _DatasetEngine:
        def __init__(self, workspace_root: Path) -> None:
            self.workspace_root = workspace_root

        def execute(self, request: RunRequestContract, *, dry_run: bool = False) -> EngineExecution:
            raise RuntimeError("not used in dataset profile direct path test")

    csv_path = tmp_path / "outputs" / "tables" / "ut_profile_dataset.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(
        "\n".join(
            [
                "paper_id,y_bin,y_count,x_num,x_cat",
                "p1,1,2,0.9,a",
                "p2,0,0,0.1,b",
                "p3,1,3,0.8,a",
                "p4,0,1,0.2,b",
                "p5,1,4,0.95,a",
                "p6,0,0,0.05,b",
                "p7,1,3,0.88,a",
                "p8,0,1,0.15,b",
                "p9,1,5,0.99,a",
                "p10,0,0,0.07,b",
                "p11,1,4,0.91,a",
                "p12,0,0,0.09,b",
                "p13,1,3,0.93,a",
                "p14,0,1,0.22,b",
                "p15,1,4,0.97,a",
                "p16,0,0,0.11,b",
                "p17,1,3,0.84,a",
                "p18,0,1,0.17,b",
                "p19,1,4,0.89,a",
                "p20,0,0,0.13,b",
                "p21,1,5,0.94,a",
                "p22,0,1,0.19,b",
                "p23,1,4,0.92,a",
                "p24,0,0,0.08,b",
                "p25,1,3,0.86,a",
                "p26,0,1,0.18,b",
                "p27,1,4,0.96,a",
                "p28,0,0,0.12,b",
                "p29,1,3,0.87,a",
                "p30,0,1,0.16,b",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    orch = RunOrchestrator(engine=_DatasetEngine(workspace_root=tmp_path))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    resp = client.get(
        "/datasets/profile",
        params={
            "dataset_path": "outputs/tables/ut_profile_dataset.csv",
            "sample_rows": 100,
            "top_n": 10,
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["row_count"] == 30
    assert payload["column_count"] == 5
    assert len(payload["question_seeds"]) >= 1
    assert any(str(row.get("label", "")).startswith("y_bin ~") for row in payload["question_seeds"])
    assert payload["resolved_dataset_path"].endswith("outputs/tables/ut_profile_dataset.csv")
    assert payload["research_mode"] is True
    if payload["question_seeds"]:
        seed = payload["question_seeds"][0]
        assert "risk_level" in seed
        assert "risk_flags" in seed

    fixed_resp = client.get(
        "/datasets/profile",
        params={
            "dataset_path": "outputs/tables/ut_profile_dataset.csv",
            "sample_rows": 100,
            "top_n": 10,
            "fixed_y": "y_bin",
            "exclude_x_cols": "x_num",
            "research_mode": "true",
        },
    )
    assert fixed_resp.status_code == 200
    fixed_payload = fixed_resp.json()
    assert fixed_payload["fixed_y"] == "y_bin"
    assert fixed_payload["exclude_x_cols"] == ["x_num"]
    assert all(str(row.get("y_col", "")) == "y_bin" for row in fixed_payload["question_seeds"])
    assert all(str(row.get("x_col", "")) != "x_num" for row in fixed_payload["question_seeds"])


def test_api_dataset_profile_from_run_artifact(tmp_path: Path) -> None:
    class _ProfileArtifactEngine:
        def __init__(self, workspace_root: Path) -> None:
            self.workspace_root = workspace_root

        def execute(self, request: RunRequestContract, *, dry_run: bool = False) -> EngineExecution:
            rel = f"outputs/tables/{request.run_id}_scan_runs.csv"
            path = self.workspace_root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                "\n".join(
                    [
                        "candidate_id,y_col,feature_name,p_boot_validation,q_value_validation",
                        "c1,y_bin,x_num,0.01,0.05",
                        "c2,y_bin,x_cat,0.02,0.06",
                        "c3,y_count,x_num,0.03,0.08",
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
                progress_message="ok",
                progress_fraction=1.0,
            )
            result = RunResultContract.create(
                run_id=request.run_id,
                mode=request.mode,
                state="succeeded",
                artifacts=RunArtifactsContract(scan_runs_csv=rel),
                counts={"scan_rows": 3},
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

    orch = RunOrchestrator(engine=_ProfileArtifactEngine(workspace_root=tmp_path))
    app = create_app(orchestrator=orch)
    client = TestClient(app)

    create = client.post(
        "/runs",
        json={"mode": "singlex_baseline", "run_id": "ut_dataset_profile_artifact"},
    )
    assert create.status_code == 202
    assert _wait_terminal_state(client, "ut_dataset_profile_artifact") == "succeeded"

    resp = client.get(
        "/datasets/profile",
        params={
            "run_id": "ut_dataset_profile_artifact",
            "artifact_key": "scan_runs_csv",
            "sample_rows": 100,
            "top_n": 5,
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["run_id"] == "ut_dataset_profile_artifact"
    assert payload["artifact_key"] == "scan_runs_csv"
    assert payload["row_count"] == 3
    assert payload["resolved_dataset_path"].endswith("ut_dataset_profile_artifact_scan_runs.csv")
