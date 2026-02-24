from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import pytest

from regspec_machine.contracts import RunErrorContract, RunRequestContract, RunResultContract, RunStatusContract
from regspec_machine.engine import EngineExecution
from regspec_machine.orchestrator import RunOrchestrator


@dataclass
class _FakeEngine:
    states: List[str]

    def execute(self, request: RunRequestContract, *, dry_run: bool = False) -> EngineExecution:
        state = self.states.pop(0) if self.states else "succeeded"
        if state == "raise":
            raise RuntimeError("boom")
        error = None
        rc = 0
        if state != "succeeded":
            rc = 2
            error = RunErrorContract.create(
                code="FAKE_ENGINE_ERROR",
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
            counts={"dummy": 1},
            governance_checks={"dry_run": bool(dry_run)},
        )
        return EngineExecution(
            request=request,
            command=("fake", request.mode),
            returncode=rc,
            status=status,
            result=result,
            stdout="fake_stdout",
            stderr="fake_stderr" if rc else "",
        )


def _request(run_id: str, *, idem: str = "") -> RunRequestContract:
    payload = {
        "mode": "singlex_baseline",
        "run_id": run_id,
    }
    if idem:
        payload["idempotency_key"] = idem
    return RunRequestContract.from_payload(payload)


def test_orchestrator_submit_execute_success_flow(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    orch = RunOrchestrator(engine=_FakeEngine(states=["succeeded"]), events_jsonl=events)
    status = orch.submit(_request("ut_orch_ok"))
    assert status.state == "queued"

    execution = orch.execute("ut_orch_ok")
    assert execution.status.state == "succeeded"
    assert execution.result.state == "succeeded"

    snap = orch.get_snapshot("ut_orch_ok")
    assert snap is not None
    assert snap.status.state == "succeeded"
    assert snap.returncode == 0
    assert events.is_file()
    lines = events.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) >= 3


def test_orchestrator_duplicate_run_id_rejected() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=[]))
    orch.submit(_request("ut_orch_dup"))
    with pytest.raises(ValueError, match="run_id already exists"):
        orch.submit(_request("ut_orch_dup"))


def test_orchestrator_idempotency_key_reuses_existing_run() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=[]))
    first = orch.submit(_request("ut_orch_idem_a", idem="idem-001"))
    second = orch.submit(_request("ut_orch_idem_b", idem="idem-001"))
    assert first.run_id == "ut_orch_idem_a"
    assert second.run_id == "ut_orch_idem_a"


def test_orchestrator_retry_after_failed_run() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=["failed", "succeeded"]), max_attempts=2)
    first = orch.run(_request("ut_orch_retry"))
    assert first.status.state == "failed"
    assert first.status.attempt == 1

    second = orch.retry("ut_orch_retry")
    assert second.status.state == "succeeded"
    assert second.status.attempt == 2


def test_orchestrator_cancel_prevents_plain_execute() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=["succeeded"]))
    orch.submit(_request("ut_orch_cancel"))
    cancelled = orch.cancel("ut_orch_cancel", reason="user cancel")
    assert cancelled.state == "cancelled"
    with pytest.raises(RuntimeError, match="use retry\\(\\) or allow_retry=True"):
        orch.execute("ut_orch_cancel")

    resumed = orch.retry("ut_orch_cancel", dry_run=True)
    assert resumed.status.state == "succeeded"
    assert resumed.status.attempt == 2


def test_orchestrator_engine_exception_maps_to_failed() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=["raise"]))
    out = orch.run(_request("ut_orch_exception"))
    assert out.status.state == "failed"
    assert out.status.error is not None
    assert out.status.error.code == "ORCHESTRATOR_ENGINE_EXCEPTION"
    assert out.result.state == "failed"


def test_orchestrator_list_snapshots_by_state() -> None:
    orch = RunOrchestrator(engine=_FakeEngine(states=["succeeded", "failed"]))
    orch.run(_request("ut_orch_list_ok"))
    orch.run(_request("ut_orch_list_fail"))

    all_rows = orch.list_snapshots()
    ok_rows = orch.list_snapshots(state="succeeded")
    fail_rows = orch.list_snapshots(state="failed")

    assert len(all_rows) == 2
    assert len(ok_rows) == 1
    assert len(fail_rows) == 1
    assert ok_rows[0].request.run_id == "ut_orch_list_ok"
    assert fail_rows[0].request.run_id == "ut_orch_list_fail"

