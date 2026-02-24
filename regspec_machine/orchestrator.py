"""L3 orchestration layer over L2 preset engine execution."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

from .contracts import RunErrorContract, RunRequestContract, RunResultContract, RunStatusContract
from .engine import EngineExecution


def _utc_now_isoz() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _tail(text: str, n: int = 4000) -> str:
    s = str(text or "")
    return s[-n:] if len(s) > n else s


class ExecutionEngine(Protocol):
    def execute(self, request: RunRequestContract, *, dry_run: bool = False) -> EngineExecution: ...


@dataclass(frozen=True)
class RunSnapshot:
    request: RunRequestContract
    status: RunStatusContract
    result: Optional[RunResultContract] = None
    command: Tuple[str, ...] = ()
    returncode: Optional[int] = None
    stdout_tail: str = ""
    stderr_tail: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "request": self.request.as_dict(),
            "status": self.status.as_dict(),
            "result": self.result.as_dict() if self.result is not None else None,
            "command": list(self.command),
            "returncode": self.returncode,
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
        }


@dataclass
class _RunEnvelope:
    request: RunRequestContract
    status: RunStatusContract
    result: Optional[RunResultContract] = None
    command: Tuple[str, ...] = ()
    returncode: Optional[int] = None
    stdout_tail: str = ""
    stderr_tail: str = ""

    def snapshot(self) -> RunSnapshot:
        return RunSnapshot(
            request=self.request,
            status=self.status,
            result=self.result,
            command=self.command,
            returncode=self.returncode,
            stdout_tail=self.stdout_tail,
            stderr_tail=self.stderr_tail,
        )


class RunOrchestrator:
    """Stateful run manager for queued/running/succeeded/failed lifecycles."""

    _ALLOWED_TRANSITIONS: Dict[str, set[str]] = {
        "queued": {"running", "cancelled"},
        "running": {"succeeded", "failed", "cancelled"},
        "failed": {"running"},
        "cancelled": {"running"},
        "succeeded": set(),
    }

    def __init__(
        self,
        *,
        engine: ExecutionEngine,
        max_attempts: int = 2,
        events_jsonl: Optional[Path | str] = None,
    ) -> None:
        if int(max_attempts) < 1:
            raise ValueError("max_attempts must be >= 1")
        self.engine = engine
        self.max_attempts = int(max_attempts)
        self._runs: Dict[str, _RunEnvelope] = {}
        self._idempotency: Dict[str, str] = {}
        self._lock = threading.RLock()
        self._events_jsonl = Path(events_jsonl) if events_jsonl else None
        if self._events_jsonl is not None:
            self._events_jsonl.parent.mkdir(parents=True, exist_ok=True)

    def _emit_event(
        self,
        *,
        event_type: str,
        envelope: _RunEnvelope,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if self._events_jsonl is None:
            return
        payload = {
            "timestamp_utc": _utc_now_isoz(),
            "event_type": str(event_type),
            "run_id": envelope.request.run_id,
            "mode": envelope.request.mode,
            "state": envelope.status.state,
            "attempt": int(envelope.status.attempt),
            "details": dict(details or {}),
        }
        with self._events_jsonl.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _transition_status(
        self,
        *,
        envelope: _RunEnvelope,
        new_state: str,
        progress_stage: str,
        progress_message: str,
        progress_fraction: Optional[float],
        error: Optional[RunErrorContract] = None,
        attempt: Optional[int] = None,
    ) -> None:
        old_state = str(envelope.status.state)
        if new_state != old_state:
            allowed = self._ALLOWED_TRANSITIONS.get(old_state, set())
            if new_state not in allowed:
                raise RuntimeError(f"invalid state transition: {old_state} -> {new_state}")

        att = int(attempt) if attempt is not None else int(envelope.status.attempt)
        envelope.status = RunStatusContract.create(
            run_id=envelope.request.run_id,
            mode=envelope.request.mode,
            state=new_state,
            created_at_utc=envelope.status.created_at_utc,
            updated_at_utc=_utc_now_isoz(),
            progress_stage=progress_stage,
            progress_message=progress_message,
            progress_fraction=progress_fraction,
            attempt=att,
            error=error,
        )

    def submit(self, request: RunRequestContract | Mapping[str, Any]) -> RunStatusContract:
        req = request if isinstance(request, RunRequestContract) else RunRequestContract.from_payload(request)
        with self._lock:
            idem = str(req.idempotency_key).strip()
            if idem and idem in self._idempotency:
                existing_run_id = self._idempotency[idem]
                return self._runs[existing_run_id].status
            if req.run_id in self._runs:
                raise ValueError(f"run_id already exists: {req.run_id}")
            now = _utc_now_isoz()
            status = RunStatusContract.create(
                run_id=req.run_id,
                mode=req.mode,
                state="queued",
                created_at_utc=now,
                updated_at_utc=now,
                progress_stage="queued",
                progress_message="submitted",
                progress_fraction=0.0,
                attempt=1,
            )
            env = _RunEnvelope(request=req, status=status)
            self._runs[req.run_id] = env
            if idem:
                self._idempotency[idem] = req.run_id
            self._emit_event(event_type="submitted", envelope=env)
            return env.status

    def get_status(self, run_id: str) -> Optional[RunStatusContract]:
        with self._lock:
            env = self._runs.get(str(run_id))
            return env.status if env is not None else None

    def get_result(self, run_id: str) -> Optional[RunResultContract]:
        with self._lock:
            env = self._runs.get(str(run_id))
            return env.result if env is not None else None

    def get_snapshot(self, run_id: str) -> Optional[RunSnapshot]:
        with self._lock:
            env = self._runs.get(str(run_id))
            return env.snapshot() if env is not None else None

    def list_snapshots(self, *, state: str = "") -> List[RunSnapshot]:
        with self._lock:
            rows = [env.snapshot() for env in self._runs.values()]
        state_text = str(state).strip().lower()
        if not state_text:
            return rows
        return [row for row in rows if str(row.status.state).strip().lower() == state_text]

    def cancel(self, run_id: str, *, reason: str = "cancel requested") -> RunStatusContract:
        with self._lock:
            env = self._runs.get(str(run_id))
            if env is None:
                raise KeyError(f"run not found: {run_id}")
            cur = str(env.status.state)
            if cur in {"succeeded", "failed"}:
                raise RuntimeError(f"cannot cancel terminal run state: {cur}")
            self._transition_status(
                envelope=env,
                new_state="cancelled",
                progress_stage="cancelled",
                progress_message=str(reason).strip() or "cancel requested",
                progress_fraction=1.0,
            )
            self._emit_event(event_type="cancelled", envelope=env, details={"reason": reason})
            return env.status

    def _can_start(self, env: _RunEnvelope, *, allow_retry: bool) -> Tuple[bool, int]:
        state = str(env.status.state)
        current_attempt = int(env.status.attempt)
        if state == "queued":
            return True, current_attempt
        if state == "running":
            raise RuntimeError(f"run already running: {env.request.run_id}")
        if state == "succeeded":
            raise RuntimeError(f"run already succeeded: {env.request.run_id}")
        if state in {"failed", "cancelled"}:
            if not allow_retry:
                raise RuntimeError(f"run is {state}; use retry() or allow_retry=True")
            next_attempt = current_attempt + 1
            if next_attempt > self.max_attempts:
                raise RuntimeError(
                    f"max attempts exceeded for run_id={env.request.run_id}: {next_attempt}>{self.max_attempts}"
                )
            return True, next_attempt
        raise RuntimeError(f"unknown run state: {state}")

    def execute(
        self,
        run_id: str,
        *,
        dry_run: bool = False,
        allow_retry: bool = False,
    ) -> EngineExecution:
        with self._lock:
            env = self._runs.get(str(run_id))
            if env is None:
                raise KeyError(f"run not found: {run_id}")
            _, next_attempt = self._can_start(env, allow_retry=allow_retry)
            self._transition_status(
                envelope=env,
                new_state="running",
                progress_stage="dispatch",
                progress_message="dispatching to L2 engine",
                progress_fraction=0.1,
                attempt=next_attempt,
            )
            request = env.request
            self._emit_event(
                event_type="started",
                envelope=env,
                details={"dry_run": bool(dry_run)},
            )

        # Execute outside lock.
        try:
            execution = self.engine.execute(request, dry_run=dry_run)
        except Exception as exc:
            with self._lock:
                env = self._runs[str(run_id)]
                err = RunErrorContract.create(
                    code="ORCHESTRATOR_ENGINE_EXCEPTION",
                    message=str(exc) or exc.__class__.__name__,
                    retryable=True,
                    details={"exception_type": exc.__class__.__name__},
                )
                self._transition_status(
                    envelope=env,
                    new_state="failed",
                    progress_stage="failed",
                    progress_message="engine raised exception",
                    progress_fraction=1.0,
                    error=err,
                    attempt=int(env.status.attempt),
                )
                env.returncode = 2
                env.stdout_tail = ""
                env.stderr_tail = str(exc)
                env.result = RunResultContract.create(
                    run_id=env.request.run_id,
                    mode=env.request.mode,
                    state="failed",
                    governance_checks={"engine_exception": True},
                )
                self._emit_event(event_type="failed_exception", envelope=env, details={"error": str(exc)})
                return EngineExecution(
                    request=env.request,
                    command=tuple(),
                    returncode=2,
                    status=env.status,
                    result=env.result,
                    stdout="",
                    stderr=str(exc),
                )

        with self._lock:
            env = self._runs[str(run_id)]
            final_state = str(execution.status.state)
            if final_state not in {"succeeded", "failed", "cancelled"}:
                final_state = "failed"
            self._transition_status(
                envelope=env,
                new_state=final_state,
                progress_stage="completed" if final_state == "succeeded" else final_state,
                progress_message=(
                    "execution completed"
                    if final_state == "succeeded"
                    else f"execution completed with state={final_state}"
                ),
                progress_fraction=1.0,
                error=execution.status.error,
                attempt=int(env.status.attempt),
            )
            env.result = execution.result
            env.command = tuple(execution.command)
            env.returncode = int(execution.returncode)
            env.stdout_tail = _tail(execution.stdout)
            env.stderr_tail = _tail(execution.stderr)
            self._emit_event(
                event_type="completed" if final_state == "succeeded" else "completed_non_success",
                envelope=env,
                details={"returncode": int(execution.returncode)},
            )
            return EngineExecution(
                request=execution.request,
                command=tuple(execution.command),
                returncode=int(execution.returncode),
                status=env.status,
                result=execution.result,
                stdout=execution.stdout,
                stderr=execution.stderr,
            )

    def retry(self, run_id: str, *, dry_run: bool = False) -> EngineExecution:
        return self.execute(run_id, dry_run=dry_run, allow_retry=True)

    def run(
        self,
        request: RunRequestContract | Mapping[str, Any],
        *,
        dry_run: bool = False,
    ) -> EngineExecution:
        status = self.submit(request)
        return self.execute(status.run_id, dry_run=dry_run, allow_retry=False)

