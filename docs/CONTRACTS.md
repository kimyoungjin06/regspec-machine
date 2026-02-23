# Contracts (L1)

This document defines the execution contracts that will be shared by:
- CLI wrappers
- future API service
- future UI client

Current source of truth in code:
- `regspec_machine/contracts.py`

## 1) Request contract

Type:
- `RunRequestContract`

Required fields:
- `mode`: one of `RUN_MODES`
- `run_id`: non-empty string

Selected optional fields:
- `scan_n_bootstrap` (int >= 0)
- `scan_max_features` (int >= 0)
- `refine_n_bootstrap` (int >= 0)
- `runner_python` (string; default `.venv/bin/python`)
- `cli_summary_top_n` (int >= 1)
- `paired_legacy_sync_validation` (bool)
- `skip_direction_review` (bool)
- `extra_args` (list of strings)
- `idempotency_key` (string <= 128 chars)

Hypothesis panel options:
- `hypothesis_window_years`
- `hypothesis_confirmatory_window_years`
- `hypothesis_time_series_precheck_mode`
- `hypothesis_auto_confirmatory_policy`
- `hypothesis_nonconfirmatory_max_tier`
- `hypothesis_time_series_min_positive_events`
- `hypothesis_time_series_min_track_positive_events`
- `hypothesis_time_series_min_positive_share`

## 2) Status contract

Type:
- `RunStatusContract`

Fields:
- `run_id`
- `mode`
- `state`: one of `queued/running/succeeded/failed/cancelled`
- `created_at_utc`
- `updated_at_utc`
- `progress_stage` (optional)
- `progress_message` (optional)
- `progress_fraction` (optional, 0.0~1.0)
- `attempt` (int >= 1)
- `error` (optional `RunErrorContract`)

## 3) Error contract

Type:
- `RunErrorContract`

Fields:
- `code` (string)
- `message` (string)
- `retryable` (bool)
- `details` (object)

## 4) Result contract

Types:
- `RunArtifactsContract`
- `RunResultContract`

Result fields:
- `run_id`, `mode`, `state`
- `artifacts` (output path manifest)
- `counts` (non-negative integer map)
- `governance_checks` (check map; includes direction-review style checks)
- `audit_hashes` (`data_hash`, `config_hash`, etc.)
- `timestamp_utc`

## 5) Notes

- These contracts are stdlib-only and intentionally independent from web frameworks.
- API layer (L4) should reuse this contract module directly.
- CLI and API should produce equivalent payloads for the same run.

## 6) L2 facade

- `regspec_machine/engine.py` provides `PresetEngine`, a contract-first facade that:
  - accepts `RunRequestContract`
  - dispatches preset CLI execution (`scripts/modeling/run_phase_b_regspec_preset.py`)
  - returns `RunStatusContract` + `RunResultContract` via `EngineExecution`
  - supports baseline shortcuts: `run_nooption`, `run_singlex`, `run_paired`

## 7) L3 orchestration

- `regspec_machine/orchestrator.py` provides `RunOrchestrator`:
  - `submit()` -> `queued`
  - `execute()` -> `running` -> `succeeded/failed`
  - `retry()` for failed/cancelled runs (attempt-bounded)
  - `cancel()` for queued/running runs
- Orchestrator stores per-run snapshots (`request/status/result/command/stdout_tail/stderr_tail`)
- Optional `events_jsonl` appends lifecycle events for audit/resume tooling.
