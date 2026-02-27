# Phase 0 Baseline Master

## Quick Start
- objective:
  - nooption + singlex 기준선 실행 결과를 동일 거버넌스(holdout/FDR/bootstrap/restart) 하에서 재현한다.
- must_run:
  - `RUN_ID=phase0_baseline TAG=phase-0 ./scripts/run_module_03.sh paired --exec`
  - `TAG=phase-0 ./scripts/run_module_03.sh contract-ci --exec`
- must_check:
  - `data/metadata/phase_b_bikard_machine_scientist_paired_preset_summary_phase0_baseline_paired.json`
  - `data/metadata/module03_contract_ci_summary_phase-0.json`
- optional:
  - `RUN_ID=phase0_baseline TAG=phase-0 ./scripts/run_module_03.sh overnight --exec`
  - `RUN_ID=phase0_baseline TAG=phase-0 ./scripts/run_module_03.sh ui-journey-smoke --exec`
- source:
  - active lock 기준: `docs/investigations/registry/active_stream_lock.yaml`

## Scope
- baseline 결과의 유의성/안정성/재현성 점검을 최우선으로 유지한다.

## Ongoing Tasks
| task_id | topic | status | next action | owner |
|---|---|---|---|---|
| T1 | paired baseline rerun | ongoing | phase-0 tag로 paired + contract 실행 후 summary 재확인 | modeling-team |
| T2 | restart stability 점검 | ongoing | overnight 결과에서 validated_rate 변동 폭 기록 | modeling-team |
| T3 | note 반영 | ongoing | 채택된 baseline 기준을 `note.md`에 누적 | modeling-team |
| T4 | constitution-based todo 확정 | ongoing | `docs/specs/TODO.md`의 T0~T2를 baseline stream 작업으로 확정 | modeling-team |

## Backlog Pointer (Constitution-Based)
- upgrade backlog: `docs/specs/TODO.md`
