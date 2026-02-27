# Phase 2 Contract Ongoing

## Quick Start
- objective:
  - module03 contract 규약과 경로 무결성을 고정한다.
- must_run:
  - `TAG=phase-2 ./modules/03_regspec_machine/scripts/run_module_03.sh contract-ci --exec`
  - `TAG=phase-2 ./modules/03_regspec_machine/scripts/run_module_03.sh migration-smoke --exec`
- must_check:
  - `data/metadata/module03_contract_ci_summary_phase-2.json`
- optional:
  - `RUN_ID=phase2_contract TAG=phase-2 ./modules/03_regspec_machine/scripts/run_module_03.sh paired --exec`

## Scope
- contract 오류 조기 탐지와 canonical 경로 고정.

## Ongoing Tasks
| task_id | topic | status | next action | owner |
|---|---|---|---|---|
| T1 | contract hardening | standby | baseline 합의 이후 규약 잠금 수행 | modeling-team |
