# Phase 3 Release Ongoing

## Quick Start
- objective:
  - module03 내부 배포용 dump와 결과 인덱스를 재현 가능한 형태로 고정한다.
- must_run:
  - `TAG=phase-3 ./modules/03_regspec_machine/scripts/run_module_03.sh dump-internal --exec`
  - `RUN_ID=phase3_release TAG=phase-3 ./modules/03_regspec_machine/scripts/run_module_03.sh paired --exec`
- must_check:
  - `Export/dumps/module03_regspec_machine_internal_phase-3.manifest.tsv`
  - `data/metadata/phase_b_bikard_machine_scientist_paired_preset_summary_phase3_release_paired.json`
- optional:
  - `TAG=phase-3 ./modules/03_regspec_machine/scripts/run_module_03.sh contract-ci --exec`

## Scope
- 내부 공유를 위한 산출물 묶음과 근거 summary 동기화.

## Ongoing Tasks
| task_id | topic | status | next action | owner |
|---|---|---|---|---|
| T1 | release pack | standby | phase2_contract 완료 후 활성화 | modeling-team |
