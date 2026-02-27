# Phase 0 Quality Ongoing

## Quick Start
- objective:
  - regspec 실행 입력/출력 메타데이터 품질을 점검해 baseline 해석 리스크를 낮춘다.
- must_run:
  - `TAG=phase-0 ./modules/03_regspec_machine/scripts/run_module_03.sh contract-ci --exec`
  - `RUN_ID=phase0_quality TAG=phase-0 ./modules/03_regspec_machine/scripts/run_module_03.sh single-nooption --exec`
- must_check:
  - `data/metadata/module03_contract_ci_summary_phase-0.json`
  - `data/metadata/phase_b_bikard_machine_scientist_preset_summary_phase0_quality_nooption_baseline.json`
- optional:
  - `RUN_ID=phase0_quality TAG=phase-0 ./modules/03_regspec_machine/scripts/run_module_03.sh single-singlex --exec`

## Scope
- 입력 결측/경로 오류/요약 스키마 누락 여부 점검.

## Ongoing Tasks
| task_id | topic | status | next action | owner |
|---|---|---|---|---|
| T1 | quality gate | standby | baseline stream 주요 이슈 종료 후 활성화 | modeling-team |
