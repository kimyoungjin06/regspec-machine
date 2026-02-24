# Module 03: RegSpec-Machine

## Scope
- 무옵션 실행과 `singlex` 실행을 동시 개선하는 탐색/검증 엔진 운영.
- holdout/FDR/bootstrap/restart 거버넌스 하에서 스펙 탐색 안정성 점검.

## Path Pointers (Current Canonical)
- architecture: `docs/design/RegSpecMachine_UI_Agent_Architecture_WBS.md`
- module docs index: `modules/03_regspec_machine/docs/README.md`
- module code: `modules/03_regspec_machine/regspec_machine/`
- module tests: `modules/03_regspec_machine/tests/`
- package manifest: `modules/03_regspec_machine/pyproject.toml`
- scanner runner: `modules/03_regspec_machine/scripts/modeling/run_phase_b_bikard_machine_scientist_scan.py`
- paired preset: `modules/03_regspec_machine/scripts/modeling/run_phase_b_regspec_preset.py`
- dashboard: `modules/03_regspec_machine/scripts/reporting/build_phase_b_regspec_dashboard.py`
- outputs: `outputs/tables/phase_b_bikard_machine_scientist_*`, `data/metadata/phase_b_bikard_machine_scientist_*`

## In/Out Contract
- in: phase-b 분석 입력(특히 policy-cited twin cohort)
- out: candidate scan logs, top models, stability/FDR summaries, paired dashboard

## Notes
- AGENTS의 Primary Objective Lock(무옵션 + `singlex`)을 직접 수행하는 모듈이다.

## Contract
- `modules/03_regspec_machine/contract.yaml`

## Entrypoint
- `modules/03_regspec_machine/scripts/run_module_03.sh`
- migration smoke: `modules/03_regspec_machine/scripts/smoke_module_03_migration_paths.sh`
- internal dump: `modules/03_regspec_machine/scripts/create_module_03_dataset_dump.sh`

예시:
```bash
./modules/03_regspec_machine/scripts/run_module_03.sh plan
RUN_ID=phase_b_regspec_pair_20260224 ./modules/03_regspec_machine/scripts/run_module_03.sh paired --exec
TAG=20260224v1 ./modules/03_regspec_machine/scripts/run_module_03.sh migration-smoke --exec
TAG=20260224v1 ./modules/03_regspec_machine/scripts/run_module_03.sh contract-ci --exec
TAG=20260224 ./modules/03_regspec_machine/scripts/run_module_03.sh dump-internal --exec
```

## Compatibility
- 기존 루트 경로(`scripts/modeling/run_phase_b_*.py`, `scripts/reporting/build_phase_b_regspec_dashboard.py`)는 wrapper로 유지된다.
