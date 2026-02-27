# Module 03: RegSpec-Machine

## Scope
- 무옵션 실행과 `singlex` 실행을 동시 개선하는 탐색/검증 엔진 운영.
- holdout/FDR/bootstrap/restart 거버넌스 하에서 스펙 탐색 안정성 점검.

## Path Pointers (Current Canonical)
- architecture: `docs/design/RegSpecMachine_UI_Agent_Architecture_WBS.md`
- module docs index: `modules/03_regspec_machine/docs/README.md`
- module specs index: `modules/03_regspec_machine/docs/specs/README.md`
- investigations index: `modules/03_regspec_machine/docs/investigations/README.md`
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
- UI journey smoke: `modules/03_regspec_machine/scripts/ui/run_ux_journey_smoke.py`

예시:
```bash
./modules/03_regspec_machine/scripts/run_module_03.sh plan
RUN_ID=phase_b_regspec_pair_20260224 ./modules/03_regspec_machine/scripts/run_module_03.sh paired --exec
RUN_ID=phase_b_regspec_overnight_20260225 \
OVERNIGHT_MAX_HOURS=8 \
OVERNIGHT_SEED_GRID=20260219,20260220,20260221,20260222 \
OVERNIGHT_BOOTSTRAP_LADDER=49,99,199 \
./modules/03_regspec_machine/scripts/run_module_03.sh overnight --exec
TAG=20260224v1 ./modules/03_regspec_machine/scripts/run_module_03.sh migration-smoke --exec
TAG=20260224v1 ./modules/03_regspec_machine/scripts/run_module_03.sh contract-ci --exec
TAG=20260224 ./modules/03_regspec_machine/scripts/run_module_03.sh dump-internal --exec
UI_BASE_URL=http://127.0.0.1:8010/ui ./modules/03_regspec_machine/scripts/run_module_03.sh ui-journey-smoke --exec
```

## Compatibility
- 2026-02-26 기준 루트 레거시 wrapper(`scripts/modeling/run_phase_b_*.py`, `scripts/reporting/build_phase_b_regspec_dashboard.py`)는 제거되었다.
- 실행/문서/계약은 모듈 canonical 경로만 사용한다.
