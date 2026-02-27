# Module 03: RegSpec-Machine

## Scope
- 무옵션 실행과 `singlex` 실행을 동시 개선하는 탐색/검증 엔진 운영.
- holdout/FDR/bootstrap/restart 거버넌스 하에서 스펙 탐색 안정성 점검.

## Path Pointers (Current Canonical)
- architecture: `docs/specs/ARCHITECTURE.md`
- module docs index: `docs/README.md`
- module specs index: `docs/specs/README.md`
- constitution: `docs/specs/CONSTITUTION.md`
- backlog/todo: `docs/specs/TODO.md`
- active stream: `docs/investigations/streams/phase0_baseline/ongoing.md`
- module code: `regspec_machine/`
- module tests: `tests/`
- package manifest: `pyproject.toml`
- scanner runner: `scripts/modeling/run_phase_b_bikard_machine_scientist_scan.py`
- paired preset: `scripts/modeling/run_phase_b_regspec_preset.py`
- dashboard: `scripts/reporting/build_phase_b_regspec_dashboard.py`
- outputs: `outputs/tables/phase_b_bikard_machine_scientist_*`, `data/metadata/phase_b_bikard_machine_scientist_*`

## In/Out Contract
- in: phase-b 분석 입력(특히 policy-cited twin cohort)
- out: candidate scan logs, top models, stability/FDR summaries, paired dashboard

## Notes
- AGENTS의 Primary Objective Lock(무옵션 + `singlex`)을 직접 수행하는 모듈이다.

## Contract
- `contract.yaml`

## Entrypoint
- `scripts/run_module_03.sh`
- migration smoke: `scripts/smoke_module_03_migration_paths.sh`
- internal dump: `scripts/create_module_03_dataset_dump.sh`
- UI journey smoke: `scripts/ui/run_ux_journey_smoke.py`

예시:
```bash
./scripts/run_module_03.sh plan
RUN_ID=phase_b_regspec_pair_20260224 ./scripts/run_module_03.sh paired --exec
RUN_ID=phase_b_regspec_overnight_20260225 \
OVERNIGHT_MAX_HOURS=8 \
OVERNIGHT_SEED_GRID=20260219,20260220,20260221,20260222 \
OVERNIGHT_BOOTSTRAP_LADDER=49,99,199 \
./scripts/run_module_03.sh overnight --exec
TAG=20260224v1 ./scripts/run_module_03.sh migration-smoke --exec
TAG=20260224v1 ./scripts/run_module_03.sh contract-ci --exec
TAG=20260224 ./scripts/run_module_03.sh dump-internal --exec
UI_BASE_URL=http://127.0.0.1:8010/ui ./scripts/run_module_03.sh ui-journey-smoke --exec
```

## Compatibility
- (monorepo 사용 시) 루트 레거시 wrapper(`scripts/modeling/run_phase_b_*.py`, `scripts/reporting/build_phase_b_regspec_dashboard.py`)는 제거 대상이다.
- 실행/문서/계약은 이 모듈의 canonical 경로(`scripts/...`, `docs/...`)만 사용한다.
