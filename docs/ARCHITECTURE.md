# Architecture

## Pipeline

1. Load/prepare input data (`module_input.py`)
2. Build/load feature registry (`feature_registry.py`)
3. Assign holdout split by policy document (`splitter.py`)
4. Run scan over track/context/spec/feature candidates (`search_engine.py`)
5. Estimate effects with conditional logit (`estimators.py`)
6. Quantify uncertainty with clustered bootstrap (`bootstrap.py`)
7. Correct multiplicity using BH-FDR (`fdr.py`)
8. Emit run artifacts (`reporting.py`)

## Key contracts

- `ScanConfig`: global runtime controls for gates, bootstrap, and audit metadata
- `feature_registry`: candidate universe and scan eligibility (`allowed_in_scan`)
- `scan_rows`: per-candidate per-split records with status/reason and audit fields
- `top_rows`: candidate-level summary from discovery + validation evidence
- `search_log`: execution trace for candidate plan and quality checks
- `RunRequestContract` / `RunStatusContract` / `RunResultContract`: L1 execution contracts for CLI/API/UI parity (`contracts.py`, `docs/CONTRACTS.md`)
- `PresetEngine`: L2 execution facade over preset CLI for `nooption/singlex/paired` with contract I/O (`engine.py`)
- `RunOrchestrator`: L3 lifecycle manager (`queued -> running -> succeeded/failed/cancelled`) over L2 (`orchestrator.py`)
- `create_app()`: L4 FastAPI service layer exposing submit/status/result/cancel/artifacts endpoints (`api.py`)
- `build_ui_page_html()`: L5 operator UI page rendered at `/ui` for run submit/monitor/review (`ui_page.py`)
- `regspec-console`: L6.1 local launcher entrypoint for cross-OS operator startup (`launcher.py`)
- `regspec-desktop`: L6.2 desktop wrapper PoC (`desktop.py`, pywebview-first with browser fallback)

## Non-goals

- This package is not a full end-user CLI by itself.
- It does not replace confirmatory analysis plans; outputs are support-only unless explicitly promoted by a separate confirmatory protocol.

## L7 Regression Gate

- `tests/test_parity_l2_l3_l4.py` verifies same-input parity for L2/L3/L4 (`succeeded` and `failed` paths).
- Governance payload parity is enforced for `search_governance.validation_used_for_search=false` and `candidate_pool_locked_pre_validation=true`.
