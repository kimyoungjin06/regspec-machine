# regspec-machine

Governed regression specification search engine for key-factor discovery with holdout validation, clustered bootstrap, and FDR control.

## Why this repository

`regspec-machine` is a standalone extraction of the key-factor scan engine.
It focuses on one job: searching regression specifications while enforcing audit-friendly guardrails.

Beginner guide (Korean, no-regression background):
- `docs/BEGINNER_GUIDE_KO.md`

## Project Priority (TwinPaper)

For TwinPaper operations, the first goal is:
- build and tune the module so that **both**:
  - the no-option path (`nooption`), and
  - the hypothesis-first singleton path (`singlex`)
  are strong and reproducible.

Direction-review order (mandatory):
1. run `nooption` baseline for current code
2. run `singlex` baseline for current code
3. apply one change set
4. rerun `nooption` + `singlex` and compare `validated_candidate`, `p/q`, and restart stability
5. only then run open-explore for secondary insight

## Core guardrails

- Strict discovery/validation split at `policy_document_id` level
- Candidate pool lock before validation (no validation-driven search)
- Cluster bootstrap estimation with finite-sample corrected `p_boot`
- Benjamini-Hochberg FDR (`q_value`) by predefined family
- Row-level audit fields (`data_hash`, `config_hash`, `feature_registry_hash`, `git_commit`, timestamps)

## Repository layout

- `regspec_machine/`: core package implementation
- `docs/`: architecture and operating notes
- `tests/`: smoke tests for split/summary behavior
- `*.py` at repository root: legacy compatibility wrappers for old import paths

Execution contracts for upcoming API/UI work:
- `docs/CONTRACTS.md`
- `regspec_machine/contracts.py`
- `regspec_machine/engine.py` (L2 facade: request -> execute -> status/result)
- `regspec_machine/orchestrator.py` (L3 lifecycle manager: submit/execute/retry/cancel)

## Install (editable)

```bash
python -m pip install -e .
```

Install API extras (FastAPI/uvicorn):

```bash
python -m pip install -e .[api]
```

## Test (dev)

```bash
python -m pip install -e .[test]
python -m pytest -q
```

L7 parity/governance regression focus:

```bash
python -m pytest -q tests/test_parity_l2_l3_l4.py
```

CI:
- GitHub Actions runs `python -m pytest -q` on push/PR (`.github/workflows/ci.yml`)
- CI also runs `regspec-build-desktop` smoke build and uploads bundle manifest artifact.

## Quick start (Python API)

```python
from pathlib import Path

from regspec_machine import (
    ScanConfig,
    apply_policy_split_file,
    build_feature_registry,
    load_and_prepare_data,
    run_key_factor_scan,
)

data, _ = load_and_prepare_data(
    dyad_base_csv=Path("outputs/tables/phase_b_bikard_policy_doc_twin_dyad_base_20260219.csv"),
    extension_feature_csv=Path("data/metadata/metadata_extension_feature_table_overton20260130.csv"),
    phase_a_covariates_csv=Path("data/processed/phase_a_model_input_strict_pairs_api_backfilled_overton20260130_labeled.csv"),
)
data, _ = apply_policy_split_file(
    data,
    split_csv=Path("outputs/tables/phase_b_keyfactor_explorer_policy_split_20260219.csv"),
    strict=True,
)
feature_registry = build_feature_registry(
    data,
    min_variation_share=0.10,
    min_nonmissing_share=0.80,
)

config = ScanConfig(
    run_id="regspec_readme_example_20260220",
    n_bootstrap=99,
    min_informative_events_estimable=20,
    min_policy_docs_informative_estimable=10,
    min_informative_events_validated=7,
    min_policy_docs_informative_validated=7,
)
scan_rows, top_rows, search_log = run_key_factor_scan(df=data, feature_registry=feature_registry, config=config)
```

## Programmatic preset execution (L2 facade)

```python
from regspec_machine import PresetEngine, RunRequestContract

engine = PresetEngine(workspace_root="/path/to/TwinPaper")
request = RunRequestContract.from_payload(
    {
        "mode": "paired_nooption_singlex",
        "run_id": "phase_b_pair_example_20260224",
        "scan_n_bootstrap": 49,
    }
)
execution = engine.execute(request)
print(execution.status.state)
print(execution.result.governance_checks)
```

Shortcut wrappers (baseline defaults):
- `engine.run_nooption(run_id=...)` -> `nooption_baseline`
- `engine.run_singlex(run_id=...)` -> `singlex_baseline`
- `engine.run_paired(run_id=...)` -> `paired_nooption_singlex`

## Programmatic workflow orchestration (L3)

```python
from regspec_machine import PresetEngine, RunOrchestrator

engine = PresetEngine(workspace_root="/path/to/TwinPaper")
orch = RunOrchestrator(engine=engine, max_attempts=2)

status = orch.submit({"mode": "paired_nooption_singlex", "run_id": "phase_b_pair_l3_example"})
execution = orch.execute(status.run_id)
print(execution.status.state)
```

## Service API (L4 FastAPI)

```python
from regspec_machine import create_app

app = create_app(workspace_root="/path/to/TwinPaper")
```

Run server:

```bash
python -m uvicorn regspec_machine.api:create_app --factory --host 127.0.0.1 --port 8000
```

Main endpoints:
- `GET /runs` (list snapshots; query: `state`, `limit`)
- `POST /runs` (submit; query params: `execute=true|false`, `dry_run=true|false`)
- `GET /runs/{run_id}` (status)
- `GET /runs/{run_id}/result` (result)
- `GET /runs/{run_id}/summary` (compact result view)
- `GET /runs/{run_id}/review` (core review panel: `validated/p/q/restart/consensus`)
- `POST /runs/{run_id}/cancel` (cancel queued/running)
- `POST /runs/{run_id}/retry` (retry failed/cancelled)
- `GET /runs/{run_id}/artifacts` (artifact manifest + existence checks)
- `GET /ui` (L5 browser console: submit + monitor + summary inspect)
  - run detail panel includes quick KPI cards (`validated`, `best p/q`, `restart`, leakage guard, consensus)

## Local Console Launcher (L6.1)

Install once:

```bash
python -m pip install -e .[api]
```

Run:

```bash
regspec-console --workspace-root /path/to/TwinPaper --open-browser
```

Portable fallback (if script entrypoint is not on PATH):

```bash
python -m regspec_machine.launcher --workspace-root /path/to/TwinPaper --open-browser
```

## Desktop Wrapper PoC (L6.2)

Install desktop extras:

```bash
python -m pip install -e .[api,desktop]
```

Run native-window first (fallback to browser if `pywebview` unavailable):

```bash
regspec-desktop --workspace-root /path/to/TwinPaper
```

Useful options:
- `--title "RegSpec-Machine"`
- `--width 1400 --height 900`
- `--host 127.0.0.1 --port 8000`

Build desktop executable (PyInstaller):

```bash
python -m pip install -e .[build]
regspec-build-desktop --project-root /path/to/regspec-machine
```

One-file windowed build:

```bash
regspec-build-desktop --project-root /path/to/regspec-machine --onefile --windowed
```

Bundle manifest is written by default to:
- `build/dist/regspec-desktop_bundle_manifest.json`

`load_and_prepare_data()` maps outcomes as:
- `y_all` from `reference_dik`
- `y_evidence` from `reference_dik_evidence_use`

## Group Similar Y Indicators

You can define grouped Y contexts from multiple similar indicators with a JSON file.

Example `y_contexts.json`:

```json
{
  "contexts": [
    {
      "context_scope": "policy_any_group",
      "source_cols": ["y_all", "y_evidence"],
      "group_mode": "any_positive"
    }
  ]
}
```

Run with:

```bash
.venv/bin/python scripts/modeling/run_phase_b_bikard_machine_scientist_scan.py \
  --y-contexts-json y_contexts.json \
  --y-contexts-merge-mode append
```

- `append`: default `y_all/y_evidence` + custom grouped contexts
- `replace`: use only contexts from JSON
- `group_mode`: `any_positive` | `all_positive` | `at_least_k` (with `threshold`)

Optional robustness flags:

```bash
--auto-scale-y-validated-gates \
--y-feasibility-mode fail_below_floor \
--scan-family-dedupe-mode atom \
--auto-bootstrap-escalation \
--escalation-n-bootstrap 499 \
--enforce-track-consensus \
--consensus-anchor-track primary_strict \
--consensus-min-anchor-tier validated_candidate \
--out-restart-stability-csv outputs/tables/custom_restart_stability.csv \
--min-free-space-mb 1024 \
--legacy-single-gate-sync-validation \
--confirmatory-y-cols y_all \
--nonconfirmatory-max-tier support_candidate
```

- `--auto-scale-y-validated-gates`: adapts validated gates per `y_col` using validation capacity.
- `--y-feasibility-mode`: `warn` | `fail_unusable` | `fail_below_floor`; choose fail-fast policy for low-capacity Y contexts.
- `--scan-family-dedupe-mode atom`: keeps one representative feature per atom family (reduces expression over-crowding).
- `--auto-bootstrap-escalation`: reruns borderline validation candidates with higher bootstrap (shortlist is selected from inference-aggregated top models when available).
- `--enforce-track-consensus`: demotes non-anchor validated candidates when the anchor track does not reach required tier.
- `--out-restart-stability-csv`: writes per-candidate restart stability diagnostics for reproducibility monitoring.
- `--min-free-space-mb`: fail fast when free disk space is below threshold before stage writes.
- `--legacy-single-gate-sync-validation`: when legacy `--min-*` flags are used, also apply them to validation gates (default is discovery-only override).
- `--confirmatory-y-cols` / `--nonconfirmatory-max-tier`: restrict which `y_col` contexts are eligible for confirmatory validation tiering in inference aggregation.

## TwinPaper example (CLI, real paths)

Run from `TwinPaper` workspace root:

```bash
cd /home/kimyoungjin06/Desktop/Workspace/1.2.8.TwinPaper
.venv/bin/python scripts/modeling/run_phase_b_bikard_machine_scientist_scan.py \
  --run-id phase_b_bikard_keyfactor_scan_readme_example_20260220 \
  --input-dyad-base-csv outputs/tables/phase_b_bikard_policy_doc_twin_dyad_base_20260219.csv \
  --input-extension-feature-csv data/metadata/metadata_extension_feature_table_overton20260130.csv \
  --input-phase-a-covariates-csv data/processed/phase_a_model_input_strict_pairs_api_backfilled_overton20260130_labeled.csv \
  --input-policy-split-csv outputs/tables/phase_b_keyfactor_explorer_policy_split_20260219.csv \
  --input-feature-registry-json data/metadata/phase_b_keyfactor_explorer_feature_registry_refresh_20260220.json \
  --gate-profile adaptive_production \
  --n-bootstrap 99 \
  --scan-max-features 160 \
  --categorical-encoding-mode onehot \
  --categorical-max-levels-per-feature 4 \
  --categorical-min-level-count 10 \
  --categorical-max-new-features 80 \
  --print-cli-summary
```

Main outputs:
- `outputs/tables/phase_b_bikard_machine_scientist_scan_runs_*.csv`
- `outputs/tables/phase_b_bikard_machine_scientist_top_models_*.csv`
- `outputs/tables/phase_b_bikard_machine_scientist_top_models_*_inference.csv` (candidate-level inference view aggregated across restarts)
- `outputs/tables/phase_b_bikard_machine_scientist_restart_stability_*.csv`
- `data/metadata/phase_b_bikard_machine_scientist_run_summary_*.json`

## Hypothesis-first run (single X)

If your confirmatory question is:
`X = is_academia_origin` effect on policy selection (`Y = y_all / y_evidence`),
prepare a minimal registry and run:

```json
{
  "meta": {"note": "single-x confirmatory registry"},
  "feature_registry": [
    {"feature_name": "is_academia_origin", "allowed_in_scan": 1}
  ]
}
```

Then pass it with `--input-feature-registry-json <path_to_json>`.

## Open-explore with auto refinement

When hypothesis is not fixed, you can run wide exploration first and then let the runner
auto-build a compact shortlist from top models and rerun it with stronger bootstrap.

```bash
cd /home/kimyoungjin06/Desktop/Workspace/1.2.8.TwinPaper
.venv/bin/python scripts/modeling/run_phase_b_bikard_machine_scientist_scan.py \
  --run-id phase_b_bikard_keyfactor_scan_openexplore_example_20260220 \
  --input-feature-registry-json data/metadata/phase_b_keyfactor_explorer_feature_registry_refresh_20260220.json \
  --gate-profile adaptive_production \
  --expression-registry-mode ms_benchmark_lite \
  --categorical-encoding-mode onehot \
  --n-bootstrap 49 \
  --auto-refine-shortlist \
  --refine-tier-mode validated_or_support \
  --refine-max-features 8 \
  --refine-dedupe-mode atom \
  --refine-n-bootstrap 499 \
  --refine-run-id-suffix refine \
  --print-cli-summary
```

Refinement artifacts are written with `_<suffix>` added to each output path
(for example `..._scan_runs_..._refine.csv`, `..._top_models_..._refine.csv`, and `..._top_models_..._refine_inference.csv`).
If `--auto-bootstrap-escalation` executes, escalation outputs follow the same pattern with `_escalate` suffix,
including `..._top_models_..._escalate_inference.csv`.

## Short command (preset launcher)

If the full CLI feels too long, use the preset launcher:

```bash
cd /home/kimyoungjin06/Desktop/Workspace/1.2.8.TwinPaper
.venv/bin/python scripts/modeling/run_phase_b_regspec_preset.py --mode openexplore_autorefine
```

Modes:
- `paired_nooption_singlex`: run `nooption_baseline` then `singlex_baseline` in one command, auto-align both branches to `y_all` context by default, and auto-write a direction-review JSON (`validated/p/q/restart-stability/singlex consensus + promotion gates`)
- `paired_nooption_singlex_hypothesis`: run `nooption_hypothesis_panel` then `singlex_hypothesis_panel` (windowed hypothesis panel: `y_3y,y_5y,y_10y`), and auto-write the same direction-review JSON
- `nooption`: minimal runner path without exploration shortcuts
- `nooption_baseline`: nooption path with governance baseline (`n_restarts=5`, auto y-gate scaling, fail on unusable Y, skip discovery-infeasible track×Y blocks, auto-disable plus-base spec under low discovery capacity)
- `nooption_hypothesis_panel`: nooption baseline + derived windowed outcomes (`--derive-y-time-windows`, default years `3,5,10`) with auto confirmatory policy (default keeps `y_5y` confirmatory, demotes low-support windows to nonconfirmatory)
- `openexplore_autorefine`: stage-1 open explore + stage-2 shortlist refinement
- `openexplore`: only stage-1 open explore
- `singlex`: only `is_academia_origin` hypothesis run
- `singlex_baseline`: singleton hypothesis run with governance baseline (`n_restarts=5`, consensus, y-feasibility fail-fast, key-only main path via `--no-base-controls`)
- `singlex_hypothesis_panel`: singlex baseline + derived windowed outcomes (`y_3y,y_5y,y_10y`) with auto confirmatory policy (default keeps `y_5y` confirmatory, `y_3y` sensitivity)

Useful overrides:
- `--run-id <custom_id>`
- `--scan-n-bootstrap <int>`
- `--refine-n-bootstrap <int>` (for `openexplore_autorefine`)
- `--out-paired-summary-json <path>` (for `paired_nooption_singlex`, writes pair-level status even on partial failure)
- `--out-direction-review-json <path>` (for paired modes, writes automated direction-review summary JSON)
- `--skip-direction-review` (for paired modes, disables auto direction-review summary generation)
- `--paired-legacy-sync-validation` (for `paired_nooption_singlex`, forwards `--legacy-single-gate-sync-validation` to child runner calls)
- `--paired-y-context-alignment y_all_only|off` (default `y_all_only`; paired baseline only)
- `--no-paired-run-singlex-on-nooption-failure` (default behavior is to run `singlex` even if `nooption` fails)
- `--hypothesis-window-years 3,5,10` (for `*_hypothesis_panel` modes; forwarded in paired hypothesis mode)
- `--hypothesis-confirmatory-window-years 3,5` (default for `*_hypothesis_panel`; avoids redundant confirmatory `y_10y` in current dataset)
- `--hypothesis-time-series-precheck-mode fail_redundant_confirmatory` (default for `*_hypothesis_panel`; fail-fast only on redundant confirmatory windows)
- `--hypothesis-auto-confirmatory-policy drop_redundant_and_low_support` (default for `*_hypothesis_panel`; auto removes low-support windows from confirmatory)
- `--hypothesis-nonconfirmatory-max-tier exploratory` (default for `*_hypothesis_panel`; sensitivity windows are capped at exploratory tier)
- `--extra-arg=--time-series-precheck-mode --extra-arg=fail_any` (fail-fast when redundant confirmatory windows or low-support track-y are detected)
- `--extra-arg=--time-series-min-track-positive-events --extra-arg=<int>` (default follows estimable event gate)
- `--dry-run` (prints the expanded long command without executing)

## Recommended singleton baseline (TwinPaper)

Use this as the default start point for every direction review:

```bash
cd /home/kimyoungjin06/Desktop/Workspace/1.2.8.TwinPaper
.venv/bin/python scripts/modeling/run_phase_b_regspec_preset.py \
  --mode singlex \
  --run-id phase_b_bikard_keyfactor_scan_singlex_baseline_<date> \
  --scan-n-bootstrap 199 \
  --extra-arg=--n-restarts --extra-arg=5 \
  --extra-arg=--enforce-track-consensus \
  --extra-arg=--consensus-anchor-track --extra-arg=primary_strict \
  --extra-arg=--consensus-min-anchor-tier --extra-arg=support_candidate \
  --extra-arg=--auto-scale-y-validated-gates \
  --extra-arg=--y-feasibility-mode --extra-arg=fail_below_floor
```

## Recommended paired baseline (TwinPaper)

Run both baseline paths together at the start of each direction review:

```bash
cd /home/kimyoungjin06/Desktop/Workspace/1.2.8.TwinPaper
.venv/bin/python scripts/modeling/run_phase_b_regspec_preset.py \
  --mode paired_nooption_singlex \
  --run-id phase_b_bikard_keyfactor_scan_pair_baseline_<date> \
  --scan-n-bootstrap 49
```

Default direction-review output is:
- `data/metadata/phase_b_bikard_machine_scientist_direction_review_<run_id>.json`

Primary checks now include:
- execution checks: `all_children_ok`, `required_fields_present`, `singlex_track_consensus_check_pass`
- promotion checks: `nooption_primary_validated_gate_pass`, `nooption_q_gate_pass`, `nooption_restart_validated_rate_gate_pass`, `nooption_promotion_gate_pass`, `primary_objective_gate_pass`

## Recommended paired hypothesis panel (TwinPaper)

Run nooption/singlex together under the same windowed hypothesis panel:

```bash
cd /home/kimyoungjin06/Desktop/Workspace/1.2.8.TwinPaper
.venv/bin/python scripts/modeling/run_phase_b_regspec_preset.py \
  --mode paired_nooption_singlex_hypothesis \
  --run-id phase_b_bikard_keyfactor_scan_pair_hypothesis_<date> \
  --scan-n-bootstrap 49 \
  --hypothesis-window-years 3,5,10
```

## Legacy compatibility

If older code imports via `analysis.modules.bikard_machine_scientist`, compatibility wrappers at repo root keep that path working.
New code should import from `regspec_machine`.
