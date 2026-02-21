# regspec-machine

Governed regression specification search engine for key-factor discovery with holdout validation, clustered bootstrap, and FDR control.

## Why this repository

`regspec-machine` is a standalone extraction of the key-factor scan engine.
It focuses on one job: searching regression specifications while enforcing audit-friendly guardrails.

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

## Install (editable)

```bash
python -m pip install -e .
```

## Test (dev)

```bash
python -m pip install -e .[test]
python -m pytest -q
```

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
--out-restart-stability-csv outputs/tables/custom_restart_stability.csv
```

- `--auto-scale-y-validated-gates`: adapts validated gates per `y_col` using validation capacity.
- `--y-feasibility-mode`: `warn` | `fail_unusable` | `fail_below_floor`; choose fail-fast policy for low-capacity Y contexts.
- `--scan-family-dedupe-mode atom`: keeps one representative feature per atom family (reduces expression over-crowding).
- `--auto-bootstrap-escalation`: reruns borderline validation candidates with higher bootstrap.
- `--enforce-track-consensus`: demotes non-anchor validated candidates when the anchor track does not reach required tier.
- `--out-restart-stability-csv`: writes per-candidate restart stability diagnostics for reproducibility monitoring.

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
(for example `..._scan_runs_..._refine.csv`).

## Short command (preset launcher)

If the full CLI feels too long, use the preset launcher:

```bash
cd /home/kimyoungjin06/Desktop/Workspace/1.2.8.TwinPaper
.venv/bin/python scripts/modeling/run_phase_b_regspec_preset.py --mode openexplore_autorefine
```

Modes:
- `openexplore_autorefine`: stage-1 open explore + stage-2 shortlist refinement
- `openexplore`: only stage-1 open explore
- `singlex`: only `is_academia_origin` hypothesis run

Useful overrides:
- `--run-id <custom_id>`
- `--scan-n-bootstrap <int>`
- `--refine-n-bootstrap <int>` (for `openexplore_autorefine`)
- `--dry-run` (prints the expanded long command without executing)

## Legacy compatibility

If older code imports via `analysis.modules.bikard_machine_scientist`, compatibility wrappers at repo root keep that path working.
New code should import from `regspec_machine`.
