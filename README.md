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

## Quick start

```python
from regspec_machine import ScanConfig, run_key_factor_scan

config = ScanConfig(run_id="example_run")
scan_rows, top_rows, search_log = run_key_factor_scan(
    df=prepared_dataframe,
    feature_registry=feature_registry_rows,
    config=config,
)
```

`prepared_dataframe` must already include split columns (`split_id`, `split_role`) and outcome/context columns (`y_all`, `y_evidence`) expected by the engine.

## Legacy compatibility

If older code imports via `analysis.modules.bikard_machine_scientist`, compatibility wrappers at repo root keep that path working.
New code should import from `regspec_machine`.
