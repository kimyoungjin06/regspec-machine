# RegSpec-Machine Constitution

## Mission
RegSpec-Machine is a **governance-first regression specification explorer**.

- Primary use case: dataset-first exploration where users want to find **robust, audit-ready** candidate specifications quickly.
- Outputs are designed for: external reproduction, internal audit, and conservative interpretation.

## Primary Objective Lock (Non-Negotiable)
Every direction review and every upgrade must start with these two baselines:

1. `nooption` baseline (defaults on the fixed dataset)
2. `singlex` baseline (hypothesis singleton: `X=is_academia_origin`)

Upgrade policy:
- If a change cannot be shown to improve (or at least not regress) **both** baselines under governance,
  the change is treated as **open-explore** only and must not become the default path.

## Governance Invariants
These rules must remain true for any run that claims "validated" results.

1. Holdout integrity
- Holdout split is by `policy_document_id`.
- Validation data must **never** be used for candidate proposal or search decisions.
- Candidate pool is locked before any discovery/validation evaluation.

2. Multiple-testing defense (FDR)
- p-values alone are insufficient when scanning many candidates.
- Default gate is q-value (`BH/FDR`) at a declared threshold (for example `q<=0.10`).
- p-value is still reported, but not used as the primary decision gate.

3. Cluster bootstrap + restart stability
- Clustered bootstrap (unit is explicitly declared, typically `policy_document_id`).
- Bootstrap success ratio gate is enforced (avoid silent partial failures).
- Restarts (seed grid) are first-class: "stable" beats "lucky".

4. Information gates (anti-fragility)
- Minimum informative events and minimum informative clusters.
- Top-1 cluster share gate (avoid single-document dominance).

5. Auditability
- Every output row includes at least: `data_hash`, `config_hash`, `feature_registry_hash`, `git_commit`, `timestamp`.
- Any auto-disable/auto-scale behavior must be recorded as explicit metadata.

## What We Benchmark (And Why)

### Machine Scientist (Guimera) Benchmarks
We do not aim to copy the full system; we selectively benchmark what improves search quality under governance:

- **No-degeneracy / equivalence hashing**: avoid testing the same model under different names.
- **Derived feature grammar (lite)**: controlled expansion (expression registry) with strict caps.
- **Complexity penalty**: prefer simpler specs when evidence is comparable.
- **Discovery-only search + locked candidate pool**: prevents validation leakage by construction.

### SHAP (Interpretability) Benchmarks
We do not need full SHAP to be useful for exploration:

- Use **SHAP-lite** contributions where valid: `contrib_j = beta_j * x_diff_j`.
- Always include uncertainty summaries (bootstrap bands) and grouping support.
- The goal is not causal attribution; it is "what moves the utility/prediction in this model".

### SGD/Adam (Optimization) Benchmarks
Optimizers are about **fit speed and convergence**, not selection governance.

- Allowed use: accelerate repeated fits (bootstrap/restarts) without changing the selection contract.
- Required reporting: speedup, failure rate, determinism drift, and rank stability vs. baseline solver.
- If optimizer changes ranking materially, it is treated as unsafe for default mode.

## Definitions (Contract Terms)
- Candidate: a fully specified `(track, context, y, spec_id, key_factor)` evaluation unit.
- `spec_id`: model spec family (example: `clogit_key_only`, `clogit_key_plus_base_controls`).
- Validated candidate: passes validation gate(s) under holdout and FDR policy.
- Restart stability: validated-rate and best-q dispersion across restart seeds.

## Default Interpretation Policy
- This tool is an **explorer**, not a claim generator.
- "Validated" means "passes conservative gates under this dataset and this governance".
- Any scientific/causal claim requires a separate confirmatory workflow.

