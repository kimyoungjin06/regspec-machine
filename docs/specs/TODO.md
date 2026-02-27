# RegSpec-Machine TODO (Constitution-Based)

This backlog is ordered by governance impact and baseline value.

## P0 (Must-Have)

### T0. Baseline Lock Enforcement
- Goal: any change must be evaluated on **both** baselines (`nooption`, `singlex`) first.
- Deliverables:
  - `scripts/run_module_03.sh` prints explicit baseline commands.
  - `contract-ci` includes at least a minimal smoke for both modes.
- Acceptance:
  - `./scripts/run_module_03.sh contract-ci --exec` passes.
  - Both baseline runs produce non-empty `scan_runs` and `top_models` outputs (or fail with actionable reasons).
- Status: Done (2026-02-27).
- Implemented: `scripts/run_module_03.sh contract-ci --exec` now runs a fast paired smoke (`paired_nooption_singlex`) when Phase-B inputs are present.

### T1. Fixed/Required Variables (Anchored Spec Mode)
- Goal: allow users to **fix a mandatory variable set** (controls and/or anchor factor) and explore incremental additions.
- Proposed interface:
  - `--control-spec-mode` (example: `both|key_only|key_plus_base_controls`)
  - optional: `--base-controls` override (comma-separated), with "present-only" resolution + audit meta
  - optional: `--base-controls-strict` (fail fast if any requested base control is missing)
- Acceptance:
  - Config hash changes when fixed controls change.
  - UI and run summary show "controls used" clearly.
  - Guardrails: if fixed controls are not estimable under the estimator (no within-event variation), surface a clear error or skip reason.
- Notes:
  - Fixed base-controls are supported (`--base-controls`, `--base-controls-strict`) and are included in `config_hash` via `controls_meta`.
  - Anchor-factor-as-mandatory-regressor (scan incremental additions given fixed anchor) is still a candidate future upgrade.

### T2. Explorer Sweep UX Compression (High ROI)
- Goal: reduce scroll, reduce whitespace, make discovery -> inspect loop faster.
- Must include:
  - Scatter hover that never blocks view (no giant overlay); click selects focus.
  - Table: sort + filter are always visible and intuitive.
  - Layout: a stable "square-ish" scatter area with aligned x/y distributions.
- Acceptance:
  - "Find best run -> open Run Details" can be done in <= 6 clicks from cold start (measured by the UX journey benchmark).
- Status: Done (2026-02-27).
- Implemented: compact Explorer joint layout + non-blocking hover + deep-dive folding + table header click-to-sort + sticky toolbar.
- Reference commits: `0540f5f`, `f76375c`, `2d784a2`.

### T3. FDR Help for Regression Users
- Goal: make q-values understandable to users who know regression but not FDR.
- Deliverables:
  - UI help panel: "Why q?" with a concrete example (many tests => false positives).
  - Default labeling: "q (FDR-adjusted)" and "p (raw)".
- Acceptance:
  - No ambiguity in UI about which gate is primary.
- Status: Done (2026-02-27).
- Implemented: UI help fold + relabeled q/p across Explorer + validation gates.
- Reference commits: `f76375c`.

## P1 (Should-Have)

### T4. Better Defaults Under Small Sample / Split Ratio
- Goal: avoid "validation always fails" defaults when validation split is small.
- Options:
  - Separate validation gates (`min_*_validated`) already exist; tighten docs and presets.
  - Optional auto-scaling for validation gates (must be fully logged if enabled).

### T5. Categorical Support Upgrade
- Goal: one-hot encoding with strict caps to prevent blow-ups.
- Acceptance:
  - category-level features are hash-stable and included in registry hash.
  - explicit caps: max levels, min counts, max new features.
- Status: Done (2026-02-27).
- Implemented: runner flags `--categorical-encoding-mode onehot` + caps (`--categorical-max-levels-per-feature`, `--categorical-min-level-count`, `--categorical-max-new-features`).
- Implemented: stable token format `cat__<source_feature>__<sanitized>_h<hash8>`.
- Implemented: derived onehot features apply within-event variation / nonmissing share gates (build-scope aligned with discovery-only registry build).

### T6. Derived Feature Grammar (MS Benchmark Lite)
- Goal: expand a small grammar safely (signed log, squares, pairwise) with hard budgets.
- Acceptance:
  - No-degeneracy (equivalence hash) applies to derived features.
  - Complexity penalty and caps prevent runaway.
- Status: Done (2026-02-27).
- Implemented: expression grammar modes (`--expression-registry-mode signed_log1p|signed_log1p_square|ms_benchmark_lite`).
- Implemented: hard budgets with safe defaults (`--expression-max-new-features`, `--expression-max-base-features`, `--expression-max-pairs`).
- Implemented: build-scope aligned gates for derived expressions (within-event variation + nonmissing share) and degenerate-signature skip.
- Implemented: regression smoke test `tests/test_expression_augmentation.py` to prevent runtime regressions in expression augmentation.

## P2 (Nice-To-Have)

### T7. Live Streaming Dashboard
- Goal: as runs complete, explorer updates without manual refresh.
- Constraints:
  - Must preserve run immutability; streaming is append-only.
  - Must not leak validation signals into search.
