## Summary
- What changed:
- Why:
- Risk level: `low` / `medium` / `high`

## Scope
- [ ] `nooption` path touched
- [ ] `singlex` path touched
- [ ] paired preset path touched
- [ ] API/UI layer touched
- [ ] docs-only change

## Required Validation
- [ ] Baseline rerun: `nooption` executed
- [ ] Baseline rerun: `singlex` executed
- [ ] Direction review generated and checked (`validated/p/q/restart/consensus`)
- [ ] Tests passed (`.venv/bin/pytest -q analysis/modules/bikard_machine_scientist/tests`)

## Governance Checklist
- [ ] `validation_used_for_search=false` preserved
- [ ] `candidate_pool_locked_pre_validation=true` preserved
- [ ] holdout/FDR/bootstrap/restart behavior unchanged or explicitly justified
- [ ] data/config/feature registry hash outputs unaffected or explicitly justified

## CLI/API/UI Parity (if applicable)
- [ ] Same input gives equivalent result between CLI and new path
- [ ] Any differences are documented with reason and impact

## Artifacts
- Run summary:
- Direction review:
- Top models inference:
- Restart stability:

## Notes for Reviewer
- Primary files:
- Known limitations:
- Follow-up tasks:
