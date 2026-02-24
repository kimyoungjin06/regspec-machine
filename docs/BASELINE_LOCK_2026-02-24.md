# Module03 Baseline Lock (2026-02-24)

## Decision
- 독립 저장소 초기 기준선은 `analysis/modules/bikard_machine_scientist`의 `HEAD`가 아니라,
  당시 검증 완료된 **working-tree snapshot**으로 고정한다.

## Why
- module03 canonical 경로 전환/테스트/CI(smoke/contract)를 통과한 상태가 working-tree snapshot 기준이었다.
- HEAD 기준으로 되돌리면 검증 상태와 코드 상태가 불일치할 수 있다.

## Connected Source Metadata
- connected repo path: `analysis/modules/bikard_machine_scientist`
- connected branch: `master`
- connected HEAD at lock time: `474348d`

## Known Divergence vs Connected HEAD
- `regspec_machine/contracts.py`
- `regspec_machine/engine.py`
- `regspec_machine/ui_page.py`
- `tests/test_engine.py`
- `tests/test_preset_and_preflight_helpers.py`

## Pre-GitInit Gate
- `.gitignore` 적용
- cache/artifact 정리(`__pycache__`, `.pyc`, `.pytest_cache`)
- module03 smoke/contract-ci/test 재통과
