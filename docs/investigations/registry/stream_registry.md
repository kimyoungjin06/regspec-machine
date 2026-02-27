# Stream Registry

| stream | purpose | status | active doc | note doc | archive dir |
|---|---|---|---|---|---|
| `phase0_baseline` | nooption + singlex 기준선 실행/비교/안정성 점검 | active | `modules/03_regspec_machine/docs/investigations/streams/phase0_baseline/ongoing.md` | `modules/03_regspec_machine/docs/investigations/streams/phase0_baseline/note.md` | `modules/03_regspec_machine/docs/investigations/streams/phase0_baseline/archive/` |
| `phase0_quality` | 입력 피처/실행 로그/요약 지표 품질 점검 | standby | `modules/03_regspec_machine/docs/investigations/streams/phase0_quality/ongoing.md` | `modules/03_regspec_machine/docs/investigations/streams/phase0_quality/note.md` | `modules/03_regspec_machine/docs/investigations/streams/phase0_quality/archive/` |
| `phase2_contract` | contract-ci + 경로 안정성 + CI 게이트 고정 | standby | `modules/03_regspec_machine/docs/investigations/streams/phase2_contract/ongoing.md` | `modules/03_regspec_machine/docs/investigations/streams/phase2_contract/note.md` | `modules/03_regspec_machine/docs/investigations/streams/phase2_contract/archive/` |
| `phase3_release` | 내부 dump/카탈로그/공개 패키지 전 단계 릴리스 | standby | `modules/03_regspec_machine/docs/investigations/streams/phase3_release/ongoing.md` | `modules/03_regspec_machine/docs/investigations/streams/phase3_release/note.md` | `modules/03_regspec_machine/docs/investigations/streams/phase3_release/archive/` |

## Rules
- 전체 streams 중 `active`는 1개만 유지한다.
- 충돌 시 `active_stream_lock.yaml`을 우선한다.
- stream 단위 실험 기록은 `ongoing.md` 단일 문서로 유지한다.
