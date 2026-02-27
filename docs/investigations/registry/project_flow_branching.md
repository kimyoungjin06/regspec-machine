# Project Flow & Branching (Single-Agent)

## Macro Flow
1. phase0_baseline
2. phase0_quality
3. phase2_contract
4. phase3_release

## Active Operational Flow
1. lock 확인
2. must_run 실행
3. must_check 검증
4. ongoing + RunLog 기록
5. 분기 처리

## Branching
| condition | branch | action |
|---|---|---|
| gate green | continue | 다음 task 진행 |
| run 실패/gate red | failure | hold + 즉시 보고 |
| 증거 불충분 | pending | 결론 보류 + 증거 큐 |
| 외부 의존성 | blocked_external | unblock 조건 기록 |
| 목표 달성 | handoff_ready | next stream 준비 |

## Stream Switch Order
1. lock 갱신
2. registry 동기화
3. new ongoing 갱신
4. RunLog 기록
