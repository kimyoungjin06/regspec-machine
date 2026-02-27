# Rulebook v0 (Pre-Restructure Freeze)

status: active  
effective_date: 2026-02-26  
mode: rule-only

## Core Rules
1. 상태 진실원은 `active_stream_lock.yaml` 하나만 사용한다.
2. 실무 업데이트는 `streams/<active>/ongoing.md`에만 기록한다.
3. 채택 결정은 `streams/<active>/note.md`에만 기록한다.
4. 실패/보류 건은 `streams/<active>/archive/`로만 이동한다.
5. `modules/03_regspec_machine/docs/investigations/legacy/`는 archive-only로 유지한다.
6. `runlog_recent.md`는 요약본이며 충돌 시 `RunLog.md`를 우선한다.
7. 스트림 전환 순서: lock 갱신 -> registry 동기화 -> ongoing 갱신.
8. 신규 규칙 추가는 “문서 갱신 + RunLog 기록” 세트로 반영한다.
9. 경로/파일명 변경은 재구조화 착수 전까지 금지한다.
10. 스크립트 변경은 버그/정합성 리스크 차단 목적만 허용한다.

## Restructure Start Conditions
1. 목표 폴더 구조안 확정
2. 마이그레이션 체크리스트 승인
3. 영향 스크립트 목록 확정
4. cutover 태그/시점 합의
