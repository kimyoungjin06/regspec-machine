# Investigations Operating Model (Low-Context First)

## 1) Design Goal
- 신규 Agent가 최소 문서만 읽고도 module03 실행/판단을 동일하게 재현하도록 한다.
- 상태 드리프트를 줄이기 위해 상태 진실원을 단일화한다.

## 2) Source-of-Truth Hierarchy
1. `modules/03_regspec_machine/docs/investigations/registry/active_stream_lock.yaml`
2. `modules/03_regspec_machine/docs/investigations/registry/project_flow_branching.md`
3. `modules/03_regspec_machine/docs/investigations/streams/<active>/ongoing.md`
4. `modules/03_regspec_machine/docs/investigations/registry/stream_registry.md`
5. `modules/03_regspec_machine/docs/investigations/registry/runlog_recent.md`
6. `docs/operations/RunLog.md` (full history)

## 3) Active Stream Protocol
- active stream은 1개만 허용한다.
- active 전환 시 반드시 `active_stream_lock.yaml`을 먼저 갱신한다.
- 전환 후 `stream_registry.md` status를 동기화한다.

## 4) Bootstrap Protocol
- 고정 개수 규칙(예: 명령 3개)을 강제하지 않는다.
- 대신 역할 기반으로 운영한다.
- `must_run`: 1~3
- `must_check`: 1~3
- `optional`: 0+

## 5) Doc Lifecycle Protocol
- 실험/진행: `streams/<stream>/ongoing.md`
- 채택/결정: `streams/<stream>/note.md`
- 미채택/보류: `streams/<stream>/archive/`
- pre-lifecycle 문서는 `legacy/pre_lifecycle/`에서 조회 전용으로 유지

## 6) Anti-Drift Checklist
1. active lock의 `active_stream`과 `active_stream_paths`가 일치하는가?
2. active stream의 `ongoing.md` 상단에 `must_run/must_check`가 있는가?
3. `stream_registry.md`의 active row가 정확히 1개인가?
4. 최근 변경이 `runlog_recent.md`에 반영됐는가?
