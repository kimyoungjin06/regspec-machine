# regspec-machine 입문 가이드 (회귀분석을 몰라도 이해하는 버전)

## 1) 이 도구를 한 줄로 말하면
`regspec-machine`은 "어떤 지표(X)가 결과(Y)와 정말 관련 있어 보이는지"를 자동으로 많이 시험해보고, 과적합을 막는 규칙으로 걸러주는 도구입니다.

## 2) 왜 필요한가
사람이 직접 모든 조합을 돌려보면:
- 시간이 오래 걸리고
- 우연히 좋아 보이는 결과를 고르기 쉽고
- 나중에 재현이 어렵습니다.

이 도구는 같은 절차를 기계적으로 반복하고, 감사 가능한 로그를 남깁니다.

## 3) 아주 쉬운 비유
오디션이라고 생각하면 됩니다.

- `discovery`: 예선 오디션 (후보를 넓게 탐색)
- `validation`: 본선 오디션 (예선 합격 후보를 별도 데이터로 재검증)
- `validated_candidate`: 예선+본선 모두 통과한 후보
- `support_candidate`: 예선은 통과했지만 본선까지는 확정 못한 후보

핵심은 "본선 점수로 예선 후보를 다시 바꾸지 않는다"입니다.

## 4) 입력과 출력
입력(요약):
- 논문/정책 문서/쌍(pair) 단위 데이터
- 후보 피처 레지스트리(또는 자동 생성)
- split 파일(탐색/검증 분리 규칙)

출력(핵심):
- `scan_runs_*.csv`: 각 후보의 split별 실행 결과
- `top_models_*.csv`: 후보 단위 요약 결과
- `top_models_*_inference.csv`: restart를 합쳐 본 후보 단위 추론 뷰
- `run_summary_*.json`: 실행 설정/해시/거버넌스 요약
- `search_log_*.jsonl`: 후보 계획/스킵/실행 로그

## 5) 꼭 알아야 할 안전장치
- `policy_document_id` 기준으로 discovery/validation 분리
- validation으로 후보를 다시 고르지 않음
- bootstrap으로 불확실성 추정
- FDR(q-value)로 다중검정 보정
- `data_hash`, `config_hash`, `feature_registry_hash`로 실행 재현성 추적

## 6) 작동 원리 (쉽게)
아래 순서대로 동작합니다.

1. 데이터 준비 + split 고정  
   먼저 데이터를 읽고, `policy_document_id` 단위로 discovery/validation을 나눕니다.

2. 후보 피처 목록 만들기  
   사용할 수 있는 피처 목록(레지스트리)을 만들거나 불러옵니다.

3. 후보 식(candidate) 계획 수립  
   `track × context(y) × spec × feature` 조합으로 시험할 후보를 만듭니다.
   이 시점에 후보 풀이 고정됩니다.

4. discovery(예선) 실행  
   각 후보를 discovery 데이터에서 적합하고, bootstrap으로 p-value를 계산합니다.

5. validation(본선) 실행  
   같은 후보를 validation 데이터에서 다시 계산합니다.
   이때 validation 결과로 후보 목록을 바꾸지 않습니다.

6. 다중검정 보정(FDR)  
   validation p-value를 family 단위로 보정해 `q_value`를 만듭니다.

7. 후보 등급 부여  
   - `validated_candidate`: validation에서 p/q 기준 통과  
   - `support_candidate`: discovery는 좋지만 validation 확정은 못함  
   - `exploratory`: 그 외

8. 재시작(restart) 통합  
   seed를 바꿔 여러 번 반복한 결과를 합쳐 안정성(재현성)을 봅니다.

9. 감사 가능한 산출물 기록  
   run summary/search log에 거버넌스/해시/설정을 남깁니다.

왜 이 구조가 중요한가:  
좋아 보이는 결과를 나중에 뒤집거나, validation을 보면서 후보를 다시 고르는 실수를 막기 위해서입니다.

## 7) 자주 쓰는 실행 모드
- `singlex_baseline`: 특정 가설 피처 1개 중심으로 안정적으로 확인
- `nooption_baseline`: 제한 없이 기본 후보군을 폭넓게 확인
- `paired_nooption_singlex`: 위 2개를 한 번에 실행해 비교

권장 시작점은 `paired_nooption_singlex`입니다.

## 8) 가장 쉬운 실행 예시
프로젝트 루트(이 모듈 디렉토리)에서:

```bash
.venv/bin/python scripts/modeling/run_phase_b_regspec_preset.py \
  --mode paired_nooption_singlex \
  --run-id phase_b_bikard_keyfactor_scan_pair_baseline_example_20260223 \
  --scan-n-bootstrap 49
```

validation 게이트를 레거시 단일 게이트로 맞춰 비교하고 싶으면:

```bash
.venv/bin/python scripts/modeling/run_phase_b_regspec_preset.py \
  --mode paired_nooption_singlex \
  --run-id phase_b_bikard_keyfactor_scan_pair_legacysync_example_20260223 \
  --scan-n-bootstrap 49 \
  --paired-legacy-sync-validation
```

## 9) 결과 해석 순서 (초보자용)
1. `run_summary_*.json`에서 `search_governance` 확인  
   - `validation_used_for_search=false`
   - `candidate_pool_locked_pre_validation=true`

2. `top_models_*_inference.csv`에서 `candidate_tier` 확인  
   - `validated_candidate`가 있으면 가장 강한 후보
   - `support_candidate`는 후속 데이터/조건에서 재검증 필요

3. `q_value_validation` 확인  
   - 보통 작을수록 보수적으로도 유의할 가능성이 큼

4. paired면 `nooption` vs `singlex`를 같이 비교  
   - 후보 수, tier 분포, 거버넌스 값이 일관적인지 확인

## 10) 오해하기 쉬운 포인트
- `validated_candidate=0`이면 "분석 실패"는 아닙니다.  
  현재 데이터에서 보수적 기준을 통과한 후보가 없다는 뜻입니다.

- p-value가 작아도 q-value(FDR 보정)가 크면 보수적으로는 확정하기 어렵습니다.

- 모델이 복잡할수록 자동으로 불리해질 수 있습니다(복잡도 페널티).

## 11) 결론
이 도구의 목적은 "가장 그럴듯해 보이는 식"을 뽑는 것이 아니라,
"검증 규칙을 통과한 후보를 재현 가능하게 제시"하는 것입니다.
