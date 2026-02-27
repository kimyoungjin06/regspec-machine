#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

detect_root() {
  local start="$1"
  local cur="$start"
  local standalone_candidate=""
  while true; do
    if [[ -f "$cur/AGENTS.md" && -d "$cur/modules" ]]; then
      echo "$cur"
      return 0
    fi
    if [[ -f "$cur/contract.yaml" && -f "$cur/pyproject.toml" && -d "$cur/regspec_machine" && -d "$cur/scripts" ]]; then
      if [[ -z "$standalone_candidate" ]]; then
        standalone_candidate="$cur"
      fi
    fi
    local parent
    parent="$(dirname "$cur")"
    if [[ "$parent" == "$cur" ]]; then
      break
    fi
    cur="$parent"
  done
  if [[ -n "$standalone_candidate" ]]; then
    echo "$standalone_candidate"
    return 0
  fi
  return 1
}

ROOT="$(detect_root "$SCRIPT_DIR")" || {
  echo "Failed to detect repository root (monorepo or standalone module03)." >&2
  exit 1
}
if [[ -d "$ROOT/modules/03_regspec_machine" ]]; then
  MOD_ROOT="$ROOT/modules/03_regspec_machine"
else
  MOD_ROOT="$ROOT"
fi

if [[ -n "${PYTHON_BIN:-}" ]]; then
  PY="$PYTHON_BIN"
elif [[ -x "$ROOT/.venv/bin/python" ]]; then
  PY="$ROOT/.venv/bin/python"
elif [[ -x "$MOD_ROOT/.venv/bin/python" ]]; then
  PY="$MOD_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY="$(command -v python3)"
else
  PY="python"
fi
MODE="${1:-plan}"
EXEC="${2:-}"
RUN_ID="${RUN_ID:-phase_b_regspec_module03_$(date +%Y%m%d)}"
TAG="${TAG:-$(date +%Y%m%d)}"
OVERNIGHT_MAX_HOURS="${OVERNIGHT_MAX_HOURS:-8}"
OVERNIGHT_SEED_GRID="${OVERNIGHT_SEED_GRID:-20260219,20260220,20260221,20260222}"
OVERNIGHT_BOOTSTRAP_LADDER="${OVERNIGHT_BOOTSTRAP_LADDER:-49,99,199}"
OVERNIGHT_SCAN_MAX_FEATURES="${OVERNIGHT_SCAN_MAX_FEATURES:-0}"
OVERNIGHT_RESUME="${OVERNIGHT_RESUME:-1}"
OVERNIGHT_STOP_ON_FATAL="${OVERNIGHT_STOP_ON_FATAL:-0}"
SCAN_SCRIPT="$MOD_ROOT/scripts/modeling/run_phase_b_bikard_machine_scientist_scan.py"
PRESET_SCRIPT="$MOD_ROOT/scripts/modeling/run_phase_b_regspec_preset.py"
DASHBOARD_SCRIPT="$MOD_ROOT/scripts/reporting/build_phase_b_regspec_dashboard.py"
SMOKE_SCRIPT="$MOD_ROOT/scripts/smoke_module_03_migration_paths.sh"
CONTRACT_CI_SCRIPT="$MOD_ROOT/scripts/check_module_03_contract_ci.py"
DUMP_SCRIPT="$MOD_ROOT/scripts/create_module_03_dataset_dump.sh"
UI_JOURNEY_SMOKE_SCRIPT="$MOD_ROOT/scripts/ui/run_ux_journey_smoke.py"

run_cmd() {
  local cmd="$1"
  if [[ "$EXEC" == "--exec" ]]; then
    echo "[EXEC] $cmd"
    eval "$cmd"
  else
    echo "[PLAN] $cmd"
  fi
}

if [[ ! -x "$PY" ]]; then
  echo "Missing python: $PY" >&2
  exit 1
fi

case "$MODE" in
  plan)
    run_cmd "\"$PY\" \"$SCAN_SCRIPT\" --help"
    run_cmd "\"$PY\" \"$PRESET_SCRIPT\" --help"
    run_cmd "\"$PY\" \"$DASHBOARD_SCRIPT\" --help"
    ;;
  single-nooption)
    run_cmd "\"$PY\" \"$PRESET_SCRIPT\" --mode nooption_baseline --run-id ${RUN_ID}_nooption"
    ;;
  single-singlex)
    run_cmd "\"$PY\" \"$PRESET_SCRIPT\" --mode singlex_baseline --run-id ${RUN_ID}_singlex"
    ;;
  paired)
    run_cmd "\"$PY\" \"$PRESET_SCRIPT\" --mode paired_nooption_singlex --run-id ${RUN_ID}_paired"
    run_cmd "\"$PY\" \"$DASHBOARD_SCRIPT\" --paired-summary-json data/metadata/phase_b_bikard_machine_scientist_paired_preset_summary_${RUN_ID}_paired.json"
    ;;
  overnight)
    OVERNIGHT_CMD="\"$PY\" \"$PRESET_SCRIPT\" --mode overnight_validation --run-id ${RUN_ID}_overnight --runner-python \"$PY\" --max-hours ${OVERNIGHT_MAX_HOURS} --seed-grid ${OVERNIGHT_SEED_GRID} --bootstrap-ladder ${OVERNIGHT_BOOTSTRAP_LADDER}"
    if [[ "${OVERNIGHT_SCAN_MAX_FEATURES}" =~ ^[0-9]+$ ]] && (( OVERNIGHT_SCAN_MAX_FEATURES > 0 )); then
      OVERNIGHT_CMD="${OVERNIGHT_CMD} --scan-max-features ${OVERNIGHT_SCAN_MAX_FEATURES}"
    fi
    if [[ "$OVERNIGHT_RESUME" == "1" || "$OVERNIGHT_RESUME" == "true" ]]; then
      OVERNIGHT_CMD="${OVERNIGHT_CMD} --resume"
    fi
    if [[ "$OVERNIGHT_STOP_ON_FATAL" == "1" || "$OVERNIGHT_STOP_ON_FATAL" == "true" ]]; then
      OVERNIGHT_CMD="${OVERNIGHT_CMD} --stop-on-fatal"
    fi
    run_cmd "$OVERNIGHT_CMD"
    ;;
  migration-smoke)
    run_cmd "TAG=${TAG} \"$SMOKE_SCRIPT\""
    ;;
  contract-ci)
    run_cmd "\"$PY\" \"$CONTRACT_CI_SCRIPT\" --tag ${TAG}"
    # Baseline smoke (fast) to enforce "nooption + singlex" lock during contract checks.
    # Only runs when core Phase-B inputs exist (monorepo environment); standalone repo skips.
    DYAD_BASE_DEFAULT="$ROOT/outputs/tables/phase_b_bikard_policy_doc_twin_dyad_base_20260219.csv"
    EXT_FEATURE_DEFAULT="$ROOT/data/metadata/metadata_extension_feature_table_overton20260130.csv"
    PA_COV_DEFAULT="$ROOT/data/processed/phase_a_model_input_strict_pairs_api_backfilled_overton20260130_labeled.csv"
    SPLIT_DEFAULT="$ROOT/outputs/tables/phase_b_keyfactor_explorer_policy_split_20260219.csv"
    if [[ -f "$DYAD_BASE_DEFAULT" && -f "$EXT_FEATURE_DEFAULT" && -f "$PA_COV_DEFAULT" && -f "$SPLIT_DEFAULT" ]]; then
      SMOKE_RUN_ID="module03_contract_ci_smoke_${TAG}"
      run_cmd "\"$PY\" \"$PRESET_SCRIPT\" --mode paired_nooption_singlex --run-id ${SMOKE_RUN_ID} --runner-python \"$PY\" --scan-n-bootstrap 9 --cli-summary-top-n 3 --extra-arg=--n-restarts=1 --extra-arg=--scan-max-features=20"
    else
      echo "[SKIP] contract-ci baseline smoke: missing core inputs (expected monorepo Phase-B artifacts)." >&2
    fi
    ;;
  dump-internal)
    run_cmd "TAG=${TAG} SCOPE=internal \"$DUMP_SCRIPT\""
    ;;
  ui-journey-smoke)
    run_cmd "\"$PY\" \"$UI_JOURNEY_SMOKE_SCRIPT\" --base-url \"${UI_BASE_URL:-http://127.0.0.1:8010/ui}\""
    ;;
  *)
    echo "Usage: $0 [plan|single-nooption|single-singlex|paired|overnight|migration-smoke|contract-ci|dump-internal|ui-journey-smoke] [--exec]" >&2
    echo "  overnight env: OVERNIGHT_MAX_HOURS, OVERNIGHT_SEED_GRID, OVERNIGHT_BOOTSTRAP_LADDER, OVERNIGHT_SCAN_MAX_FEATURES, OVERNIGHT_RESUME, OVERNIGHT_STOP_ON_FATAL" >&2
    exit 2
    ;;
esac
