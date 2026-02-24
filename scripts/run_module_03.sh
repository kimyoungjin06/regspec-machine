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
SCAN_SCRIPT="$MOD_ROOT/scripts/modeling/run_phase_b_bikard_machine_scientist_scan.py"
PRESET_SCRIPT="$MOD_ROOT/scripts/modeling/run_phase_b_regspec_preset.py"
DASHBOARD_SCRIPT="$MOD_ROOT/scripts/reporting/build_phase_b_regspec_dashboard.py"
SMOKE_SCRIPT="$MOD_ROOT/scripts/smoke_module_03_migration_paths.sh"
CONTRACT_CI_SCRIPT="$MOD_ROOT/scripts/check_module_03_contract_ci.py"
DUMP_SCRIPT="$MOD_ROOT/scripts/create_module_03_dataset_dump.sh"

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
  migration-smoke)
    run_cmd "TAG=${TAG} \"$SMOKE_SCRIPT\""
    ;;
  contract-ci)
    run_cmd "\"$PY\" \"$CONTRACT_CI_SCRIPT\" --tag ${TAG}"
    ;;
  dump-internal)
    run_cmd "TAG=${TAG} SCOPE=internal \"$DUMP_SCRIPT\""
    ;;
  *)
    echo "Usage: $0 [plan|single-nooption|single-singlex|paired|migration-smoke|contract-ci|dump-internal] [--exec]" >&2
    exit 2
    ;;
esac
