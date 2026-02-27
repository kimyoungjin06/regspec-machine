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
if [[ "$MOD_ROOT" == "$ROOT" ]]; then
  IS_MONOREPO=0
else
  IS_MONOREPO=1
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
TAG="${TAG:-$(date +%Y%m%d)}"
OUT_JSON="${OUT_JSON:-$ROOT/data/metadata/module03_migration_smoke_summary_${TAG}.json}"

if [[ ! -x "$PY" ]]; then
  echo "Missing python: $PY" >&2
  exit 1
fi

new_modeling_dir="$MOD_ROOT/scripts/modeling"
new_reporting_dir="$MOD_ROOT/scripts/reporting"
legacy_modeling_dir="$ROOT/scripts/modeling"
legacy_reporting_dir="$ROOT/scripts/reporting"

new_modeling_files=(
  run_phase_b_bikard_machine_scientist_scan.py
  run_phase_b_regspec_preset.py
)
new_reporting_files=(
  build_phase_b_regspec_dashboard.py
)
legacy_modeling_files=(
  run_phase_b_bikard_machine_scientist_scan.py
  run_phase_b_regspec_preset.py
)
legacy_reporting_files=(
  build_phase_b_regspec_dashboard.py
)

asset_paths=(
  "$MOD_ROOT/README.md"
  "$MOD_ROOT/contract.yaml"
  "$MOD_ROOT/pyproject.toml"
  "$MOD_ROOT/docs/README.md"
  "$MOD_ROOT/scripts/run_module_03.sh"
  "$MOD_ROOT/scripts/smoke_module_03_migration_paths.sh"
  "$MOD_ROOT/regspec_machine/__init__.py"
)

pass_count=0
fail_count=0
detail_rows=()

check_help() {
  local label="$1"
  local path="$2"
  if "$PY" "$path" --help >/dev/null 2>&1; then
    pass_count=$((pass_count + 1))
    detail_rows+=("{\"label\":\"$label\",\"path\":\"${path#$ROOT/}\",\"status\":\"pass\"}")
  else
    fail_count=$((fail_count + 1))
    detail_rows+=("{\"label\":\"$label\",\"path\":\"${path#$ROOT/}\",\"status\":\"fail\"}")
  fi
}

check_path_exists() {
  local label="$1"
  local path="$2"
  if [[ -e "$path" ]]; then
    pass_count=$((pass_count + 1))
    detail_rows+=("{\"label\":\"$label\",\"path\":\"${path#$ROOT/}\",\"status\":\"pass\"}")
  else
    fail_count=$((fail_count + 1))
    detail_rows+=("{\"label\":\"$label\",\"path\":\"${path#$ROOT/}\",\"status\":\"fail\"}")
  fi
}

check_path_missing() {
  local label="$1"
  local path="$2"
  if [[ ! -e "$path" ]]; then
    pass_count=$((pass_count + 1))
    detail_rows+=("{\"label\":\"$label\",\"path\":\"${path#$ROOT/}\",\"status\":\"pass\"}")
  else
    fail_count=$((fail_count + 1))
    detail_rows+=("{\"label\":\"$label\",\"path\":\"${path#$ROOT/}\",\"status\":\"fail\"}")
  fi
}

for f in "${new_modeling_files[@]}"; do
  check_help "new" "$new_modeling_dir/$f"
done
for f in "${new_reporting_files[@]}"; do
  check_help "new" "$new_reporting_dir/$f"
done
if [[ "$IS_MONOREPO" -eq 1 ]]; then
  for f in "${legacy_modeling_files[@]}"; do
    check_path_missing "legacy_removed" "$legacy_modeling_dir/$f"
  done
  for f in "${legacy_reporting_files[@]}"; do
    check_path_missing "legacy_removed" "$legacy_reporting_dir/$f"
  done
fi
for p in "${asset_paths[@]}"; do
  check_path_exists "asset" "$p"
done

mkdir -p "$(dirname "$OUT_JSON")"
{
  echo "{"
  echo "  \"module_id\": \"03_regspec_machine\","
  echo "  \"analysis_phase\": \"module03_migration_smoke\","
  echo "  \"generated_at_utc\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\","
  echo "  \"counts\": {\"pass\": $pass_count, \"fail\": $fail_count, \"total\": $((pass_count+fail_count))},"
  echo "  \"details\": ["
  for i in "${!detail_rows[@]}"; do
    if [[ "$i" -gt 0 ]]; then
      echo ","
    fi
    echo -n "    ${detail_rows[$i]}"
  done
  echo
  echo "  ]"
  echo "}"
} > "$OUT_JSON"

echo "smoke_summary=${OUT_JSON#$ROOT/}"
echo "pass=$pass_count fail=$fail_count total=$((pass_count+fail_count))"

if [[ "$fail_count" -gt 0 ]]; then
  exit 1
fi
