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
  MOD_PREFIX="modules/03_regspec_machine"
else
  MOD_ROOT="$ROOT"
  MOD_PREFIX=""
fi
OUT_DIR="${OUT_DIR:-$ROOT/Export/dumps}"
TAG="${TAG:-$(date +%Y%m%d)}"
SCOPE="${SCOPE:-internal}"
INCLUDE_ARCHIVE_DOCS="${INCLUDE_ARCHIVE_DOCS:-1}"

if [[ "$SCOPE" != "internal" ]]; then
  echo "Only SCOPE=internal is currently supported." >&2
  exit 2
fi

DUMP_BASENAME="module03_regspec_machine_${SCOPE}_${TAG}"
TMP_DIR="$ROOT/tmp/${DUMP_BASENAME}"
FILELIST="$TMP_DIR/filelist.txt"
MANIFEST_TSV="$TMP_DIR/manifest.tsv"
SUMMARY_JSON="$TMP_DIR/summary.json"
ARCHIVE_PATH="$OUT_DIR/${DUMP_BASENAME}.tar.gz"

mkdir -p "$OUT_DIR" "$TMP_DIR"
: > "$FILELIST"

add_glob_files() {
  local pattern="$1"
  local base="$ROOT"
  shopt -s nullglob
  for f in "$base"/$pattern; do
    if [[ -f "$f" ]]; then
      echo "${f#$ROOT/}" >> "$FILELIST"
    fi
  done
  shopt -u nullglob
}

add_mod_glob_files() {
  local pattern="$1"
  if [[ -n "$MOD_PREFIX" ]]; then
    add_glob_files "$MOD_PREFIX/$pattern"
  else
    add_glob_files "$pattern"
  fi
}

# module-level docs/contracts/scripts
add_glob_files "modules/README.md"
add_mod_glob_files "README.md"
add_mod_glob_files "contract.yaml"
add_mod_glob_files "docs/*.md"
add_mod_glob_files "scripts/*.sh"
add_mod_glob_files "scripts/*.py"
add_mod_glob_files "scripts/modeling/*.py"
add_mod_glob_files "scripts/reporting/*.py"
add_mod_glob_files ".github/workflows/*.yml"
add_mod_glob_files ".github/*.md"
add_mod_glob_files ".gitignore"

# canonical module code package (module-local)
add_mod_glob_files "pyproject.toml"
add_mod_glob_files "__init__.py"
add_mod_glob_files "regspec_machine/*.py"
add_mod_glob_files "tests/*.py"

# legacy wrappers and related runner utility
add_glob_files "scripts/modeling/run_phase_b_bikard_machine_scientist_scan.py"
add_glob_files "scripts/modeling/run_phase_b_regspec_preset.py"
add_glob_files "scripts/reporting/build_phase_b_regspec_dashboard.py"
add_glob_files "scripts/modeling/profile_phase_b_bikard_machine_scientist_ab.py"
add_glob_files "scripts/README.md"

# operational outputs (phase-b regspec)
add_glob_files "outputs/tables/phase_b_bikard_machine_scientist_*"
add_glob_files "data/metadata/phase_b_bikard_machine_scientist_*"
add_glob_files "outputs/reports/phase_b_bikard_machine_scientist_*"
add_glob_files "outputs/reports/regspec_compare/*"
add_glob_files "data/metadata/module03_*"
add_glob_files "outputs/tables/module03_*"

# design / migration / operation docs
add_glob_files "docs/design/RegSpecMachine_UI_Agent_Architecture_WBS.md"
add_glob_files "docs/design/Module03_RegSpecMachine_DocumentCurationPlan_2026-02-24.md"
add_glob_files "docs/design/TwinPaper_ModuleMigration_Backlog_v1_2026-02-24.md"
add_glob_files "docs/design/TwinPaper_Modular_Split_v1_2026-02-24.md"
add_glob_files "docs/operations/RunLog.md"

# optional archived docs related to module03 curation
if [[ "$INCLUDE_ARCHIVE_DOCS" == "1" ]]; then
  add_glob_files "archive/modules/03_regspec_machine/docs/20260224_pre_v2_doc_curation/*"
  add_glob_files "archive/docs/operations/investigations/20260224_regspec_machine_doc_curation/*"
fi

sort -u "$FILELIST" -o "$FILELIST"

if [[ ! -s "$FILELIST" ]]; then
  echo "No files matched for dump." >&2
  exit 1
fi

{
  printf "relative_path\tsize_bytes\tsha256\n"
  while IFS= read -r rel; do
    abs="$ROOT/$rel"
    if [[ -f "$abs" ]]; then
      size="$(stat -c%s "$abs")"
      sha="$(sha256sum "$abs" | awk '{print $1}')"
      printf "%s\t%s\t%s\n" "$rel" "$size" "$sha"
    fi
  done < "$FILELIST"
} > "$MANIFEST_TSV"

tar -czf "$ARCHIVE_PATH" -C "$ROOT" -T "$FILELIST"

n_files="$(($(wc -l < "$FILELIST")))"
payload_bytes="$(awk -F'\t' 'NR>1 {s+=$2} END {printf "%.0f\n", s+0}' "$MANIFEST_TSV")"
archive_bytes="$(stat -c%s "$ARCHIVE_PATH")"

cat > "$SUMMARY_JSON" <<EOF_SUMMARY
{
  "module": "03_regspec_machine",
  "scope": "$SCOPE",
  "tag": "$TAG",
  "include_archive_docs": $INCLUDE_ARCHIVE_DOCS,
  "n_files": $n_files,
  "payload_bytes": $payload_bytes,
  "archive_path": "${ARCHIVE_PATH#$ROOT/}",
  "archive_bytes": $archive_bytes,
  "manifest_tsv": "${MANIFEST_TSV#$ROOT/}",
  "filelist": "${FILELIST#$ROOT/}"
}
EOF_SUMMARY

cp "$MANIFEST_TSV" "$OUT_DIR/${DUMP_BASENAME}.manifest.tsv"
cp "$SUMMARY_JSON" "$OUT_DIR/${DUMP_BASENAME}.summary.json"

echo "archive=$ARCHIVE_PATH"
echo "manifest=$OUT_DIR/${DUMP_BASENAME}.manifest.tsv"
echo "summary=$OUT_DIR/${DUMP_BASENAME}.summary.json"
