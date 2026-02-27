#!/usr/bin/env python3
"""Module 03 contract CI checker (wrapper removal + contract integrity)."""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple


WRAPPER_TO_CANONICAL_LOCAL: Sequence[Tuple[str, str]] = (
    (
        "scripts/modeling/run_phase_b_bikard_machine_scientist_scan.py",
        "scripts/modeling/run_phase_b_bikard_machine_scientist_scan.py",
    ),
    (
        "scripts/modeling/run_phase_b_regspec_preset.py",
        "scripts/modeling/run_phase_b_regspec_preset.py",
    ),
    (
        "scripts/reporting/build_phase_b_regspec_dashboard.py",
        "scripts/reporting/build_phase_b_regspec_dashboard.py",
    ),
)

CANONICAL_ONLY_LOCAL: Sequence[str] = (
    "scripts/check_module_03_contract_ci.py",
    "scripts/smoke_module_03_migration_paths.sh",
    "scripts/create_module_03_dataset_dump.sh",
)

REQUIRED_ASSETS_LOCAL: Sequence[str] = (
    "README.md",
    "contract.yaml",
    "pyproject.toml",
    "docs/README.md",
    "scripts/run_module_03.sh",
    "scripts/smoke_module_03_migration_paths.sh",
    "scripts/check_module_03_contract_ci.py",
    "regspec_machine/__init__.py",
)

IGNORED_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    "node_modules",
    "tmp",
    "Export",
}

DOC_EXCLUDE_PREFIXES = (
    "docs/operations/RunLog.md",
    "docs/operations/investigations/",
)

# Operational checks should catch root-wrapper references used by executable code,
# but ignore module03-local files where relative script paths are legitimate.
OPERATIONAL_EXCLUDE_PREFIXES = (
    "modules/03_regspec_machine/",
)


@dataclass(frozen=True)
class RefHit:
    relpath: str
    count: int


def detect_repo_root(start: Path) -> Path:
    cur = start.resolve()
    standalone_candidate: Path | None = None
    for cand in [cur, *cur.parents]:
        if (cand / "AGENTS.md").exists() and (cand / "modules").exists():
            return cand
        if (
            (cand / "contract.yaml").is_file()
            and (cand / "pyproject.toml").is_file()
            and (cand / "regspec_machine").is_dir()
            and (cand / "scripts").is_dir()
        ):
            if standalone_candidate is None:
                standalone_candidate = cand
    if standalone_candidate is not None:
        return standalone_candidate
    raise FileNotFoundError("Could not locate repository root (monorepo or standalone module03) from script path.")


def detect_module_root(root: Path) -> Path:
    monorepo_module = root / "modules" / "03_regspec_machine"
    if monorepo_module.is_dir():
        return monorepo_module
    return root


def module_relpath(module_root: Path, root: Path, rel: str) -> str:
    rel_clean = str(rel).strip().lstrip("./")
    try:
        return str((module_root / rel_clean).resolve().relative_to(root.resolve())).replace("\\", "/")
    except Exception:
        return str((module_root / rel_clean)).replace("\\", "/")


def _normalize_contract_path_token(token: str) -> str:
    t = str(token or "").strip().replace("\\", "/").lstrip("./")
    prefix = "modules/03_regspec_machine/"
    if t.startswith(prefix):
        t = t[len(prefix) :]
    return t


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--tag", default=datetime.now(timezone.utc).strftime("%Y%m%d"))
    p.add_argument("--python-bin", default=".venv/bin/python")
    p.add_argument(
        "--out-summary-json",
        default="",
        help="Default: data/metadata/module03_contract_ci_summary_<tag>.json",
    )
    p.add_argument(
        "--out-wrapper-audit-csv",
        default="",
        help="Default: outputs/tables/module03_wrapper_deprecation_audit_<tag>.csv",
    )
    p.add_argument(
        "--strict-removal-gate",
        action="store_true",
        help="Exit non-zero if any legacy wrapper is not removal-ready.",
    )
    return p.parse_args()


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")


def safe_help_check(py_bin: Path, target: Path) -> bool:
    try:
        proc = subprocess.run(
            [str(py_bin), str(target), "--help"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except Exception:
        return False
    return proc.returncode == 0


def iter_text_files(root: Path, roots: Iterable[str], allowed_suffixes: Sequence[str]) -> Iterable[Path]:
    allowed = {s.lower() for s in allowed_suffixes}
    for rel_root in roots:
        base = root / rel_root
        if not base.exists():
            continue
        for p in base.rglob("*"):
            if not p.is_file():
                continue
            if any(part in IGNORED_DIRS for part in p.parts):
                continue
            if p.suffix.lower() not in allowed:
                continue
            yield p


def count_references(
    root: Path,
    needle: str,
    scan_roots: Sequence[str],
    excluded_paths: Sequence[str],
    excluded_prefixes: Sequence[str],
    allowed_suffixes: Sequence[str],
) -> List[RefHit]:
    hits: List[RefHit] = []
    excluded_set = set(excluded_paths)
    pattern = re.compile(rf"(?<![/A-Za-z0-9_.-]){re.escape(needle)}(?![A-Za-z0-9_.-])")
    for path in iter_text_files(root, scan_roots, allowed_suffixes=allowed_suffixes):
        rel = path.relative_to(root).as_posix()
        if rel in excluded_set:
            continue
        if any(rel.startswith(prefix) for prefix in excluded_prefixes):
            continue
        content = read_text(path)
        cnt = len(pattern.findall(content))
        if cnt > 0:
            hits.append(RefHit(relpath=rel, count=cnt))
    return sorted(hits, key=lambda x: (x.relpath, x.count))


def parse_contract_canonical_scripts(contract_path: Path) -> List[str]:
    lines = contract_path.read_text(encoding="utf-8").splitlines()
    in_block = False
    entries: List[str] = []
    for line in lines:
        if line.startswith("  canonical_scripts:"):
            in_block = True
            continue
        if in_block:
            if line.startswith("    - "):
                entries.append(line.split("- ", 1)[1].strip())
                continue
            if line.strip() == "":
                continue
            break
    return entries


def parse_contract_shell_entrypoints(contract_path: Path) -> List[str]:
    lines = contract_path.read_text(encoding="utf-8").splitlines()
    in_entry = False
    in_shell = False
    entries: List[str] = []
    for line in lines:
        if line.startswith("entrypoints:"):
            in_entry = True
            continue
        if in_entry and line.startswith("  shell:"):
            in_shell = True
            continue
        if in_entry and in_shell:
            if line.startswith("    - "):
                entries.append(line.split("- ", 1)[1].strip())
                continue
            if line.strip() == "":
                continue
            if not line.startswith("    "):
                break
    return entries


def parse_contract_required_inputs(contract_path: Path) -> List[str]:
    lines = contract_path.read_text(encoding="utf-8").splitlines()
    in_inputs = False
    in_required = False
    entries: List[str] = []
    for line in lines:
        if line.startswith("inputs:"):
            in_inputs = True
            in_required = False
            continue
        if in_inputs and line.startswith("  required:"):
            in_required = True
            continue
        if in_inputs and line.startswith("  optional:") and in_required:
            break
        if in_inputs and in_required:
            if line.startswith("    - "):
                item = line.split("- ", 1)[1].split(" #", 1)[0].strip()
                if item:
                    entries.append(item)
                continue
            if line.startswith("  ") and not line.startswith("    "):
                break
    return entries


def path_entry_exists(root: Path, entry: str) -> bool:
    token = str(entry).strip()
    if not token:
        return False
    if any(ch in token for ch in ("*", "?", "[")):
        return any(p.is_file() for p in root.glob(token))
    return (root / token).exists()


def main() -> int:
    args = parse_args()
    root = detect_repo_root(Path(__file__).resolve())
    module_root = detect_module_root(root)
    is_monorepo = module_root != root
    try:
        module_prefix = str(module_root.relative_to(root)).replace("\\", "/")
    except Exception:
        module_prefix = ""
    if module_prefix == ".":
        module_prefix = ""

    def mrel(rel: str) -> str:
        return module_relpath(module_root, root, rel)

    py_raw = str(args.python_bin).strip()
    py_bin = Path(py_raw)
    if py_bin.is_absolute():
        if not py_bin.exists():
            raise FileNotFoundError(f"python not found: {py_bin}")
    else:
        candidates = [root / py_bin, module_root / py_bin]
        found = next((c for c in candidates if c.exists()), None)
        if found is not None:
            py_bin = found
        else:
            fallback = shutil.which(py_raw) or shutil.which("python3") or shutil.which("python")
            if fallback:
                py_bin = Path(fallback)
            else:
                raise FileNotFoundError(f"python not found from relative path: {py_raw}")

    tag = str(args.tag).strip() or datetime.now(timezone.utc).strftime("%Y%m%d")
    out_summary = (
        root / args.out_summary_json
        if str(args.out_summary_json).strip()
        else root / f"data/metadata/module03_contract_ci_summary_{tag}.json"
    )
    out_wrapper_csv = (
        root / args.out_wrapper_audit_csv
        if str(args.out_wrapper_audit_csv).strip()
        else root / f"outputs/tables/module03_wrapper_deprecation_audit_{tag}.csv"
    )
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_wrapper_csv.parent.mkdir(parents=True, exist_ok=True)

    wrapper_to_canonical: Sequence[Tuple[str, str]] = (
        tuple((legacy, mrel(local_canonical)) for legacy, local_canonical in WRAPPER_TO_CANONICAL_LOCAL)
        if is_monorepo
        else tuple()
    )
    canonical_only = tuple(mrel(item) for item in CANONICAL_ONLY_LOCAL)
    canonical_scripts_local = [c for _, c in WRAPPER_TO_CANONICAL_LOCAL] + list(CANONICAL_ONLY_LOCAL)
    canonical_scripts_expected = [mrel(item) for item in canonical_scripts_local]
    required_assets = tuple(mrel(item) for item in REQUIRED_ASSETS_LOCAL)

    wrapper_rows: List[dict] = []
    wrappers_any_fail = False
    n_wrappers_removal_ready = 0

    scan_roots_operational = ("modules", "scripts") if is_monorepo else ("scripts",)
    scan_roots_docs = ("modules", "docs") if is_monorepo else ("docs",)
    excluded_paths = ("modules/MODULE_PATH_MAP.csv",) if is_monorepo else tuple()
    self_rel = Path(__file__).resolve().relative_to(root).as_posix()

    for wrapper_rel, canonical_rel in wrapper_to_canonical:
        wrapper_path = root / wrapper_rel
        canonical_path = root / canonical_rel
        wrapper_exists = wrapper_path.exists()
        canonical_exists = canonical_path.exists()

        wrapper_text = read_text(wrapper_path) if wrapper_exists else ""
        marker_ok = canonical_rel in wrapper_text if wrapper_exists else True
        wrapper_help_ok = bool(wrapper_exists and safe_help_check(py_bin, wrapper_path)) if wrapper_exists else True
        canonical_help_ok = bool(canonical_exists and safe_help_check(py_bin, canonical_path))

        operational_hits = count_references(
            root=root,
            needle=wrapper_rel,
            scan_roots=scan_roots_operational,
            excluded_paths=(wrapper_rel, self_rel, *excluded_paths),
            excluded_prefixes=OPERATIONAL_EXCLUDE_PREFIXES,
            allowed_suffixes=(".py", ".sh"),
        )
        docs_hits = count_references(
            root=root,
            needle=wrapper_rel,
            scan_roots=scan_roots_docs,
            excluded_paths=(wrapper_rel,),
            excluded_prefixes=DOC_EXCLUDE_PREFIXES,
            allowed_suffixes=(".md", ".txt", ".yaml", ".yml"),
        )

        operational_ref_count = sum(h.count for h in operational_hits)
        docs_ref_count = sum(h.count for h in docs_hits)

        blocking_reasons: List[str] = []
        if not canonical_exists:
            blocking_reasons.append("missing_canonical")
        if not canonical_help_ok:
            blocking_reasons.append("canonical_help_fail")
        if wrapper_exists:
            blocking_reasons.append("wrapper_still_present")
            if not marker_ok:
                blocking_reasons.append("wrapper_target_mismatch")
            if not wrapper_help_ok:
                blocking_reasons.append("wrapper_help_fail")
        if operational_ref_count > 0:
            blocking_reasons.append("operational_refs_present")

        removal_ready = len(blocking_reasons) == 0
        if removal_ready:
            n_wrappers_removal_ready += 1
        else:
            wrappers_any_fail = True

        wrapper_rows.append(
            {
                "wrapper_path": wrapper_rel,
                "canonical_path": canonical_rel,
                "wrapper_exists": int(wrapper_exists),
                "canonical_exists": int(canonical_exists),
                "wrapper_help_ok": int(wrapper_help_ok),
                "canonical_help_ok": int(canonical_help_ok),
                "wrapper_forward_marker_ok": int(marker_ok),
                "operational_ref_count": int(operational_ref_count),
                "docs_ref_count": int(docs_ref_count),
                "operational_ref_files": ";".join(h.relpath for h in operational_hits),
                "docs_ref_files": ";".join(h.relpath for h in docs_hits),
                "removal_ready": int(removal_ready),
                "blocking_reasons": ",".join(blocking_reasons),
            }
        )

    contract_path = module_root / "contract.yaml"
    contract_scripts = parse_contract_canonical_scripts(contract_path)
    expected_scripts = canonical_scripts_expected
    expected_scripts_norm = [_normalize_contract_path_token(x) for x in expected_scripts]
    contract_scripts_norm = [_normalize_contract_path_token(x) for x in contract_scripts]
    contract_scripts_match = contract_scripts_norm == expected_scripts_norm
    contract_shell = parse_contract_shell_entrypoints(contract_path)

    required_inputs = parse_contract_required_inputs(contract_path)
    required_input_checks = {rel: path_entry_exists(root, rel) for rel in required_inputs}
    required_inputs_check_mode = "enforced_monorepo" if is_monorepo else "informational_standalone"
    required_inputs_ok = (all(required_input_checks.values()) if required_input_checks else True) if is_monorepo else True

    required_asset_rows = []
    required_assets_ok = True
    for rel in required_assets:
        exists = (root / rel).exists()
        required_asset_rows.append({"path": rel, "exists": int(exists)})
        required_assets_ok = required_assets_ok and exists

    wrappers_total = int(len(wrapper_rows))
    wrappers_blocked = int(wrappers_total - n_wrappers_removal_ready)

    checks = {
        "wrappers_any_fail": bool(wrappers_any_fail),
        "wrappers_all_removal_ready": wrappers_blocked == 0,
        "required_assets_ok": bool(required_assets_ok),
        "required_inputs_ok": bool(required_inputs_ok),
        "contract_canonical_scripts_match_expected": bool(contract_scripts_match),
    }
    overall_ok = (
        checks["required_assets_ok"]
        and checks["required_inputs_ok"]
        and checks["contract_canonical_scripts_match_expected"]
        and (wrappers_total == 0 or checks["wrappers_all_removal_ready"])
    )

    summary = {
        "module_id": "03_regspec_machine",
        "analysis_phase": "module03_contract_ci",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "inputs": {
            "python_bin": str(py_bin.relative_to(root) if py_bin.is_relative_to(root) else py_bin),
            "contract_yaml": str(contract_path.relative_to(root) if contract_path.is_relative_to(root) else contract_path),
            "topology": "monorepo" if is_monorepo else "standalone_module03",
            "module_root": str(module_root.relative_to(root) if module_root.is_relative_to(root) else module_root),
        },
        "checks": checks,
        "counts": {
            "wrappers_total": wrappers_total,
            "wrappers_with_failures": wrappers_blocked,
            "wrappers_removal_ready": int(n_wrappers_removal_ready),
            "required_assets_total": int(len(required_asset_rows)),
            "required_assets_missing": int(sum(1 for r in required_asset_rows if r["exists"] == 0)),
            "required_inputs_total": int(len(required_input_checks)),
            "required_inputs_missing": int(sum(1 for v in required_input_checks.values() if not v)),
        },
        "wrapper_gate": {
            "n_total": wrappers_total,
            "n_removal_ready": int(n_wrappers_removal_ready),
            "n_blocked": wrappers_blocked,
            "strict_mode": bool(args.strict_removal_gate),
            "policy": "wrappers_removed_required",
        },
        "required_inputs": required_input_checks,
        "required_inputs_check_mode": required_inputs_check_mode,
        "contract": {
            "canonical_scripts_expected": expected_scripts,
            "canonical_scripts_actual": contract_scripts,
            "canonical_scripts_expected_normalized": expected_scripts_norm,
            "canonical_scripts_actual_normalized": contract_scripts_norm,
            "shell_entrypoints_actual": contract_shell,
            "module_prefix": module_prefix,
        },
        "outputs": {
            "wrapper_audit_csv": str(out_wrapper_csv.relative_to(root)),
            "summary_json": str(out_summary.relative_to(root)),
        },
        "status": {
            "overall_ok": bool(overall_ok),
        },
    }
    out_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    fieldnames = [
        "wrapper_path",
        "canonical_path",
        "wrapper_exists",
        "canonical_exists",
        "wrapper_help_ok",
        "canonical_help_ok",
        "wrapper_forward_marker_ok",
        "operational_ref_count",
        "docs_ref_count",
        "operational_ref_files",
        "docs_ref_files",
        "removal_ready",
        "blocking_reasons",
    ]
    with out_wrapper_csv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in wrapper_rows:
            writer.writerow(row)

    if args.strict_removal_gate and wrappers_blocked > 0:
        return 2
    if not overall_ok:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
