#!/usr/bin/env python3
"""Build a JSON + HTML dashboard from paired regspec preset summaries."""

from __future__ import annotations

import argparse
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _detect_repo_root(start: Path) -> Path:
    cur = start.resolve()
    for cand in [cur, *cur.parents]:
        if (cand / "AGENTS.md").is_file() and (cand / "modules").is_dir():
            return cand
    return Path(__file__).resolve().parents[2]


ROOT = _detect_repo_root(Path(__file__).resolve().parent)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(text).strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "run"


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    if isinstance(value, (int, float)):
        return bool(int(value))
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n", ""}:
        return False
    return bool(default)


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _latest_paired_summary() -> Path:
    candidates = sorted(
        ROOT.glob("data/metadata/phase_b_bikard_machine_scientist_paired_preset_summary_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError("no paired preset summary found under data/metadata")
    return candidates[0]


def _resolve_run_summary_path(run_id: str) -> Path:
    rid = _slug(run_id)
    return ROOT / f"data/metadata/phase_b_bikard_machine_scientist_run_summary_{rid}.json"


def _normalize_output_path(raw_path: str) -> Path:
    p = Path(str(raw_path).strip())
    if p.is_absolute():
        return p
    return ROOT / p


def _resolve_child_run_summary_path(child: Dict[str, Any], run_id: str) -> Tuple[Optional[Path], str]:
    outputs = child.get("outputs", {}) if isinstance(child.get("outputs"), dict) else {}
    for key in ("run_summary_json", "summary"):
        raw = outputs.get(key)
        text = str(raw).strip() if raw is not None else ""
        if text:
            return _normalize_output_path(text), f"child_outputs:{key}"
    rid = str(run_id).strip()
    if rid:
        return _resolve_run_summary_path(rid), "run_id_inferred"
    return None, "unresolved"


def _extract_branch_metrics(run_summary: Dict[str, Any]) -> Dict[str, Any]:
    counts = run_summary.get("counts", {}) if isinstance(run_summary, dict) else {}
    tiers = counts.get("candidate_tier_top_rows_inference")
    if not isinstance(tiers, dict) or not tiers:
        tiers = counts.get("candidate_tier_top_rows") if isinstance(counts.get("candidate_tier_top_rows"), dict) else {}
    restart = run_summary.get("restart", {}) if isinstance(run_summary, dict) else {}
    inference_meta = restart.get("inference_aggregation_summary", {}) if isinstance(restart.get("inference_aggregation_summary"), dict) else {}
    bootstrap_escalation = run_summary.get("bootstrap_escalation", {}) if isinstance(run_summary.get("bootstrap_escalation"), dict) else {}
    refinement = run_summary.get("refinement", {}) if isinstance(run_summary.get("refinement"), dict) else {}
    outputs = run_summary.get("outputs", {}) if isinstance(run_summary.get("outputs"), dict) else {}
    gate_meta = run_summary.get("gate_meta", {}) if isinstance(run_summary.get("gate_meta"), dict) else {}
    config = run_summary.get("config", {}) if isinstance(run_summary.get("config"), dict) else {}
    search_governance = (
        run_summary.get("search_governance", {}) if isinstance(run_summary.get("search_governance"), dict) else {}
    )
    if not search_governance and isinstance(config.get("search_governance"), dict):
        search_governance = config.get("search_governance", {})
    audit_hashes = run_summary.get("audit_hashes", {}) if isinstance(run_summary.get("audit_hashes"), dict) else {}

    validated_gate_source = str(gate_meta.get("validated_gate_source", config.get("validated_gate_source", "")))
    legacy_sync = _as_bool(
        gate_meta.get(
            "legacy_single_gate_sync_validation",
            config.get("legacy_single_gate_sync_validation", False),
        )
    )
    validation_stage_policy = str(search_governance.get("validation_stage_policy", ""))
    validation_used_for_search = _as_bool(
        search_governance.get(
            "validation_used_for_search",
            run_summary.get("validation_used_for_search", config.get("validation_used_for_search", False)),
        )
    )
    candidate_pool_locked_pre_validation = _as_bool(
        search_governance.get(
            "candidate_pool_locked_pre_validation",
            run_summary.get(
                "candidate_pool_locked_pre_validation",
                config.get("candidate_pool_locked_pre_validation", True),
            ),
        ),
        default=True,
    )

    return {
        "scan_rows": _as_int(counts.get("scan_rows"), 0),
        "top_rows": _as_int(counts.get("top_rows"), 0),
        "top_rows_inference": _as_int(counts.get("top_rows_inference"), 0),
        "validated_inference": _as_int(tiers.get("validated_candidate"), 0),
        "support_inference": _as_int(tiers.get("support_candidate"), 0),
        "exploratory_inference": _as_int(tiers.get("exploratory"), 0),
        "n_candidates_validated": _as_int(inference_meta.get("n_candidates_validated"), 0),
        "n_candidates_validation_ok": _as_int(inference_meta.get("n_candidates_validation_ok"), 0),
        "n_candidates_q_nonnull": _as_int(inference_meta.get("n_candidates_q_nonnull"), 0),
        "bootstrap_escalation_enabled": bool(bootstrap_escalation.get("enabled", False)),
        "bootstrap_escalation_executed": bool(bootstrap_escalation.get("executed", False)),
        "bootstrap_escalation_skip_reason": str(bootstrap_escalation.get("skip_reason", "")),
        "refinement_enabled": bool(refinement.get("enabled", False)),
        "refinement_executed": bool(refinement.get("executed", False)),
        "refinement_shortlist_source": str(refinement.get("shortlist_source", "")),
        "out_scan_runs_csv": str(outputs.get("scan_runs_csv", "")),
        "out_top_models_csv": str(outputs.get("top_models_csv", "")),
        "out_top_models_inference_csv": str(outputs.get("top_models_inference_csv", "")),
        "out_run_summary_json": str(outputs.get("run_summary_json", "")),
        "validated_gate_source": validated_gate_source,
        "legacy_single_gate_sync_validation": int(legacy_sync),
        "validation_stage_policy": validation_stage_policy,
        "validation_used_for_search": int(validation_used_for_search),
        "candidate_pool_locked_pre_validation": int(candidate_pool_locked_pre_validation),
        "data_hash": str(audit_hashes.get("data_hash", "")),
        "config_hash": str(audit_hashes.get("config_hash", "")),
    }


def _build_payload(paired_summary_path: Path, paired_summary: Dict[str, Any]) -> Dict[str, Any]:
    children = paired_summary.get("children", [])
    branch_rows: List[Dict[str, Any]] = []
    by_mode: Dict[str, Dict[str, Any]] = {}
    for child in children:
        if not isinstance(child, dict):
            continue
        mode = str(child.get("mode", ""))
        run_id = str(child.get("run_id", ""))
        declared_outputs = child.get("outputs", {}) if isinstance(child.get("outputs"), dict) else {}
        declared_outputs = {str(k): str(v) for k, v in declared_outputs.items()}
        run_summary_path, run_summary_path_source = _resolve_child_run_summary_path(child, run_id)
        run_summary_exists = bool(run_summary_path is not None and run_summary_path.is_file())
        run_summary = _read_json(run_summary_path) if run_summary_exists and run_summary_path is not None else {}
        metrics = _extract_branch_metrics(run_summary) if run_summary_exists else {}
        row = {
            "mode": mode,
            "run_id": run_id,
            "status": str(child.get("status", "")),
            "returncode": _as_int(child.get("returncode"), 0),
            "error": str(child.get("error", "")),
            "declared_outputs": declared_outputs,
            "run_summary_path": str(run_summary_path) if run_summary_path is not None else "",
            "run_summary_path_source": run_summary_path_source,
            "run_summary_exists": run_summary_exists,
            "metrics": metrics,
        }
        branch_rows.append(row)
        by_mode[mode] = row

    nooption = by_mode.get("nooption_baseline", {})
    singlex = by_mode.get("singlex_baseline", {})
    nooption_m = nooption.get("metrics", {}) if isinstance(nooption.get("metrics"), dict) else {}
    singlex_m = singlex.get("metrics", {}) if isinstance(singlex.get("metrics"), dict) else {}

    comparison = {
        "scan_rows_delta_nooption_minus_singlex": _as_int(nooption_m.get("scan_rows"), 0)
        - _as_int(singlex_m.get("scan_rows"), 0),
        "top_rows_delta_nooption_minus_singlex": _as_int(nooption_m.get("top_rows"), 0)
        - _as_int(singlex_m.get("top_rows"), 0),
        "top_rows_inference_delta_nooption_minus_singlex": _as_int(nooption_m.get("top_rows_inference"), 0)
        - _as_int(singlex_m.get("top_rows_inference"), 0),
        "validated_inference_delta_nooption_minus_singlex": _as_int(nooption_m.get("validated_inference"), 0)
        - _as_int(singlex_m.get("validated_inference"), 0),
        "singlex_support_or_better_inference": _as_int(singlex_m.get("validated_inference"), 0)
        + _as_int(singlex_m.get("support_inference"), 0),
    }
    governance = {
        "nooption_validated_gate_source": str(nooption_m.get("validated_gate_source", "")),
        "singlex_validated_gate_source": str(singlex_m.get("validated_gate_source", "")),
        "nooption_legacy_single_gate_sync_validation": _as_int(
            nooption_m.get("legacy_single_gate_sync_validation"),
            0,
        ),
        "singlex_legacy_single_gate_sync_validation": _as_int(
            singlex_m.get("legacy_single_gate_sync_validation"),
            0,
        ),
        "nooption_validation_stage_policy": str(nooption_m.get("validation_stage_policy", "")),
        "singlex_validation_stage_policy": str(singlex_m.get("validation_stage_policy", "")),
        "all_branches_validation_used_for_search_false": int(
            all(_as_int((row.get("metrics", {}) or {}).get("validation_used_for_search"), 0) == 0 for row in branch_rows)
        ),
        "all_branches_candidate_pool_locked_pre_validation": int(
            all(
                _as_int((row.get("metrics", {}) or {}).get("candidate_pool_locked_pre_validation"), 0) == 1
                for row in branch_rows
            )
        ),
    }

    return {
        "generated_at_utc": _utc_timestamp(),
        "source": {
            "paired_summary_json": str(paired_summary_path),
        },
        "paired": {
            "run_id": str(paired_summary.get("run_id", "")),
            "status": str(paired_summary.get("status", "")),
            "timestamp_utc": str(paired_summary.get("timestamp_utc", "")),
            "timestamp_utc_finished": str(paired_summary.get("timestamp_utc_finished", "")),
            "children_count": len(branch_rows),
        },
        "branches": branch_rows,
        "comparison": comparison,
        "governance": governance,
    }


def _status_class(status: str) -> str:
    s = str(status).strip().lower()
    if s in {"ok", "success"}:
        return "ok"
    if "skip" in s:
        return "skip"
    if s in {"partial_failure", "failed", "error"}:
        return "fail"
    return "neutral"


def _fmt_int(value: Any) -> str:
    return str(_as_int(value, 0))


def _fmt_bool(value: Any) -> str:
    return "true" if _as_bool(value) else "false"


def _render_branch_metrics_table(branches: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for row in branches:
        metrics = row.get("metrics", {}) if isinstance(row.get("metrics"), dict) else {}
        lines.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('mode', '')))}</td>"
            f"<td><span class='pill {_status_class(row.get('status', ''))}'>{html.escape(str(row.get('status', '')))}</span></td>"
            f"<td>{html.escape(str(row.get('run_id', '')))}</td>"
            f"<td>{html.escape(str(metrics.get('validated_gate_source', '')))}</td>"
            f"<td>{html.escape(str(metrics.get('validation_stage_policy', '')))}</td>"
            f"<td>{_fmt_bool(metrics.get('validation_used_for_search'))}</td>"
            f"<td>{_fmt_bool(metrics.get('candidate_pool_locked_pre_validation'))}</td>"
            f"<td>{_fmt_int(metrics.get('scan_rows'))}</td>"
            f"<td>{_fmt_int(metrics.get('top_rows'))}</td>"
            f"<td>{_fmt_int(metrics.get('top_rows_inference'))}</td>"
            f"<td>{_fmt_int(metrics.get('validated_inference'))}</td>"
            f"<td>{_fmt_int(metrics.get('support_inference'))}</td>"
            f"<td>{_fmt_int(metrics.get('exploratory_inference'))}</td>"
            f"<td>{_fmt_int(metrics.get('n_candidates_validation_ok'))}</td>"
            f"<td>{_fmt_int(metrics.get('n_candidates_q_nonnull'))}</td>"
            "</tr>"
        )
    return "\n".join(lines)


def _render_child_exec_table(branches: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for row in branches:
        lines.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('mode', '')))}</td>"
            f"<td><span class='pill {_status_class(row.get('status', ''))}'>{html.escape(str(row.get('status', '')))}</span></td>"
            f"<td>{_fmt_int(row.get('returncode'))}</td>"
            f"<td>{html.escape(str(row.get('run_summary_exists', False)))}</td>"
            f"<td>{html.escape(str(row.get('run_summary_path_source', '')))}</td>"
            f"<td><code>{html.escape(str(row.get('run_summary_path', '')))}</code></td>"
            "</tr>"
        )
    return "\n".join(lines)


def _render_html(payload: Dict[str, Any], title: str) -> str:
    paired = payload.get("paired", {}) if isinstance(payload.get("paired"), dict) else {}
    branches = payload.get("branches", []) if isinstance(payload.get("branches"), list) else []
    comparison = payload.get("comparison", {}) if isinstance(payload.get("comparison"), dict) else {}
    governance = payload.get("governance", {}) if isinstance(payload.get("governance"), dict) else {}
    paired_status = str(paired.get("status", ""))
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #f5f8ff;
      --card: #ffffff;
      --ink: #0f172a;
      --muted: #5b6476;
      --line: #dbe2f0;
      --ok: #1f7a4f;
      --okbg: #e8f6ef;
      --skip: #8a6f17;
      --skipbg: #fff6d9;
      --fail: #a12626;
      --failbg: #fdecec;
      --neutral: #51607a;
      --neutralbg: #edf1fa;
    }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Noto Sans", sans-serif;
      color: var(--ink);
      background: linear-gradient(170deg, #edf2ff 0%, #f8fbff 50%, #eef7ff 100%);
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px;
    }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    .muted {{ color: var(--muted); }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin: 18px 0 10px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px 14px;
      box-shadow: 0 8px 24px rgba(40, 64, 112, 0.06);
    }}
    .label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }}
    .value {{ font-size: 22px; font-weight: 700; margin-top: 4px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      overflow: hidden;
      margin-top: 10px;
      font-size: 14px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 9px 10px;
      text-align: left;
      vertical-align: top;
    }}
    th {{ background: #eef3ff; font-weight: 700; }}
    tr:last-child td {{ border-bottom: 0; }}
    .pill {{
      display: inline-block;
      font-size: 12px;
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 999px;
    }}
    .ok {{ color: var(--ok); background: var(--okbg); }}
    .skip {{ color: var(--skip); background: var(--skipbg); }}
    .fail {{ color: var(--fail); background: var(--failbg); }}
    .neutral {{ color: var(--neutral); background: var(--neutralbg); }}
    .section {{ margin-top: 22px; }}
    code {{ font-size: 12px; }}
    pre {{
      margin-top: 12px;
      background: #0b1220;
      color: #d9e3ff;
      padding: 12px;
      border-radius: 10px;
      overflow-x: auto;
      font-size: 12px;
      line-height: 1.4;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>{html.escape(title)}</h1>
    <div class="muted">Generated at {html.escape(str(payload.get("generated_at_utc", "")))} (UTC)</div>
    <div class="grid">
      <div class="card">
        <div class="label">Paired Run ID</div>
        <div class="value">{html.escape(str(paired.get("run_id", "")))}</div>
      </div>
      <div class="card">
        <div class="label">Paired Status</div>
        <div class="value"><span class="pill {_status_class(paired_status)}">{html.escape(paired_status)}</span></div>
      </div>
      <div class="card">
        <div class="label">Children</div>
        <div class="value">{_fmt_int(paired.get("children_count"))}</div>
      </div>
      <div class="card">
        <div class="label">Inference Delta (Nooption - Singlex)</div>
        <div class="value">{_fmt_int(comparison.get("top_rows_inference_delta_nooption_minus_singlex"))}</div>
      </div>
      <div class="card">
        <div class="label">Nooption Gate Source</div>
        <div class="value" style="font-size:14px">{html.escape(str(governance.get("nooption_validated_gate_source", "")))}</div>
      </div>
      <div class="card">
        <div class="label">Singlex Gate Source</div>
        <div class="value" style="font-size:14px">{html.escape(str(governance.get("singlex_validated_gate_source", "")))}</div>
      </div>
      <div class="card">
        <div class="label">Search Governance</div>
        <div class="value" style="font-size:14px">
          used_for_search={_fmt_bool(not _as_bool(governance.get("all_branches_validation_used_for_search_false")))} /
          pool_locked={_fmt_bool(governance.get("all_branches_candidate_pool_locked_pre_validation"))}
        </div>
      </div>
    </div>

    <div class="section">
      <h2>Child Execution</h2>
      <table>
        <thead>
          <tr>
            <th>Mode</th>
            <th>Status</th>
            <th>Return Code</th>
            <th>Run Summary Exists</th>
            <th>Path Source</th>
            <th>Run Summary Path</th>
          </tr>
        </thead>
        <tbody>
          {_render_child_exec_table(branches)}
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Branch Metrics (Inference-aware)</h2>
      <table>
        <thead>
          <tr>
            <th>Mode</th>
            <th>Status</th>
            <th>Run ID</th>
            <th>Scan Rows</th>
            <th>Gate Source</th>
            <th>Validation Policy</th>
            <th>Validation Used For Search</th>
            <th>Pool Locked Pre Validation</th>
            <th>Top Rows</th>
            <th>Top Rows Inference</th>
            <th>Validated</th>
            <th>Support</th>
            <th>Exploratory</th>
            <th>Validation-OK Cands</th>
            <th>Q-Nonnull Cands</th>
          </tr>
        </thead>
        <tbody>
          {_render_branch_metrics_table(branches)}
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Comparison Snapshot</h2>
      <table>
        <thead>
          <tr><th>Metric</th><th>Value</th></tr>
        </thead>
        <tbody>
          <tr><td>scan_rows_delta_nooption_minus_singlex</td><td>{_fmt_int(comparison.get("scan_rows_delta_nooption_minus_singlex"))}</td></tr>
          <tr><td>top_rows_delta_nooption_minus_singlex</td><td>{_fmt_int(comparison.get("top_rows_delta_nooption_minus_singlex"))}</td></tr>
          <tr><td>top_rows_inference_delta_nooption_minus_singlex</td><td>{_fmt_int(comparison.get("top_rows_inference_delta_nooption_minus_singlex"))}</td></tr>
          <tr><td>validated_inference_delta_nooption_minus_singlex</td><td>{_fmt_int(comparison.get("validated_inference_delta_nooption_minus_singlex"))}</td></tr>
          <tr><td>singlex_support_or_better_inference</td><td>{_fmt_int(comparison.get("singlex_support_or_better_inference"))}</td></tr>
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Source</h2>
      <div><code>{html.escape(str(payload.get("source", {}).get("paired_summary_json", "")))}</code></div>
      <pre>{html.escape(json.dumps(payload, ensure_ascii=False, indent=2))}</pre>
    </div>
  </div>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--paired-summary-json", default="")
    p.add_argument("--out-json", default="")
    p.add_argument("--out-html", default="")
    p.add_argument("--title", default="Phase-B RegSpec Paired Dashboard")
    return p.parse_args()


def _resolve_paths(args: argparse.Namespace, paired_summary_path: Path, paired_run_id: str) -> Tuple[Path, Path]:
    slug = _slug(paired_run_id) if paired_run_id else _slug(paired_summary_path.stem)
    out_json = (
        Path(args.out_json)
        if str(args.out_json).strip()
        else ROOT / f"data/metadata/phase_b_bikard_machine_scientist_dashboard_{slug}.json"
    )
    out_html = (
        Path(args.out_html)
        if str(args.out_html).strip()
        else ROOT / f"outputs/reports/phase_b_bikard_machine_scientist_dashboard_{slug}.html"
    )
    return out_json, out_html


def main() -> int:
    args = parse_args()
    paired_summary_path = Path(args.paired_summary_json) if str(args.paired_summary_json).strip() else _latest_paired_summary()
    if not paired_summary_path.exists():
        raise FileNotFoundError(f"paired summary not found: {paired_summary_path}")
    paired_summary = _read_json(paired_summary_path)
    paired_run_id = str(paired_summary.get("run_id", ""))
    payload = _build_payload(paired_summary_path, paired_summary)
    out_json, out_html = _resolve_paths(args, paired_summary_path, paired_run_id)
    _write_json(out_json, payload)
    _write_text(out_html, _render_html(payload, title=str(args.title)))
    print(
        json.dumps(
            {
                "paired_summary_json": str(paired_summary_path),
                "out_json": str(out_json),
                "out_html": str(out_html),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
