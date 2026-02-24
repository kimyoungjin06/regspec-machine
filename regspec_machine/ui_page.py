"""Small L5 operator UI served from the L4 FastAPI app."""

from __future__ import annotations

from typing import Iterable


RUN_STATE_VALUES = ("all", "queued", "running", "succeeded", "failed", "cancelled")


def build_ui_page_html(*, run_modes: Iterable[str]) -> str:
    mode_options = "\n".join(
        f'<option value="{mode}">{mode}</option>' for mode in list(run_modes)
    )
    mode_filter_options = "\n".join(
        ['<option value="">all</option>']
        + [f'<option value="{mode}">{mode}</option>' for mode in list(run_modes)]
    )
    state_options = "\n".join(
        f'<option value="{state if state != "all" else ""}">{state}</option>'
        for state in RUN_STATE_VALUES
    )
    return (
        """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>RegSpec-Machine Operator Console</title>
  <style>
    :root {
      --ink: #102542;
      --sky: #d8ecff;
      --paper: #f7fbff;
      --accent: #ff7b00;
      --ok: #1d7a32;
      --fail: #a11f2f;
      --muted: #5f6f82;
      --line: #c6d6e8;
      --warn: #9d6a00;
    }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Noto Sans KR", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, #ffe9d6 0%, #ffe9d6 15%, transparent 45%),
        radial-gradient(circle at 10% 10%, #e4f4ff 0%, #e4f4ff 20%, transparent 55%),
        var(--paper);
    }
    .page {
      max-width: 1240px;
      margin: 0 auto;
      padding: 18px;
    }
    .header {
      border: 1px solid var(--line);
      background: linear-gradient(120deg, #ffffff, #eaf5ff);
      border-radius: 14px;
      padding: 14px 18px;
      margin-bottom: 14px;
    }
    .title {
      font-family: "Space Grotesk", "IBM Plex Sans", sans-serif;
      font-weight: 700;
      font-size: 20px;
      margin: 0 0 6px;
      letter-spacing: 0.2px;
    }
    .sub {
      margin: 0;
      color: var(--muted);
      font-size: 13px;
    }
    .help-card {
      margin-bottom: 14px;
    }
    .help-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }
    @media (min-width: 960px) {
      .help-grid {
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }
    }
    .help-item {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #f8fbff;
      padding: 10px;
    }
    .help-item h3 {
      margin: 0 0 6px;
      font-size: 13px;
      font-family: "Space Grotesk", "IBM Plex Sans", sans-serif;
    }
    .help-item p {
      margin: 0;
      font-size: 12px;
      color: var(--muted);
      line-height: 1.4;
    }
    .grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }
    @media (min-width: 980px) {
      .grid {
        grid-template-columns: 1.02fr 1fr;
      }
    }
    .card {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #ffffff;
      padding: 14px;
      box-shadow: 0 8px 22px rgba(16, 37, 66, 0.06);
    }
    .card h2 {
      margin: 0 0 10px;
      font-size: 16px;
      font-family: "Space Grotesk", "IBM Plex Sans", sans-serif;
    }
    .row {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
      margin-bottom: 10px;
    }
    @media (min-width: 560px) {
      .row {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
    label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 4px;
    }
    input, select, textarea {
      width: 100%;
      box-sizing: border-box;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 13px;
      color: var(--ink);
      background: #fff;
    }
    textarea {
      min-height: 68px;
      resize: vertical;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
    }
    .inline {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .inline input[type="checkbox"] {
      width: auto;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 9px 12px;
      font-weight: 600;
      cursor: pointer;
      color: #fff;
      background: var(--ink);
    }
    button.secondary {
      background: var(--accent);
    }
    button.ghost {
      color: var(--ink);
      background: #eaf1f9;
    }
    button.warn {
      background: var(--warn);
    }
    button:disabled {
      opacity: 0.6;
      cursor: default;
    }
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 8px;
    }
    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
      background: #f3f8ff;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      overflow: auto;
      max-height: 260px;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .hint-box {
      margin-top: 6px;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #f8fbff;
      font-size: 12px;
      color: var(--muted);
    }
    .notice {
      margin-top: 8px;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #f8fbff;
      font-size: 12px;
      color: var(--muted);
    }
    .notice-ok {
      border-color: #b7e2c0;
      background: #f2fcf5;
      color: var(--ok);
    }
    .notice-error {
      border-color: #f0bcc4;
      background: #fff5f7;
      color: var(--fail);
    }
    .status-pill {
      border-radius: 999px;
      font-size: 11px;
      padding: 2px 8px;
      color: #fff;
      display: inline-block;
      line-height: 1.3;
    }
    .state-succeeded { background: var(--ok); }
    .state-failed, .state-cancelled { background: var(--fail); }
    .state-running { background: var(--accent); }
    .state-queued { background: #5a6f88; }
    .run-table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 10px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 760px;
      font-size: 12px;
    }
    th, td {
      border-bottom: 1px solid #edf2f7;
      padding: 7px 8px;
      text-align: left;
      vertical-align: top;
    }
    th {
      position: sticky;
      top: 0;
      background: #f5f9ff;
      z-index: 1;
    }
    td .mini {
      margin-right: 4px;
      margin-bottom: 3px;
      padding: 4px 7px;
      border-radius: 7px;
      border: 0;
      cursor: pointer;
      font-size: 11px;
    }
    .run-row-selected td {
      background: #eef6ff;
    }
    #runs_table_wrap.runs-compact .runs-col-detail {
      display: none;
    }
    .footer-note {
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
    }
    .kpi-grid {
      margin-top: 8px;
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
    }
    @media (min-width: 700px) {
      .kpi-grid {
        grid-template-columns: repeat(4, minmax(0, 1fr));
      }
    }
    .kpi-card {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #f8fbff;
      padding: 8px;
    }
    .kpi-label {
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 3px;
    }
    .kpi-value {
      font-size: 16px;
      font-family: "Space Grotesk", "IBM Plex Sans", sans-serif;
      font-weight: 700;
      color: var(--ink);
      line-height: 1.1;
    }
    .kpi-pass { color: var(--ok); }
    .kpi-fail { color: var(--fail); }
    .kpi-warn { color: var(--accent); }
    .compact-table table {
      min-width: 0;
    }
    .compact-table th {
      width: 42%;
      position: static;
      background: #f8fbff;
      color: var(--muted);
      font-weight: 600;
    }
    .kv-chip-wrap {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
    }
    .kv-chip {
      display: inline-block;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #f8fbff;
      padding: 2px 8px;
      font-size: 11px;
      color: var(--muted);
    }
    .compare-head {
      font-family: "Space Grotesk", "IBM Plex Sans", sans-serif;
      font-size: 12px;
      color: var(--muted);
      letter-spacing: 0.2px;
    }
    .compare-delta-up {
      color: var(--ok);
      font-weight: 600;
    }
    .compare-delta-down {
      color: var(--fail);
      font-weight: 600;
    }
    .compare-delta-same {
      color: var(--muted);
      font-weight: 600;
    }
    .compare-badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #f8fbff;
      font-size: 11px;
      font-weight: 600;
      color: var(--muted);
    }
    .compare-badge-pass {
      border-color: #b7e2c0;
      background: #f2fcf5;
      color: var(--ok);
    }
    .compare-badge-fail {
      border-color: #f0bcc4;
      background: #fff5f7;
      color: var(--fail);
    }
    .compare-insight {
      margin-top: 10px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #f8fbff;
      padding: 10px;
    }
    .compare-insight h3 {
      margin: 0 0 6px;
      font-size: 13px;
      font-family: "Space Grotesk", "IBM Plex Sans", sans-serif;
      color: var(--ink);
    }
    .compare-insight ul {
      margin: 0;
      padding-left: 18px;
      display: grid;
      gap: 4px;
    }
    .compare-insight li {
      font-size: 12px;
      color: var(--muted);
      line-height: 1.35;
    }
    .viz-grid {
      margin-top: 10px;
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }
    @media (min-width: 860px) {
      .viz-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
    .viz-card {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #f8fbff;
      padding: 8px;
    }
    .viz-card h3 {
      margin: 0 0 6px;
      font-size: 12px;
      font-family: "Space Grotesk", "IBM Plex Sans", sans-serif;
      color: var(--ink);
    }
    .explorer-joint-grid {
      margin-top: 10px;
      display: grid;
      grid-template-columns: minmax(0, 1fr) 240px;
      grid-template-rows: auto auto;
      gap: 8px;
      align-items: stretch;
    }
    .explorer-joint-top {
      grid-column: 1;
      grid-row: 1;
    }
    .explorer-joint-kpi {
      grid-column: 2;
      grid-row: 1;
    }
    .explorer-joint-main {
      grid-column: 1;
      grid-row: 2;
    }
    .explorer-joint-side {
      grid-column: 2;
      grid-row: 2;
    }
    @media (max-width: 980px) {
      .explorer-joint-grid {
        grid-template-columns: 1fr;
        grid-template-rows: auto;
      }
      .explorer-joint-top,
      .explorer-joint-kpi,
      .explorer-joint-main,
      .explorer-joint-side {
        grid-column: auto;
        grid-row: auto;
      }
    }
    .marginal-wrap {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      min-height: 132px;
      overflow: hidden;
    }
    .marginal-wrap svg {
      display: block;
      width: 100%;
      height: auto;
    }
    .marginal-wrap.side {
      min-height: 0;
    }
    .marginal-wrap.side svg {
      height: auto;
    }
    .marginal-bar {
      cursor: pointer;
      transition: opacity 120ms ease, stroke-width 120ms ease;
    }
    .marginal-bar-active {
      stroke: #102542 !important;
      stroke-width: 1.2 !important;
    }
    .joint-kpi-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px;
    }
    .joint-kpi-item {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #f8fbff;
      padding: 6px;
    }
    .joint-kpi-label {
      font-size: 10px;
      color: var(--muted);
      margin-bottom: 3px;
    }
    .joint-kpi-value {
      font-size: 12px;
      color: var(--ink);
      font-weight: 700;
    }
    .viz-card-wide {
      grid-column: 1 / -1;
    }
    .bar-list {
      display: grid;
      gap: 6px;
    }
    .bar-row {
      display: grid;
      grid-template-columns: 1.2fr 1fr auto;
      gap: 6px;
      align-items: center;
      font-size: 11px;
    }
    .bar-label {
      color: var(--muted);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .bar-track {
      background: #e6eef9;
      border-radius: 999px;
      height: 8px;
      overflow: hidden;
    }
    .bar-fill {
      height: 100%;
      background: linear-gradient(90deg, #3f6ca8, #2f82c7);
    }
    .bar-value {
      color: var(--ink);
      font-weight: 600;
      min-width: 42px;
      text-align: right;
    }
    .scatter-wrap {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      min-height: 0;
      aspect-ratio: 1 / 1;
      max-width: 100%;
      overflow: hidden;
    }
    .scatter-wrap svg {
      display: block;
      width: 100%;
      height: auto;
    }
    .scatter-point {
      cursor: pointer;
      transition: stroke-width 120ms ease, fill-opacity 120ms ease;
    }
    .scatter-point-active {
      stroke: #102542 !important;
      stroke-width: 1.3 !important;
      fill-opacity: 1 !important;
    }
    .scatter-empty {
      color: var(--muted);
      font-size: 12px;
      padding: 10px;
    }
    .equation-factor-box {
      min-height: 92px;
      font-family: "IBM Plex Mono", "Fira Mono", monospace;
      font-size: 12px;
      line-height: 1.45;
      white-space: pre-wrap;
    }
    .equation-chart-wrap {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      min-height: 260px;
      overflow: hidden;
    }
    .equation-chart-wrap svg {
      display: block;
      width: 100%;
      height: auto;
    }
    .equation-chart-empty {
      color: var(--muted);
      font-size: 12px;
      padding: 10px;
    }
    .eq-chip {
      display: inline-block;
      padding: 2px 8px;
      margin: 1px 4px 1px 0;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #eef5ff;
      color: var(--ink);
      font-size: 11px;
      font-weight: 600;
    }
    .interp-ok {
      color: var(--ok) !important;
      font-weight: 600;
    }
    .interp-warn {
      color: var(--warn) !important;
      font-weight: 600;
    }
    .interp-fail {
      color: var(--fail) !important;
      font-weight: 600;
    }
    .progress-meta {
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 4px;
    }
    .progress-track {
      width: 120px;
      height: 6px;
      border-radius: 999px;
      background: #e8eef6;
      overflow: hidden;
    }
    .progress-fill {
      height: 100%;
      background: linear-gradient(90deg, #4d7bb3, #2f6bc2);
    }
    details summary {
      cursor: pointer;
      font-size: 12px;
      color: var(--muted);
      margin: 8px 0;
      user-select: none;
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="header">
      <h1 class="title">RegSpec-Machine Console (L5)</h1>
      <p class="sub">Run nooption/singlex/paired presets, monitor execution state, and inspect summary from one browser screen.</p>
      <p class="sub">Health: <span id="health_text">checking...</span></p>
    </section>

    <section class="card help-card">
      <h2>Quick Start</h2>
      <div class="help-grid">
        <div class="help-item">
          <h3>1) Baseline Pair</h3>
          <p>Start with <code>paired_nooption_singlex</code> and check validated / p / q / restart / consensus together.</p>
        </div>
        <div class="help-item">
          <h3>2) Singlex Confirmation</h3>
          <p>Use <code>singlex_baseline</code> to verify hypothesis-first singleton behavior under governance guards.</p>
        </div>
        <div class="help-item">
          <h3>3) Read Structured Output</h3>
          <p>Use interpreted summary table and KPI cards first. Raw JSON is available in the debug fold only.</p>
        </div>
      </div>
    </section>

    <section class="card help-card">
      <h2>Load Previous Results (Recommended)</h2>
      <div class="help-grid">
        <div class="help-item">
          <h3>Path A: Run Result</h3>
          <p>Load latest run summary/KPI directly (validated/p/q/restart/consensus).</p>
          <div class="toolbar">
            <button id="load_prev_refresh_btn" class="ghost">1) Refresh Sources</button>
            <button id="load_prev_latest_run_btn" class="secondary">2) Load Latest Run Result</button>
          </div>
        </div>
        <div class="help-item">
          <h3>Path B: Saved Report</h3>
          <p>Open previously exported report files from outputs/reports.</p>
          <div class="toolbar">
            <button id="load_prev_latest_report_btn" class="secondary">Load Latest Saved Report</button>
          </div>
        </div>
        <div class="help-item">
          <h3>Path C: Data Inputs</h3>
          <p>Load Data Config restores input defaults only, not run result metrics.</p>
          <div class="toolbar">
            <button id="load_prev_data_config_btn" class="ghost">Load Data Config (inputs only)</button>
          </div>
        </div>
      </div>
    </section>

    <section class="grid">
      <div class="card">
        <h2>Run Submit</h2>
        <div class="row">
          <div>
            <label for="mode">Mode</label>
            <select id="mode">
__MODE_OPTIONS__
            </select>
          </div>
          <div>
            <label for="run_id">Run ID</label>
            <input id="run_id" placeholder="letters/numbers/._:- (3-128)" />
          </div>
        </div>
        <div id="mode_help_text" class="hint-box"></div>
        <div class="toolbar">
          <button id="preset_pair_btn" class="ghost">Preset: paired baseline</button>
          <button id="preset_singlex_btn" class="ghost">Preset: singlex baseline</button>
          <button id="preset_nooption_btn" class="ghost">Preset: nooption baseline</button>
        </div>
        <div class="toolbar">
          <button id="run_pair_now_btn" class="secondary">Run paired baseline now</button>
          <button id="run_singlex_now_btn" class="secondary">Run singlex baseline now</button>
          <button id="run_nooption_now_btn" class="secondary">Run nooption baseline now</button>
        </div>
        <div class="row">
          <div>
            <label for="scan_n_bootstrap">scan_n_bootstrap (optional)</label>
            <input id="scan_n_bootstrap" type="number" min="0" step="1" placeholder="49" />
          </div>
          <div>
            <label for="scan_max_features">scan_max_features (optional)</label>
            <input id="scan_max_features" type="number" min="0" step="1" placeholder="160" />
          </div>
        </div>
        <div class="row">
          <div>
            <label for="refine_n_bootstrap">refine_n_bootstrap (optional)</label>
            <input id="refine_n_bootstrap" type="number" min="0" step="1" placeholder="499" />
          </div>
          <div>
            <label for="idempotency_key">idempotency_key (optional)</label>
            <input id="idempotency_key" placeholder="request key" />
          </div>
        </div>
        <div class="row">
          <div>
            <label for="extra_args">extra_args (one per line)</label>
            <textarea id="extra_args" placeholder="--print-cli-summary"></textarea>
          </div>
          <div>
            <label>submit status</label>
            <div id="submit_notice" class="notice">ready</div>
          </div>
        </div>
        <div class="toolbar">
          <label class="inline"><input id="execute" type="checkbox" checked /> execute now</label>
          <label class="inline"><input id="dry_run" type="checkbox" /> dry run</label>
          <label class="inline"><input id="skip_direction_review" type="checkbox" /> skip direction review</label>
        </div>
        <div class="toolbar">
          <button id="submit_btn" class="secondary">Submit Run</button>
          <button id="new_run_id_btn" class="ghost">New Run ID</button>
        </div>
        <details>
          <summary>Submit Response (raw)</summary>
          <div id="submit_response" class="mono">{}</div>
        </details>
      </div>

      <div class="card">
        <h2>Run Details</h2>
        <div class="row">
          <div>
            <label for="detail_run_id">run_id</label>
            <input id="detail_run_id" placeholder="type run_id then inspect" />
          </div>
          <div>
            <label for="detail_status_filter">status filter</label>
            <select id="detail_status_filter">
__STATE_OPTIONS__
            </select>
          </div>
        </div>
        <div class="row">
          <div>
            <label for="list_mode_filter">mode filter</label>
            <select id="list_mode_filter">
__MODE_FILTER_OPTIONS__
            </select>
          </div>
          <div>
            <label for="list_run_id_filter">run_id contains</label>
            <input id="list_run_id_filter" placeholder="optional run_id substring filter" />
          </div>
        </div>
        <div class="toolbar">
          <button id="inspect_btn">Inspect</button>
          <button id="refresh_runs_btn" class="ghost">Refresh Run List</button>
          <button id="inspect_profile_btn" class="secondary">Inspect + Profile Dataset</button>
          <label class="inline"><input id="auto_refresh" type="checkbox" checked /> auto refresh (4s)</label>
        </div>
        <div class="row">
          <div>
            <label for="recent_run_select">recent/history run</label>
            <select id="recent_run_select">
              <option value="">select from run list after refresh</option>
            </select>
          </div>
          <div>
            <label>quick action</label>
            <div class="toolbar">
              <button id="load_recent_run_btn" class="ghost">Load Selected Run Result</button>
            </div>
          </div>
        </div>
        <div id="detail_notice" class="notice">Select a run to inspect.</div>
        <div id="detail_verdict" class="notice">Primary verdict appears after inspect.</div>
        <label>Decision Signals (state / governance / evidence / stability)</label>
        <div id="review_cards" class="kpi-grid"></div>
        <div class="footer-note">Heuristic gates: validated >= 1, q <= 0.10, restart mean >= 0.60 (for quick triage only).</div>
        <label>Key Metrics Table</label>
        <div class="run-table-wrap compact-table">
          <table>
            <tbody id="detail_overview_tbody"></tbody>
          </table>
        </div>
        <details>
          <summary>Raw JSON (debug)</summary>
          <label>status</label>
          <div id="status_box" class="mono">{}</div>
          <label>summary</label>
          <div id="summary_box" class="mono">{}</div>
          <label>review</label>
          <div id="review_box" class="mono">{}</div>
        </details>
      </div>
    </section>

    <section class="card" style="margin-top: 14px;">
      <h2>Baseline Compare (nooption vs singlex)</h2>
      <div class="row">
        <div>
          <label for="compare_nooption_run_id">nooption run_id</label>
          <input id="compare_nooption_run_id" placeholder="phase_b_...__nooption_baseline" />
        </div>
        <div>
          <label for="compare_singlex_run_id">singlex run_id</label>
          <input id="compare_singlex_run_id" placeholder="phase_b_...__singlex" />
        </div>
      </div>
      <div class="toolbar">
        <button id="compare_btn">Compare Runs</button>
        <button id="compare_from_detail_btn" class="ghost">Use detail run as pair key</button>
        <button id="save_compare_outputs_btn" class="secondary" disabled>Save compare to outputs/</button>
        <button id="export_compare_md_btn" class="ghost" disabled>Export compare.md</button>
        <button id="export_compare_json_btn" class="ghost" disabled>Export compare.json</button>
      </div>
      <div id="compare_notice" class="notice">Set two run IDs and run compare.</div>
      <div class="run-table-wrap compact-table" style="margin-top: 8px;">
        <table>
          <thead>
            <tr>
              <th class="compare-head">metric</th>
              <th class="compare-head">nooption</th>
              <th class="compare-head">singlex</th>
              <th class="compare-head">delta / winner</th>
            </tr>
          </thead>
          <tbody id="compare_tbody"></tbody>
        </table>
      </div>
      <div class="compare-insight">
        <h3>Direction Review Hints</h3>
        <ul id="compare_interp_list">
          <li>Compare runs to generate hints.</li>
        </ul>
      </div>
    </section>

    <section class="card" style="margin-top: 14px;">
      <h2>Saved Reports</h2>
      <div class="row">
        <div>
          <label for="saved_report_kind">report kind</label>
          <select id="saved_report_kind">
            <option value="all">all</option>
            <option value="regspec_compare">regspec_compare</option>
            <option value="regspec_dataset_profile_compare">regspec_dataset_profile_compare</option>
          </select>
        </div>
        <div>
          <label for="saved_report_select">saved file</label>
          <select id="saved_report_select">
            <option value="">refresh to load saved report list</option>
          </select>
        </div>
      </div>
      <div class="toolbar">
        <button id="refresh_saved_reports_btn" class="ghost">Refresh Saved Reports</button>
        <button id="load_saved_report_btn" class="secondary">Load Saved Report (file result)</button>
      </div>
      <div id="saved_reports_notice" class="notice">ready</div>
      <label>Saved Report Summary</label>
      <div class="run-table-wrap compact-table">
        <table>
          <tbody id="saved_report_meta_tbody"></tbody>
        </table>
      </div>
      <details>
        <summary>Saved Report Preview</summary>
        <div id="saved_report_box" class="mono">{}</div>
      </details>
    </section>

    <section class="card" style="margin-top: 14px;">
      <h2>Dataset Explorer (Question Seeder)</h2>
      <div class="row">
        <div>
          <label for="dataset_run_id">run_id (optional)</label>
          <input id="dataset_run_id" placeholder="optional run_id to resolve artifacts" />
        </div>
        <div>
          <label for="dataset_path">dataset_path (optional)</label>
          <input id="dataset_path" placeholder="outputs/tables/....csv" />
        </div>
      </div>
      <div class="row">
        <div>
          <label for="dataset_candidate_select">dataset candidate (recent outputs)</label>
          <select id="dataset_candidate_select">
            <option value="">refresh to load candidates</option>
          </select>
        </div>
        <div>
          <label>candidate actions</label>
          <div class="toolbar">
            <button id="refresh_dataset_candidates_btn" class="ghost">Refresh Candidates</button>
            <button id="use_dataset_candidate_btn" class="ghost">Use Selected Candidate</button>
          </div>
        </div>
      </div>
      <div class="row">
        <div>
          <label for="dataset_artifact_key">artifact key</label>
          <select id="dataset_artifact_key">
            <option value="auto">auto</option>
            <option value="scan_runs_csv">scan_runs_csv</option>
            <option value="top_models_inference_csv">top_models_inference_csv</option>
            <option value="top_models_csv">top_models_csv</option>
          </select>
        </div>
        <div>
          <label for="dataset_sample_rows">sample_rows</label>
          <input id="dataset_sample_rows" type="number" min="100" step="100" value="20000" />
        </div>
      </div>
      <div class="row">
        <div>
          <label for="dataset_top_n">question seed top_n</label>
          <input id="dataset_top_n" type="number" min="1" max="100" step="1" value="20" />
        </div>
        <div>
          <label for="dataset_research_mode">research mode (leakage-safe filtering)</label>
          <div class="inline">
            <input id="dataset_research_mode" type="checkbox" checked />
            <span style="font-size: 12px; color: var(--muted);">on (recommended)</span>
          </div>
        </div>
      </div>
      <div class="row">
        <div>
          <label for="dataset_fixed_y">fixed Y (optional)</label>
          <input id="dataset_fixed_y" placeholder="e.g. policy_cited_5y" />
        </div>
        <div>
          <label for="dataset_exclude_x_cols">exclude X columns (comma separated)</label>
          <input id="dataset_exclude_x_cols" placeholder="e.g. policy_cite_count_5y, q_value_validation" />
        </div>
      </div>
      <div class="row">
        <div>
          <label>profile status</label>
          <div id="profile_notice" class="notice">ready</div>
        </div>
        <div>
          <label>candidate status</label>
          <div id="dataset_candidates_notice" class="notice">not loaded</div>
        </div>
      </div>
      <div class="row">
        <div>
          <label>dataset config</label>
          <div id="dataset_config_notice" class="notice">not loaded</div>
        </div>
      </div>
      <div class="toolbar">
        <button id="profile_btn" class="secondary">Analyze Dataset</button>
        <button id="load_dataset_config_btn" class="ghost">Load Data Config (inputs only)</button>
        <button id="save_dataset_config_btn" class="ghost">Save Data Config</button>
      </div>
      <label>Profile Summary</label>
      <div class="run-table-wrap compact-table">
        <table>
          <tbody id="profile_overview_tbody"></tbody>
        </table>
      </div>
      <div class="viz-grid">
        <div class="viz-card">
          <h3>Missing Share Top</h3>
          <div id="profile_missing_chart" class="bar-list"></div>
        </div>
        <div class="viz-card">
          <h3>Question Seed Score Top</h3>
          <div id="profile_seed_score_chart" class="bar-list"></div>
        </div>
      </div>
      <label>Question Seeds</label>
      <div class="run-table-wrap" style="margin-top: 8px;">
        <table>
          <thead>
            <tr>
              <th>rank</th>
              <th>question</th>
              <th>score</th>
              <th>support_rows</th>
              <th>risk</th>
              <th>signal</th>
            </tr>
          </thead>
          <tbody id="profile_seed_tbody"></tbody>
        </table>
      </div>
      <details>
        <summary>Profile Raw JSON (debug)</summary>
        <div id="profile_raw_box" class="mono">{}</div>
      </details>
    </section>

    <section class="card" style="margin-top: 14px;">
      <h2>Run Monitor</h2>
      <div class="toolbar">
        <button id="runs_refresh_now_btn" class="ghost">Refresh Monitor</button>
        <button id="runs_load_latest_result_btn" class="secondary">Load Latest Succeeded Result</button>
        <label for="runs_view_preset" style="margin:0; align-self:center;">view</label>
        <select id="runs_view_preset" style="width:auto; min-width:200px;">
          <option value="actionable" selected>action needed (recommended)</option>
          <option value="latest">latest first</option>
          <option value="all">all rows</option>
        </select>
        <label for="runs_row_limit" style="margin:0; align-self:center;">rows</label>
        <select id="runs_row_limit" style="width:auto; min-width:86px;">
          <option value="20" selected>20</option>
          <option value="50">50</option>
          <option value="100">100</option>
          <option value="200">200</option>
        </select>
        <label for="runs_sort_key" style="margin:0; align-self:center;">sort</label>
        <select id="runs_sort_key" style="width:auto; min-width:180px;">
          <option value="updated_desc">updated (newest)</option>
          <option value="updated_asc">updated (oldest)</option>
          <option value="run_id_asc">run_id (A-Z)</option>
          <option value="mode_asc">mode (A-Z)</option>
          <option value="state_asc">state (A-Z)</option>
        </select>
        <label class="inline"><input id="runs_compact_view" type="checkbox" checked /> compact</label>
        <label class="inline"><input id="runs_only_with_result" type="checkbox" /> only with result</label>
      </div>
      <div class="kpi-grid" id="runs_kpi_cards"></div>
      <div id="runs_insight" class="hint-box">focus on action needed view first; switch to all rows only for audit.</div>
      <div id="runs_notice" class="notice">ready</div>
      <div class="run-table-wrap runs-compact" id="runs_table_wrap">
        <table id="runs_table">
          <thead>
            <tr>
              <th>run_id</th>
              <th>mode</th>
              <th>state</th>
              <th class="runs-col-detail">progress</th>
              <th class="runs-col-detail">attempt</th>
              <th>updated_utc</th>
              <th class="runs-col-detail">source</th>
              <th>result</th>
              <th class="runs-col-detail">counts</th>
              <th>actions</th>
            </tr>
          </thead>
          <tbody id="runs_tbody"></tbody>
        </table>
      </div>
      <div class="footer-note">Actions call API endpoints directly. Cancel/Retry follow orchestrator transition rules.</div>
    </section>

    <section class="card" style="margin-top: 14px;">
      <h2>Explorer Sweep (Across Runs)</h2>
      <div class="toolbar">
        <button id="explorer_refresh_btn" class="secondary">Refresh Explorer</button>
        <label for="explorer_mode_scope" style="margin:0; align-self:center;">mode</label>
        <select id="explorer_mode_scope" style="width:auto; min-width:160px;">
          <option value="all">all</option>
          <option value="singlex">singlex*</option>
          <option value="nooption">nooption*</option>
          <option value="paired">paired*</option>
        </select>
        <label for="explorer_run_like" style="margin:0; align-self:center;">run_id contains</label>
        <input id="explorer_run_like" style="width:220px;" placeholder="optional keyword filter" />
        <label for="explorer_q_threshold" style="margin:0; align-self:center;">strong q <=</label>
        <input id="explorer_q_threshold" type="number" min="0" max="1" step="0.01" value="0.10" style="width:90px;" />
        <label for="explorer_top_n" style="margin:0; align-self:center;">top_n</label>
        <select id="explorer_top_n" style="width:auto; min-width:80px;">
          <option value="10">10</option>
          <option value="20" selected>20</option>
          <option value="30">30</option>
          <option value="50">50</option>
        </select>
      </div>
      <div id="explorer_notice" class="notice">ready</div>
      <div class="kpi-grid" id="explorer_kpi_cards"></div>
      <div class="explorer-joint-grid">
        <div class="viz-card explorer-joint-top">
          <h3>Best q Distribution (x-axis)</h3>
          <div id="explorer_best_q_marginal" class="marginal-wrap"></div>
        </div>
        <div class="viz-card explorer-joint-kpi">
          <h3>Joint Summary</h3>
          <div id="explorer_best_qp_joint_kpis" class="joint-kpi-grid"></div>
          <div class="toolbar" style="margin-top:8px;">
            <button id="explorer_joint_clear_btn" class="ghost">Clear Joint Filters</button>
          </div>
        </div>
        <div class="viz-card explorer-joint-main">
          <h3>Best q vs Best p Scatter (run-level)</h3>
          <div id="explorer_best_qp_scatter" class="scatter-wrap"></div>
          <div id="explorer_best_qp_meta" class="footer-note">points: 0</div>
          <div id="explorer_best_qp_hover" class="hint-box">hover/click a point to inspect run-level metrics</div>
        </div>
        <div class="viz-card explorer-joint-side">
          <h3>Best p Distribution (y-axis)</h3>
          <div id="explorer_best_p_marginal" class="marginal-wrap side"></div>
        </div>
      </div>
      <label>Top Combinations</label>
      <div class="toolbar">
        <label for="explorer_combo_filter_text" style="margin:0; align-self:center;">factor contains</label>
        <input id="explorer_combo_filter_text" style="width:220px;" placeholder="e.g. is_academia_origin" />
        <label for="explorer_combo_sort_key" style="margin:0; align-self:center;">sort</label>
        <select id="explorer_combo_sort_key" style="width:auto; min-width:190px;">
          <option value="q_best_asc" selected>q_best (low)</option>
          <option value="q_median_asc">q_median (low)</option>
          <option value="n_runs_desc">n_runs (high)</option>
          <option value="strong_share_desc">strong_share (high)</option>
          <option value="validated_share_desc">validated_share (high)</option>
          <option value="key_factor_asc">key_factor (A-Z)</option>
        </select>
        <label for="explorer_combo_limit" style="margin:0; align-self:center;">rows</label>
        <select id="explorer_combo_limit" style="width:auto; min-width:84px;">
          <option value="10">10</option>
          <option value="20" selected>20</option>
          <option value="50">50</option>
          <option value="100">100</option>
        </select>
        <button id="explorer_combo_reset_btn" class="ghost">Reset Filter</button>
      </div>
      <div id="explorer_combo_notice" class="hint-box">showing 0 / 0 combinations</div>
      <div class="run-table-wrap" style="margin-top: 8px;">
        <table>
          <thead>
            <tr>
              <th>rank</th>
              <th>key_factor</th>
              <th>track/context/spec</th>
              <th>n_runs</th>
              <th>q_best</th>
              <th>q_median</th>
              <th>strong_share</th>
              <th>validated_share</th>
            </tr>
          </thead>
          <tbody id="explorer_combo_tbody"></tbody>
        </table>
      </div>
      <label>Top Key Factors</label>
      <div class="run-table-wrap" style="margin-top: 8px;">
        <table>
          <thead>
            <tr>
              <th>rank</th>
              <th>key_factor</th>
              <th>n_runs</th>
              <th>q_best</th>
              <th>q_median</th>
              <th>strong_share</th>
              <th>validated_share</th>
            </tr>
          </thead>
          <tbody id="explorer_factor_tbody"></tbody>
        </table>
      </div>
      <label>Equation Builder (Stepwise Fit + Uncertainty)</label>
      <div class="toolbar">
        <label for="eq_run_id" style="margin:0; align-self:center;">run_id</label>
        <input id="eq_run_id" style="width:280px;" placeholder="run summary source run_id" />
        <label for="eq_track" style="margin:0; align-self:center;">track</label>
        <select id="eq_track" style="width:auto; min-width:170px;">
          <option value="primary_strict">primary_strict</option>
          <option value="sensitivity_broad_company_no_edu">sensitivity_broad_company_no_edu</option>
          <option value="all">all</option>
        </select>
        <label for="eq_y_col" style="margin:0; align-self:center;">y</label>
        <select id="eq_y_col" style="width:auto; min-width:120px;">
          <option value="y_all">y_all</option>
          <option value="y_evidence">y_evidence</option>
        </select>
        <label for="eq_split_role" style="margin:0; align-self:center;">split</label>
        <select id="eq_split_role" style="width:auto; min-width:120px;">
          <option value="validation" selected>validation</option>
          <option value="discovery">discovery</option>
          <option value="all">all</option>
        </select>
      </div>
      <div class="toolbar">
        <label for="eq_top_k" style="margin:0; align-self:center;">top_k factors</label>
        <select id="eq_top_k" style="width:auto; min-width:80px;">
          <option value="3">3</option>
          <option value="5" selected>5</option>
          <option value="8">8</option>
          <option value="10">10</option>
        </select>
        <label for="eq_max_steps" style="margin:0; align-self:center;">max_steps</label>
        <select id="eq_max_steps" style="width:auto; min-width:92px;">
          <option value="3">3</option>
          <option value="5" selected>5</option>
          <option value="8">8</option>
          <option value="10">10</option>
          <option value="15">15</option>
        </select>
        <label for="eq_bootstrap_n" style="margin:0; align-self:center;">bootstrap</label>
        <select id="eq_bootstrap_n" style="width:auto; min-width:88px;">
          <option value="0">0</option>
          <option value="19">19</option>
          <option value="49" selected>49</option>
          <option value="99">99</option>
        </select>
        <label class="inline"><input id="eq_include_base_controls" type="checkbox" checked /> include base controls</label>
        <label class="inline"><input id="eq_include_baseline" type="checkbox" checked /> show baseline step</label>
        <button id="eq_use_top_factors_btn" class="ghost">Use Explorer Top Factors</button>
        <button id="eq_build_btn" class="secondary">Build Step Curve</button>
      </div>
      <div class="row">
        <div>
          <label for="eq_factor_list">factor list (one per line or comma separated)</label>
          <textarea id="eq_factor_list" class="equation-factor-box" placeholder="is_academia_origin&#10;pa__institutions_distinct_count"></textarea>
        </div>
      </div>
      <div id="eq_notice" class="notice">ready</div>
      <div class="run-table-wrap compact-table">
        <table>
          <tbody id="eq_meta_tbody"></tbody>
        </table>
      </div>
      <div class="viz-card" style="margin-top: 8px;">
        <h3>Event Accuracy by Step (95% uncertainty)</h3>
        <div id="eq_curve_chart" class="equation-chart-wrap"></div>
        <div id="eq_curve_hint" class="footer-note">accuracy = correctly predicted choice events / informative events (conditional logit)</div>
      </div>
      <div class="run-table-wrap" style="margin-top: 8px;">
        <table>
          <thead>
            <tr>
              <th>step</th>
              <th>added factor</th>
              <th>atoms</th>
              <th>equation</th>
              <th>events</th>
              <th>accuracy mean</th>
              <th>accuracy 95% CI</th>
              <th>delta acc</th>
              <th>delta 95% CI</th>
              <th>|delta| share</th>
              <th>llf/event mean</th>
              <th>bootstrap</th>
            </tr>
          </thead>
          <tbody id="eq_step_tbody"></tbody>
        </table>
      </div>
      <label>SHAP-lite Atom Contributions</label>
      <div id="eq_group_notice" class="hint-box">group coverage: 0 atoms</div>
      <div class="run-table-wrap" style="margin-top: 8px;">
        <table>
          <thead>
            <tr>
              <th>rank</th>
              <th>atom</th>
              <th>contrib sum</th>
              <th>|contrib| share</th>
              <th>steps</th>
              <th>factors</th>
            </tr>
          </thead>
          <tbody id="eq_group_tbody"></tbody>
        </table>
      </div>
      <label>High-Affinity Pairs</label>
      <div class="run-table-wrap" style="margin-top: 8px;">
        <table>
          <thead>
            <tr>
              <th>rank</th>
              <th>pair</th>
              <th>co_runs</th>
              <th>run_share</th>
              <th>jaccard</th>
              <th>lift</th>
            </tr>
          </thead>
          <tbody id="explorer_pair_tbody"></tbody>
        </table>
      </div>
      <label>Grouped Factors (high co-occurrence)</label>
      <div class="toolbar">
        <label for="explorer_cluster_filter_text" style="margin:0; align-self:center;">factor contains</label>
        <input id="explorer_cluster_filter_text" style="width:220px;" placeholder="e.g. is_academia_origin" />
        <label for="explorer_cluster_min_support" style="margin:0; align-self:center;">min run support</label>
        <select id="explorer_cluster_min_support" style="width:auto; min-width:92px;">
          <option value="1" selected>1+</option>
          <option value="2">2+</option>
          <option value="3">3+</option>
          <option value="5">5+</option>
          <option value="10">10+</option>
        </select>
        <label for="explorer_cluster_sort_key" style="margin:0; align-self:center;">sort</label>
        <select id="explorer_cluster_sort_key" style="width:auto; min-width:180px;">
          <option value="support_desc" selected>run support (high)</option>
          <option value="factors_desc">n_factors (high)</option>
          <option value="signature_asc">cluster (A-Z)</option>
        </select>
        <label for="explorer_cluster_limit" style="margin:0; align-self:center;">rows</label>
        <select id="explorer_cluster_limit" style="width:auto; min-width:84px;">
          <option value="10">10</option>
          <option value="20" selected>20</option>
          <option value="50">50</option>
          <option value="100">100</option>
        </select>
        <button id="explorer_cluster_reset_btn" class="ghost">Reset Filter</button>
      </div>
      <div id="explorer_cluster_notice" class="hint-box">showing 0 / 0 clusters</div>
      <div class="run-table-wrap" style="margin-top: 8px;">
        <table>
          <thead>
            <tr>
              <th>cluster</th>
              <th>n_factors</th>
              <th>run_support</th>
              <th>factors</th>
            </tr>
          </thead>
          <tbody id="explorer_cluster_tbody"></tbody>
        </table>
      </div>
      <details>
        <summary>Explorer Raw JSON (debug)</summary>
        <div id="explorer_raw_box" class="mono">{}</div>
      </details>
    </section>
  </div>

  <script>
    function byId(id) { return document.getElementById(id); }

    const MODE_HELP = {
      "paired_nooption_singlex": "Recommended baseline: run nooption + singlex together and compare direction-review checks.",
      "singlex_baseline": "Hypothesis-first singleton baseline under governance defaults.",
      "nooption_baseline": "Wide baseline scan with governance defaults.",
      "paired_nooption_singlex_hypothesis": "Paired execution over hypothesis panel windows.",
      "singlex_hypothesis_panel": "Singleton path over time-window hypothesis panel.",
      "nooption_hypothesis_panel": "Nooption path over time-window hypothesis panel.",
      "openexplore_autorefine": "Exploration then auto-refine shortlist with stronger bootstrap.",
      "openexplore": "Exploratory mode without refine shortcut.",
      "overnight_validation": "Long-running paired sweep across seed/bootstrap grid with checkpoint resume.",
      "singlex": "Legacy singleton mode.",
      "nooption": "Legacy nooption mode."
    };

    const UI_STATE = {
      submitBusy: false,
      inspectBusy: false,
      profileBusy: false,
      runsBusy: false,
      healthBusy: false,
      compareBusy: false,
      savedReportsBusy: false,
      savedReports: [],
      lastRunRows: [],
      selectedRunId: "",
      datasetCandidatesBusy: false,
      datasetCandidates: [],
      explorerBusy: false,
      explorerPayload: null,
      explorerJointSelection: { q_idx: -1, p_idx: -1 },
      equationBusy: false,
      equationPayload: null,
      compareSnapshot: null,
    };

    const RUN_ID_PATTERN = /^[A-Za-z0-9._:-]{3,128}$/;

    function makeDefaultRunId(prefix) {
      const now = new Date();
      const iso = now.toISOString().replace(/[-:TZ.]/g, "").slice(0, 14);
      const head = String(prefix || "ui_run").replace(/[^A-Za-z0-9._:-]/g, "_");
      return head + "_" + iso;
    }

    function prettyJson(obj) {
      return JSON.stringify(obj || {}, null, 2);
    }

    function fmt(v, digits) {
      if (v === null || v === undefined || v === "") return "-";
      if (typeof v === "number" && Number.isFinite(v)) {
        if (typeof digits === "number") return v.toFixed(digits);
        return String(v);
      }
      const n = Number(v);
      if (Number.isFinite(n)) {
        if (typeof digits === "number") return n.toFixed(digits);
        return String(n);
      }
      return String(v);
    }

    function fmtSigned(v, digits) {
      const n = Number(v);
      if (!Number.isFinite(n)) return "-";
      const text = typeof digits === "number" ? n.toFixed(digits) : String(n);
      if (n > 0) return "+" + text;
      return text;
    }

    function numericOrNull(v) {
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    }

    function boolOrNull(v) {
      if (v === true) return true;
      if (v === false) return false;
      return null;
    }

    function createCompareBadge(value) {
      const span = document.createElement("span");
      span.className = "compare-badge";
      if (value === true) {
        span.classList.add("compare-badge-pass");
        span.textContent = "pass";
        return span;
      }
      if (value === false) {
        span.classList.add("compare-badge-fail");
        span.textContent = "fail";
        return span;
      }
      span.textContent = "unknown";
      return span;
    }

    function setNotice(el, kind, message) {
      el.className = "notice";
      if (kind === "ok") el.classList.add("notice-ok");
      if (kind === "error") el.classList.add("notice-error");
      el.textContent = message;
    }

    function createStatusPill(state) {
      const value = String(state || "unknown").toLowerCase();
      const span = document.createElement("span");
      span.className = "status-pill state-" + value;
      span.textContent = value;
      return span;
    }

    function parseOptionalNonNegativeInt(inputId, label) {
      const raw = String(byId(inputId).value || "").trim();
      if (!raw) return null;
      const n = Number(raw);
      if (!Number.isInteger(n) || n < 0) {
        throw new Error(label + " must be a non-negative integer");
      }
      return n;
    }

    function parseBoundedInt(inputId, label, minValue, maxValue) {
      const raw = String(byId(inputId).value || "").trim();
      if (!raw) {
        throw new Error(label + " is required");
      }
      const n = Number(raw);
      if (!Number.isInteger(n) || n < Number(minValue) || n > Number(maxValue)) {
        throw new Error(label + " must be an integer in [" + String(minValue) + ", " + String(maxValue) + "]");
      }
      return n;
    }

    function applyRunIdToDatasetInputs(runId) {
      const rid = validateRunId(String(runId || "").trim());
      byId("dataset_run_id").value = rid;
      byId("dataset_artifact_key").value = "auto";
      byId("dataset_path").value = "";
    }

    function datasetConfigPayloadFromInputs() {
      return {
        dataset_path: String(byId("dataset_path").value || "").trim(),
        run_id: String(byId("dataset_run_id").value || "").trim(),
        artifact_key: String(byId("dataset_artifact_key").value || "auto").trim(),
        sample_rows: parseBoundedInt("dataset_sample_rows", "dataset_sample_rows", 100, 500000),
        top_n: parseBoundedInt("dataset_top_n", "dataset_top_n", 1, 100),
        research_mode: Boolean(byId("dataset_research_mode").checked),
        fixed_y: String(byId("dataset_fixed_y").value || "").trim(),
        exclude_x_cols: String(byId("dataset_exclude_x_cols").value || "").trim(),
      };
    }

    function applyDatasetConfig(config) {
      const cfg = config && typeof config === "object" ? config : {};
      byId("dataset_path").value = String(cfg.dataset_path || "");
      byId("dataset_run_id").value = String(cfg.run_id || "");
      byId("dataset_artifact_key").value = String(cfg.artifact_key || "auto");
      if (cfg.sample_rows !== undefined && cfg.sample_rows !== null) {
        byId("dataset_sample_rows").value = String(cfg.sample_rows);
      }
      if (cfg.top_n !== undefined && cfg.top_n !== null) {
        byId("dataset_top_n").value = String(cfg.top_n);
      }
      byId("dataset_research_mode").checked = Boolean(cfg.research_mode !== false);
      byId("dataset_fixed_y").value = String(cfg.fixed_y || "");
      byId("dataset_exclude_x_cols").value = String(cfg.exclude_x_cols || "");
    }

    function renderDatasetCandidates(rows) {
      const data = Array.isArray(rows) ? rows : [];
      const select = byId("dataset_candidate_select");
      const prev = String(select.value || "").trim();
      select.replaceChildren();

      const first = document.createElement("option");
      first.value = "";
      first.textContent = data.length ? "select dataset candidate" : "no dataset candidates";
      select.appendChild(first);

      for (const row of data) {
        const path = String(row.dataset_path || "").trim();
        if (!path) continue;
        const opt = document.createElement("option");
        opt.value = path;
        const runId = String(row.run_id || "-");
        const artifact = String(row.artifact_key || "-");
        const mode = String(row.mode || "-");
        opt.textContent = runId + " [" + mode + " / " + artifact + "]";
        select.appendChild(opt);
      }

      if (prev && data.some((row) => String(row.dataset_path || "") === prev)) {
        select.value = prev;
      }
    }

    function selectedDatasetCandidate() {
      const path = String(byId("dataset_candidate_select").value || "").trim();
      if (!path) return null;
      const rows = Array.isArray(UI_STATE.datasetCandidates) ? UI_STATE.datasetCandidates : [];
      return rows.find((row) => String(row.dataset_path || "") === path) || null;
    }

    function applyDatasetCandidate(row) {
      const item = row && typeof row === "object" ? row : {};
      if (item.run_id) {
        byId("dataset_run_id").value = String(item.run_id);
      }
      if (item.dataset_path) {
        byId("dataset_path").value = String(item.dataset_path);
      }
      if (item.artifact_key) {
        byId("dataset_artifact_key").value = String(item.artifact_key);
      }
    }

    function renderRecentRunSelect(rows) {
      const select = byId("recent_run_select");
      const selectedPrev = String(select.value || "").trim();
      select.replaceChildren();

      const first = document.createElement("option");
      first.value = "";
      first.textContent = "select recent/history run";
      select.appendChild(first);

      for (const row of (Array.isArray(rows) ? rows : []).slice(0, 120)) {
        const rid = String(row.run_id || "").trim();
        if (!rid) continue;
        const option = document.createElement("option");
        option.value = rid;
        option.textContent = rid + " [" + String(row.mode || "-") + " / " + String(row.state || "-") + "]";
        select.appendChild(option);
      }

      if (selectedPrev && Array.from(select.options).some((o) => String(o.value) === selectedPrev)) {
        select.value = selectedPrev;
      }
    }

    async function inspectFromRecentSelect() {
      const rid = String(byId("recent_run_select").value || "").trim();
      if (!rid) {
        setNotice(byId("detail_notice"), "error", "select a run from recent/history run first");
        return;
      }
      byId("detail_run_id").value = rid;
      await inspectRun(rid);
    }

    async function inspectAndProfileFromDetail() {
      const fallback = String(byId("recent_run_select").value || "").trim();
      const ridText = String(byId("detail_run_id").value || "").trim() || fallback;
      if (!ridText) {
        setNotice(byId("detail_notice"), "error", "detail run_id or recent run selection is required");
        return;
      }
      const rid = validateRunId(ridText);
      byId("detail_run_id").value = rid;
      await inspectRun(rid);
      applyRunIdToDatasetInputs(rid);
      await runDatasetProfile();
    }

    function pickLatestRunRow(rows, opts) {
      const data = Array.isArray(rows) ? rows.slice() : [];
      if (!data.length) return null;
      const options = opts && typeof opts === "object" ? opts : {};
      const requireResult = Boolean(options.requireResult);
      data.sort(
        (a, b) => String(b && b.updated_at_utc || "").localeCompare(String(a && a.updated_at_utc || ""))
      );
      if (requireResult) {
        const withResult = data.find((row) => Boolean(row && row.has_result));
        if (withResult) return withResult;
      }
      const succeeded = data.find((row) => String(row && row.state || "").toLowerCase() === "succeeded");
      return succeeded || data[0] || null;
    }

    function runNeedsAction(row) {
      const state = String(row && row.state || "").toLowerCase();
      return state === "failed" || state === "cancelled" || state === "running" || state === "queued";
    }

    function filteredAndSortedRunRows(rows) {
      const data = Array.isArray(rows) ? rows.slice() : [];
      const onlyWithResult = Boolean(byId("runs_only_with_result").checked);
      const viewPreset = String(byId("runs_view_preset").value || "actionable");
      const rowLimit = Number(byId("runs_row_limit").value || "20");
      const sortKey = String(byId("runs_sort_key").value || "updated_desc");
      let filtered = data;
      if (onlyWithResult) {
        filtered = filtered.filter((row) => Boolean(row && row.has_result));
      }

      const cmpText = (a, b) => String(a || "").localeCompare(String(b || ""));
      const cmpUpdated = (a, b) =>
        String(a && a.updated_at_utc || "").localeCompare(String(b && b.updated_at_utc || ""));
      if (sortKey === "updated_asc") {
        filtered.sort((a, b) => cmpUpdated(a, b));
      } else if (sortKey === "run_id_asc") {
        filtered.sort((a, b) => cmpText(a && a.run_id, b && b.run_id));
      } else if (sortKey === "mode_asc") {
        filtered.sort((a, b) => cmpText(a && a.mode, b && b.mode));
      } else if (sortKey === "state_asc") {
        filtered.sort((a, b) => cmpText(a && a.state, b && b.state));
      } else {
        filtered.sort((a, b) => cmpUpdated(b, a));
      }

      if (viewPreset === "actionable") {
        const selected = String(UI_STATE.selectedRunId || "");
        let focusRows = filtered.filter((row) => runNeedsAction(row));
        if (selected && !focusRows.some((row) => String(row && row.run_id || "") === selected)) {
          const selectedRow = filtered.find((row) => String(row && row.run_id || "") === selected);
          if (selectedRow) focusRows = [selectedRow].concat(focusRows);
        }
        if (!focusRows.length) {
          const latestResult = pickLatestRunRow(filtered, { requireResult: true });
          if (latestResult) focusRows = [latestResult];
        }
        filtered = focusRows;
      }

      if (Number.isFinite(rowLimit) && rowLimit > 0 && filtered.length > rowLimit) {
        filtered = filtered.slice(0, rowLimit);
      }
      return filtered;
    }

    function renderRunsKpiCards(rows) {
      const wrap = byId("runs_kpi_cards");
      wrap.replaceChildren();
      const data = Array.isArray(rows) ? rows : [];
      const counts = {
        total: data.length,
        succeeded: 0,
        running: 0,
        queued: 0,
        failed: 0,
        cancelled: 0,
        with_result: 0,
      };
      for (const row of data) {
        const state = String(row && row.state || "").toLowerCase();
        if (state in counts) {
          counts[state] += 1;
        }
        if (row && row.has_result) {
          counts.with_result += 1;
        }
      }
      const cards = [
        { label: "total", value: counts.total, cls: "" },
        { label: "succeeded", value: counts.succeeded, cls: "kpi-pass" },
        { label: "running", value: counts.running, cls: "kpi-warn" },
        { label: "queued", value: counts.queued, cls: "kpi-warn" },
        { label: "failed", value: counts.failed, cls: "kpi-fail" },
        { label: "cancelled", value: counts.cancelled, cls: "kpi-fail" },
        { label: "with result", value: counts.with_result, cls: "" },
      ];
      for (const card of cards) {
        const cardEl = document.createElement("div");
        cardEl.className = "kpi-card";
        const labelEl = document.createElement("div");
        labelEl.className = "kpi-label";
        labelEl.textContent = card.label;
        const valueEl = document.createElement("div");
        valueEl.className = "kpi-value" + (card.cls ? " " + card.cls : "");
        valueEl.textContent = fmt(card.value);
        cardEl.appendChild(labelEl);
        cardEl.appendChild(valueEl);
        wrap.appendChild(cardEl);
      }
    }

    function renderRunsInsight(visibleRows, allRows) {
      const visible = Array.isArray(visibleRows) ? visibleRows : [];
      const full = Array.isArray(allRows) ? allRows : visible;
      const failed = full.filter((row) => String(row && row.state || "").toLowerCase() === "failed").length;
      const cancelled = full.filter((row) => String(row && row.state || "").toLowerCase() === "cancelled").length;
      const running = full.filter((row) => String(row && row.state || "").toLowerCase() === "running").length;
      const queued = full.filter((row) => String(row && row.state || "").toLowerCase() === "queued").length;
      const withResult = full.filter((row) => Boolean(row && row.has_result)).length;
      const hidden = Math.max(0, full.length - visible.length);
      const msg = [];
      if (failed > 0 || cancelled > 0) {
        msg.push("action: failed/cancelled " + String(failed + cancelled) + " runs -> inspect and retry if needed.");
      } else if (running > 0 || queued > 0) {
        msg.push("status: running/queued " + String(running + queued) + " runs.");
      } else if (withResult > 0) {
        msg.push("status: result-ready " + String(withResult) + " runs. use 'Load Latest Succeeded Result'.");
      } else {
        msg.push("status: no result-ready runs yet.");
      }
      if (hidden > 0) {
        msg.push("view limited: " + String(hidden) + " rows hidden by preset/limit.");
      }
      byId("runs_insight").textContent = msg.join(" ");
    }

    function applyRunsCompactView() {
      const compact = Boolean(byId("runs_compact_view").checked);
      byId("runs_table_wrap").classList.toggle("runs-compact", compact);
    }

    async function refreshAllPreviousSources() {
      setNotice(byId("detail_notice"), "", "refreshing run/report/data sources...");
      await Promise.all([refreshRuns(), refreshSavedReports(), refreshDatasetCandidates()]);
      setNotice(byId("detail_notice"), "ok", "sources refreshed");
    }

    async function loadLatestRunResultGuided() {
      if (!Array.isArray(UI_STATE.lastRunRows) || !UI_STATE.lastRunRows.length) {
        await refreshRuns();
      }
      const visible = filteredAndSortedRunRows(UI_STATE.lastRunRows || []);
      const row = pickLatestRunRow(visible.length ? visible : UI_STATE.lastRunRows, { requireResult: true });
      if (!row || !row.run_id) {
        setNotice(byId("detail_notice"), "error", "no run history found");
        return;
      }
      const rid = String(row.run_id);
      byId("detail_run_id").value = rid;
      if (Array.from(byId("recent_run_select").options || []).some((o) => String(o.value) === rid)) {
        byId("recent_run_select").value = rid;
      }
      await inspectRun(rid);
    }

    async function loadLatestSavedReportGuided() {
      if (!Array.isArray(UI_STATE.savedReports) || !UI_STATE.savedReports.length) {
        await refreshSavedReports();
      }
      const rows = Array.isArray(UI_STATE.savedReports) ? UI_STATE.savedReports : [];
      const row = rows[0] || null;
      if (!row || !row.relative_path) {
        setNotice(byId("saved_reports_notice"), "error", "no saved reports found");
        return;
      }
      byId("saved_report_select").value = String(row.relative_path);
      await loadSavedReport();
    }

    function setSubmitBusy(flag) {
      UI_STATE.submitBusy = Boolean(flag);
      const btn = byId("submit_btn");
      btn.disabled = UI_STATE.submitBusy;
      btn.textContent = UI_STATE.submitBusy ? "Submitting..." : "Submit Run";
      byId("run_pair_now_btn").disabled = UI_STATE.submitBusy;
      byId("run_singlex_now_btn").disabled = UI_STATE.submitBusy;
      byId("run_nooption_now_btn").disabled = UI_STATE.submitBusy;
    }

    function setInspectBusy(flag) {
      UI_STATE.inspectBusy = Boolean(flag);
      const btn = byId("inspect_btn");
      btn.disabled = UI_STATE.inspectBusy;
      btn.textContent = UI_STATE.inspectBusy ? "Inspecting..." : "Inspect";
      byId("inspect_profile_btn").disabled = UI_STATE.inspectBusy;
      byId("load_recent_run_btn").disabled = UI_STATE.inspectBusy;
    }

    function setProfileBusy(flag) {
      UI_STATE.profileBusy = Boolean(flag);
      const btn = byId("profile_btn");
      btn.disabled = UI_STATE.profileBusy;
      btn.textContent = UI_STATE.profileBusy ? "Analyzing..." : "Analyze Dataset";
    }

    function setCompareBusy(flag) {
      UI_STATE.compareBusy = Boolean(flag);
      const btn = byId("compare_btn");
      btn.disabled = UI_STATE.compareBusy;
      btn.textContent = UI_STATE.compareBusy ? "Comparing..." : "Compare Runs";
      byId("compare_from_detail_btn").disabled = UI_STATE.compareBusy;
      updateCompareExportButtons();
    }

    function setSavedReportsBusy(flag) {
      UI_STATE.savedReportsBusy = Boolean(flag);
      byId("refresh_saved_reports_btn").disabled = UI_STATE.savedReportsBusy;
      byId("load_saved_report_btn").disabled = UI_STATE.savedReportsBusy;
      byId("saved_report_select").disabled = UI_STATE.savedReportsBusy;
    }

    function setDatasetCandidatesBusy(flag) {
      UI_STATE.datasetCandidatesBusy = Boolean(flag);
      byId("refresh_dataset_candidates_btn").disabled = UI_STATE.datasetCandidatesBusy;
      byId("use_dataset_candidate_btn").disabled = UI_STATE.datasetCandidatesBusy;
      byId("dataset_candidate_select").disabled = UI_STATE.datasetCandidatesBusy;
    }

    function hasCompareSnapshot() {
      return Boolean(
        UI_STATE.compareSnapshot &&
        UI_STATE.compareSnapshot.nooption &&
        UI_STATE.compareSnapshot.singlex
      );
    }

    function readCompareRunIds() {
      return {
        nooption: String(byId("compare_nooption_run_id").value || "").trim(),
        singlex: String(byId("compare_singlex_run_id").value || "").trim(),
      };
    }

    function hasValidCompareRunIds() {
      const ids = readCompareRunIds();
      return RUN_ID_PATTERN.test(ids.nooption) && RUN_ID_PATTERN.test(ids.singlex);
    }

    function updateCompareExportButtons() {
      const hasSnapshot = hasCompareSnapshot();
      const hasIds = hasValidCompareRunIds();
      byId("save_compare_outputs_btn").disabled = UI_STATE.compareBusy || !(hasSnapshot || hasIds);
      byId("export_compare_md_btn").disabled = UI_STATE.compareBusy || !hasSnapshot;
      byId("export_compare_json_btn").disabled = UI_STATE.compareBusy || !hasSnapshot;
    }

    function sanitizeFileNamePart(v) {
      const text = String(v || "unknown")
        .trim()
        .replace(/[^A-Za-z0-9._-]+/g, "_")
        .replace(/_+/g, "_")
        .replace(/^_+|_+$/g, "");
      return text || "unknown";
    }

    function compareStateValue(value) {
      const b = boolOrNull(value);
      if (b === true) return "pass";
      if (b === false) return "fail";
      return "unknown";
    }

    function compareRunRows(nooptionSnap, singlexSnap) {
      const rows = [];

      rows.push({
        metric: "run_id",
        nooption: String(nooptionSnap.run_id || "-"),
        singlex: String(singlexSnap.run_id || "-"),
        delta: "-",
      });
      rows.push({
        metric: "mode",
        nooption: String(nooptionSnap.mode || "-"),
        singlex: String(singlexSnap.mode || "-"),
        delta: "-",
      });
      rows.push({
        metric: "state",
        nooption: String(nooptionSnap.state || "unknown"),
        singlex: String(singlexSnap.state || "unknown"),
        delta: "-",
      });

      const validatedDelta = compareNumericMetric(nooptionSnap.validated, singlexSnap.validated, "higher", 0);
      rows.push({
        metric: "validated_candidates",
        nooption: fmt(nooptionSnap.validated),
        singlex: fmt(singlexSnap.validated),
        delta: validatedDelta.text,
      });

      const supportDelta = compareNumericMetric(nooptionSnap.support, singlexSnap.support, "higher", 0);
      rows.push({
        metric: "support_candidates",
        nooption: fmt(nooptionSnap.support),
        singlex: fmt(singlexSnap.support),
        delta: supportDelta.text,
      });

      const pDelta = compareNumericMetric(nooptionSnap.best_p, singlexSnap.best_p, "lower", 4);
      rows.push({
        metric: "best_p_validation",
        nooption: fmt(nooptionSnap.best_p, 4),
        singlex: fmt(singlexSnap.best_p, 4),
        delta: pDelta.text,
      });

      const qDelta = compareNumericMetric(nooptionSnap.best_q, singlexSnap.best_q, "lower", 4);
      rows.push({
        metric: "best_q_validation",
        nooption: fmt(nooptionSnap.best_q, 4),
        singlex: fmt(singlexSnap.best_q, 4),
        delta: qDelta.text,
      });

      const restartMaxDelta = compareNumericMetric(nooptionSnap.restart_max, singlexSnap.restart_max, "higher", 3);
      rows.push({
        metric: "restart_validated_rate_max",
        nooption: fmt(nooptionSnap.restart_max, 3),
        singlex: fmt(singlexSnap.restart_max, 3),
        delta: restartMaxDelta.text,
      });

      const restartMeanDelta = compareNumericMetric(nooptionSnap.restart_mean, singlexSnap.restart_mean, "higher", 3);
      rows.push({
        metric: "restart_validated_rate_mean",
        nooption: fmt(nooptionSnap.restart_mean, 3),
        singlex: fmt(singlexSnap.restart_mean, 3),
        delta: restartMeanDelta.text,
      });

      const consensusDelta = compareBooleanMetric(nooptionSnap.consensus, singlexSnap.consensus);
      rows.push({
        metric: "track_consensus_enforced",
        nooption: compareStateValue(nooptionSnap.consensus),
        singlex: compareStateValue(singlexSnap.consensus),
        delta: consensusDelta.text,
      });

      const leakageDelta = compareBooleanMetric(nooptionSnap.leakage_guard, singlexSnap.leakage_guard);
      rows.push({
        metric: "validation_used_for_search_false",
        nooption: compareStateValue(nooptionSnap.leakage_guard),
        singlex: compareStateValue(singlexSnap.leakage_guard),
        delta: leakageDelta.text,
      });

      const poolDelta = compareBooleanMetric(nooptionSnap.pool_lock, singlexSnap.pool_lock);
      rows.push({
        metric: "candidate_pool_locked_pre_validation_true",
        nooption: compareStateValue(nooptionSnap.pool_lock),
        singlex: compareStateValue(singlexSnap.pool_lock),
        delta: poolDelta.text,
      });

      return rows;
    }

    function getCompareSnapshotForExport() {
      if (!hasCompareSnapshot()) {
        throw new Error("no comparison snapshot yet; run compare first");
      }
      return UI_STATE.compareSnapshot;
    }

    function buildCompareExportBaseName(snapshot) {
      const noId = sanitizeFileNamePart(snapshot.nooption && snapshot.nooption.run_id);
      const sxId = sanitizeFileNamePart(snapshot.singlex && snapshot.singlex.run_id);
      return "compare_" + noId + "__vs__" + sxId;
    }

    function downloadTextFile(filename, content, mimeType) {
      const blob = new Blob([String(content)], { type: String(mimeType || "text/plain") + ";charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 0);
    }

    function markdownCell(text) {
      return String(text === null || text === undefined ? "-" : text).replace(/\\|/g, "\\|");
    }

    function buildCompareExportPayload(snapshot) {
      const rows = compareRunRows(snapshot.nooption, snapshot.singlex);
      return {
        exported_at_utc: new Date().toISOString(),
        compared_at_utc: snapshot.compared_at_utc || null,
        nooption: snapshot.nooption,
        singlex: snapshot.singlex,
        hints: Array.isArray(snapshot.hints) ? snapshot.hints : [],
        rows: rows,
      };
    }

    function renderCompareMarkdown(payload) {
      const lines = [];
      lines.push("# Baseline Compare Summary");
      lines.push("");
      lines.push("- compared_at_utc: " + String(payload.compared_at_utc || "-"));
      lines.push("- exported_at_utc: " + String(payload.exported_at_utc || "-"));
      lines.push("- nooption_run_id: " + String(payload.nooption && payload.nooption.run_id || "-"));
      lines.push("- singlex_run_id: " + String(payload.singlex && payload.singlex.run_id || "-"));
      lines.push("");
      lines.push("## Metrics");
      lines.push("");
      lines.push("| metric | nooption | singlex | delta |");
      lines.push("| --- | --- | --- | --- |");
      for (const row of payload.rows || []) {
        lines.push(
          "| " + markdownCell(row.metric) +
          " | " + markdownCell(row.nooption) +
          " | " + markdownCell(row.singlex) +
          " | " + markdownCell(row.delta) + " |"
        );
      }
      lines.push("");
      lines.push("## Direction Review Hints");
      lines.push("");
      const hints = Array.isArray(payload.hints) ? payload.hints : [];
      if (!hints.length) {
        lines.push("- (none)");
      } else {
        for (const hint of hints) {
          const level = String(hint && hint.level ? hint.level : "info").toUpperCase();
          const text = String(hint && hint.text ? hint.text : "-");
          lines.push("- [" + level + "] " + text);
        }
      }
      lines.push("");
      return lines.join("\\n");
    }

    function exportCompareJson() {
      const snapshot = getCompareSnapshotForExport();
      const payload = buildCompareExportPayload(snapshot);
      const file = buildCompareExportBaseName(snapshot) + ".json";
      downloadTextFile(file, JSON.stringify(payload, null, 2), "application/json");
      setNotice(byId("compare_notice"), "ok", "exported: " + file);
    }

    function exportCompareMarkdown() {
      const snapshot = getCompareSnapshotForExport();
      const payload = buildCompareExportPayload(snapshot);
      const file = buildCompareExportBaseName(snapshot) + ".md";
      downloadTextFile(file, renderCompareMarkdown(payload), "text/markdown");
      setNotice(byId("compare_notice"), "ok", "exported: " + file);
    }

    async function saveCompareOutputsToWorkspace() {
      const snapshot = hasCompareSnapshot() ? getCompareSnapshotForExport() : null;
      const ids = snapshot
        ? {
            nooption: String(snapshot.nooption && snapshot.nooption.run_id || "").trim(),
            singlex: String(snapshot.singlex && snapshot.singlex.run_id || "").trim(),
          }
        : readCompareRunIds();
      const payload = {
        nooption_run_id: validateRunId(ids.nooption),
        singlex_run_id: validateRunId(ids.singlex),
      };
      if (!payload.nooption_run_id || !payload.singlex_run_id) {
        throw new Error("compare snapshot is missing run ids");
      }
      const out = await fetchJson("/compare/export", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const outputs = out && out.outputs ? out.outputs : {};
      const sources = out && out.sources ? out.sources : {};
      const jsonPath = String(outputs.json || "").trim();
      const mdPath = String(outputs.markdown || "").trim();
      const sourceText =
        "nooption=" + String(sources.nooption || "-") + ", singlex=" + String(sources.singlex || "-");
      setNotice(
        byId("compare_notice"),
        "ok",
        "saved to outputs: " + (jsonPath || "-") + " / " + (mdPath || "-") +
          " (source: " + sourceText + ")"
      );
    }

    function validateRunId(runId) {
      const rid = String(runId || "").trim();
      if (!RUN_ID_PATTERN.test(rid)) {
        throw new Error("run_id must match [A-Za-z0-9._:-] and be 3-128 chars");
      }
      return rid;
    }

    async function fetchJson(url, options) {
      const resp = await fetch(url, options || {});
      const text = await resp.text();
      let payload = {};
      try {
        payload = text ? JSON.parse(text) : {};
      } catch (_err) {
        payload = { raw: text };
      }
      if (!resp.ok) {
        let detail = "";
        if (payload && typeof payload === "object") {
          const rawDetail = payload.detail;
          if (typeof rawDetail === "string" && rawDetail.trim()) {
            detail = rawDetail.trim();
          } else if (rawDetail && typeof rawDetail === "object") {
            detail = prettyJson(rawDetail);
          } else if (typeof payload.raw === "string" && payload.raw.trim()) {
            detail = payload.raw.trim();
          }
        }
        if (!detail) {
          detail = "request failed";
        }
        throw new Error("HTTP " + String(resp.status) + ": " + detail);
      }
      return payload;
    }

    function payloadFromForm() {
      const lines = byId("extra_args").value
        .split("\\n")
        .map((v) => v.trim())
        .filter((v) => v.length > 0);

      const payload = {
        mode: byId("mode").value,
        run_id: validateRunId(byId("run_id").value),
        skip_direction_review: byId("skip_direction_review").checked,
        extra_args: lines,
      };

      const scanN = parseOptionalNonNegativeInt("scan_n_bootstrap", "scan_n_bootstrap");
      const scanMax = parseOptionalNonNegativeInt("scan_max_features", "scan_max_features");
      const refineN = parseOptionalNonNegativeInt("refine_n_bootstrap", "refine_n_bootstrap");
      if (scanN !== null) payload.scan_n_bootstrap = scanN;
      if (scanMax !== null) payload.scan_max_features = scanMax;
      if (refineN !== null) payload.refine_n_bootstrap = refineN;

      const idem = String(byId("idempotency_key").value || "").trim();
      if (idem) payload.idempotency_key = idem;

      return payload;
    }

    function updateModeHelp() {
      const mode = byId("mode").value;
      const text = MODE_HELP[mode] || "Use baseline presets unless you need a specific exploratory mode.";
      byId("mode_help_text").textContent = text;
    }

    function applyPreset(mode, prefix) {
      const modeSel = byId("mode");
      const options = Array.from(modeSel.options || []).map((o) => o.value);
      if (options.includes(mode)) {
        modeSel.value = mode;
      }
      byId("run_id").value = makeDefaultRunId(prefix || mode);
      updateModeHelp();
    }

    function classifyQSignal(qValue) {
      const q = numericOrNull(qValue);
      if (q === null) return { text: "unknown", cls: "kpi-warn" };
      if (q <= 0.10) return { text: "strong", cls: "kpi-pass" };
      if (q <= 0.20) return { text: "borderline", cls: "kpi-warn" };
      return { text: "weak", cls: "kpi-fail" };
    }

    function classifyRestartSignal(restartMean) {
      const n = numericOrNull(restartMean);
      if (n === null) return { text: "unknown", cls: "kpi-warn" };
      if (n >= 0.60) return { text: "stable", cls: "kpi-pass" };
      if (n >= 0.40) return { text: "watch", cls: "kpi-warn" };
      return { text: "unstable", cls: "kpi-fail" };
    }

    function evaluateDetailSignals(statusResp, reviewResp) {
      const status = statusResp && statusResp.status ? statusResp.status : {};
      const review = reviewResp && reviewResp.review ? reviewResp.review : {};
      const metrics = review.metrics || {};
      const gov = review.governance || {};
      const state = String(status.state || review.state || "").toLowerCase();
      const stateDone = state === "succeeded";
      const leakOk = gov.validation_used_for_search_false === true;
      const lockOk = gov.candidate_pool_locked_pre_validation_true === true;
      const governanceOk = leakOk && lockOk;
      const validated = numericOrNull(metrics.validated_candidate_count);
      const q = numericOrNull(metrics.best_q_validation);
      const restartMean = numericOrNull(metrics.restart_validated_rate_mean);
      const evidenceOk = validated !== null && validated >= 1 && q !== null && q <= 0.10;
      const stabilityOk = restartMean !== null && restartMean >= 0.60;
      return {
        state,
        stateDone,
        leakOk,
        lockOk,
        governanceOk,
        evidenceOk,
        stabilityOk,
        qSignal: classifyQSignal(q),
        restartSignal: classifyRestartSignal(restartMean),
        validated: validated,
        q: q,
        restartMean: restartMean,
      };
    }

    function renderDetailVerdict(statusResp, reviewResp) {
      const box = byId("detail_verdict");
      const signal = evaluateDetailSignals(statusResp, reviewResp);
      if (!signal.stateDone) {
        const failState = signal.state === "failed" || signal.state === "cancelled";
        setNotice(
          box,
          failState ? "error" : "",
          failState
            ? "Verdict: not interpretable yet (run failed/cancelled)."
            : "Verdict: waiting for completion before interpreting statistics."
        );
        return;
      }
      if (!signal.governanceOk) {
        setNotice(
          box,
          "error",
          "Verdict: do not trust yet (governance gate failed: leakage/pool lock)."
        );
        return;
      }
      if (signal.evidenceOk && signal.stabilityOk) {
        setNotice(
          box,
          "ok",
          "Verdict: promotable candidate signal (governance pass + q<=0.10 + restart stable)."
        );
        return;
      }
      if (signal.evidenceOk && !signal.stabilityOk) {
        setNotice(
          box,
          "",
          "Verdict: evidence exists but stability is weak (check restart and rerun bootstrap)."
        );
        return;
      }
      setNotice(
        box,
        "",
        "Verdict: governance pass, but strong statistical evidence is not yet confirmed."
      );
    }

    function renderReviewCards(statusResp, reviewResp) {
      const wrap = byId("review_cards");
      wrap.replaceChildren();
      const review = reviewResp && reviewResp.review ? reviewResp.review : null;
      if (!review) return;

      const metrics = review.metrics || {};
      const gov = review.governance || {};
      const signal = evaluateDetailSignals(statusResp, reviewResp);
      const stateCls = signal.stateDone ? "kpi-pass" : (signal.state === "failed" || signal.state === "cancelled" ? "kpi-fail" : "kpi-warn");
      const evidenceText = signal.evidenceOk ? "strong" : (signal.validated !== null && signal.validated >= 1 ? "weak/borderline" : "none");
      const evidenceCls = signal.evidenceOk ? "kpi-pass" : (signal.validated !== null && signal.validated >= 1 ? "kpi-warn" : "kpi-fail");
      const demoted = numericOrNull(gov.track_consensus_demoted_rows);

      const cards = [
        { label: "state", value: signal.state || "unknown", cls: stateCls },
        { label: "governance", value: signal.governanceOk ? "pass" : "fail", cls: signal.governanceOk ? "kpi-pass" : "kpi-fail" },
        { label: "evidence", value: evidenceText, cls: evidenceCls },
        { label: "stability", value: signal.restartSignal.text, cls: signal.restartSignal.cls },
        { label: "validated", value: fmt(metrics.validated_candidate_count), cls: evidenceCls },
        { label: "best q", value: fmt(metrics.best_q_validation, 4), cls: signal.qSignal.cls },
        { label: "restart mean", value: fmt(metrics.restart_validated_rate_mean, 3), cls: signal.restartSignal.cls },
        { label: "consensus demoted", value: demoted === null ? "-" : fmt(demoted), cls: demoted === null || demoted === 0 ? "kpi-pass" : "kpi-warn" },
      ];

      for (const card of cards) {
        const cardEl = document.createElement("div");
        cardEl.className = "kpi-card";

        const labelEl = document.createElement("div");
        labelEl.className = "kpi-label";
        labelEl.textContent = card.label;

        const valueEl = document.createElement("div");
        valueEl.className = "kpi-value" + (card.cls ? " " + card.cls : "");
        valueEl.textContent = card.value;

        cardEl.appendChild(labelEl);
        cardEl.appendChild(valueEl);
        wrap.appendChild(cardEl);
      }
    }

    function appendOverviewRow(tbody, label, value, cls) {
      const tr = document.createElement("tr");
      const th = document.createElement("th");
      th.textContent = label;
      const td = document.createElement("td");
      if (value && typeof value === "object" && value.nodeType) {
        td.appendChild(value);
      } else {
        td.textContent = String(value === null || value === undefined || value === "" ? "-" : value);
      }
      if (cls) td.classList.add(cls);
      tr.appendChild(th);
      tr.appendChild(td);
      tbody.appendChild(tr);
    }

    function renderBarList(containerId, rows, labelKey, valueKey, digits) {
      const wrap = byId(containerId);
      wrap.replaceChildren();
      const data = Array.isArray(rows) ? rows : [];
      if (!data.length) {
        const empty = document.createElement("div");
        empty.className = "bar-label";
        empty.textContent = "(none)";
        wrap.appendChild(empty);
        return;
      }
      const maxVal = Math.max(
        1e-12,
        ...data.map((row) => {
          const n = Number(row && row[valueKey]);
          return Number.isFinite(n) ? n : 0;
        })
      );
      for (const row of data) {
        const label = String(row && row[labelKey] ? row[labelKey] : "-");
        const n = Number(row && row[valueKey]);
        const value = Number.isFinite(n) ? n : 0;
        const ratio = Math.max(0, Math.min(1, value / maxVal));

        const line = document.createElement("div");
        line.className = "bar-row";

        const labelEl = document.createElement("div");
        labelEl.className = "bar-label";
        labelEl.title = label;
        labelEl.textContent = label;

        const track = document.createElement("div");
        track.className = "bar-track";
        const fill = document.createElement("div");
        fill.className = "bar-fill";
        fill.style.width = String(Math.round(ratio * 1000) / 10) + "%";
        track.appendChild(fill);

        const valueEl = document.createElement("div");
        valueEl.className = "bar-value";
        valueEl.textContent = fmt(value, digits);

        line.appendChild(labelEl);
        line.appendChild(track);
        line.appendChild(valueEl);
        wrap.appendChild(line);
      }
    }

    function renderQuestionSeedTable(rows) {
      const tbody = byId("profile_seed_tbody");
      tbody.replaceChildren();
      const data = Array.isArray(rows) ? rows : [];
      if (!data.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 6;
        td.textContent = "No question seeds found.";
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
      }
      for (const row of data) {
        const tr = document.createElement("tr");

        const tdRank = document.createElement("td");
        tdRank.textContent = String(row.rank || "-");
        tr.appendChild(tdRank);

        const tdQ = document.createElement("td");
        tdQ.textContent = String(row.label || ((row.y_col || "-") + " ~ " + (row.x_col || "-")));
        tr.appendChild(tdQ);

        const tdScore = document.createElement("td");
        tdScore.textContent = fmt(row.score, 4);
        tr.appendChild(tdScore);

        const tdSupport = document.createElement("td");
        tdSupport.textContent = String(row.support_rows === undefined ? "-" : row.support_rows);
        tr.appendChild(tdSupport);

        const tdRisk = document.createElement("td");
        const level = String(row.risk_level || "low");
        const flags = Array.isArray(row.risk_flags) ? row.risk_flags : [];
        tdRisk.textContent = flags.length ? (level + " [" + flags.join(", ") + "]") : level;
        tr.appendChild(tdRisk);

        const tdSignal = document.createElement("td");
        tdSignal.textContent = String(row.signal_summary || "-");
        tr.appendChild(tdSignal);

        tbody.appendChild(tr);
      }
    }

    function setExplorerBusy(flag) {
      UI_STATE.explorerBusy = Boolean(flag);
      byId("explorer_refresh_btn").disabled = UI_STATE.explorerBusy;
      byId("explorer_mode_scope").disabled = UI_STATE.explorerBusy;
      byId("explorer_run_like").disabled = UI_STATE.explorerBusy;
      byId("explorer_q_threshold").disabled = UI_STATE.explorerBusy;
      byId("explorer_top_n").disabled = UI_STATE.explorerBusy;
      byId("explorer_joint_clear_btn").disabled = UI_STATE.explorerBusy;
      byId("explorer_combo_filter_text").disabled = UI_STATE.explorerBusy;
      byId("explorer_combo_sort_key").disabled = UI_STATE.explorerBusy;
      byId("explorer_combo_limit").disabled = UI_STATE.explorerBusy;
      byId("explorer_combo_reset_btn").disabled = UI_STATE.explorerBusy;
    }

    function setEquationBusy(flag) {
      UI_STATE.equationBusy = Boolean(flag);
      byId("eq_build_btn").disabled = UI_STATE.equationBusy;
      byId("eq_use_top_factors_btn").disabled = UI_STATE.equationBusy;
      byId("eq_run_id").disabled = UI_STATE.equationBusy;
      byId("eq_track").disabled = UI_STATE.equationBusy;
      byId("eq_y_col").disabled = UI_STATE.equationBusy;
      byId("eq_split_role").disabled = UI_STATE.equationBusy;
      byId("eq_top_k").disabled = UI_STATE.equationBusy;
      byId("eq_max_steps").disabled = UI_STATE.equationBusy;
      byId("eq_bootstrap_n").disabled = UI_STATE.equationBusy;
      byId("eq_include_base_controls").disabled = UI_STATE.equationBusy;
      byId("eq_include_baseline").disabled = UI_STATE.equationBusy;
      byId("eq_factor_list").disabled = UI_STATE.equationBusy;
    }

    function renderExplorerKpiCards(payload) {
      const wrap = byId("explorer_kpi_cards");
      wrap.replaceChildren();
      const totals = payload && payload.totals ? payload.totals : {};
      const cards = [
        { label: "runs considered", value: totals.runs_considered, cls: "" },
        { label: "runs w/ inference", value: totals.runs_with_inference, cls: "" },
        { label: "candidate rows", value: totals.candidate_rows, cls: "" },
        { label: "validation-ok rows", value: totals.validation_ok_rows, cls: "" },
        { label: "strong rows (q)", value: totals.strong_rows_q, cls: "kpi-pass" },
        { label: "distinct factors", value: totals.distinct_key_factors, cls: "" },
        { label: "distinct combos", value: totals.distinct_combinations, cls: "" },
        { label: "strong runs", value: totals.strong_runs, cls: "kpi-warn" },
      ];
      for (const card of cards) {
        const cardEl = document.createElement("div");
        cardEl.className = "kpi-card";
        const labelEl = document.createElement("div");
        labelEl.className = "kpi-label";
        labelEl.textContent = card.label;
        const valueEl = document.createElement("div");
        valueEl.className = "kpi-value" + (card.cls ? " " + card.cls : "");
        valueEl.textContent = fmt(card.value);
        cardEl.appendChild(labelEl);
        cardEl.appendChild(valueEl);
        wrap.appendChild(cardEl);
      }
    }

    function filteredExplorerComboRows(rows) {
      const data = Array.isArray(rows) ? rows.slice() : [];
      const q = String(byId("explorer_combo_filter_text").value || "").trim().toLowerCase();
      const sortKey = String(byId("explorer_combo_sort_key").value || "q_best_asc");
      const rowLimitRaw = Number(byId("explorer_combo_limit").value || "20");
      const rowLimit = Number.isFinite(rowLimitRaw) ? Math.max(1, Math.floor(rowLimitRaw)) : 20;

      let filtered = data;
      if (q) {
        filtered = filtered.filter((row) => {
          const keyFactor = String(row && row.key_factor || "").toLowerCase();
          const comboText =
            String(row && row.track || "").toLowerCase() + " " +
            String(row && row.context_scope || "").toLowerCase() + " " +
            String(row && row.spec_id || "").toLowerCase();
          return keyFactor.includes(q) || comboText.includes(q);
        });
      }

      const safeNum = (v, fallback) => {
        const n = Number(v);
        return Number.isFinite(n) ? n : fallback;
      };

      if (sortKey === "q_median_asc") {
        filtered.sort((a, b) =>
          safeNum(a && a.q_median, 9.99) - safeNum(b && b.q_median, 9.99) ||
          safeNum(b && b.n_runs, 0) - safeNum(a && a.n_runs, 0) ||
          String(a && a.key_factor || "").localeCompare(String(b && b.key_factor || ""))
        );
      } else if (sortKey === "n_runs_desc") {
        filtered.sort((a, b) =>
          safeNum(b && b.n_runs, 0) - safeNum(a && a.n_runs, 0) ||
          safeNum(a && a.q_best, 9.99) - safeNum(b && b.q_best, 9.99)
        );
      } else if (sortKey === "strong_share_desc") {
        filtered.sort((a, b) =>
          safeNum(b && b.strong_share_q, 0) - safeNum(a && a.strong_share_q, 0) ||
          safeNum(a && a.q_best, 9.99) - safeNum(b && b.q_best, 9.99)
        );
      } else if (sortKey === "validated_share_desc") {
        filtered.sort((a, b) =>
          safeNum(b && b.validated_share, 0) - safeNum(a && a.validated_share, 0) ||
          safeNum(a && a.q_best, 9.99) - safeNum(b && b.q_best, 9.99)
        );
      } else if (sortKey === "key_factor_asc") {
        filtered.sort((a, b) =>
          String(a && a.key_factor || "").localeCompare(String(b && b.key_factor || "")) ||
          safeNum(a && a.q_best, 9.99) - safeNum(b && b.q_best, 9.99)
        );
      } else {
        filtered.sort((a, b) =>
          safeNum(a && a.q_best, 9.99) - safeNum(b && b.q_best, 9.99) ||
          safeNum(b && b.n_runs, 0) - safeNum(a && a.n_runs, 0) ||
          String(a && a.key_factor || "").localeCompare(String(b && b.key_factor || ""))
        );
      }

      const limited = filtered.slice(0, rowLimit);
      return {
        rows: limited,
        total: data.length,
        filtered: filtered.length,
      };
    }

    function renderExplorerComboTable(rows) {
      const tbody = byId("explorer_combo_tbody");
      tbody.replaceChildren();
      const out = filteredExplorerComboRows(rows);
      const data = out.rows;
      byId("explorer_combo_notice").textContent =
        "showing " + String(data.length) + " / " + String(out.filtered) + " combinations (from " + String(out.total) + ")";
      if (!data.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 8;
        td.textContent = "no combination rows";
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
      }
      for (let i = 0; i < data.length; i += 1) {
        const row = data[i] || {};
        const tr = document.createElement("tr");
        const cells = [
          String(i + 1),
          String(row.key_factor || "-"),
          String(row.track || "-") + " / " + String(row.context_scope || "-") + " / " + String(row.spec_id || "-"),
          fmt(row.n_runs),
          fmt(row.q_best, 4),
          fmt(row.q_median, 4),
          fmt(row.strong_share_q, 3),
          fmt(row.validated_share, 3),
        ];
        for (const cell of cells) {
          const td = document.createElement("td");
          td.textContent = cell;
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function rerenderExplorerComboFromState() {
      const payload = UI_STATE.explorerPayload && typeof UI_STATE.explorerPayload === "object"
        ? UI_STATE.explorerPayload
        : {};
      renderExplorerComboTable(payload.top_combinations || []);
    }

    function resetExplorerComboControls() {
      byId("explorer_combo_filter_text").value = "";
      byId("explorer_combo_sort_key").value = "q_best_asc";
      byId("explorer_combo_limit").value = "20";
    }

    function renderExplorerFactorTable(rows) {
      const tbody = byId("explorer_factor_tbody");
      tbody.replaceChildren();
      const data = Array.isArray(rows) ? rows : [];
      if (!data.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 7;
        td.textContent = "no factor rows";
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
      }
      for (let i = 0; i < data.length; i += 1) {
        const row = data[i] || {};
        const tr = document.createElement("tr");
        const cells = [
          String(i + 1),
          String(row.key_factor || "-"),
          fmt(row.n_runs),
          fmt(row.q_best, 4),
          fmt(row.q_median, 4),
          fmt(row.strong_share_q, 3),
          fmt(row.validated_share, 3),
        ];
        for (const cell of cells) {
          const td = document.createElement("td");
          td.textContent = cell;
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function renderExplorerPairTable(rows) {
      const tbody = byId("explorer_pair_tbody");
      tbody.replaceChildren();
      const data = Array.isArray(rows) ? rows : [];
      if (!data.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 6;
        td.textContent = "no affinity pairs";
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
      }
      for (let i = 0; i < data.length; i += 1) {
        const row = data[i] || {};
        const tr = document.createElement("tr");
        const cells = [
          String(i + 1),
          String(row.key_factor_a || "-") + " ~ " + String(row.key_factor_b || "-"),
          fmt(row.co_runs),
          fmt(row.run_share, 3),
          fmt(row.jaccard, 3),
          fmt(row.lift, 3),
        ];
        for (const cell of cells) {
          const td = document.createElement("td");
          td.textContent = cell;
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function filteredExplorerClusterRows(rows) {
      const data = Array.isArray(rows) ? rows.slice() : [];
      const q = String(byId("explorer_cluster_filter_text").value || "").trim().toLowerCase();
      const minSupportRaw = Number(byId("explorer_cluster_min_support").value || "1");
      const sortKey = String(byId("explorer_cluster_sort_key").value || "support_desc");
      const rowLimitRaw = Number(byId("explorer_cluster_limit").value || "20");
      const minSupport = Number.isFinite(minSupportRaw) ? Math.max(1, Math.floor(minSupportRaw)) : 1;
      const rowLimit = Number.isFinite(rowLimitRaw) ? Math.max(1, Math.floor(rowLimitRaw)) : 20;

      let filtered = data.filter((row) => Number(row && row.run_support || 0) >= minSupport);
      if (q) {
        filtered = filtered.filter((row) => {
          const signature = String(row && row.cluster_signature || "").toLowerCase();
          const factors = Array.isArray(row && row.factors) ? row.factors : [];
          return signature.includes(q) || factors.some((item) => String(item || "").toLowerCase().includes(q));
        });
      }

      if (sortKey === "factors_desc") {
        filtered.sort((a, b) => {
          const fa = Number(a && a.n_factors || 0);
          const fb = Number(b && b.n_factors || 0);
          if (fb !== fa) return fb - fa;
          const sa = Number(a && a.run_support || 0);
          const sb = Number(b && b.run_support || 0);
          if (sb !== sa) return sb - sa;
          return String(a && a.cluster_signature || "").localeCompare(String(b && b.cluster_signature || ""));
        });
      } else if (sortKey === "signature_asc") {
        filtered.sort((a, b) => String(a && a.cluster_signature || "").localeCompare(String(b && b.cluster_signature || "")));
      } else {
        filtered.sort((a, b) => {
          const sa = Number(a && a.run_support || 0);
          const sb = Number(b && b.run_support || 0);
          if (sb !== sa) return sb - sa;
          const fa = Number(a && a.n_factors || 0);
          const fb = Number(b && b.n_factors || 0);
          if (fb !== fa) return fb - fa;
          return String(a && a.cluster_signature || "").localeCompare(String(b && b.cluster_signature || ""));
        });
      }

      const limited = filtered.slice(0, rowLimit);
      return {
        rows: limited,
        total: data.length,
        filtered: filtered.length,
      };
    }

    function renderExplorerClusterTable(rows) {
      const tbody = byId("explorer_cluster_tbody");
      tbody.replaceChildren();
      const out = filteredExplorerClusterRows(rows);
      const data = out.rows;
      byId("explorer_cluster_notice").textContent =
        "showing " + String(data.length) + " / " + String(out.filtered) + " clusters (from " + String(out.total) + ")";
      if (!data.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 4;
        td.textContent = "no high co-occurrence groups";
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
      }
      for (const row of data) {
        const tr = document.createElement("tr");
        const factors = Array.isArray(row.factors) ? row.factors : [];
        const cells = [
          String(row.cluster_signature || "-"),
          fmt(row.n_factors),
          fmt(row.run_support),
          factors.join(", "),
        ];
        for (const cell of cells) {
          const td = document.createElement("td");
          td.textContent = cell;
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function rerenderExplorerClusterFromState() {
      const payload = UI_STATE.explorerPayload && typeof UI_STATE.explorerPayload === "object"
        ? UI_STATE.explorerPayload
        : {};
      renderExplorerClusterTable(payload.factor_clusters || []);
    }

    function resetExplorerClusterControls() {
      byId("explorer_cluster_filter_text").value = "";
      byId("explorer_cluster_min_support").value = "1";
      byId("explorer_cluster_sort_key").value = "support_desc";
      byId("explorer_cluster_limit").value = "20";
    }

    function explorerTopFactors(topK) {
      const payload = UI_STATE.explorerPayload && typeof UI_STATE.explorerPayload === "object"
        ? UI_STATE.explorerPayload
        : {};
      const rows = Array.isArray(payload.top_key_factors) ? payload.top_key_factors : [];
      const n = Number.isFinite(Number(topK)) ? Math.max(1, Math.floor(Number(topK))) : 5;
      const out = [];
      const seen = new Set();
      for (const row of rows) {
        const fac = String(row && row.key_factor || "").trim();
        if (!fac || seen.has(fac)) continue;
        seen.add(fac);
        out.push(fac);
        if (out.length >= n) break;
      }
      return out;
    }

    function applyExplorerTopFactorsToEquationInput() {
      const topK = Number(byId("eq_top_k").value || "5");
      const factors = explorerTopFactors(topK);
      if (!factors.length) {
        setNotice(byId("eq_notice"), "error", "no explorer top factors available; refresh Explorer first");
        return false;
      }
      byId("eq_factor_list").value = factors.join("\\n");
      setNotice(byId("eq_notice"), "ok", "loaded " + String(factors.length) + " factors from Explorer top list");
      return true;
    }

    function parseEquationFactorsInput() {
      const raw = String(byId("eq_factor_list").value || "").trim();
      if (!raw) return [];
      const seen = new Set();
      const out = [];
      const chunks = raw.split(/\\n+/);
      for (const chunk of chunks) {
        const parts = String(chunk || "").split(",");
        for (const part of parts) {
          const item = String(part || "").trim();
          if (!item || seen.has(item)) continue;
          seen.add(item);
          out.push(item);
        }
      }
      return out;
    }

    function renderEquationMeta(payload) {
      const tbody = byId("eq_meta_tbody");
      tbody.replaceChildren();
      const summary = payload && payload.summary && typeof payload.summary === "object" ? payload.summary : {};
      const settings = payload && payload.settings && typeof payload.settings === "object" ? payload.settings : {};
      const missing = Array.isArray(payload && payload.missing_factors) ? payload.missing_factors : [];
      const used = Array.isArray(payload && payload.factors_used) ? payload.factors_used : [];
      appendOverviewRow(tbody, "run_id", payload && payload.run_id ? payload.run_id : "-");
      appendOverviewRow(tbody, "track", payload && payload.track ? payload.track : "-");
      appendOverviewRow(tbody, "split_role", payload && payload.split_role ? payload.split_role : "-");
      appendOverviewRow(tbody, "y_col", payload && payload.y_col ? payload.y_col : "-");
      appendOverviewRow(tbody, "factors used", fmt(used.length));
      appendOverviewRow(tbody, "missing factors", missing.length ? missing.join(", ") : "-");
      appendOverviewRow(tbody, "best step", fmt(summary.best_step));
      appendOverviewRow(tbody, "best factor", summary.best_added_factor || "-");
      appendOverviewRow(tbody, "best accuracy mean", fmt(summary.best_accuracy_mean, 3));
      appendOverviewRow(tbody, "final accuracy mean", fmt(summary.final_accuracy_mean, 3));
      appendOverviewRow(tbody, "contrib total (signed)", fmt(summary.contribution_accuracy_total_signed, 4));
      appendOverviewRow(tbody, "contrib total (abs)", fmt(summary.contribution_accuracy_total_abs, 4));
      appendOverviewRow(tbody, "top positive factor", summary.top_positive_contributor || "-");
      appendOverviewRow(tbody, "top negative factor", summary.top_negative_contributor || "-");
      appendOverviewRow(tbody, "atom groups", fmt(summary.n_contribution_groups));
      appendOverviewRow(tbody, "bootstrap", fmt(settings.n_bootstrap));
      appendOverviewRow(tbody, "split seed/ratio", fmt(settings.split_seed) + " / " + fmt(settings.split_ratio, 3));
    }

    function renderEquationStepTable(rows) {
      const tbody = byId("eq_step_tbody");
      tbody.replaceChildren();
      const data = Array.isArray(rows) ? rows : [];
      if (!data.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 12;
        td.textContent = "no equation steps";
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
      }

      for (const row of data) {
        const tr = document.createElement("tr");
        const stepLabel = Boolean(row && row.is_baseline)
          ? "0 (baseline)"
          : String(row && row.step !== undefined ? row.step : "-");
        const factorLabel = String(row && row.added_factor || "-");
        const equation = String(row && row.equation || "-");
        const eventsText = fmt(row && row.n_events);
        const accMean = fmt(row && row.accuracy_mean, 3);
        const accCi = "[" + fmt(row && row.accuracy_ci_low, 3) + ", " + fmt(row && row.accuracy_ci_high, 3) + "]";
        const dAcc = fmt(row && row.delta_accuracy_mean, 3);
        const dAccCi = "[" + fmt(row && row.delta_accuracy_ci_low, 3) + ", " + fmt(row && row.delta_accuracy_ci_high, 3) + "]";
        const llfPer = fmt(row && row.llf_per_event_mean, 4);
        const atoms = Array.isArray(row && row.contribution_atoms)
          ? row.contribution_atoms.map((x) => String(x || "").trim()).filter((x) => x).join(", ")
          : "-";
        const shareAbs = fmt(row && row.contribution_accuracy_share_abs, 3);
        const bs = fmt(row && row.bootstrap_success) + " / " + fmt(row && row.bootstrap_attempted);
        const cells = [stepLabel, factorLabel, atoms || "-", equation, eventsText, accMean, accCi, dAcc, dAccCi, shareAbs, llfPer, bs];
        for (const cell of cells) {
          const td = document.createElement("td");
          td.textContent = cell;
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function renderEquationContributionGroups(payload) {
      const tbody = byId("eq_group_tbody");
      const notice = byId("eq_group_notice");
      tbody.replaceChildren();
      const rows = Array.isArray(payload && payload.contribution_groups)
        ? payload.contribution_groups
        : [];
      if (!rows.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 6;
        td.textContent = "no atom contribution groups";
        tr.appendChild(td);
        tbody.appendChild(tr);
        notice.textContent = "group coverage: 0 atoms";
        return;
      }
      let rank = 1;
      for (const row of rows) {
        const tr = document.createElement("tr");
        const factors = Array.isArray(row && row.factors)
          ? row.factors.map((x) => String(x || "").trim()).filter((x) => x).join(", ")
          : "-";
        const cells = [
          String(rank),
          String(row && row.feature_atom || "-"),
          fmt(row && row.contribution_accuracy_sum, 4),
          fmt(row && row.contribution_accuracy_share_abs, 3),
          fmt(row && row.n_steps),
          factors || "-",
        ];
        for (const cell of cells) {
          const td = document.createElement("td");
          td.textContent = cell;
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
        rank += 1;
      }
      notice.textContent = "group coverage: " + String(rows.length) + " atoms";
    }

    function renderEquationCurve(rows) {
      const wrap = byId("eq_curve_chart");
      const hint = byId("eq_curve_hint");
      wrap.replaceChildren();
      const data = Array.isArray(rows) ? rows : [];
      const points = data
        .map((row) => ({
          step: Number(row && row.step),
          is_baseline: Boolean(row && row.is_baseline),
          factor: String(row && row.added_factor || ""),
          mean: Number(row && row.accuracy_mean),
          lo: Number(row && row.accuracy_ci_low),
          hi: Number(row && row.accuracy_ci_high),
          delta: Number(row && row.delta_accuracy_mean),
        }))
        .filter((pt) => Number.isFinite(pt.step) && Number.isFinite(pt.mean));
      if (!points.length) {
        const empty = document.createElement("div");
        empty.className = "equation-chart-empty";
        empty.textContent = "(no stepwise accuracy points)";
        wrap.appendChild(empty);
        hint.textContent = "no numeric step metrics available";
        return;
      }

      const width = Math.max(360, wrap.clientWidth || 640);
      const height = 280;
      const margin = { top: 14, right: 16, bottom: 40, left: 48 };
      const plotW = Math.max(120, width - margin.left - margin.right);
      const plotH = Math.max(100, height - margin.top - margin.bottom);
      const minYRaw = Math.min(...points.map((pt) => Number.isFinite(pt.lo) ? pt.lo : pt.mean));
      const maxYRaw = Math.max(...points.map((pt) => Number.isFinite(pt.hi) ? pt.hi : pt.mean));
      const yMin = Math.max(0, minYRaw - 0.03);
      const yMax = Math.min(1, Math.max(yMin + 0.05, maxYRaw + 0.03));
      const xMax = Math.max(...points.map((pt) => pt.step), 1);

      const ns = "http://www.w3.org/2000/svg";
      const svg = document.createElementNS(ns, "svg");
      svg.setAttribute("viewBox", "0 0 " + String(width) + " " + String(height));
      const mk = (tag, attrs) => {
        const el = document.createElementNS(ns, tag);
        for (const key in attrs) el.setAttribute(key, String(attrs[key]));
        return el;
      };
      const xPos = (x) => margin.left + ((x / Math.max(1, xMax)) * plotW);
      const yPos = (y) => margin.top + plotH - (((y - yMin) / Math.max(1e-9, yMax - yMin)) * plotH);

      svg.appendChild(mk("rect", {
        x: margin.left,
        y: margin.top,
        width: plotW,
        height: plotH,
        fill: "#f8fbff",
        stroke: "#d8e6f5",
        "stroke-width": 1,
      }));

      for (let i = 0; i <= 4; i += 1) {
        const t = i / 4;
        const yVal = yMin + (yMax - yMin) * t;
        const y = yPos(yVal);
        svg.appendChild(mk("line", {
          x1: margin.left,
          y1: y,
          x2: margin.left + plotW,
          y2: y,
          stroke: "#edf3fb",
          "stroke-width": 1,
        }));
        const lbl = mk("text", {
          x: margin.left - 6,
          y: y + 3,
          "text-anchor": "end",
          fill: "#5f6f82",
          "font-size": 10,
        });
        lbl.textContent = fmt(yVal, 2);
        svg.appendChild(lbl);
      }

      const polyPts = points.map((pt) => String(xPos(pt.step)) + "," + String(yPos(pt.mean))).join(" ");
      svg.appendChild(mk("polyline", {
        points: polyPts,
        fill: "none",
        stroke: "#2f6bc2",
        "stroke-width": 1.8,
      }));

      for (const pt of points) {
        const x = xPos(pt.step);
        const y = yPos(pt.mean);
        const lo = Number.isFinite(pt.lo) ? yPos(pt.lo) : y;
        const hi = Number.isFinite(pt.hi) ? yPos(pt.hi) : y;
        svg.appendChild(mk("line", {
          x1: x,
          y1: hi,
          x2: x,
          y2: lo,
          stroke: "#8ca8cb",
          "stroke-width": 1.2,
        }));
        svg.appendChild(mk("line", {
          x1: x - 4,
          y1: hi,
          x2: x + 4,
          y2: hi,
          stroke: "#8ca8cb",
          "stroke-width": 1.2,
        }));
        svg.appendChild(mk("line", {
          x1: x - 4,
          y1: lo,
          x2: x + 4,
          y2: lo,
          stroke: "#8ca8cb",
          "stroke-width": 1.2,
        }));
        const dot = mk("circle", {
          cx: x,
          cy: y,
          r: 3.6,
          fill: pt.is_baseline ? "#5f6f82" : "#2f6bc2",
          stroke: "#ffffff",
          "stroke-width": 0.9,
        });
        const title = document.createElementNS(ns, "title");
        title.textContent =
          "step=" + String(pt.step) +
          " | factor=" + (pt.factor || "(baseline)") +
          " | acc=" + fmt(pt.mean, 3) +
          " | 95%CI=[" + fmt(pt.lo, 3) + ", " + fmt(pt.hi, 3) + "]" +
          " | delta=" + fmt(pt.delta, 3);
        dot.appendChild(title);
        svg.appendChild(dot);

        const xl = mk("text", {
          x: x,
          y: margin.top + plotH + 14,
          "text-anchor": "middle",
          fill: "#5f6f82",
          "font-size": 10,
        });
        xl.textContent = String(pt.step);
        svg.appendChild(xl);
      }

      svg.appendChild(mk("line", {
        x1: margin.left,
        y1: margin.top + plotH,
        x2: margin.left + plotW,
        y2: margin.top + plotH,
        stroke: "#9fb4cc",
        "stroke-width": 1.2,
      }));
      svg.appendChild(mk("line", {
        x1: margin.left,
        y1: margin.top,
        x2: margin.left,
        y2: margin.top + plotH,
        stroke: "#9fb4cc",
        "stroke-width": 1.2,
      }));

      const xLabel = mk("text", {
        x: margin.left + plotW / 2,
        y: height - 8,
        "text-anchor": "middle",
        fill: "#384a61",
        "font-size": 11,
      });
      xLabel.textContent = "step (added factors)";
      svg.appendChild(xLabel);

      const yLabel = mk("text", {
        x: 14,
        y: margin.top + plotH / 2,
        transform: "rotate(-90 14 " + String(margin.top + plotH / 2) + ")",
        "text-anchor": "middle",
        fill: "#384a61",
        "font-size": 11,
      });
      yLabel.textContent = "event accuracy";
      svg.appendChild(yLabel);

      wrap.appendChild(svg);
      const finalPt = points[points.length - 1];
      hint.textContent =
        "step count=" + String(points.length) +
        " | final acc=" + fmt(finalPt.mean, 3) +
        " | y-axis range=[" + fmt(yMin, 2) + ", " + fmt(yMax, 2) + "]";
    }

    async function buildEquationPath() {
      if (UI_STATE.equationBusy) return;
      const notice = byId("eq_notice");
      try {
        let runId = String(byId("eq_run_id").value || "").trim();
        if (!runId) {
          runId = String(byId("detail_run_id").value || "").trim();
          if (!runId) {
            const topRuns = UI_STATE.explorerPayload && Array.isArray(UI_STATE.explorerPayload.top_runs)
              ? UI_STATE.explorerPayload.top_runs
              : [];
            if (topRuns.length) runId = String(topRuns[0].run_id || "").trim();
          }
          if (runId) byId("eq_run_id").value = runId;
        }
        if (!runId) throw new Error("run_id is required (inspect run or type run_id)");
        runId = validateRunId(runId);

        let factors = parseEquationFactorsInput();
        if (!factors.length) {
          const ok = applyExplorerTopFactorsToEquationInput();
          if (!ok) throw new Error("factor list is empty");
          factors = parseEquationFactorsInput();
        }
        if (!factors.length) throw new Error("factor list is empty");

        const payload = {
          run_id: runId,
          track: String(byId("eq_track").value || "primary_strict"),
          y_col: String(byId("eq_y_col").value || "y_all"),
          split_role: String(byId("eq_split_role").value || "validation"),
          factors: factors,
          n_bootstrap: Number(byId("eq_bootstrap_n").value || "49"),
          max_steps: Number(byId("eq_max_steps").value || "5"),
          include_base_controls: Boolean(byId("eq_include_base_controls").checked),
          include_baseline: Boolean(byId("eq_include_baseline").checked),
        };

        setEquationBusy(true);
        setNotice(notice, "", "building stepwise equation curve...");
        const out = await fetchJson("/explorer/equation-path", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        UI_STATE.equationPayload = out;
        renderEquationMeta(out);
        renderEquationCurve(out.steps || []);
        renderEquationStepTable(out.steps || []);
        renderEquationContributionGroups(out);
        const summary = out && out.summary ? out.summary : {};
        setNotice(
          notice,
          "ok",
          "built: steps=" + String(summary.n_steps || 0) +
            ", best_step=" + String(summary.best_step || "-") +
            ", best_acc=" + fmt(summary.best_accuracy_mean, 3)
        );
      } catch (err) {
        setNotice(notice, "error", String(err && err.message ? err.message : err));
      } finally {
        setEquationBusy(false);
      }
    }

    function ensureExplorerJointSelection() {
      const raw = UI_STATE.explorerJointSelection && typeof UI_STATE.explorerJointSelection === "object"
        ? UI_STATE.explorerJointSelection
        : {};
      const qRaw = Number(raw.q_idx);
      const pRaw = Number(raw.p_idx);
      UI_STATE.explorerJointSelection = {
        q_idx: (Number.isFinite(qRaw) && qRaw >= 0) ? Math.floor(qRaw) : -1,
        p_idx: (Number.isFinite(pRaw) && pRaw >= 0) ? Math.floor(pRaw) : -1,
      };
      return UI_STATE.explorerJointSelection;
    }

    function resetExplorerJointSelection() {
      UI_STATE.explorerJointSelection = { q_idx: -1, p_idx: -1 };
    }

    function explorerJointBinIndex(value, axisMax, nBins) {
      const v = Number(value);
      if (!Number.isFinite(v)) return -1;
      const n = Math.max(1, Math.floor(Number(nBins) || 1));
      const maxV = Number.isFinite(axisMax) && axisMax > 0 ? axisMax : 1.0;
      const clamped = Math.max(0, Math.min(maxV, v));
      if (clamped >= maxV) return n - 1;
      const idx = Math.floor((clamped / maxV) * n);
      return Math.max(0, Math.min(n - 1, idx));
    }

    function explorerJointRangeLabel(bin) {
      const b = bin && typeof bin === "object" ? bin : {};
      return fmt(Number(b.start || 0), 3) + "-" + fmt(Number(b.end || 0), 3);
    }

    function buildExplorerJointData(payload, qGate) {
      const raw = Array.isArray(payload && payload.run_scatter_points) && payload.run_scatter_points.length
        ? payload.run_scatter_points
        : (Array.isArray(payload && payload.top_runs) ? payload.top_runs : []);
      const points = [];
      for (const row of raw) {
        const qRaw = Number(row && row.best_q_validation);
        const pRaw = Number(row && row.best_p_validation);
        if (!Number.isFinite(qRaw) || !Number.isFinite(pRaw)) continue;
        points.push({
          run_id: String(row && row.run_id || "-"),
          mode: String(row && row.mode || "-"),
          q: Math.max(0, qRaw),
          p: Math.max(0, pRaw),
          validated: Number(row && row.validated_candidate_count || 0),
          q_idx: -1,
          p_idx: -1,
        });
      }

      const maxQObs = points.length ? Math.max(1e-9, ...points.map((pt) => pt.q)) : 1e-9;
      const maxPObs = points.length ? Math.max(1e-9, ...points.map((pt) => pt.p)) : 1e-9;
      const qGateSafe = Number.isFinite(qGate) ? Math.max(0, Math.min(1, qGate)) : 0.10;
      const pGate = 0.05;
      const xMax = Math.max(0.10, Math.min(1.0, Math.max(maxQObs * 1.05, qGateSafe * 1.2)));
      const yMax = Math.max(0.10, Math.min(1.0, Math.max(maxPObs * 1.05, pGate * 1.2)));

      const qBinCount = 10;
      const pBinCount = 10;
      const qBins = Array.from({ length: qBinCount }, (_, idx) => {
        const start = (xMax * idx) / qBinCount;
        const end = (xMax * (idx + 1)) / qBinCount;
        return { idx: idx, start: start, end: end, count: 0 };
      });
      const pBins = Array.from({ length: pBinCount }, (_, idx) => {
        const start = (yMax * idx) / pBinCount;
        const end = (yMax * (idx + 1)) / pBinCount;
        return { idx: idx, start: start, end: end, count: 0 };
      });

      for (const pt of points) {
        const qIdx = explorerJointBinIndex(pt.q, xMax, qBinCount);
        const pIdx = explorerJointBinIndex(pt.p, yMax, pBinCount);
        pt.q_idx = qIdx;
        pt.p_idx = pIdx;
        if (qIdx >= 0 && qIdx < qBins.length) qBins[qIdx].count += 1;
        if (pIdx >= 0 && pIdx < pBins.length) pBins[pIdx].count += 1;
      }

      return {
        points: points,
        q_bins: qBins,
        p_bins: pBins,
        x_max: xMax,
        y_max: yMax,
        q_gate: qGateSafe,
        p_gate: pGate,
      };
    }

    function pointMatchesExplorerJointSelection(point, selection) {
      const qSel = Number(selection && selection.q_idx);
      const pSel = Number(selection && selection.p_idx);
      if (Number.isFinite(qSel) && qSel >= 0 && Number(point && point.q_idx) !== Math.floor(qSel)) return false;
      if (Number.isFinite(pSel) && pSel >= 0 && Number(point && point.p_idx) !== Math.floor(pSel)) return false;
      return true;
    }

    function computeExplorerJointLayout() {
      const scatterWrap = byId("explorer_best_qp_scatter");
      const qWrap = byId("explorer_best_q_marginal");
      const pWrap = byId("explorer_best_p_marginal");
      const rectW = (el, fallback) => {
        if (!el) return fallback;
        const r = el.getBoundingClientRect ? el.getBoundingClientRect() : null;
        const w = r && Number.isFinite(r.width) ? r.width : (el.clientWidth || 0);
        return Math.max(0, Math.floor(w || fallback));
      };
      const scatterSide = Math.max(240, rectW(scatterWrap, 420));
      const scatter = {
        width: scatterSide,
        height: scatterSide,
        margin: { top: 14, right: 18, bottom: 40, left: 50 },
      };
      scatter.plot_w = Math.max(120, scatter.width - scatter.margin.left - scatter.margin.right);
      scatter.plot_h = Math.max(120, scatter.height - scatter.margin.top - scatter.margin.bottom);

      const top = {
        width: scatter.width,
        height: 132,
        margin: { top: 8, right: scatter.margin.right, bottom: 24, left: scatter.margin.left },
      };
      top.plot_w = Math.max(120, top.width - top.margin.left - top.margin.right);
      top.plot_h = Math.max(50, top.height - top.margin.top - top.margin.bottom);

      const sideWidth = Math.max(180, rectW(pWrap, 240));
      const side = {
        width: sideWidth,
        height: scatter.height,
        margin: { top: scatter.margin.top, right: 10, bottom: scatter.margin.bottom, left: 66 },
      };
      side.plot_w = Math.max(80, side.width - side.margin.left - side.margin.right);
      side.plot_h = Math.max(120, side.height - side.margin.top - side.margin.bottom);

      scatterWrap.style.height = String(scatter.height) + "px";
      qWrap.style.height = String(top.height) + "px";
      pWrap.style.height = String(side.height) + "px";
      return { scatter: scatter, top: top, side: side };
    }

    function explorerJointX(layout, data, value) {
      const v = Number(value);
      const xMax = Number(data && data.x_max || 1.0);
      const clamped = Number.isFinite(v) ? Math.max(0, Math.min(xMax, v)) : 0;
      return layout.scatter.margin.left + (clamped / xMax) * layout.scatter.plot_w;
    }

    function explorerJointY(layout, data, value) {
      const v = Number(value);
      const yMax = Number(data && data.y_max || 1.0);
      const clamped = Number.isFinite(v) ? Math.max(0, Math.min(yMax, v)) : 0;
      return layout.scatter.margin.top + layout.scatter.plot_h - (clamped / yMax) * layout.scatter.plot_h;
    }

    function renderExplorerJointKpis(data, selection, qGate) {
      const wrap = byId("explorer_best_qp_joint_kpis");
      wrap.replaceChildren();
      const points = Array.isArray(data && data.points) ? data.points : [];
      const shown = points.filter((pt) => pointMatchesExplorerJointSelection(pt, selection));
      const qPass = shown.filter((pt) => pt.q <= qGate).length;
      const pPass = shown.filter((pt) => pt.p <= 0.05).length;
      const bothPass = shown.filter((pt) => pt.q <= qGate && pt.p <= 0.05).length;
      const qSelLabel = (selection.q_idx >= 0 && data.q_bins[selection.q_idx]) ? explorerJointRangeLabel(data.q_bins[selection.q_idx]) : "all";
      const pSelLabel = (selection.p_idx >= 0 && data.p_bins[selection.p_idx]) ? explorerJointRangeLabel(data.p_bins[selection.p_idx]) : "all";
      const cards = [
        { label: "visible/total", value: String(shown.length) + " / " + String(points.length) },
        { label: "q<=" + fmt(qGate, 2), value: String(qPass) },
        { label: "p<=0.05", value: String(pPass) },
        { label: "both pass", value: String(bothPass) },
        { label: "q filter", value: qSelLabel },
        { label: "p filter", value: pSelLabel },
      ];
      for (const card of cards) {
        const el = document.createElement("div");
        el.className = "joint-kpi-item";
        const l = document.createElement("div");
        l.className = "joint-kpi-label";
        l.textContent = card.label;
        const v = document.createElement("div");
        v.className = "joint-kpi-value";
        v.textContent = card.value;
        el.appendChild(l);
        el.appendChild(v);
        wrap.appendChild(el);
      }
    }

    function renderExplorerQMarginal(data, selection, layout) {
      const wrap = byId("explorer_best_q_marginal");
      wrap.replaceChildren();
      const bins = Array.isArray(data && data.q_bins) ? data.q_bins : [];
      const points = Array.isArray(data && data.points) ? data.points : [];
      if (!bins.length) {
        const empty = document.createElement("div");
        empty.className = "scatter-empty";
        empty.textContent = "(no q distribution)";
        wrap.appendChild(empty);
        return;
      }

      const selectedCounts = new Array(bins.length).fill(0);
      for (const pt of points) {
        if (selection.p_idx >= 0 && Number(pt && pt.p_idx) !== Number(selection.p_idx)) continue;
        const qIdx = Number(pt && pt.q_idx);
        if (Number.isFinite(qIdx) && qIdx >= 0 && qIdx < selectedCounts.length) {
          selectedCounts[qIdx] += 1;
        }
      }
      const maxCount = Math.max(1, ...bins.map((bin) => Number(bin.count || 0)));
      const width = layout.top.width;
      const height = layout.top.height;
      const ns = "http://www.w3.org/2000/svg";
      const svg = document.createElementNS(ns, "svg");
      svg.setAttribute("viewBox", "0 0 " + String(width) + " " + String(height));
      const mk = (tag, attrs) => {
        const el = document.createElementNS(ns, tag);
        for (const key in attrs) el.setAttribute(key, String(attrs[key]));
        return el;
      };

      svg.appendChild(mk("rect", { x: layout.top.margin.left, y: layout.top.margin.top, width: layout.top.plot_w, height: layout.top.plot_h, fill: "#f8fbff", stroke: "#d8e6f5", "stroke-width": 1 }));
      for (const bin of bins) {
        const x0 = explorerJointX(layout, data, bin.start);
        const x1 = explorerJointX(layout, data, bin.end);
        const barW = Math.max(2, x1 - x0 - 1);
        const full = Number(bin.count || 0);
        const selected = Number(selectedCounts[bin.idx] || 0);
        const fullH = (full / maxCount) * layout.top.plot_h;
        const selH = (selected / maxCount) * layout.top.plot_h;
        const active = Number(selection.q_idx) === Number(bin.idx);

        const base = mk("rect", {
          x: x0,
          y: layout.top.margin.top + layout.top.plot_h - fullH,
          width: barW,
          height: fullH,
          fill: "#d8e8fb",
          stroke: active ? "#102542" : "#9fb4cc",
          "stroke-width": active ? 1.2 : 0.6,
          class: "marginal-bar" + (active ? " marginal-bar-active" : ""),
        });
        base.appendChild(document.createElementNS(ns, "title"));
        base.firstChild.textContent = explorerJointRangeLabel(bin) + ": total=" + String(full) + ", linked=" + String(selected);
        base.addEventListener("click", () => {
          const next = ensureExplorerJointSelection();
          next.q_idx = Number(next.q_idx) === Number(bin.idx) ? -1 : Number(bin.idx);
          rerenderExplorerDistributionsFromState();
        });
        svg.appendChild(base);

        const overlay = mk("rect", {
          x: x0,
          y: layout.top.margin.top + layout.top.plot_h - selH,
          width: barW,
          height: selH,
          fill: active ? "#2f6bc2" : "#5c90d1",
          "fill-opacity": 0.85,
          "pointer-events": "none",
        });
        svg.appendChild(overlay);

        if (bin.idx % 2 === 1 || bin.idx === bins.length - 1) {
          const label = mk("text", {
            x: x1,
            y: layout.top.margin.top + layout.top.plot_h + 12,
            "text-anchor": "end",
            fill: "#5f6f82",
            "font-size": 9,
          });
          label.textContent = fmt(bin.end, 2);
          svg.appendChild(label);
        }
      }
      if (data.q_gate <= data.x_max) {
        const qLine = explorerJointX(layout, data, data.q_gate);
        svg.appendChild(mk("line", {
          x1: qLine,
          y1: layout.top.margin.top,
          x2: qLine,
          y2: layout.top.margin.top + layout.top.plot_h,
          stroke: "#ff7b00",
          "stroke-width": 1.2,
          "stroke-dasharray": "4 3",
        }));
      }
      wrap.appendChild(svg);
    }

    function renderExplorerPMarginal(data, selection, layout) {
      const wrap = byId("explorer_best_p_marginal");
      wrap.replaceChildren();
      const bins = Array.isArray(data && data.p_bins) ? data.p_bins : [];
      const points = Array.isArray(data && data.points) ? data.points : [];
      if (!bins.length) {
        const empty = document.createElement("div");
        empty.className = "scatter-empty";
        empty.textContent = "(no p distribution)";
        wrap.appendChild(empty);
        return;
      }

      const selectedCounts = new Array(bins.length).fill(0);
      for (const pt of points) {
        if (selection.q_idx >= 0 && Number(pt && pt.q_idx) !== Number(selection.q_idx)) continue;
        const pIdx = Number(pt && pt.p_idx);
        if (Number.isFinite(pIdx) && pIdx >= 0 && pIdx < selectedCounts.length) {
          selectedCounts[pIdx] += 1;
        }
      }
      const maxCount = Math.max(1, ...bins.map((bin) => Number(bin.count || 0)));
      const width = layout.side.width;
      const height = layout.side.height;
      const ns = "http://www.w3.org/2000/svg";
      const svg = document.createElementNS(ns, "svg");
      svg.setAttribute("viewBox", "0 0 " + String(width) + " " + String(height));
      const mk = (tag, attrs) => {
        const el = document.createElementNS(ns, tag);
        for (const key in attrs) el.setAttribute(key, String(attrs[key]));
        return el;
      };

      svg.appendChild(mk("rect", { x: layout.side.margin.left, y: layout.side.margin.top, width: layout.side.plot_w, height: layout.side.plot_h, fill: "#f8fbff", stroke: "#d8e6f5", "stroke-width": 1 }));
      for (const bin of bins) {
        const yTop = explorerJointY(layout, data, bin.end);
        const yBottom = explorerJointY(layout, data, bin.start);
        const barH = Math.max(2, yBottom - yTop - 1);
        const full = Number(bin.count || 0);
        const selected = Number(selectedCounts[bin.idx] || 0);
        const fullW = (full / maxCount) * layout.side.plot_w;
        const selW = (selected / maxCount) * layout.side.plot_w;
        const active = Number(selection.p_idx) === Number(bin.idx);

        const base = mk("rect", {
          x: layout.side.margin.left,
          y: yTop,
          width: fullW,
          height: barH,
          fill: "#d8e8fb",
          stroke: active ? "#102542" : "#9fb4cc",
          "stroke-width": active ? 1.2 : 0.6,
          class: "marginal-bar" + (active ? " marginal-bar-active" : ""),
        });
        base.appendChild(document.createElementNS(ns, "title"));
        base.firstChild.textContent = explorerJointRangeLabel(bin) + ": total=" + String(full) + ", linked=" + String(selected);
        base.addEventListener("click", () => {
          const next = ensureExplorerJointSelection();
          next.p_idx = Number(next.p_idx) === Number(bin.idx) ? -1 : Number(bin.idx);
          rerenderExplorerDistributionsFromState();
        });
        svg.appendChild(base);

        const overlay = mk("rect", {
          x: layout.side.margin.left,
          y: yTop,
          width: selW,
          height: barH,
          fill: active ? "#2f6bc2" : "#5c90d1",
          "fill-opacity": 0.85,
          "pointer-events": "none",
        });
        svg.appendChild(overlay);

        if (bin.idx % 2 === 1 || bin.idx === bins.length - 1) {
          const label = mk("text", {
            x: layout.side.margin.left - 4,
            y: yTop + barH / 2 + 3,
            "text-anchor": "end",
            fill: "#5f6f82",
            "font-size": 9,
          });
          label.textContent = fmt(bin.end, 2);
          svg.appendChild(label);
        }
      }
      if (data.p_gate <= data.y_max) {
        const pLine = explorerJointY(layout, data, data.p_gate);
        svg.appendChild(mk("line", {
          x1: layout.side.margin.left,
          y1: pLine,
          x2: layout.side.margin.left + layout.side.plot_w,
          y2: pLine,
          stroke: "#5f6f82",
          "stroke-width": 1.2,
          "stroke-dasharray": "4 3",
        }));
      }
      wrap.appendChild(svg);
    }

    function renderExplorerBestQpScatter(data, selection, layout) {
      const wrap = byId("explorer_best_qp_scatter");
      const meta = byId("explorer_best_qp_meta");
      const hover = byId("explorer_best_qp_hover");
      wrap.replaceChildren();
      hover.textContent = "hover a point for metrics; click to load Run Details";

      const points = Array.isArray(data && data.points) ? data.points : [];
      if (!points.length) {
        const empty = document.createElement("div");
        empty.className = "scatter-empty";
        empty.textContent = "(no run-level points with both best q and best p)";
        wrap.appendChild(empty);
        meta.textContent = "points: 0";
        hover.textContent = "no points available for hover";
        return;
      }

      const width = layout.scatter.width;
      const height = layout.scatter.height;
      const ns = "http://www.w3.org/2000/svg";
      const svg = document.createElementNS(ns, "svg");
      svg.setAttribute("viewBox", "0 0 " + String(width) + " " + String(height));
      svg.setAttribute("aria-label", "best q vs best p scatter");
      const mk = (tag, attrs) => {
        const el = document.createElementNS(ns, tag);
        for (const key in attrs) el.setAttribute(key, String(attrs[key]));
        return el;
      };

      svg.appendChild(mk("rect", { x: layout.scatter.margin.left, y: layout.scatter.margin.top, width: layout.scatter.plot_w, height: layout.scatter.plot_h, fill: "#f8fbff", stroke: "#d8e6f5", "stroke-width": 1 }));
      const tickSteps = 4;
      for (let i = 0; i <= tickSteps; i += 1) {
        const qTick = (data.x_max * i) / tickSteps;
        const pTick = (data.y_max * i) / tickSteps;
        const x = explorerJointX(layout, data, qTick);
        const y = explorerJointY(layout, data, pTick);
        svg.appendChild(mk("line", { x1: x, y1: layout.scatter.margin.top, x2: x, y2: layout.scatter.margin.top + layout.scatter.plot_h, stroke: "#edf3fb", "stroke-width": 1 }));
        svg.appendChild(mk("line", { x1: layout.scatter.margin.left, y1: y, x2: layout.scatter.margin.left + layout.scatter.plot_w, y2: y, stroke: "#edf3fb", "stroke-width": 1 }));
        const qText = mk("text", { x: x, y: layout.scatter.margin.top + layout.scatter.plot_h + 14, "text-anchor": "middle", fill: "#5f6f82", "font-size": 10 });
        qText.textContent = fmt(qTick, 2);
        svg.appendChild(qText);
        const pText = mk("text", { x: layout.scatter.margin.left - 6, y: y + 3, "text-anchor": "end", fill: "#5f6f82", "font-size": 10 });
        pText.textContent = fmt(pTick, 2);
        svg.appendChild(pText);
      }
      svg.appendChild(mk("line", { x1: layout.scatter.margin.left, y1: layout.scatter.margin.top + layout.scatter.plot_h, x2: layout.scatter.margin.left + layout.scatter.plot_w, y2: layout.scatter.margin.top + layout.scatter.plot_h, stroke: "#9fb4cc", "stroke-width": 1.2 }));
      svg.appendChild(mk("line", { x1: layout.scatter.margin.left, y1: layout.scatter.margin.top, x2: layout.scatter.margin.left, y2: layout.scatter.margin.top + layout.scatter.plot_h, stroke: "#9fb4cc", "stroke-width": 1.2 }));

      if (data.q_gate <= data.x_max) {
        const qLine = explorerJointX(layout, data, data.q_gate);
        svg.appendChild(mk("line", { x1: qLine, y1: layout.scatter.margin.top, x2: qLine, y2: layout.scatter.margin.top + layout.scatter.plot_h, stroke: "#ff7b00", "stroke-width": 1.4, "stroke-dasharray": "4 3" }));
      }
      if (data.p_gate <= data.y_max) {
        const pLine = explorerJointY(layout, data, data.p_gate);
        svg.appendChild(mk("line", { x1: layout.scatter.margin.left, y1: pLine, x2: layout.scatter.margin.left + layout.scatter.plot_w, y2: pLine, stroke: "#5f6f82", "stroke-width": 1.2, "stroke-dasharray": "4 3" }));
      }

      const shown = points.filter((pt) => pointMatchesExplorerJointSelection(pt, selection));
      let shownQPass = 0;
      let shownPPass = 0;
      let shownBoth = 0;
      let activeDot = null;
      const setActiveDot = (dot) => {
        if (activeDot && activeDot !== dot) activeDot.classList.remove("scatter-point-active");
        activeDot = dot;
        if (activeDot) activeDot.classList.add("scatter-point-active");
      };
      const updateHoverText = (pt, qPass, pPass) => {
        const qLabel = data.q_bins[pt.q_idx] ? explorerJointRangeLabel(data.q_bins[pt.q_idx]) : "-";
        const pLabel = data.p_bins[pt.p_idx] ? explorerJointRangeLabel(data.p_bins[pt.p_idx]) : "-";
        const tierText = qPass && pPass ? "q+p pass" : (qPass ? "q pass only" : "q/p below gate");
        hover.textContent =
          pt.run_id + " [" + pt.mode + "]" +
          " | q=" + fmt(pt.q, 4) +
          " | p=" + fmt(pt.p, 4) +
          " | validated=" + fmt(pt.validated, 0) +
          " | bins: " + qLabel + " / " + pLabel +
          " | " + tierText;
      };
      const loadRunDetailsFromPoint = async (pt, qPass, pPass, dot) => {
        setActiveDot(dot);
        updateHoverText(pt, qPass, pPass);
        const rid = String(pt.run_id || "").trim();
        if (!rid || rid === "-") {
          setNotice(byId("detail_notice"), "error", "selected point has no run_id");
          return;
        }
        hover.textContent = pt.run_id + " [" + pt.mode + "] | loading details...";
        try {
          await inspectRun(rid);
          hover.textContent = pt.run_id + " [" + pt.mode + "] | loaded to Run Details";
        } catch (err) {
          setNotice(byId("detail_notice"), "error", String(err && err.message ? err.message : err));
        }
      };

      for (const pt of points) {
        const selected = pointMatchesExplorerJointSelection(pt, selection);
        const qPass = pt.q <= data.q_gate;
        const pPass = pt.p <= data.p_gate;
        if (selected && qPass) shownQPass += 1;
        if (selected && pPass) shownPPass += 1;
        if (selected && qPass && pPass) shownBoth += 1;
        const radius = (selected ? 3.4 : 2.5) + Math.min(2.0, Math.max(0, pt.validated) * 0.20);
        const color = (qPass && pPass) ? "#1d7a32" : (qPass ? "#2f82c7" : "#8ca0b6");
        const dot = mk("circle", {
          cx: explorerJointX(layout, data, pt.q),
          cy: explorerJointY(layout, data, pt.p),
          r: radius,
          fill: color,
          "fill-opacity": selected ? 0.86 : 0.18,
          stroke: "#ffffff",
          "stroke-width": 0.8,
          class: "scatter-point",
        });
        const title = document.createElementNS(ns, "title");
        title.textContent =
          pt.run_id + " [" + pt.mode + "]" +
          " q=" + fmt(pt.q, 4) +
          ", p=" + fmt(pt.p, 4) +
          ", validated=" + fmt(pt.validated, 0);
        dot.appendChild(title);
        dot.addEventListener("mouseenter", () => {
          setActiveDot(dot);
          updateHoverText(pt, qPass, pPass);
        });
        dot.addEventListener("mousemove", () => {
          setActiveDot(dot);
          updateHoverText(pt, qPass, pPass);
        });
        dot.addEventListener("click", () => {
          loadRunDetailsFromPoint(pt, qPass, pPass, dot);
        });
        svg.appendChild(dot);
      }

      const xLabel = mk("text", { x: layout.scatter.margin.left + layout.scatter.plot_w / 2, y: height - 8, "text-anchor": "middle", fill: "#384a61", "font-size": 11 });
      xLabel.textContent = "best q (validation)";
      svg.appendChild(xLabel);
      const yLabel = mk("text", { x: 14, y: layout.scatter.margin.top + layout.scatter.plot_h / 2, transform: "rotate(-90 14 " + String(layout.scatter.margin.top + layout.scatter.plot_h / 2) + ")", "text-anchor": "middle", fill: "#384a61", "font-size": 11 });
      yLabel.textContent = "best p (validation)";
      svg.appendChild(yLabel);

      wrap.appendChild(svg);
      meta.textContent =
        "visible points: " + String(shown.length) + " / " + String(points.length) +
        " | q<=" + fmt(data.q_gate, 2) + ": " + String(shownQPass) +
        " | p<=0.05: " + String(shownPPass) +
        " | both: " + String(shownBoth);
    }

    function renderExplorerDistributions(payload) {
      const qGateRaw = Number(byId("explorer_q_threshold").value || "0.10");
      const qGate = Number.isFinite(qGateRaw) ? Math.max(0, Math.min(1, qGateRaw)) : 0.10;
      const data = buildExplorerJointData(payload, qGate);
      const selection = ensureExplorerJointSelection();
      if (selection.q_idx >= data.q_bins.length) selection.q_idx = -1;
      if (selection.p_idx >= data.p_bins.length) selection.p_idx = -1;
      const layout = computeExplorerJointLayout();
      renderExplorerQMarginal(data, selection, layout);
      renderExplorerPMarginal(data, selection, layout);
      renderExplorerBestQpScatter(data, selection, layout);
      renderExplorerJointKpis(data, selection, qGate);
    }

    function rerenderExplorerDistributionsFromState() {
      const payload = UI_STATE.explorerPayload && typeof UI_STATE.explorerPayload === "object"
        ? UI_STATE.explorerPayload
        : {};
      renderExplorerDistributions(payload);
    }

    async function refreshExplorerSummary() {
      if (UI_STATE.explorerBusy) return;
      const notice = byId("explorer_notice");
      try {
        setExplorerBusy(true);
        setNotice(notice, "", "aggregating sweep summary across runs...");
        const modeScope = String(byId("explorer_mode_scope").value || "all").trim();
        const runLike = String(byId("explorer_run_like").value || "").trim();
        const qThresholdRaw = Number(byId("explorer_q_threshold").value || "0.10");
        const topN = Number(byId("explorer_top_n").value || "20");
        const qThreshold = Number.isFinite(qThresholdRaw) ? Math.max(0, Math.min(1, qThresholdRaw)) : 0.10;

        const params = new URLSearchParams();
        params.set("mode_scope", modeScope || "all");
        if (runLike) params.set("run_id_contains", runLike);
        params.set("q_threshold", String(qThreshold));
        params.set("top_n", String(Number.isFinite(topN) ? Math.max(5, Math.min(200, Math.floor(topN))) : 20));
        params.set("limit_runs", "200");

        const out = await fetchJson("/explorer/summary?" + params.toString());
        UI_STATE.explorerPayload = out;
        byId("explorer_raw_box").textContent = prettyJson(out);
        renderExplorerKpiCards(out);
        renderExplorerDistributions(out);
        renderExplorerComboTable(out.top_combinations || []);
        renderExplorerFactorTable(out.top_key_factors || []);
        renderExplorerPairTable(out.top_affinity_pairs || []);
        renderExplorerClusterTable(out.factor_clusters || []);
        if (!String(byId("eq_run_id").value || "").trim()) {
          const topRuns = Array.isArray(out.top_runs) ? out.top_runs : [];
          if (topRuns.length && topRuns[0] && topRuns[0].run_id) {
            byId("eq_run_id").value = String(topRuns[0].run_id);
          }
        }
        if (!String(byId("eq_factor_list").value || "").trim()) {
          applyExplorerTopFactorsToEquationInput();
        }
        const totals = out && out.totals ? out.totals : {};
        setNotice(
          notice,
          "ok",
          "aggregated: runs=" + String(totals.runs_considered || 0) +
            ", inference=" + String(totals.runs_with_inference || 0) +
            ", combos=" + String(totals.distinct_combinations || 0)
        );
      } catch (err) {
        setNotice(notice, "error", String(err && err.message ? err.message : err));
      } finally {
        setExplorerBusy(false);
      }
    }

    function renderProfileOverview(payload) {
      const tbody = byId("profile_overview_tbody");
      tbody.replaceChildren();
      appendOverviewRow(tbody, "resolved_dataset_path", payload.resolved_dataset_path || payload.dataset_path || "-");
      appendOverviewRow(tbody, "run_id", payload.run_id || "-");
      appendOverviewRow(tbody, "source", payload.source || "-");
      appendOverviewRow(tbody, "artifact_key", payload.artifact_key || "-");
      appendOverviewRow(tbody, "rows (sampled)", fmt(payload.row_count));
      appendOverviewRow(tbody, "columns", fmt(payload.column_count));
      appendOverviewRow(tbody, "research_mode", payload.research_mode ? "on" : "off");
      appendOverviewRow(tbody, "fixed_y", payload.fixed_y || "-");
      appendOverviewRow(
        tbody,
        "exclude_x_cols",
        Array.isArray(payload.exclude_x_cols) ? payload.exclude_x_cols.join(", ") || "-" : "-"
      );
      appendOverviewRow(tbody, "y candidates", fmt(Array.isArray(payload.y_candidates) ? payload.y_candidates.length : 0));
      appendOverviewRow(tbody, "x candidates", fmt(Array.isArray(payload.x_candidates) ? payload.x_candidates.length : 0));
      appendOverviewRow(
        tbody,
        "question seeds",
        fmt(Array.isArray(payload.question_seeds) ? payload.question_seeds.length : 0)
      );
      appendOverviewRow(tbody, "cache_hit", payload.cache_hit ? "yes" : "no");
    }

    async function runDatasetProfile() {
      if (UI_STATE.profileBusy) return;
      const notice = byId("profile_notice");
      try {
        const sampleRows = parseBoundedInt("dataset_sample_rows", "dataset_sample_rows", 100, 500000);
        const topN = parseBoundedInt("dataset_top_n", "dataset_top_n", 1, 100);
        const params = new URLSearchParams();
        const runId = String(byId("dataset_run_id").value || "").trim();
        const datasetPath = String(byId("dataset_path").value || "").trim();
        const artifactKey = String(byId("dataset_artifact_key").value || "auto").trim();
        const researchMode = byId("dataset_research_mode").checked;
        const fixedY = String(byId("dataset_fixed_y").value || "").trim();
        const excludeX = String(byId("dataset_exclude_x_cols").value || "").trim();
        if (runId) params.set("run_id", validateRunId(runId));
        if (datasetPath) params.set("dataset_path", datasetPath);
        if (artifactKey) params.set("artifact_key", artifactKey);
        params.set("sample_rows", String(sampleRows));
        params.set("top_n", String(topN));
        params.set("research_mode", researchMode ? "true" : "false");
        if (fixedY) params.set("fixed_y", fixedY);
        if (excludeX) params.set("exclude_x_cols", excludeX);

        setProfileBusy(true);
        setNotice(notice, "", "profiling dataset...");

        const payload = await fetchJson("/datasets/profile?" + params.toString());
        byId("profile_raw_box").textContent = prettyJson(payload);
        renderProfileOverview(payload);
        renderQuestionSeedTable(payload.question_seeds || []);
        renderBarList(
          "profile_missing_chart",
          payload.charts && payload.charts.missing_share_top ? payload.charts.missing_share_top : [],
          "column",
          "missing_share",
          3
        );
        renderBarList(
          "profile_seed_score_chart",
          payload.charts && payload.charts.seed_score_top ? payload.charts.seed_score_top : [],
          "label",
          "score",
          3
        );

        if (payload.resolved_dataset_path) {
          byId("dataset_path").value = String(payload.resolved_dataset_path);
        }
        if (payload.run_id && !String(byId("dataset_run_id").value || "").trim()) {
          byId("dataset_run_id").value = String(payload.run_id);
        }
        if (payload.fixed_y) {
          byId("dataset_fixed_y").value = String(payload.fixed_y);
        }
        if (Array.isArray(payload.exclude_x_cols)) {
          byId("dataset_exclude_x_cols").value = payload.exclude_x_cols.join(", ");
        }
        byId("dataset_research_mode").checked = Boolean(payload.research_mode);
        setNotice(
          notice,
          "ok",
          "profiled: rows=" + String(payload.row_count || "-") +
            ", cols=" + String(payload.column_count || "-") +
            ", seeds=" + String((payload.question_seeds || []).length)
        );
      } catch (err) {
        setNotice(notice, "error", String(err && err.message ? err.message : err));
      } finally {
        setProfileBusy(false);
      }
    }

    async function loadDatasetConfig() {
      const notice = byId("dataset_config_notice");
      try {
        setNotice(notice, "", "loading data config...");
        const payload = await fetchJson("/datasets/config");
        const config = payload && payload.config && typeof payload.config === "object" ? payload.config : {};
        applyDatasetConfig(config);
        const exists = Boolean(payload.exists);
        setNotice(
          notice,
          exists ? "ok" : "",
          exists ? "loaded saved data config" : "no saved config, using defaults"
        );
      } catch (err) {
        setNotice(notice, "error", String(err && err.message ? err.message : err));
      }
    }

    async function saveDatasetConfig() {
      const notice = byId("dataset_config_notice");
      try {
        const payload = datasetConfigPayloadFromInputs();
        setNotice(notice, "", "saving data config...");
        const out = await fetchJson("/datasets/config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        setNotice(notice, "ok", "saved data config: " + String(out.updated_at_utc || "-"));
      } catch (err) {
        setNotice(notice, "error", String(err && err.message ? err.message : err));
      }
    }

    async function refreshDatasetCandidates() {
      if (UI_STATE.datasetCandidatesBusy) return;
      const notice = byId("dataset_candidates_notice");
      try {
        setDatasetCandidatesBusy(true);
        setNotice(notice, "", "loading dataset candidates...");
        const payload = await fetchJson("/datasets/candidates?limit=120");
        const rows = Array.isArray(payload.rows) ? payload.rows : [];
        UI_STATE.datasetCandidates = rows;
        renderDatasetCandidates(rows);
        const first = rows[0];
        if (first) {
          applyDatasetCandidate(first);
        }
        setNotice(notice, "ok", "dataset candidates loaded: " + String(rows.length));
      } catch (err) {
        setNotice(notice, "error", String(err && err.message ? err.message : err));
      } finally {
        setDatasetCandidatesBusy(false);
      }
    }

    function useSelectedDatasetCandidate() {
      const notice = byId("dataset_candidates_notice");
      const row = selectedDatasetCandidate();
      if (!row) {
        setNotice(notice, "error", "select dataset candidate first");
        return;
      }
      applyDatasetCandidate(row);
      setNotice(
        notice,
        "ok",
        "applied candidate: " + String(row.run_id || "-") + " [" + String(row.artifact_key || "-") + "]"
      );
    }

    function renderSavedReportMeta(row) {
      const tbody = byId("saved_report_meta_tbody");
      tbody.replaceChildren();
      const data = row && typeof row === "object" ? row : {};
      appendOverviewRow(tbody, "kind", data.kind || "-");
      appendOverviewRow(tbody, "file_name", data.file_name || "-");
      appendOverviewRow(tbody, "relative_path", data.relative_path || "-");
      appendOverviewRow(tbody, "size_bytes", fmt(data.size_bytes));
      appendOverviewRow(tbody, "modified_at_utc", data.modified_at_utc || "-");
    }

    function selectedSavedReportRow() {
      const rel = String(byId("saved_report_select").value || "").trim();
      if (!rel) return null;
      const rows = Array.isArray(UI_STATE.savedReports) ? UI_STATE.savedReports : [];
      return rows.find((row) => String(row.relative_path || "") === rel) || null;
    }

    async function refreshSavedReports() {
      if (UI_STATE.savedReportsBusy) return;
      const notice = byId("saved_reports_notice");
      try {
        const kind = String(byId("saved_report_kind").value || "all").trim();
        setSavedReportsBusy(true);
        setNotice(notice, "", "loading saved reports...");
        const payload = await fetchJson("/reports/saved?kind=" + encodeURIComponent(kind) + "&limit=200");
        const rows = Array.isArray(payload.rows) ? payload.rows : [];
        UI_STATE.savedReports = rows;

        const select = byId("saved_report_select");
        const prev = String(select.value || "").trim();
        select.replaceChildren();

        const first = document.createElement("option");
        first.value = "";
        first.textContent = rows.length ? "select saved report file" : "no saved reports found";
        select.appendChild(first);
        for (const row of rows) {
          const rel = String(row.relative_path || "").trim();
          if (!rel) continue;
          const opt = document.createElement("option");
          opt.value = rel;
          opt.textContent = String(row.modified_at_utc || "-") + " | " + rel;
          select.appendChild(opt);
        }
        if (prev && rows.some((row) => String(row.relative_path || "") === prev)) {
          select.value = prev;
        }
        renderSavedReportMeta(selectedSavedReportRow() || rows[0] || {});
        setNotice(notice, "ok", "saved reports loaded: " + String(rows.length));
      } catch (err) {
        setNotice(notice, "error", String(err && err.message ? err.message : err));
      } finally {
        setSavedReportsBusy(false);
      }
    }

    async function loadSavedReport() {
      if (UI_STATE.savedReportsBusy) return;
      const notice = byId("saved_reports_notice");
      const rel = String(byId("saved_report_select").value || "").trim();
      if (!rel) {
        setNotice(notice, "error", "select a saved report first");
        return;
      }
      try {
        setSavedReportsBusy(true);
        setNotice(notice, "", "loading saved report...");
        const params = new URLSearchParams();
        params.set("relative_path", rel);
        params.set("max_chars", "200000");
        const payload = await fetchJson("/reports/read?" + params.toString());
        const parsed = payload && payload.parsed_json && typeof payload.parsed_json === "object"
          ? payload.parsed_json
          : null;
        byId("saved_report_box").textContent = parsed ? prettyJson(parsed) : String(payload.text || "");
        const row = selectedSavedReportRow();
        renderSavedReportMeta(
          row || {
            kind: "-",
            file_name: rel.split("/").slice(-1)[0] || rel,
            relative_path: rel,
            size_bytes: payload.size_chars,
            modified_at_utc: "-",
          }
        );
        setNotice(
          notice,
          "ok",
          "loaded report: " + rel + (payload.truncated ? " (preview truncated)" : "")
        );
      } catch (err) {
        setNotice(notice, "error", String(err && err.message ? err.message : err));
      } finally {
        setSavedReportsBusy(false);
      }
    }

    function renderDetailOverview(statusResp, summaryResp, reviewResp) {
      const tbody = byId("detail_overview_tbody");
      tbody.replaceChildren();

      const status = statusResp && statusResp.status ? statusResp.status : {};
      const summary = summaryResp && summaryResp.summary ? summaryResp.summary : {};
      const review = reviewResp && reviewResp.review ? reviewResp.review : {};
      const metrics = review.metrics || {};
      const gov = review.governance || {};
      const source = String(
        (statusResp && statusResp.source) ||
        (summaryResp && summaryResp.source) ||
        (reviewResp && reviewResp.source) ||
        "-"
      );
      const signal = evaluateDetailSignals(statusResp, reviewResp);

      const progressFraction = Number(status.progress_fraction);
      const progressText = Number.isFinite(progressFraction)
        ? fmt(progressFraction * 100, 1) + "%"
        : "-";

      appendOverviewRow(tbody, "run_id", status.run_id || summary.run_id || review.run_id || "-");
      appendOverviewRow(tbody, "mode", status.mode || summary.mode || review.mode || "-");
      appendOverviewRow(tbody, "state", createStatusPill(status.state || summary.state || "unknown"));
      appendOverviewRow(tbody, "updated_utc", status.updated_at_utc || "-");
      appendOverviewRow(tbody, "source", source);
      appendOverviewRow(
        tbody,
        "governance",
        signal.governanceOk ? "pass" : "fail",
        signal.governanceOk ? "kpi-pass" : "kpi-fail"
      );
      appendOverviewRow(
        tbody,
        "evidence",
        signal.evidenceOk ? "strong" : (signal.validated !== null && signal.validated >= 1 ? "weak/borderline" : "none"),
        signal.evidenceOk ? "kpi-pass" : (signal.validated !== null && signal.validated >= 1 ? "kpi-warn" : "kpi-fail")
      );
      appendOverviewRow(tbody, "validated candidates", fmt(metrics.validated_candidate_count));
      appendOverviewRow(tbody, "best q (validation)", fmt(metrics.best_q_validation, 4));
      appendOverviewRow(tbody, "restart validated mean", fmt(metrics.restart_validated_rate_mean, 3));
      appendOverviewRow(
        tbody,
        "stability",
        signal.restartSignal.text,
        signal.restartSignal.cls
      );
      appendOverviewRow(
        tbody,
        "track consensus",
        gov.track_consensus_enforced ? "enforced" : "off"
      );
      appendOverviewRow(
        tbody,
        "leakage guard",
        signal.leakOk ? "pass" : "fail",
        signal.leakOk ? "kpi-pass" : "kpi-fail"
      );
      appendOverviewRow(
        tbody,
        "pool lock",
        signal.lockOk ? "pass" : "fail",
        signal.lockOk ? "kpi-pass" : "kpi-fail"
      );
      if (!signal.stateDone) {
        appendOverviewRow(tbody, "progress stage", status.progress_stage || "-");
        appendOverviewRow(tbody, "progress", progressText);
      }
    }

    function appendCompareRow(tbody, label, nooptionValue, singlexValue, deltaValue, deltaCls) {
      const tr = document.createElement("tr");

      const th = document.createElement("th");
      th.textContent = label;
      tr.appendChild(th);

      const tdNooption = document.createElement("td");
      if (nooptionValue && typeof nooptionValue === "object" && nooptionValue.nodeType) {
        tdNooption.appendChild(nooptionValue);
      } else {
        tdNooption.textContent = String(
          nooptionValue === null || nooptionValue === undefined || nooptionValue === "" ? "-" : nooptionValue
        );
      }
      tr.appendChild(tdNooption);

      const tdSinglex = document.createElement("td");
      if (singlexValue && typeof singlexValue === "object" && singlexValue.nodeType) {
        tdSinglex.appendChild(singlexValue);
      } else {
        tdSinglex.textContent = String(
          singlexValue === null || singlexValue === undefined || singlexValue === "" ? "-" : singlexValue
        );
      }
      tr.appendChild(tdSinglex);

      const tdDelta = document.createElement("td");
      tdDelta.textContent = String(
        deltaValue === null || deltaValue === undefined || deltaValue === "" ? "-" : deltaValue
      );
      if (deltaCls) tdDelta.classList.add(deltaCls);
      tr.appendChild(tdDelta);

      tbody.appendChild(tr);
    }

    function compareNumericMetric(nooptionValue, singlexValue, preference, digits) {
      const noN = numericOrNull(nooptionValue);
      const sxN = numericOrNull(singlexValue);
      if (noN === null || sxN === null) return { text: "-", cls: "" };

      const diff = sxN - noN;
      if (Math.abs(diff) < 1e-12) {
        return { text: "0 (same)", cls: "compare-delta-same" };
      }

      const singlexBetter = preference === "lower" ? diff < 0 : diff > 0;
      const winner = singlexBetter ? "singlex better" : "nooption better";
      return {
        text: fmtSigned(diff, digits) + " (" + winner + ")",
        cls: singlexBetter ? "compare-delta-up" : "compare-delta-down",
      };
    }

    function compareBooleanMetric(nooptionValue, singlexValue) {
      const noB = boolOrNull(nooptionValue);
      const sxB = boolOrNull(singlexValue);
      if (noB === null || sxB === null) return { text: "-", cls: "" };
      if (noB === sxB) return { text: "same", cls: "compare-delta-same" };
      if (!noB && sxB) return { text: "singlex better", cls: "compare-delta-up" };
      return { text: "nooption better", cls: "compare-delta-down" };
    }

    function appendCompareHint(listEl, text, level) {
      const li = document.createElement("li");
      li.textContent = text;
      if (level === "ok") li.classList.add("interp-ok");
      if (level === "warn") li.classList.add("interp-warn");
      if (level === "fail") li.classList.add("interp-fail");
      listEl.appendChild(li);
    }

    function qThresholdOrDefault(snap) {
      const q = numericOrNull(snap && snap.q_threshold);
      return q === null ? 0.1 : q;
    }

    function isGovernancePass(snap) {
      return (
        boolOrNull(snap && snap.consensus) === true &&
        boolOrNull(snap && snap.leakage_guard) === true &&
        boolOrNull(snap && snap.pool_lock) === true
      );
    }

    function isQPass(snap) {
      const q = numericOrNull(snap && snap.best_q);
      if (q === null) return false;
      return q <= qThresholdOrDefault(snap);
    }

    function scoreSnapshot(snap) {
      let score = 0;
      if (isGovernancePass(snap)) score += 4;
      else score -= 4;

      if (isQPass(snap)) score += 2;

      const p = numericOrNull(snap && snap.best_p);
      if (p !== null && p <= 0.05) score += 1;

      const validated = numericOrNull(snap && snap.validated);
      if (validated !== null && validated > 0) score += 1;

      const restartMean = numericOrNull(snap && snap.restart_mean);
      if (restartMean !== null && restartMean >= 0.6) score += 1;
      return score;
    }

    function inferPairRunIds(seedRunId) {
      const rid = validateRunId(seedRunId);
      const noSuffix = "__nooption_baseline";
      const sxSuffix = "__singlex";
      const noHypSuffix = "__nooption_hypothesis_panel";
      const sxHypSuffix = "__singlex_hypothesis_panel";
      if (rid.endsWith(noSuffix)) {
        const head = rid.slice(0, -noSuffix.length);
        return { nooption: rid, singlex: head + sxSuffix };
      }
      if (rid.endsWith(sxSuffix)) {
        const head = rid.slice(0, -sxSuffix.length);
        return { nooption: head + noSuffix, singlex: rid };
      }
      if (rid.endsWith(noHypSuffix)) {
        const head = rid.slice(0, -noHypSuffix.length);
        return { nooption: rid, singlex: head + sxHypSuffix };
      }
      if (rid.endsWith(sxHypSuffix)) {
        const head = rid.slice(0, -sxHypSuffix.length);
        return { nooption: head + noHypSuffix, singlex: rid };
      }
      const mode = String(byId("mode").value || "").toLowerCase();
      const preferHypothesis = mode.includes("hypothesis") || rid.toLowerCase().includes("hypothesis");
      if (preferHypothesis) {
        return { nooption: rid + noHypSuffix, singlex: rid + sxHypSuffix };
      }
      return { nooption: rid + noSuffix, singlex: rid + sxSuffix };
    }

    async function fetchRunBundle(runId) {
      const rid = validateRunId(runId);
      const enc = encodeURIComponent(rid);
      const [statusResp, summaryResp, reviewResp] = await Promise.all([
        fetchJson("/runs/" + enc),
        fetchJson("/runs/" + enc + "/summary"),
        fetchJson("/runs/" + enc + "/review"),
      ]);
      return { runId: rid, statusResp: statusResp, summaryResp: summaryResp, reviewResp: reviewResp };
    }

    function extractCompareSnapshot(bundle) {
      const status = bundle.statusResp && bundle.statusResp.status ? bundle.statusResp.status : {};
      const summary = bundle.summaryResp && bundle.summaryResp.summary ? bundle.summaryResp.summary : {};
      const review = bundle.reviewResp && bundle.reviewResp.review ? bundle.reviewResp.review : {};
      const metrics = review.metrics || {};
      const gov = review.governance || {};
      return {
        run_id: status.run_id || summary.run_id || review.run_id || bundle.runId || "",
        mode: status.mode || summary.mode || review.mode || "",
        state: status.state || summary.state || review.state || "",
        validated: numericOrNull(metrics.validated_candidate_count),
        support: numericOrNull(metrics.support_candidate_count),
        best_p: numericOrNull(metrics.best_p_validation),
        best_q: numericOrNull(metrics.best_q_validation),
        restart_max: numericOrNull(metrics.restart_validated_rate_max),
        restart_mean: numericOrNull(metrics.restart_validated_rate_mean),
        q_threshold: numericOrNull(metrics.q_threshold_validation ?? metrics.q_threshold),
        consensus: boolOrNull(gov.track_consensus_enforced),
        leakage_guard: boolOrNull(gov.validation_used_for_search_false),
        pool_lock: boolOrNull(gov.candidate_pool_locked_pre_validation_true),
      };
    }

    function renderCompareInterpretation(nooptionSnap, singlexSnap) {
      const list = byId("compare_interp_list");
      list.replaceChildren();
      const hints = [];
      function addHint(text, level) {
        appendCompareHint(list, text, level);
        hints.push({ text: String(text), level: String(level || "info") });
      }

      const noState = String(nooptionSnap.state || "").toLowerCase();
      const sxState = String(singlexSnap.state || "").toLowerCase();
      if (noState !== "succeeded" || sxState !== "succeeded") {
        addHint(
          "One or both runs are not succeeded yet. Treat this board as interim only.",
          "warn"
        );
      }

      const noGov = isGovernancePass(nooptionSnap);
      const sxGov = isGovernancePass(singlexSnap);
      if (noGov && sxGov) {
        addHint("Governance gates pass on both runs.", "ok");
      } else if (sxGov && !noGov) {
        addHint("Governance gates favor singlex (nooption has at least one fail).", "warn");
      } else if (!sxGov && noGov) {
        addHint("Governance gates favor nooption (singlex has at least one fail).", "warn");
      } else {
        addHint("Governance gates fail on both runs. Do not promote either result yet.", "fail");
      }

      const noQ = numericOrNull(nooptionSnap.best_q);
      const sxQ = numericOrNull(singlexSnap.best_q);
      const noQThr = qThresholdOrDefault(nooptionSnap);
      const sxQThr = qThresholdOrDefault(singlexSnap);
      const noQText = noQ === null ? "NA" : fmt(noQ, 4) + " <= " + fmt(noQThr, 4);
      const sxQText = sxQ === null ? "NA" : fmt(sxQ, 4) + " <= " + fmt(sxQThr, 4);
      addHint(
        "q-threshold check: nooption(" + noQText + "), singlex(" + sxQText + ").",
        isQPass(nooptionSnap) || isQPass(singlexSnap) ? "ok" : "warn"
      );

      const restartDelta = compareNumericMetric(nooptionSnap.restart_mean, singlexSnap.restart_mean, "higher", 3);
      addHint(
        "restart mean: nooption=" + fmt(nooptionSnap.restart_mean, 3) +
          ", singlex=" + fmt(singlexSnap.restart_mean, 3) + " => " + restartDelta.text + ".",
        restartDelta.cls === "compare-delta-up" ? "ok" : restartDelta.cls === "compare-delta-down" ? "warn" : ""
      );

      const validatedDelta = compareNumericMetric(nooptionSnap.validated, singlexSnap.validated, "higher", 0);
      addHint(
        "validated candidates: nooption=" + fmt(nooptionSnap.validated) +
          ", singlex=" + fmt(singlexSnap.validated) + " => " + validatedDelta.text + ".",
        validatedDelta.cls === "compare-delta-same" ? "" : "warn"
      );

      const noScore = scoreSnapshot(nooptionSnap);
      const sxScore = scoreSnapshot(singlexSnap);
      const scoreDiff = sxScore - noScore;
      if (scoreDiff >= 2) {
        addHint(
          "provisional pick: singlex (composite score " + String(sxScore) + " vs " + String(noScore) + ").",
          "ok"
        );
      } else if (scoreDiff <= -2) {
        addHint(
          "provisional pick: nooption (composite score " + String(noScore) + " vs " + String(sxScore) + ").",
          "ok"
        );
      } else {
        addHint(
          "provisional pick: no clear winner (composite score " + String(noScore) + " vs " + String(sxScore) + ").",
          "warn"
        );
      }
      return hints;
    }

    function renderCompareBoard(nooptionSnap, singlexSnap) {
      const tbody = byId("compare_tbody");
      tbody.replaceChildren();

      appendCompareRow(tbody, "run_id", nooptionSnap.run_id, singlexSnap.run_id, "-", "");
      appendCompareRow(tbody, "mode", nooptionSnap.mode, singlexSnap.mode, "-", "");
      appendCompareRow(
        tbody,
        "state",
        createStatusPill(nooptionSnap.state || "unknown"),
        createStatusPill(singlexSnap.state || "unknown"),
        "-",
        ""
      );

      const validatedDelta = compareNumericMetric(nooptionSnap.validated, singlexSnap.validated, "higher", 0);
      appendCompareRow(
        tbody,
        "validated candidates",
        fmt(nooptionSnap.validated),
        fmt(singlexSnap.validated),
        validatedDelta.text,
        validatedDelta.cls
      );

      const supportDelta = compareNumericMetric(nooptionSnap.support, singlexSnap.support, "higher", 0);
      appendCompareRow(
        tbody,
        "support candidates",
        fmt(nooptionSnap.support),
        fmt(singlexSnap.support),
        supportDelta.text,
        supportDelta.cls
      );

      const pDelta = compareNumericMetric(nooptionSnap.best_p, singlexSnap.best_p, "lower", 4);
      appendCompareRow(
        tbody,
        "best p (lower better)",
        fmt(nooptionSnap.best_p, 4),
        fmt(singlexSnap.best_p, 4),
        pDelta.text,
        pDelta.cls
      );

      const qDelta = compareNumericMetric(nooptionSnap.best_q, singlexSnap.best_q, "lower", 4);
      appendCompareRow(
        tbody,
        "best q (lower better)",
        fmt(nooptionSnap.best_q, 4),
        fmt(singlexSnap.best_q, 4),
        qDelta.text,
        qDelta.cls
      );

      const restartMaxDelta = compareNumericMetric(nooptionSnap.restart_max, singlexSnap.restart_max, "higher", 3);
      appendCompareRow(
        tbody,
        "restart max (higher better)",
        fmt(nooptionSnap.restart_max, 3),
        fmt(singlexSnap.restart_max, 3),
        restartMaxDelta.text,
        restartMaxDelta.cls
      );

      const restartMeanDelta = compareNumericMetric(nooptionSnap.restart_mean, singlexSnap.restart_mean, "higher", 3);
      appendCompareRow(
        tbody,
        "restart mean (higher better)",
        fmt(nooptionSnap.restart_mean, 3),
        fmt(singlexSnap.restart_mean, 3),
        restartMeanDelta.text,
        restartMeanDelta.cls
      );

      const consensusDelta = compareBooleanMetric(nooptionSnap.consensus, singlexSnap.consensus);
      appendCompareRow(
        tbody,
        "consensus (pass/fail)",
        createCompareBadge(nooptionSnap.consensus),
        createCompareBadge(singlexSnap.consensus),
        consensusDelta.text,
        consensusDelta.cls
      );

      const leakageDelta = compareBooleanMetric(nooptionSnap.leakage_guard, singlexSnap.leakage_guard);
      appendCompareRow(
        tbody,
        "leakage guard (pass/fail)",
        createCompareBadge(nooptionSnap.leakage_guard),
        createCompareBadge(singlexSnap.leakage_guard),
        leakageDelta.text,
        leakageDelta.cls
      );

      const poolDelta = compareBooleanMetric(nooptionSnap.pool_lock, singlexSnap.pool_lock);
      appendCompareRow(
        tbody,
        "pool lock (pass/fail)",
        createCompareBadge(nooptionSnap.pool_lock),
        createCompareBadge(singlexSnap.pool_lock),
        poolDelta.text,
        poolDelta.cls
      );

      const hints = renderCompareInterpretation(nooptionSnap, singlexSnap);
      UI_STATE.compareSnapshot = {
        compared_at_utc: new Date().toISOString(),
        nooption: nooptionSnap,
        singlex: singlexSnap,
        hints: hints,
      };
      updateCompareExportButtons();
    }

    function applyInferredPairFromSeed(seedRunId) {
      const pair = inferPairRunIds(seedRunId);
      byId("compare_nooption_run_id").value = pair.nooption;
      byId("compare_singlex_run_id").value = pair.singlex;
      return pair;
    }

    async function compareRuns() {
      if (UI_STATE.compareBusy) return;
      const notice = byId("compare_notice");

      try {
        const noRun = validateRunId(byId("compare_nooption_run_id").value);
        const sxRun = validateRunId(byId("compare_singlex_run_id").value);
        setCompareBusy(true);
        UI_STATE.compareSnapshot = null;
        setNotice(notice, "", "loading comparison...");
        updateCompareExportButtons();

        const [noBundle, sxBundle] = await Promise.all([
          fetchRunBundle(noRun),
          fetchRunBundle(sxRun),
        ]);

        const noSnap = extractCompareSnapshot(noBundle);
        const sxSnap = extractCompareSnapshot(sxBundle);
        renderCompareBoard(noSnap, sxSnap);
        setNotice(notice, "ok", "compared: " + noRun + " vs " + sxRun);
      } catch (err) {
        UI_STATE.compareSnapshot = null;
        updateCompareExportButtons();
        setNotice(notice, "error", String(err && err.message ? err.message : err));
      } finally {
        setCompareBusy(false);
      }
    }

    function compareFromDetailSeed() {
      try {
        const rid = validateRunId(byId("detail_run_id").value);
        const pair = applyInferredPairFromSeed(rid);
        setNotice(
          byId("compare_notice"),
          "",
          "pair inferred from detail run: " + pair.nooption + " / " + pair.singlex
        );
      } catch (err) {
        setNotice(byId("compare_notice"), "error", String(err && err.message ? err.message : err));
      }
    }

    function renderCountsCell(td, counts) {
      const data = counts && typeof counts === "object" ? counts : {};
      const keys = Object.keys(data);
      if (!keys.length) {
        td.textContent = "-";
        return;
      }
      const wrap = document.createElement("div");
      wrap.className = "kv-chip-wrap";
      for (const key of keys.slice(0, 5)) {
        const chip = document.createElement("span");
        chip.className = "kv-chip";
        chip.textContent = key + "=" + String(data[key]);
        wrap.appendChild(chip);
      }
      if (keys.length > 5) {
        const chip = document.createElement("span");
        chip.className = "kv-chip";
        chip.textContent = "+" + String(keys.length - 5) + " more";
        wrap.appendChild(chip);
      }
      td.appendChild(wrap);
    }

    function renderProgressCell(td, row) {
      const stage = String(row.progress_stage || "-");
      const fraction = Number(row.progress_fraction);
      const hasPct = Number.isFinite(fraction);
      const pct = hasPct ? Math.max(0, Math.min(100, Math.round(fraction * 100))) : null;

      const meta = document.createElement("div");
      meta.className = "progress-meta";
      meta.textContent = hasPct ? stage + " (" + String(pct) + "%)" : stage;
      td.appendChild(meta);

      if (hasPct) {
        const track = document.createElement("div");
        track.className = "progress-track";
        const fill = document.createElement("div");
        fill.className = "progress-fill";
        fill.style.width = String(pct) + "%";
        track.appendChild(fill);
        td.appendChild(track);
      }
    }

    function renderSourceCell(td, row) {
      const source = String(row && row.source || "live");
      const chip = document.createElement("span");
      chip.className = "kv-chip";
      chip.textContent = source;
      td.appendChild(chip);
    }

    function renderResultCell(td, row) {
      const hasResult = Boolean(row && row.has_result);
      const badge = document.createElement("span");
      badge.className = "compare-badge" + (hasResult ? " compare-badge-pass" : "");
      badge.textContent = hasResult ? "available" : "none";
      td.appendChild(badge);
    }

    function highlightSelectedRunRow() {
      const selected = String(UI_STATE.selectedRunId || "");
      for (const tr of Array.from(byId("runs_tbody").querySelectorAll("tr"))) {
        const inspectBtn = tr.querySelector("button[data-action='inspect']");
        const rid = inspectBtn ? String(inspectBtn.dataset.runId || "") : "";
        tr.classList.toggle("run-row-selected", Boolean(selected) && rid === selected);
      }
    }

    async function submitRun(overrides) {
      if (UI_STATE.submitBusy) return;
      const submitNotice = byId("submit_notice");
      try {
        const payload = payloadFromForm();
        if (overrides && typeof overrides === "object") {
          if (overrides.mode) payload.mode = String(overrides.mode);
          if (overrides.run_id) payload.run_id = validateRunId(String(overrides.run_id));
        }
        const execute = byId("execute").checked ? "true" : "false";
        const dryRun = byId("dry_run").checked ? "true" : "false";

        setSubmitBusy(true);
        setNotice(submitNotice, "", "submitting...");

        const out = await fetchJson("/runs?execute=" + execute + "&dry_run=" + dryRun, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });

        byId("submit_response").textContent = prettyJson(out);
        if (out && out.status && out.status.run_id) {
          byId("detail_run_id").value = out.status.run_id;
        }
        setNotice(submitNotice, "ok", "submitted: " + payload.run_id);
        await refreshRuns();
      } catch (err) {
        setNotice(submitNotice, "error", String(err && err.message ? err.message : err));
      } finally {
        setSubmitBusy(false);
      }
    }

    async function runPresetNow(mode, prefix) {
      applyPreset(mode, prefix || mode);
      await submitRun({
        mode: mode,
        run_id: byId("run_id").value,
      });
    }

    async function inspectRun(runId) {
      const detailNotice = byId("detail_notice");
      const ridInput = String(runId || byId("detail_run_id").value || "").trim();
      if (!ridInput) {
        setNotice(detailNotice, "error", "run_id is required");
        return;
      }
      const rid = validateRunId(ridInput);
      if (UI_STATE.inspectBusy) return;

      UI_STATE.selectedRunId = rid;
      highlightSelectedRunRow();
      setInspectBusy(true);
      byId("detail_run_id").value = rid;
      if (Array.from(byId("recent_run_select").options || []).some((o) => String(o.value) === rid)) {
        byId("recent_run_select").value = rid;
      }
      setNotice(detailNotice, "", "loading run details...");

      try {
        const enc = encodeURIComponent(rid);
        const [status, summary, review] = await Promise.all([
          fetchJson("/runs/" + enc),
          fetchJson("/runs/" + enc + "/summary"),
          fetchJson("/runs/" + enc + "/review"),
        ]);

        byId("status_box").textContent = prettyJson(status);
        byId("summary_box").textContent = prettyJson(summary);
        byId("review_box").textContent = prettyJson(review);

        renderDetailVerdict(status, review);
        renderReviewCards(status, review);
        renderDetailOverview(status, summary, review);
        setNotice(detailNotice, "ok", "loaded: " + rid);
        if (!String(byId("dataset_run_id").value || "").trim()) {
          byId("dataset_run_id").value = rid;
        }
        if (!String(byId("eq_run_id").value || "").trim()) {
          byId("eq_run_id").value = rid;
        }
        if (!String(byId("compare_nooption_run_id").value || "").trim() &&
            !String(byId("compare_singlex_run_id").value || "").trim()) {
          try {
            const pair = applyInferredPairFromSeed(rid);
            setNotice(
              byId("compare_notice"),
              "",
              "pair inferred from detail run: " + pair.nooption + " / " + pair.singlex
            );
          } catch (_err) {
            // no-op: detail run may not follow pair naming.
          }
        }
      } catch (err) {
        setNotice(detailNotice, "error", String(err && err.message ? err.message : err));
      } finally {
        setInspectBusy(false);
      }
    }

    async function refreshRuns() {
      if (UI_STATE.runsBusy) return;
      UI_STATE.runsBusy = true;
      try {
        const state = byId("detail_status_filter").value.trim();
        const mode = String(byId("list_mode_filter").value || "").trim();
        const runIdLike = String(byId("list_run_id_filter").value || "").trim();
        const query = new URLSearchParams();
        query.set("limit", "200");
        query.set("include_history", "true");
        if (state) query.set("state", state);
        if (mode) query.set("mode", mode);
        if (runIdLike) query.set("run_id_contains", runIdLike);
        const payload = await fetchJson("/runs?" + query.toString());
        const fetchedRows = Array.isArray(payload.rows) ? payload.rows : [];
        UI_STATE.lastRunRows = fetchedRows;
        const rows = filteredAndSortedRunRows(fetchedRows);
        applyRunsCompactView();
        renderRecentRunSelect(rows);
        renderRunsKpiCards(rows);
        renderRunsInsight(rows, fetchedRows);

        const tbody = byId("runs_tbody");
        tbody.replaceChildren();

        for (const row of rows) {
          const tr = document.createElement("tr");
          const runIdText = String(row.run_id || "");
          if (runIdText && runIdText === String(UI_STATE.selectedRunId || "")) {
            tr.className = "run-row-selected";
          }

          const tdRunId = document.createElement("td");
          tdRunId.textContent = runIdText || "-";
          tr.appendChild(tdRunId);

          const tdMode = document.createElement("td");
          tdMode.textContent = String(row.mode || "-");
          tr.appendChild(tdMode);

          const tdState = document.createElement("td");
          tdState.appendChild(createStatusPill(row.state));
          tr.appendChild(tdState);

          const tdProgress = document.createElement("td");
          tdProgress.className = "runs-col-detail";
          renderProgressCell(tdProgress, row);
          tr.appendChild(tdProgress);

          const tdAttempt = document.createElement("td");
          tdAttempt.className = "runs-col-detail";
          tdAttempt.textContent = String(row.attempt === undefined ? "-" : row.attempt);
          tr.appendChild(tdAttempt);

          const tdUpdated = document.createElement("td");
          tdUpdated.textContent = String(row.updated_at_utc || "-");
          tr.appendChild(tdUpdated);

          const tdSource = document.createElement("td");
          tdSource.className = "runs-col-detail";
          renderSourceCell(tdSource, row);
          tr.appendChild(tdSource);

          const tdResult = document.createElement("td");
          renderResultCell(tdResult, row);
          tr.appendChild(tdResult);

          const tdCounts = document.createElement("td");
          tdCounts.className = "runs-col-detail";
          renderCountsCell(tdCounts, row.counts);
          tr.appendChild(tdCounts);

          const tdActions = document.createElement("td");
          const inspectBtn = document.createElement("button");
          inspectBtn.className = "mini";
          inspectBtn.dataset.action = "inspect";
          inspectBtn.dataset.runId = String(row.run_id || "");
          inspectBtn.textContent = "inspect";

          const inspectProfileBtn = document.createElement("button");
          inspectProfileBtn.className = "mini";
          inspectProfileBtn.dataset.action = "inspect_profile";
          inspectProfileBtn.dataset.runId = String(row.run_id || "");
          inspectProfileBtn.textContent = "inspect+profile";
          inspectProfileBtn.disabled = !Boolean(row && row.has_result);
          if (inspectProfileBtn.disabled) {
            inspectProfileBtn.title = "result artifact is not available yet";
          }

          const cancelBtn = document.createElement("button");
          cancelBtn.className = "mini warn";
          cancelBtn.dataset.action = "cancel";
          cancelBtn.dataset.runId = String(row.run_id || "");
          cancelBtn.textContent = "cancel";

          const retryBtn = document.createElement("button");
          retryBtn.className = "mini";
          retryBtn.dataset.action = "retry";
          retryBtn.dataset.runId = String(row.run_id || "");
          retryBtn.textContent = "retry";

          const stateText = String(row.state || "").toLowerCase();
          const isLive = String(row.source || "live") === "live";
          cancelBtn.disabled = !isLive || stateText === "succeeded" || stateText === "failed" || stateText === "cancelled";
          retryBtn.disabled = !isLive || !(stateText === "failed" || stateText === "cancelled");

          tdActions.appendChild(inspectBtn);
          tdActions.appendChild(inspectProfileBtn);
          tdActions.appendChild(cancelBtn);
          tdActions.appendChild(retryBtn);
          tr.appendChild(tdActions);

          tbody.appendChild(tr);
        }
        highlightSelectedRunRow();
        const withResultCount = rows.filter((row) => Boolean(row && row.has_result)).length;
        setNotice(
          byId("runs_notice"),
          "",
          "loaded rows: " + String(rows.length) +
            " (total: " + String(fetchedRows.length) + ", with result: " + String(withResultCount) + ")"
        );
      } catch (err) {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      } finally {
        UI_STATE.runsBusy = false;
      }
    }

    async function runAction(action, runId) {
      const rid = String(runId || "").trim();
      if (!rid) return;
      if (action === "inspect") {
        UI_STATE.selectedRunId = rid;
        await inspectRun(rid);
        return;
      }
      if (action === "inspect_profile") {
        UI_STATE.selectedRunId = rid;
        await inspectRun(rid);
        applyRunIdToDatasetInputs(rid);
        await runDatasetProfile();
        return;
      }
      if (action === "cancel") {
        if (!window.confirm("Cancel run " + rid + "?")) return;
        await fetchJson("/runs/" + encodeURIComponent(rid) + "/cancel?reason=ui_request", { method: "POST" });
      } else if (action === "retry") {
        if (!window.confirm("Retry run " + rid + "?")) return;
        await fetchJson("/runs/" + encodeURIComponent(rid) + "/retry", { method: "POST" });
      }
      await refreshRuns();
      UI_STATE.selectedRunId = rid;
      await inspectRun(rid);
    }

    async function refreshHealth() {
      if (UI_STATE.healthBusy) return;
      UI_STATE.healthBusy = true;
      try {
        const payload = await fetchJson("/healthz");
        byId("health_text").textContent = "ok=true, run_count=" + payload.run_count + ", utc=" + payload.timestamp_utc;
      } catch (err) {
        byId("health_text").textContent = "error: " + String(err && err.message ? err.message : err);
      } finally {
        UI_STATE.healthBusy = false;
      }
    }

    async function loadDatasetDefaultCandidate() {
      try {
        const payload = await fetchJson("/datasets/candidates?limit=1");
        const rows = payload && Array.isArray(payload.rows) ? payload.rows : [];
        if (!rows.length) return;
        const row = rows[0] || {};
        if (!String(byId("dataset_run_id").value || "").trim() && row.run_id) {
          byId("dataset_run_id").value = String(row.run_id);
        }
        if (!String(byId("dataset_path").value || "").trim() && row.dataset_path) {
          byId("dataset_path").value = String(row.dataset_path);
        }
        if (row.artifact_key) {
          byId("dataset_artifact_key").value = String(row.artifact_key);
        }
      } catch (_err) {
        // no-op: dataset profile can still run with manual input.
      }
    }

    async function onInspectClick() {
      try {
        await inspectRun();
      } catch (err) {
        setNotice(byId("detail_notice"), "error", String(err && err.message ? err.message : err));
      }
    }

    async function onRefreshRunsClick() {
      try {
        await refreshRuns();
      } catch (err) {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      }
    }

    async function tickAutoRefresh() {
      if (!byId("auto_refresh").checked) return;
      await Promise.all([refreshHealth(), refreshRuns()]);
    }

    byId("submit_btn").addEventListener("click", submitRun);
    byId("new_run_id_btn").addEventListener("click", () => {
      byId("run_id").value = makeDefaultRunId(byId("mode").value || "ui_run");
    });
    byId("load_prev_refresh_btn").addEventListener("click", () => {
      refreshAllPreviousSources().catch((err) => {
        setNotice(byId("detail_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("load_prev_latest_run_btn").addEventListener("click", () => {
      loadLatestRunResultGuided().catch((err) => {
        setNotice(byId("detail_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("load_prev_latest_report_btn").addEventListener("click", () => {
      loadLatestSavedReportGuided().catch((err) => {
        setNotice(byId("saved_reports_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("load_prev_data_config_btn").addEventListener("click", () => {
      loadDatasetConfig().catch((err) => {
        setNotice(byId("dataset_config_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("inspect_btn").addEventListener("click", onInspectClick);
    byId("inspect_profile_btn").addEventListener("click", () => {
      inspectAndProfileFromDetail().catch((err) => {
        setNotice(byId("detail_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("runs_refresh_now_btn").addEventListener("click", onRefreshRunsClick);
    byId("runs_load_latest_result_btn").addEventListener("click", () => {
      loadLatestRunResultGuided().catch((err) => {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("explorer_refresh_btn").addEventListener("click", () => {
      refreshExplorerSummary().catch((err) => {
        setNotice(byId("explorer_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("explorer_mode_scope").addEventListener("change", () => {
      refreshExplorerSummary().catch((err) => {
        setNotice(byId("explorer_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("explorer_q_threshold").addEventListener("change", () => {
      refreshExplorerSummary().catch((err) => {
        setNotice(byId("explorer_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("explorer_top_n").addEventListener("change", () => {
      refreshExplorerSummary().catch((err) => {
        setNotice(byId("explorer_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("explorer_run_like").addEventListener("keydown", (evt) => {
      if (evt.key !== "Enter") return;
      refreshExplorerSummary().catch((err) => {
        setNotice(byId("explorer_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("explorer_joint_clear_btn").addEventListener("click", () => {
      resetExplorerJointSelection();
      rerenderExplorerDistributionsFromState();
    });
    byId("explorer_combo_filter_text").addEventListener("input", () => {
      rerenderExplorerComboFromState();
    });
    byId("explorer_combo_sort_key").addEventListener("change", () => {
      rerenderExplorerComboFromState();
    });
    byId("explorer_combo_limit").addEventListener("change", () => {
      rerenderExplorerComboFromState();
    });
    byId("explorer_combo_reset_btn").addEventListener("click", () => {
      resetExplorerComboControls();
      rerenderExplorerComboFromState();
    });
    byId("explorer_cluster_filter_text").addEventListener("input", () => {
      rerenderExplorerClusterFromState();
    });
    byId("explorer_cluster_min_support").addEventListener("change", () => {
      rerenderExplorerClusterFromState();
    });
    byId("explorer_cluster_sort_key").addEventListener("change", () => {
      rerenderExplorerClusterFromState();
    });
    byId("explorer_cluster_limit").addEventListener("change", () => {
      rerenderExplorerClusterFromState();
    });
    byId("explorer_cluster_reset_btn").addEventListener("click", () => {
      resetExplorerClusterControls();
      rerenderExplorerClusterFromState();
    });
    byId("eq_use_top_factors_btn").addEventListener("click", () => {
      try {
        applyExplorerTopFactorsToEquationInput();
      } catch (err) {
        setNotice(byId("eq_notice"), "error", String(err && err.message ? err.message : err));
      }
    });
    byId("eq_build_btn").addEventListener("click", () => {
      buildEquationPath().catch((err) => {
        setNotice(byId("eq_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("eq_top_k").addEventListener("change", () => {
      if (!String(byId("eq_factor_list").value || "").trim()) {
        applyExplorerTopFactorsToEquationInput();
      }
    });
    byId("refresh_runs_btn").addEventListener("click", onRefreshRunsClick);
    byId("load_recent_run_btn").addEventListener("click", () => {
      inspectFromRecentSelect().catch((err) => {
        setNotice(byId("detail_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("recent_run_select").addEventListener("change", () => {
      const rid = String(byId("recent_run_select").value || "").trim();
      if (!rid) return;
      byId("detail_run_id").value = rid;
    });
    byId("profile_btn").addEventListener("click", () => {
      runDatasetProfile().catch((err) => {
        setNotice(byId("profile_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("refresh_dataset_candidates_btn").addEventListener("click", () => {
      refreshDatasetCandidates().catch((err) => {
        setNotice(byId("dataset_candidates_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("use_dataset_candidate_btn").addEventListener("click", () => {
      try {
        useSelectedDatasetCandidate();
      } catch (err) {
        setNotice(byId("dataset_candidates_notice"), "error", String(err && err.message ? err.message : err));
      }
    });
    byId("dataset_candidate_select").addEventListener("change", () => {
      const row = selectedDatasetCandidate();
      if (!row) return;
      applyDatasetCandidate(row);
    });
    byId("load_dataset_config_btn").addEventListener("click", () => {
      loadDatasetConfig().catch((err) => {
        setNotice(byId("dataset_config_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("save_dataset_config_btn").addEventListener("click", () => {
      saveDatasetConfig().catch((err) => {
        setNotice(byId("dataset_config_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("refresh_saved_reports_btn").addEventListener("click", () => {
      refreshSavedReports().catch((err) => {
        setNotice(byId("saved_reports_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("load_saved_report_btn").addEventListener("click", () => {
      loadSavedReport().catch((err) => {
        setNotice(byId("saved_reports_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("saved_report_kind").addEventListener("change", () => {
      refreshSavedReports().catch((err) => {
        setNotice(byId("saved_reports_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("saved_report_select").addEventListener("change", () => {
      const row = selectedSavedReportRow();
      if (row) renderSavedReportMeta(row);
    });

    byId("preset_pair_btn").addEventListener("click", () => {
      applyPreset("paired_nooption_singlex", "pair_baseline");
    });
    byId("preset_singlex_btn").addEventListener("click", () => {
      applyPreset("singlex_baseline", "singlex_baseline");
    });
    byId("preset_nooption_btn").addEventListener("click", () => {
      applyPreset("nooption_baseline", "nooption_baseline");
    });
    byId("run_pair_now_btn").addEventListener("click", () => {
      runPresetNow("paired_nooption_singlex", "pair_baseline").catch((err) => {
        setNotice(byId("submit_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("run_singlex_now_btn").addEventListener("click", () => {
      runPresetNow("singlex_baseline", "singlex_baseline").catch((err) => {
        setNotice(byId("submit_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("run_nooption_now_btn").addEventListener("click", () => {
      runPresetNow("nooption_baseline", "nooption_baseline").catch((err) => {
        setNotice(byId("submit_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("compare_btn").addEventListener("click", () => {
      compareRuns().catch((err) => {
        setNotice(byId("compare_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("compare_from_detail_btn").addEventListener("click", compareFromDetailSeed);
    byId("compare_nooption_run_id").addEventListener("input", updateCompareExportButtons);
    byId("compare_singlex_run_id").addEventListener("input", updateCompareExportButtons);
    byId("export_compare_md_btn").addEventListener("click", () => {
      try {
        exportCompareMarkdown();
      } catch (err) {
        setNotice(byId("compare_notice"), "error", String(err && err.message ? err.message : err));
      }
    });
    byId("export_compare_json_btn").addEventListener("click", () => {
      try {
        exportCompareJson();
      } catch (err) {
        setNotice(byId("compare_notice"), "error", String(err && err.message ? err.message : err));
      }
    });
    byId("save_compare_outputs_btn").addEventListener("click", () => {
      saveCompareOutputsToWorkspace().catch((err) => {
        setNotice(byId("compare_notice"), "error", String(err && err.message ? err.message : err));
      });
    });

    byId("mode").addEventListener("change", updateModeHelp);
    byId("detail_status_filter").addEventListener("change", () => {
      onRefreshRunsClick().catch((err) => {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("list_mode_filter").addEventListener("change", () => {
      onRefreshRunsClick().catch((err) => {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("runs_sort_key").addEventListener("change", () => {
      onRefreshRunsClick().catch((err) => {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("runs_view_preset").addEventListener("change", () => {
      onRefreshRunsClick().catch((err) => {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("runs_row_limit").addEventListener("change", () => {
      onRefreshRunsClick().catch((err) => {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("runs_compact_view").addEventListener("change", () => {
      applyRunsCompactView();
      onRefreshRunsClick().catch((err) => {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("runs_only_with_result").addEventListener("change", () => {
      onRefreshRunsClick().catch((err) => {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      });
    });
    byId("list_run_id_filter").addEventListener("keydown", (evt) => {
      if (evt.key !== "Enter") return;
      onRefreshRunsClick().catch((err) => {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      });
    });

    byId("runs_tbody").addEventListener("click", async (evt) => {
      const target = evt.target;
      const btn = target && target.closest ? target.closest("button[data-action]") : null;
      if (!btn) return;

      const action = btn.dataset.action;
      const runId = btn.dataset.runId;
      if (!action || !runId) return;

      try {
        await runAction(action, runId);
      } catch (err) {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      }
    });

    let explorerResizeTimer = null;
    window.addEventListener("resize", () => {
      if (explorerResizeTimer) clearTimeout(explorerResizeTimer);
      explorerResizeTimer = setTimeout(() => {
        rerenderExplorerDistributionsFromState();
      }, 120);
    });

    const preferredModes = ["paired_nooption_singlex", "singlex_baseline", "nooption_baseline"];
    const modeSelect = byId("mode");
    const modeValues = Array.from(modeSelect.options || []).map((o) => o.value);
    const defaultMode = preferredModes.find((m) => modeValues.includes(m)) || modeValues[0] || "";
    if (defaultMode) modeSelect.value = defaultMode;

    byId("run_id").value = makeDefaultRunId(defaultMode || "ui_run");
    updateModeHelp();
    updateCompareExportButtons();
    applyRunsCompactView();

    refreshHealth();
    loadDatasetConfig().then(() => {
      const hasAny = String(byId("dataset_path").value || "").trim() || String(byId("dataset_run_id").value || "").trim();
      if (!hasAny) {
        loadDatasetDefaultCandidate();
      }
    }).catch((err) => {
      setNotice(byId("dataset_config_notice"), "error", String(err && err.message ? err.message : err));
      loadDatasetDefaultCandidate();
    });
    refreshDatasetCandidates();
    refreshSavedReports();
    refreshRuns();
    refreshExplorerSummary();
    setInterval(() => {
      tickAutoRefresh().catch((err) => {
        setNotice(byId("runs_notice"), "error", String(err && err.message ? err.message : err));
      });
    }, 4000);
  </script>
</body>
</html>
        """
        .replace("__MODE_OPTIONS__", mode_options)
        .replace("__MODE_FILTER_OPTIONS__", mode_filter_options)
        .replace("__STATE_OPTIONS__", state_options)
    )
