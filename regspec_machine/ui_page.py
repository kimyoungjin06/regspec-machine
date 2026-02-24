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
          <label class="inline"><input id="auto_refresh" type="checkbox" checked /> auto refresh (4s)</label>
        </div>
        <div id="detail_notice" class="notice">Select a run to inspect.</div>
        <label>Interpreted Summary</label>
        <div class="run-table-wrap compact-table">
          <table>
            <tbody id="detail_overview_tbody"></tbody>
          </table>
        </div>
        <label>Review (validated / p / q / restart / consensus)</label>
        <div id="review_cards" class="kpi-grid"></div>
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
      </div>
      <div class="toolbar">
        <button id="profile_btn" class="secondary">Analyze Dataset</button>
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
      <div id="runs_notice" class="notice">ready</div>
      <div class="run-table-wrap">
        <table>
          <thead>
            <tr>
              <th>run_id</th>
              <th>mode</th>
              <th>state</th>
              <th>progress</th>
              <th>attempt</th>
              <th>updated_utc</th>
              <th>counts</th>
              <th>actions</th>
            </tr>
          </thead>
          <tbody id="runs_tbody"></tbody>
        </table>
      </div>
      <div class="footer-note">Actions call API endpoints directly. Cancel/Retry follow orchestrator transition rules.</div>
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
      return lines.join("\n");
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
        .split("\n")
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

    function renderReviewCards(reviewResp) {
      const wrap = byId("review_cards");
      wrap.replaceChildren();
      const review = reviewResp && reviewResp.review ? reviewResp.review : null;
      if (!review) return;

      const metrics = review.metrics || {};
      const gov = review.governance || {};
      const leakOk = gov.validation_used_for_search_false === true;
      const lockOk = gov.candidate_pool_locked_pre_validation_true === true;

      const cards = [
        { label: "validated", value: fmt(metrics.validated_candidate_count), cls: "" },
        { label: "best p", value: fmt(metrics.best_p_validation, 4), cls: "" },
        { label: "best q", value: fmt(metrics.best_q_validation, 4), cls: "" },
        { label: "restart max", value: fmt(metrics.restart_validated_rate_max, 3), cls: "" },
        { label: "restart mean", value: fmt(metrics.restart_validated_rate_mean, 3), cls: "" },
        {
          label: "consensus",
          value: gov.track_consensus_enforced ? "enforced" : "off",
          cls: gov.track_consensus_enforced ? "kpi-pass" : "kpi-warn",
        },
        {
          label: "leakage guard",
          value: leakOk ? "pass" : "fail",
          cls: leakOk ? "kpi-pass" : "kpi-fail",
        },
        {
          label: "pool lock",
          value: lockOk ? "pass" : "fail",
          cls: lockOk ? "kpi-pass" : "kpi-fail",
        },
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

    function renderDetailOverview(statusResp, summaryResp, reviewResp) {
      const tbody = byId("detail_overview_tbody");
      tbody.replaceChildren();

      const status = statusResp && statusResp.status ? statusResp.status : {};
      const summary = summaryResp && summaryResp.summary ? summaryResp.summary : {};
      const review = reviewResp && reviewResp.review ? reviewResp.review : {};
      const metrics = review.metrics || {};
      const gov = review.governance || {};

      const progressFraction = Number(status.progress_fraction);
      const progressText = Number.isFinite(progressFraction)
        ? fmt(progressFraction * 100, 1) + "%"
        : "-";

      appendOverviewRow(tbody, "run_id", status.run_id || summary.run_id || review.run_id || "-");
      appendOverviewRow(tbody, "mode", status.mode || summary.mode || review.mode || "-");
      appendOverviewRow(tbody, "state", createStatusPill(status.state || summary.state || "unknown"));
      appendOverviewRow(tbody, "progress stage", status.progress_stage || "-");
      appendOverviewRow(tbody, "progress", progressText);
      appendOverviewRow(tbody, "updated_utc", status.updated_at_utc || "-");
      appendOverviewRow(tbody, "validated candidates", fmt(metrics.validated_candidate_count));
      appendOverviewRow(tbody, "support candidates", fmt(metrics.support_candidate_count));
      appendOverviewRow(tbody, "best p (validation)", fmt(metrics.best_p_validation, 4));
      appendOverviewRow(tbody, "best q (validation)", fmt(metrics.best_q_validation, 4));
      appendOverviewRow(tbody, "restart validated max", fmt(metrics.restart_validated_rate_max, 3));
      appendOverviewRow(tbody, "restart validated mean", fmt(metrics.restart_validated_rate_mean, 3));
      appendOverviewRow(
        tbody,
        "track consensus",
        gov.track_consensus_enforced ? "enforced" : "off",
        gov.track_consensus_enforced ? "kpi-pass" : "kpi-warn"
      );
      appendOverviewRow(
        tbody,
        "leakage guard",
        gov.validation_used_for_search_false === true ? "pass" : "fail",
        gov.validation_used_for_search_false === true ? "kpi-pass" : "kpi-fail"
      );
      appendOverviewRow(
        tbody,
        "pool lock",
        gov.candidate_pool_locked_pre_validation_true === true ? "pass" : "fail",
        gov.candidate_pool_locked_pre_validation_true === true ? "kpi-pass" : "kpi-fail"
      );
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

      setInspectBusy(true);
      byId("detail_run_id").value = rid;
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

        renderReviewCards(review);
        renderDetailOverview(status, summary, review);
        setNotice(detailNotice, "ok", "loaded: " + rid);
        if (!String(byId("dataset_run_id").value || "").trim()) {
          byId("dataset_run_id").value = rid;
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
        const rows = payload.rows || [];

        const tbody = byId("runs_tbody");
        tbody.replaceChildren();

        for (const row of rows) {
          const tr = document.createElement("tr");

          const tdRunId = document.createElement("td");
          tdRunId.textContent = String(row.run_id || "-");
          tr.appendChild(tdRunId);

          const tdMode = document.createElement("td");
          tdMode.textContent = String(row.mode || "-");
          tr.appendChild(tdMode);

          const tdState = document.createElement("td");
          tdState.appendChild(createStatusPill(row.state));
          tr.appendChild(tdState);

          const tdProgress = document.createElement("td");
          renderProgressCell(tdProgress, row);
          tr.appendChild(tdProgress);

          const tdAttempt = document.createElement("td");
          tdAttempt.textContent = String(row.attempt === undefined ? "-" : row.attempt);
          tr.appendChild(tdAttempt);

          const tdUpdated = document.createElement("td");
          tdUpdated.textContent = String(row.updated_at_utc || "-");
          tr.appendChild(tdUpdated);

          const tdCounts = document.createElement("td");
          renderCountsCell(tdCounts, row.counts);
          tr.appendChild(tdCounts);

          const tdActions = document.createElement("td");
          const inspectBtn = document.createElement("button");
          inspectBtn.className = "mini";
          inspectBtn.dataset.action = "inspect";
          inspectBtn.dataset.runId = String(row.run_id || "");
          inspectBtn.textContent = "inspect";

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
          cancelBtn.disabled = stateText === "succeeded" || stateText === "failed" || stateText === "cancelled";
          retryBtn.disabled = !(stateText === "failed" || stateText === "cancelled");

          tdActions.appendChild(inspectBtn);
          tdActions.appendChild(cancelBtn);
          tdActions.appendChild(retryBtn);
          tr.appendChild(tdActions);

          tbody.appendChild(tr);
        }
        setNotice(byId("runs_notice"), "", "loaded rows: " + String(rows.length));
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
        await inspectRun(rid);
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
    byId("inspect_btn").addEventListener("click", onInspectClick);
    byId("refresh_runs_btn").addEventListener("click", onRefreshRunsClick);
    byId("profile_btn").addEventListener("click", () => {
      runDatasetProfile().catch((err) => {
        setNotice(byId("profile_notice"), "error", String(err && err.message ? err.message : err));
      });
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

    const preferredModes = ["paired_nooption_singlex", "singlex_baseline", "nooption_baseline"];
    const modeSelect = byId("mode");
    const modeValues = Array.from(modeSelect.options || []).map((o) => o.value);
    const defaultMode = preferredModes.find((m) => modeValues.includes(m)) || modeValues[0] || "";
    if (defaultMode) modeSelect.value = defaultMode;

    byId("run_id").value = makeDefaultRunId(defaultMode || "ui_run");
    updateModeHelp();
    updateCompareExportButtons();

    refreshHealth();
    loadDatasetDefaultCandidate();
    refreshRuns();
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
