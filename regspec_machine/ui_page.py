"""Small L5 operator UI served from the L4 FastAPI app."""

from __future__ import annotations

from typing import Iterable


RUN_STATE_VALUES = ("all", "queued", "running", "succeeded", "failed", "cancelled")


def build_ui_page_html(*, run_modes: Iterable[str]) -> str:
    mode_options = "\n".join(
        f'<option value="{mode}">{mode}</option>' for mode in list(run_modes)
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
      <h2>Run Monitor</h2>
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
      runsBusy: false,
      healthBusy: false,
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
        throw new Error(prettyJson(payload));
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
        const query = state ? ("?state=" + encodeURIComponent(state) + "&limit=200") : "?limit=200";
        const payload = await fetchJson("/runs" + query);
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

          tdActions.appendChild(inspectBtn);
          tdActions.appendChild(cancelBtn);
          tdActions.appendChild(retryBtn);
          tr.appendChild(tdActions);

          tbody.appendChild(tr);
        }
      } catch (err) {
        setNotice(byId("submit_notice"), "error", String(err && err.message ? err.message : err));
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
        setNotice(byId("submit_notice"), "error", String(err && err.message ? err.message : err));
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

    byId("mode").addEventListener("change", updateModeHelp);

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
        setNotice(byId("submit_notice"), "error", String(err && err.message ? err.message : err));
      }
    });

    const preferredModes = ["paired_nooption_singlex", "singlex_baseline", "nooption_baseline"];
    const modeSelect = byId("mode");
    const modeValues = Array.from(modeSelect.options || []).map((o) => o.value);
    const defaultMode = preferredModes.find((m) => modeValues.includes(m)) || modeValues[0] || "";
    if (defaultMode) modeSelect.value = defaultMode;

    byId("run_id").value = makeDefaultRunId(defaultMode || "ui_run");
    updateModeHelp();

    refreshHealth();
    refreshRuns();
    setInterval(() => {
      tickAutoRefresh().catch((err) => {
        setNotice(byId("submit_notice"), "error", String(err && err.message ? err.message : err));
      });
    }, 4000);
  </script>
</body>
</html>
        """
        .replace("__MODE_OPTIONS__", mode_options)
        .replace("__STATE_OPTIONS__", state_options)
    )
