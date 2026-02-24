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
      max-width: 1180px;
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
    .grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }
    @media (min-width: 980px) {
      .grid {
        grid-template-columns: 1.05fr 1fr;
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
    .status-pill {
      border-radius: 999px;
      font-size: 11px;
      padding: 2px 8px;
      color: #fff;
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
  </style>
</head>
<body>
  <div class="page">
    <section class="header">
      <h1 class="title">RegSpec-Machine Console (L5)</h1>
      <p class="sub">Run nooption/singlex/paired presets, monitor execution state, and inspect summary from one browser screen.</p>
      <p class="sub">Health: <span id="health_text">checking...</span></p>
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
            <input id="run_id" />
          </div>
        </div>
        <div class="row">
          <div>
            <label for="scan_n_bootstrap">scan_n_bootstrap</label>
            <input id="scan_n_bootstrap" type="number" min="0" step="1" placeholder="49" />
          </div>
          <div>
            <label for="scan_max_features">scan_max_features</label>
            <input id="scan_max_features" type="number" min="0" step="1" placeholder="160" />
          </div>
        </div>
        <div class="row">
          <div>
            <label for="refine_n_bootstrap">refine_n_bootstrap</label>
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
            <label for="submit_response">submit response</label>
            <div id="submit_response" class="mono">{}</div>
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
        <label>status</label>
        <div id="status_box" class="mono">{}</div>
        <label>summary</label>
        <div id="summary_box" class="mono">{}</div>
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

    function makeDefaultRunId() {
      const now = new Date();
      const iso = now.toISOString().replace(/[-:TZ.]/g, "").slice(0, 14);
      return "ui_run_" + iso;
    }

    function asIntOrZero(value) {
      const n = parseInt(value, 10);
      return Number.isFinite(n) && n >= 0 ? n : 0;
    }

    function prettyJson(obj) {
      return JSON.stringify(obj || {}, null, 2);
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
        .split("\\n")
        .map((v) => v.trim())
        .filter((v) => v.length > 0);
      return {
        mode: byId("mode").value,
        run_id: byId("run_id").value.trim(),
        scan_n_bootstrap: asIntOrZero(byId("scan_n_bootstrap").value),
        scan_max_features: asIntOrZero(byId("scan_max_features").value),
        refine_n_bootstrap: asIntOrZero(byId("refine_n_bootstrap").value),
        skip_direction_review: byId("skip_direction_review").checked,
        idempotency_key: byId("idempotency_key").value.trim(),
        extra_args: lines,
      };
    }

    function statusPill(state) {
      const cls = "status-pill state-" + String(state || "").toLowerCase();
      return '<span class="' + cls + '">' + String(state || "unknown") + "</span>";
    }

    async function submitRun() {
      const payload = payloadFromForm();
      if (!payload.run_id) {
        byId("submit_response").textContent = "run_id is required";
        return;
      }
      const execute = byId("execute").checked ? "true" : "false";
      const dryRun = byId("dry_run").checked ? "true" : "false";
      try {
        const out = await fetchJson("/runs?execute=" + execute + "&dry_run=" + dryRun, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        byId("submit_response").textContent = prettyJson(out);
        if (out && out.status && out.status.run_id) {
          byId("detail_run_id").value = out.status.run_id;
        }
        await refreshRuns();
      } catch (err) {
        byId("submit_response").textContent = String(err.message || err);
      }
    }

    async function inspectRun(runId) {
      const rid = String(runId || byId("detail_run_id").value || "").trim();
      if (!rid) return;
      byId("detail_run_id").value = rid;
      const status = await fetchJson("/runs/" + encodeURIComponent(rid));
      byId("status_box").textContent = prettyJson(status);
      const summary = await fetchJson("/runs/" + encodeURIComponent(rid) + "/summary");
      byId("summary_box").textContent = prettyJson(summary);
    }

    async function refreshRuns() {
      const state = byId("detail_status_filter").value.trim();
      const query = state ? ("?state=" + encodeURIComponent(state) + "&limit=200") : "?limit=200";
      const payload = await fetchJson("/runs" + query);
      const rows = payload.rows || [];
      const tbody = byId("runs_tbody");
      tbody.innerHTML = "";
      for (const row of rows) {
        const tr = document.createElement("tr");
        const counts = row.counts ? JSON.stringify(row.counts) : "{}";
        tr.innerHTML =
          "<td>" + row.run_id + "</td>" +
          "<td>" + row.mode + "</td>" +
          "<td>" + statusPill(row.state) + "</td>" +
          "<td>" + row.attempt + "</td>" +
          "<td>" + row.updated_at_utc + "</td>" +
          "<td><div class='mono' style='max-height:80px;'>" + counts + "</div></td>" +
          "<td>" +
            "<button class='mini' data-action='inspect' data-run-id='" + row.run_id + "'>inspect</button>" +
            "<button class='mini' data-action='cancel' data-run-id='" + row.run_id + "'>cancel</button>" +
            "<button class='mini' data-action='retry' data-run-id='" + row.run_id + "'>retry</button>" +
          "</td>";
        tbody.appendChild(tr);
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
        await fetchJson("/runs/" + encodeURIComponent(rid) + "/cancel?reason=ui_request", { method: "POST" });
      } else if (action === "retry") {
        await fetchJson("/runs/" + encodeURIComponent(rid) + "/retry", { method: "POST" });
      }
      await refreshRuns();
      await inspectRun(rid);
    }

    async function refreshHealth() {
      try {
        const payload = await fetchJson("/healthz");
        byId("health_text").textContent = "ok=true, run_count=" + payload.run_count + ", utc=" + payload.timestamp_utc;
      } catch (err) {
        byId("health_text").textContent = "error: " + String(err.message || err);
      }
    }

    byId("submit_btn").addEventListener("click", submitRun);
    byId("new_run_id_btn").addEventListener("click", () => {
      byId("run_id").value = makeDefaultRunId();
    });
    byId("inspect_btn").addEventListener("click", () => inspectRun());
    byId("refresh_runs_btn").addEventListener("click", refreshRuns);
    byId("runs_tbody").addEventListener("click", async (evt) => {
      const target = evt.target;
      if (!target || !target.dataset) return;
      const action = target.dataset.action;
      const runId = target.dataset.runId;
      if (!action || !runId) return;
      try {
        await runAction(action, runId);
      } catch (err) {
        byId("submit_response").textContent = String(err.message || err);
      }
    });

    byId("run_id").value = makeDefaultRunId();
    refreshHealth();
    refreshRuns();
    setInterval(() => {
      if (byId("auto_refresh").checked) {
        refreshHealth();
        refreshRuns();
      }
    }, 4000);
  </script>
</body>
</html>
        """
        .replace("__MODE_OPTIONS__", mode_options)
        .replace("__STATE_OPTIONS__", state_options)
    )

