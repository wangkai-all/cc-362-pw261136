"use strict";

const $ = (sel) => document.querySelector(sel);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
}[c]));

let currentTask = null;
let pollTimer = null;

async function api(path, body) {
  const res = await fetch(path, {
    method: body === undefined ? "GET" : "POST",
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data.error || `HTTP ${res.status}`);
    err.data = data;
    throw err;
  }
  return data;
}

function pill(ok, text) {
  return `<span class="pill ${ok ? "good" : "bad"}">${esc(text)}</span>`;
}

function short(sha) {
  return sha ? sha.slice(0, 12) : "—";
}

function kv(pairs) {
  return `<div class="kv">${pairs.map(([k, v]) =>
    `<span>${esc(k)}</span><span>${v}</span>`).join("")}</div>`;
}

async function refresh() {
  try {
    const data = await api("/api/overview");
    renderStatus(data);
    renderConfig(data);
    renderEntries(data);
    renderMock(data);
    if (currentTask) {
      try {
        const task = await api(`/api/task/${currentTask}`);
        renderTask(task);
        await loadEvidence(currentTask);
        if (["committed", "aborted"].includes(task.state)) {
          renderEvents();
        }
      } catch (err) {
        $("#task").innerHTML = `<p class="hint">task error: ${esc(err.message)}</p>`;
      }
    }
    renderEvents(data);
  } catch (err) {
    $("#statusline").textContent = `console error: ${err.message}`;
  }
}

function renderStatus(data) {
  const cfg = data.published?.body;
  const entries = data.entries?.entries || [];
  const down = entries.filter((e) => !e.reachable).length;
  const match = entries.filter((e) => e.matches_published).length;
  $("#statusline").textContent = cfg
    ? `published ${cfg.version} · body ${short(cfg.body_sha256)} · ` +
      `${entries.length - down}/${entries.length} entries reachable · ` +
      `${match}/${entries.length} confirm published`
    : "no published configuration";
}

function renderConfig(data) {
  const cfg = data.published?.body;
  if (!cfg) {
    $("#current-config").textContent = "nothing published";
    return;
  }
  $("#current-config").textContent = JSON.stringify({
    version: cfg.version,
    body_sha256: cfg.body_sha256,
    fingerprint: cfg.fingerprint,
    scope_hash: cfg.scope_hash,
    mapping: cfg.mapping,
    scope: cfg.scope,
  }, null, 2);
}

function renderEntries(data) {
  const entries = data.entries?.entries || [];
  if (!entries.length) {
    $("#entries").innerHTML = `<p class="hint">no entries registered</p>`;
    return;
  }
  $("#entries").innerHTML = entries.map((e) => {
    const obs = e.observation || {};
    const reach = e.reachable;
    const linkUp = e.prepared_state?.link_up;
    const prepared = e.prepared_state?.prepared || [];
    return `<div class="entry">
      <div class="head">
        <span><span class="name">${esc(e.entry_id)}</span>
          <span class="kind">${esc(e.kind)} · ${esc(e.url)}</span></span>
        <span>
          ${reach ? pill(true, "process up") : pill(false, "process gone")}
          ${linkUp === false ? pill(false, "link severed") :
            (linkUp === true ? `<span class="pill">link up</span>` : "")}
          ${e.matches_published ? pill(true, "matches") :
            (obs.body_sha256 ? pill(false, "differs") : `<span class="pill">unseen</span>`)}
        </span>
      </div>
      ${kv([
        ["source", esc(JSON.stringify(e.source))],
        ["observed version", esc(obs.version || "—")],
        ["observed body", esc(short(obs.body_sha256))],
        ["config read", esc(obs.source || "—")],
        ["mapping fp", esc(short(obs.fingerprint))],
        ["scope hash", esc(short(obs.scope_hash))],
        ["prepared dirs", esc(prepared.length ? prepared.map(
          (p) => `${p.task_id} (${p.record_count ?? "?"} rows)`).join(", ") : "none")],
      ])}
      <div class="row">
        <button class="secondary" data-outage="${esc(e.entry_id)}"
          data-down="${linkUp === false ? "false" : "true"}"
          ${reach ? "" : "disabled"}>
          ${linkUp === false ? "Restore link" : "Sever link"}
        </button>
      </div>
    </div>`;
  }).join("");
  document.querySelectorAll("[data-outage]").forEach((btn) => {
    btn.onclick = async () => {
      btn.disabled = true;
      try {
        await api("/api/outage", {
          entry: btn.dataset.outage, down: btn.dataset.down === "true" });
      } finally {
        btn.disabled = false;
        await refresh();
      }
    };
  });
}

function renderMock(data) {
  const mock = data.mock || {};
  const outage = mock.outage || {};
  const down = Object.entries(outage).filter(([, v]) => v).map(([k]) => k);
  $("#mock-state").textContent = down.length
    ? `provider links severed: ${down.join(", ")}` : "";
}

function renderTask(task) {
  const statePill = task.state === "committed" ? pill(true, task.state)
    : task.state === "aborted" ? pill(false, task.state)
    : task.state === "ready" ? pill(false, "ready (staged)")
    : `<span class="pill warn">${esc(task.state)}</span>`;
  const entries = task.entries.map((e) => {
    const v = e.verification || {};
    const cfg = e.config_evidence || {};
    return `<div class="entry">
      <div class="head"><span class="name">${esc(e.entry_id)}</span>
        ${e.state === "verified" ? pill(true, "verified")
          : e.state === "failed" ? pill(false, "failed")
          : `<span class="pill">${esc(e.state)}</span>`}</div>
      ${kv([
        ["rows stored", esc(`${e.stored_rows} (published ${e.published_rows})`)],
        ["config body", esc(short(cfg.body_sha256))],
        ["config read", esc(cfg.source || "—")],
        ["source bytes", esc(short((e.source_evidence || {}).raw_sha256))],
        ["digest", esc(short(e.digest))],
        ["re-read digest", esc(short(v.re_read_digest))],
        ["error", esc(e.error || "—")],
      ])}
    </div>`;
  }).join("");
  const actions = task.state === "ready"
    ? `<div class="row">
         <button id="commit-btn">Publish staged rows</button>
         <button id="abort-btn" class="secondary">Abort &amp; withdraw</button>
       </div>`
    : ["pending", "confirming", "preparing", "verifying"].includes(task.state)
      ? `<div class="row"><button id="abort-btn" class="secondary">
           Abort &amp; withdraw</button></div>` : "";
  $("#task").innerHTML = `
    <div class="task-state">${statePill}
      <span class="hint">${esc(task.task_id)} · gate ${esc(task.gate)} ·
      publish ${esc(task.publish_mode)} · pinned ${esc(short(task.body_sha256))}</span></div>
    <div class="hint">${esc(task.note || "")}</div>
    <div class="entry-grid">${entries}</div>
    <div class="hint">stored ${task.stored_rows} · published ${task.published_rows}
      ${task.rollbacks_pending ? ` · rollbacks pending ${task.rollbacks_pending}` : ""}</div>
    ${actions}`;
  const commitBtn = $("#commit-btn");
  if (commitBtn) commitBtn.onclick = async () => {
    commitBtn.disabled = true;
    try { await api(`/api/task/${task.task_id}/commit`, {}); }
    catch (err) { alert(`publish refused: ${err.message}`); }
    await refresh();
  };
  const abortBtn = $("#abort-btn");
  if (abortBtn) abortBtn.onclick = async () => {
    abortBtn.disabled = true;
    try { await api(`/api/task/${task.task_id}/abort`, { reason: "console abort" }); }
    catch (err) { alert(err.message); }
    await refresh();
  };
}

async function loadEvidence(taskId) {
  const task = await api(`/api/task/${taskId}`);
  $("#evidence").innerHTML = task.entries.map((e) => {
    const cfg = e.config_evidence || {};
    const src = e.source_evidence || {};
    const ver = e.verification || {};
    return `<div class="evidence-entry">
      <div class="head"><span class="name">${esc(e.entry_id)}</span>
        <span class="hint">${esc(task.task_id)}</span></div>
      ${kv([
        ["entry digest", esc(e.digest || "—")],
        ["config version", esc(cfg.version || "—")],
        ["config body sha", esc(cfg.body_sha256 || "—")],
        ["config etag", esc(cfg.etag || "—")],
        ["config read at", esc(cfg.read_at || "—")],
        ["config read id", esc(cfg.read_id || "—")],
        ["source sha", esc(src.raw_sha256 || "—")],
        ["source urls", esc((src.urls || []).join(" ") || "—")],
        ["re-read sha", esc(ver.re_read_source_sha256 || "—")],
        ["re-read digest", esc(ver.re_read_digest || "—")],
        ["verified", esc(ver.ok ? `yes at ${ver.checked_at}` : "no")],
      ])}
      <details><summary>config evidence JSON</summary>
        <pre>${esc(JSON.stringify(cfg, null, 2))}</pre></details>
      <details><summary>source evidence JSON</summary>
        <pre>${esc(JSON.stringify(src, null, 2))}</pre></details>
      <details><summary>verification JSON</summary>
        <pre>${esc(JSON.stringify(ver, null, 2))}</pre></details>
    </div>`;
  }).join("") || `<p class="hint">no entries yet</p>`;
}

function renderEvents(data) {
  const events = data.events?.events || [];
  $("#events").innerHTML = events.slice(-80).map((e) =>
    `<li><span class="k">${esc(e.kind)}</span>
      ${esc(e.entry_id || "")} ${esc(e.version || "")}
      ${esc(e.detail)}</li>`).join("");
}

$("#publish-btn").onclick = async () => {
  const btn = $("#publish-btn");
  btn.disabled = true;
  try {
    const result = await api("/api/config/publish", { preset: $("#preset").value });
    $("#config-result").textContent = JSON.stringify(result.published, null, 2);
  } catch (err) {
    $("#config-result").textContent = `refused: ${err.message}`;
  } finally {
    btn.disabled = false;
    await refresh();
  }
};

$("#partial-btn").onclick = async () => {
  try {
    await api("/api/config/partial", {
      mapping: { id: "sku", title: "label", price: "price_cents" } });
    $("#config-result").textContent = "UNEXPECTED: partial update accepted";
  } catch (err) {
    $("#config-result").textContent = `correctly refused: ${err.message}`;
  }
};

$("#observe-btn").onclick = async () => {
  const btn = $("#observe-btn");
  btn.disabled = true;
  try {
    const result = await api("/api/observe", {});
    const failed = result.results.filter((r) => !r.ok);
    $("#statusline").textContent = failed.length
      ? `observe: ${failed.length} entr${failed.length === 1 ? "y" : "ies"} failed`
      : "observe: all entries confirmed live";
  } catch (err) {
    $("#statusline").textContent = `observe failed: ${err.message}`;
  } finally {
    btn.disabled = false;
    await refresh();
  }
};

$("#reconnect-btn").onclick = async () => {
  for (const entry of ["api", "file", "page"]) {
    await api("/api/outage", { entry, down: false }).catch(() => {});
  }
  await refresh();
};

$("#run-btn").onclick = async () => {
  const btn = $("#run-btn");
  btn.disabled = true;
  try {
    const task = await api("/api/task", {
      gate: $("#gate").value, publish: $("#publish-mode").value });
    currentTask = task.task_id;
    renderTask(task);
  } catch (err) {
    $("#task").innerHTML = `<p class="hint">refused: ${esc(err.message)}</p>`;
  } finally {
    btn.disabled = false;
    await refresh();
  }
};

setInterval(refresh, 1500);
refresh();
