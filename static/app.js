"use strict";

// Plain DOM + canvas. No build step, no dependencies.

const $ = (sel) => document.querySelector(sel);
const el = (tag, attrs = {}, text) => {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  if (text !== undefined) n.textContent = text;
  return n;
};

const fmt = (n, digits = 2) =>
  n === null || n === undefined || Number.isNaN(n)
    ? "—"
    : Number(n).toLocaleString(undefined, { maximumFractionDigits: digits });

const shortKey = (k) => (k && k.length > 16 ? `${k.slice(0, 6)}…${k.slice(-6)}` : k || "—");
const clockTime = (ts) => new Date(ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(body.error || `${r.status} ${r.statusText}`);
  }
  return r.json();
}

// --------------------------------------------------------------------------
// Tabs
// --------------------------------------------------------------------------

document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((p) => p.classList.add("hidden"));
    btn.classList.add("active");
    $(`#tab-${btn.dataset.tab}`).classList.remove("hidden");
    if (btn.dataset.tab === "ohlcv") loadTokens();
  });
});

// --------------------------------------------------------------------------
// Status polling
// --------------------------------------------------------------------------

let ingestWasRunning = false;

function statCard(label, value) {
  const c = el("div", { class: "stat" });
  c.append(el("div", { class: "label" }, label), el("div", { class: "value" }, value));
  return c;
}

async function pollStatus() {
  try {
    const s = await getJSON("/api/status");
    const settled = s.slots_done + s.slots_skipped + s.slots_failed + s.slots_from_previous_run;
    const pct = s.slots_total ? Math.min(100, (settled / s.slots_total) * 100) : 0;
    $("#progress-bar").style.width = `${pct}%`;

    const parts = [
      s.ingest_done ? "ingest complete" : `ingesting ${fmt(pct, 1)}%`,
      `${fmt(settled, 0)}/${fmt(s.slots_total, 0)} slots`,
      `${fmt(s.transactions, 0)} txs`,
      `${fmt(s.trades, 0)} trades`,
      `${fmt(s.slots_per_s, 2)} slots/s`,
    ];
    if (s.writer_paused) parts.push("WRITER PAUSED");
    if (s.slots_failed) parts.push(`${s.slots_failed} failed`);
    $("#status-line").textContent = parts.join(" · ");

    renderPipeline(s);

    // Refresh the data views once ingestion finishes.
    if (ingestWasRunning && s.ingest_done) {
      loadContention();
      loadTokens();
    }
    ingestWasRunning = !s.ingest_done;
  } catch (e) {
    $("#status-line").textContent = `status unavailable: ${e.message}`;
  }
}

function renderPipeline(s) {
  const cards = $("#pipeline-cards");
  cards.replaceChildren(
    statCard("Fetch queue", `${s.fetch_queue} (peak ${s.peak_fetch_queue})`),
    statCard("Write queue", `${s.write_queue} (peak ${s.peak_write_queue})`),
    statCard("Writer", s.writer_paused ? "PAUSED" : "running"),
    statCard("Blocks written", fmt(s.blocks_written, 0)),
    statCard("Write batches", fmt(s.write_batches, 0)),
    statCard("Blocks per batch", s.write_batches ? fmt(s.blocks_written / s.write_batches, 1) : "—"),
    statCard("Slots skipped", fmt(s.slots_skipped, 0)),
    statCard("Slots failed", fmt(s.slots_failed, 0)),
    statCard("Elapsed", `${fmt(s.elapsed_s, 1)} s`)
  );

  const tbody = $("#exclusions").querySelector("tbody");
  tbody.replaceChildren();
  for (const x of s.exclusions || []) {
    const tr = el("tr");
    tr.append(el("td", {}, x.reason), el("td", {}, fmt(x.count, 0)));
    tbody.append(tr);
  }
}

// --------------------------------------------------------------------------
// Contention
// --------------------------------------------------------------------------

async function loadContention() {
  const from = $("#from-slot").value;
  const to = $("#to-slot").value;
  const qs = new URLSearchParams();
  if (from) qs.set("from", from);
  if (to) qs.set("to", to);

  try {
    const d = await getJSON(`/api/contention?${qs}`);
    const s = d.summary;

    $("#contention-summary").replaceChildren(
      statCard("Slots analysed", fmt(s.slots, 0)),
      statCard("Transactions", fmt(s.transactions, 0)),
      statCard("Avg depth", fmt(s.avg_depth, 1)),
      statCard("Max depth", fmt(s.max_depth, 0)),
      statCard("Avg parallelism", `${fmt(s.avg_parallelism, 2)}×`),
      statCard("Txs excl. votes", fmt(s.transactions_novote, 0)),
      statCard("Avg depth excl. votes", fmt(s.avg_depth_novote, 1)),
      statCard("Avg parallelism excl. votes", `${fmt(s.avg_parallelism_novote, 2)}×`)
    );

    fillRows($("#depth-hist"), d.depth_histogram, (b) => [b.bucket, fmt(b.count, 0)]);
    fillRows($("#top-accounts"), d.top_accounts, (a) => [
      { text: shortKey(a.account), cls: "mono", title: a.account },
      fmt(a.delays, 0),
      fmt(a.write_locks, 0),
      fmt(a.read_locks, 0),
      fmt(a.slots, 0),
    ]);
    fillRows($("#top-programs"), d.top_programs, (p) => [
      { text: shortKey(p.program), cls: "mono", title: p.program },
      fmt(p.delays, 0),
    ]);
    fillRows($("#slot-table"), d.slots, (r) => [
      String(r.slot),
      fmt(r.tx_count, 0),
      fmt(r.depth, 0),
      `${fmt(r.parallelism, 2)}×`,
      fmt(r.tx_count_novote, 0),
      fmt(r.depth_novote, 0),
      `${fmt(r.parallelism_novote, 2)}×`,
    ]);
  } catch (e) {
    $("#status-line").textContent = `contention query failed: ${e.message}`;
  }
}

function fillRows(table, rows, mapper) {
  const tbody = table.querySelector("tbody");
  tbody.replaceChildren();
  for (const row of rows || []) {
    const tr = el("tr");
    for (const cell of mapper(row)) {
      if (typeof cell === "object") {
        const td = el("td", { class: cell.cls || "" }, cell.text);
        if (cell.title) td.title = cell.title;
        tr.append(td);
      } else {
        tr.append(el("td", {}, cell));
      }
    }
    tbody.append(tr);
  }
}

$("#reload-contention").addEventListener("click", loadContention);

// --------------------------------------------------------------------------
// Tokens and candles
// --------------------------------------------------------------------------

let tokensLoaded = false;

async function loadTokens() {
  try {
    const tokens = await getJSON("/api/tokens");
    const select = $("#mint-select");
    const previous = select.value;

    select.replaceChildren();
    for (const t of tokens) {
      select.append(
        el("option", { value: t.mint }, `${shortKey(t.mint)} — ${fmt(t.trades, 0)} trades, ${fmt(t.volume_sol, 2)} SOL`)
      );
    }
    if (previous && tokens.some((t) => t.mint === previous)) select.value = previous;

    fillRows($("#token-table"), tokens, (t) => [
      { text: shortKey(t.mint), cls: "mono", title: t.mint },
      fmt(t.trades, 0),
      fmt(t.volume_sol, 3),
      fmt(t.last_price_sol, 9),
    ]);

    document.querySelectorAll("#token-table tbody tr").forEach((tr, i) => {
      tr.classList.add("clickable");
      tr.addEventListener("click", () => {
        select.value = tokens[i].mint;
        loadCandles();
      });
    });

    if (!tokensLoaded && tokens.length) {
      tokensLoaded = true;
      loadCandles();
    } else if (tokens.length) {
      loadCandles();
    } else {
      $("#candle-info").textContent = "no priced tokens yet";
      drawChart([]);
    }
  } catch (e) {
    $("#candle-info").textContent = `token list failed: ${e.message}`;
  }
}

async function loadCandles() {
  const mint = $("#mint-select").value;
  const interval = $("#interval-select").value;
  if (!mint) return;

  try {
    const d = await getJSON(`/api/ohlcv?mint=${encodeURIComponent(mint)}&interval=${interval}`);
    $("#candle-info").textContent = `${d.candles.length} ${interval} candles · ${shortKey(mint)}`;
    drawChart(d.candles);
  } catch (e) {
    $("#candle-info").textContent = `candles failed: ${e.message}`;
    drawChart([]);
  }
}

$("#mint-select").addEventListener("change", loadCandles);
$("#interval-select").addEventListener("change", loadCandles);

// --------------------------------------------------------------------------
// Candlestick chart, drawn by hand on a canvas
// --------------------------------------------------------------------------

function drawChart(candles) {
  const canvas = $("#chart");
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const cssWidth = canvas.clientWidth || 1200;
  const cssHeight = 460;

  canvas.width = cssWidth * dpr;
  canvas.height = cssHeight * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssWidth, cssHeight);

  const css = getComputedStyle(document.documentElement);
  const colors = {
    line: css.getPropertyValue("--line").trim() || "#2a2f3a",
    muted: css.getPropertyValue("--muted").trim() || "#99a1b3",
    up: css.getPropertyValue("--up").trim() || "#2ebd85",
    down: css.getPropertyValue("--down").trim() || "#e35d6a",
  };

  if (!candles.length) {
    ctx.fillStyle = colors.muted;
    ctx.font = "13px ui-sans-serif, sans-serif";
    ctx.textAlign = "center";
    ctx.fillText("no candles for this selection", cssWidth / 2, cssHeight / 2);
    return;
  }

  const pad = { left: 76, right: 16, top: 16, bottom: 28 };
  const volumeHeight = 90;
  const priceHeight = cssHeight - pad.top - pad.bottom - volumeHeight - 12;
  const plotWidth = cssWidth - pad.left - pad.right;

  let hi = -Infinity;
  let lo = Infinity;
  let maxVol = 0;
  for (const c of candles) {
    hi = Math.max(hi, c.high);
    lo = Math.min(lo, c.low);
    maxVol = Math.max(maxVol, c.volume_sol);
  }
  if (hi === lo) {
    hi *= 1.05;
    lo *= 0.95;
  }
  const span = hi - lo || 1;
  hi += span * 0.06;
  lo = Math.max(0, lo - span * 0.06);

  const yPrice = (p) => pad.top + priceHeight - ((p - lo) / (hi - lo)) * priceHeight;
  const slot = plotWidth / candles.length;
  const bodyWidth = Math.max(1, Math.min(14, slot * 0.62));

  // Price grid and axis labels.
  ctx.strokeStyle = colors.line;
  ctx.fillStyle = colors.muted;
  ctx.lineWidth = 1;
  ctx.font = "11px ui-monospace, monospace";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let i = 0; i <= 4; i++) {
    const price = lo + ((hi - lo) * i) / 4;
    const y = Math.round(yPrice(price)) + 0.5;
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(cssWidth - pad.right, y);
    ctx.stroke();
    ctx.fillText(price.toPrecision(4), pad.left - 8, y);
  }

  // Candles.
  candles.forEach((c, i) => {
    const x = pad.left + i * slot + slot / 2;
    const rising = c.close >= c.open;
    const color = rising ? colors.up : colors.down;

    ctx.strokeStyle = color;
    ctx.fillStyle = color;

    ctx.beginPath();
    ctx.moveTo(Math.round(x) + 0.5, yPrice(c.high));
    ctx.lineTo(Math.round(x) + 0.5, yPrice(c.low));
    ctx.stroke();

    const yOpen = yPrice(c.open);
    const yClose = yPrice(c.close);
    const top = Math.min(yOpen, yClose);
    const height = Math.max(1, Math.abs(yClose - yOpen));
    ctx.fillRect(x - bodyWidth / 2, top, bodyWidth, height);
  });

  // Volume bars.
  const volTop = pad.top + priceHeight + 12;
  ctx.fillStyle = colors.muted;
  ctx.textAlign = "right";
  ctx.fillText(`${maxVol.toPrecision(3)} SOL`, pad.left - 8, volTop + 6);

  candles.forEach((c, i) => {
    const x = pad.left + i * slot + slot / 2;
    const h = maxVol > 0 ? (c.volume_sol / maxVol) * volumeHeight : 0;
    ctx.fillStyle = c.close >= c.open ? colors.up : colors.down;
    ctx.globalAlpha = 0.55;
    ctx.fillRect(x - bodyWidth / 2, volTop + volumeHeight - h, bodyWidth, h);
    ctx.globalAlpha = 1;
  });

  // Time axis: a handful of evenly spaced labels.
  ctx.fillStyle = colors.muted;
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  const ticks = Math.min(8, candles.length);
  for (let i = 0; i < ticks; i++) {
    const idx = Math.floor((i * (candles.length - 1)) / Math.max(1, ticks - 1));
    const x = pad.left + idx * slot + slot / 2;
    ctx.fillText(clockTime(candles[idx].bucket_ts), x, cssHeight - pad.bottom + 8);
  }
}

window.addEventListener("resize", () => {
  const mint = $("#mint-select").value;
  if (mint && !$("#tab-ohlcv").classList.contains("hidden")) loadCandles();
});

// --------------------------------------------------------------------------

pollStatus();
setInterval(pollStatus, 1000);
loadContention();
