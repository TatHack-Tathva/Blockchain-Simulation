"use strict";
// Lightweight polling dashboard. Every value that can come from other nodes (names, ids,
// keys) is rendered with textContent - never innerHTML - so peers can't inject markup.
const POLL_MS = 2000;
const $ = (id) => document.getElementById(id);
let selectedBlock = null;
let consensus = null;

function el(tag, text, cls) {
  const e = document.createElement(tag);
  if (text !== undefined && text !== null) e.textContent = String(text);
  if (cls) e.className = cls;
  return e;
}
function short(s, n = 12) { return s ? (s.length > n ? s.slice(0, n) + "…" : s) : "—"; }
function keyLabel(name, key) { return name || (key ? short(key.replace(/-----[^-]+-----|\s/g, ""), 14) : "—"); }
function fmtTime(ms) { return ms ? new Date(ms).toLocaleTimeString() : "—"; }

async function api(path, options) {
  const r = await fetch(path, Object.assign({ headers: { "Content-Type": "application/json" } }, options || {}));
  const data = await r.json().catch(() => ({}));
  return { status: r.status, data };
}

function fillDl(dl, rows) {
  dl.replaceChildren();
  for (const [k, v] of rows) { dl.append(el("dt", k), el("dd", v === undefined || v === null || v === "" ? "—" : v)); }
}

function fillRows(tbody, rows, onClick) {
  tbody.replaceChildren();
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const cell of row.cells) {
      const td = el("td", cell.text, cell.cls);
      if (cell.title) td.title = cell.title;
      tr.append(td);
    }
    if (onClick) { tr.className = "clickable"; tr.addEventListener("click", () => onClick(row.key)); }
    tbody.append(tr);
  }
}

function renderNode(n) {
  consensus = n.consensus;
  document.title = `${n.name} · ${n.consensus.toUpperCase()}`;
  $("title").textContent = n.name;
  $("consensus-badge").textContent = n.consensus.toUpperCase();
  $("role-badge").textContent = n.role;
  $("role-badge").className = "badge " + n.role;
  $("state").textContent = "state: " + n.state;
  $("height").textContent = `(height ${n.height})`;
  fillDl($("node-info"), [
    ["Node ID", n.node_id], ["Address", n.host], ["Port", n.port], ["Consensus", n.consensus],
    ["Status", n.role], ["Chain height", n.height], ["Balance", n.balance], ["Pending txs", n.mempool_size],
    ["Genesis", short(n.genesis_hash, 16)], ["Parameters", JSON.stringify(n.consensus_params)],
  ]);
  const extra = [];
  if (n.consensus === "pos") {
    extra.push(["Staker", n.staker ? "yes" : "no"], ["Staked this epoch", n.staked_amt],
      ["Epoch", `${n.seconds_since_epoch_start}s / ${n.epoch_time}s`],
      ["Current stakers", (n.current_stakers || []).map((s) => `${keyLabel(s.name, s.staker)}:${s.amount}`).join(", ")],
      ["Slashed blocks", (n.slashed_blocks || []).length]);
    $("stake-form").classList.toggle("hidden", !n.staker);
  } else if (n.consensus === "poa") {
    extra.push(["Authority", n.miner ? "yes" : "no"], ["Admin", n.is_admin ? "this node" : short(n.admin_node_id)],
      ["Authorities", (n.authorities || []).map((a) => a.name || short(a.node_id)).join(", ")],
      ["Round time", `${n.round_time}s`]);
  } else {
    extra.push(["Miner", n.miner ? "yes" : "no"], ["Difficulty", n.difficulty]);
  }
  const dl = el("dl"); fillDl(dl, extra); $("consensus-extra").replaceChildren(dl);
  const s = n.signalling;
  fillDl($("room-info"), [
    ["Room", n.room], ["Signalling", s ? s.url : "not configured"], ["State", s ? s.state : "—"],
    ["Members", s ? s.members : "—"], ["Connects", s ? s.connects : "—"], ["Last error", s ? s.last_error : ""],
  ]);
  const rej = $("rejections"); rej.replaceChildren();
  for (const r of (n.recent_rejections || []).slice().reverse()) {
    rej.append(el("li", `${new Date(r.ts * 1000).toLocaleTimeString()} ${r.what}: ${r.reason}`));
  }
}

function renderNetwork(net) {
  $("conn-counts").textContent = `(inbound ${net.connections.inbound}, outbound ${net.connections.outbound})`;
  fillRows($("peers"), net.peers.map((p) => ({ cells: [
    { text: p.name }, { text: short(p.node_id), title: p.node_id }, { text: p.host }, { text: p.port },
    { text: p.consensus || "—" }, { text: p.role, cls: p.role === "malicious" ? "bad" : "ok" },
    { text: p.connected ? [p.inbound ? "in" : "", p.outbound ? "out" : ""].filter(Boolean).join("+") || "yes" : "disconnected",
      cls: p.connected ? "ok" : "bad" },
    { text: p.in_room ? "yes" : "no" },
  ] })));
  const dl = $("peer-names"); dl.replaceChildren();
  for (const p of net.peers) { const o = document.createElement("option"); o.value = p.name; dl.append(o); }
}

function renderBlocks(blocks) {
  fillRows($("blocks"), blocks.map((b) => ({ key: b.height, cells: [
    { text: b.height }, { text: short(b.hash, 16), title: b.hash },
    { text: keyLabel(b.creator_name, b.creator), title: b.creator || "" },
    { text: b.validator ? keyLabel(b.creator_name, b.validator) : "—" },
    { text: b.tx_count }, { text: fmtTime(b.ts) },
  ] })), showBlock);
}

async function showBlock(height) {
  selectedBlock = height;
  const { status, data } = await api(`/api/blocks/${height}`);
  const box = $("block-detail");
  if (status !== 200) { box.classList.add("hidden"); return; }
  box.replaceChildren(el("h2", `Block ${data.height}`));
  const dl = el("dl");
  fillDl(dl, [["Hash", data.hash], ["Previous", data.prev_hash], ["Creator", keyLabel(data.creator_name, data.creator)],
    ["Validator", data.validator ? keyLabel(data.creator_name, data.validator) : "—"],
    ["Transactions", data.tx_count], ["Time", fmtTime(data.ts)]]);
  box.append(dl);
  const table = el("table"); const head = el("thead"); const hr = el("tr"); const tb = el("tbody");
  for (const h of ["ID", "From", "To", "Amount", "Type", "Status", "Block"]) hr.append(el("th", h));
  head.append(hr); table.append(head, tb);
  fillRows(tb, data.transactions.map(txRow));
  box.append(table);
  box.classList.remove("hidden");
}

function txRow(t) {
  return { cells: [
    { text: short(t.id, 10), title: t.id }, { text: keyLabel(t.sender_name, t.sender === "Genesis" ? "Genesis" : t.sender) },
    { text: t.type === "transfer" || t.type === "genesis" ? keyLabel(t.receiver_name, t.receiver) : t.receiver },
    { text: t.amount }, { text: t.type }, { text: t.status, cls: t.status === "confirmed" ? "ok" : "" },
    { text: t.block_height ?? "—" },
  ] };
}

async function refresh() {
  try {
    const [node, net, blocks, txs] = await Promise.all([
      api("/api/node"), api("/api/network"), api("/api/blocks?limit=25"), api("/api/transactions?limit=25")]);
    renderNode(node.data); renderNetwork(net.data); renderBlocks(blocks.data.blocks);
    fillRows($("transactions"), txs.data.transactions.map(txRow));
    $("conn-status").textContent = "live"; $("conn-status").className = "status ok";
  } catch (e) {
    $("conn-status").textContent = "backend unreachable"; $("conn-status").className = "status bad";
  }
}

async function refreshRooms() {
  const { status, data } = await api("/api/rooms");
  if (status !== 200) return;
  fillRows($("rooms"), (data.rooms || []).map((r) => ({ cells: [{ text: r.room }, { text: r.consensus }, { text: r.members }] })));
}

function show(target, ok, message) { target.textContent = message; target.className = "result " + (ok ? "ok" : "bad"); }

$("tx-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const amount = Number($("tx-amount").value);
  const { status, data } = await api("/api/transactions", { method: "POST",
    body: JSON.stringify({ receiver: $("tx-receiver").value, amount }) });
  show($("tx-result"), status === 201, status === 201 ? `accepted: ${data.transaction.id}` : `rejected: ${data.error}`);
  refresh();
});

$("stake-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const { status, data } = await api("/api/stake", { method: "POST",
    body: JSON.stringify({ amount: parseInt($("stake-amount").value, 10) }) });
  show($("stake-result"), status === 201, status === 201 ? `staked ${data.stake.amount}` : data.error);
  refresh();
});

$("room-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const action = ev.submitter && ev.submitter.dataset.action === "join" ? "join" : "create";
  const { status, data } = await api(`/api/rooms/${action}`, { method: "POST",
    body: JSON.stringify({ room: $("room-name").value }) });
  show($("room-result"), status === 200, status === 200 ? `${action}d room ${data.room}` : `${data.code || ""} ${data.error}`);
  refresh(); refreshRooms();
});

refresh(); refreshRooms();
setInterval(refresh, POLL_MS);
setInterval(refreshRooms, POLL_MS * 5);
