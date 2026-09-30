"use strict";
/*
 * Network monitor dashboard.
 *
 * Everything shown comes from the node's existing JSON API. The activity log is derived by
 * comparing consecutive snapshots (it records what this dashboard observed, with the time it
 * observed it), plus the results of actions submitted from this page. Values that can come
 * from other nodes (names, ids, keys, reasons) are always rendered with textContent.
 */
(function () {
  const POLL_MS = 2000;
  const MAX_EVENTS = 20;
  const SVG_NS = "http://www.w3.org/2000/svg";
  const $ = (id) => document.getElementById(id);

  const state = {
    prev: null,            // previous snapshot, for change detection
    lastOk: 0,             // time of the last successful poll
    failing: false,
    events: [],
    seenRejections: new Set(),
    rejectedTotal: 0,
    invalidBlocks: 0,
    invalidTxs: 0,
    localRejected: [],     // transactions rejected when submitted from this page
    expanded: new Set(),   // expanded block hashes in the explorer
    blockDetails: new Map(),
    pollCount: 0,
    roomGenesis: null,
  };

  // ------------------------------------------------------------------ helpers

  function el(tag, text, cls) {
    const e = document.createElement(tag);
    if (text !== undefined && text !== null) e.textContent = String(text);
    if (cls) e.className = cls;
    return e;
  }
  function svg(tag, attrs, text) {
    const e = document.createElementNS(SVG_NS, tag);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    if (text !== undefined) e.textContent = String(text);
    return e;
  }
  function short(s, n) {
    n = n || 12;
    if (s === null || s === undefined || s === "") return "—";
    s = String(s);
    return s.length > n ? s.slice(0, n) + "…" : s;
  }
  function keyShort(pem) {
    if (!pem) return "—";
    if (pem === "Genesis") return "Genesis";
    return short(pem.replace(/-----[^-]+-----|\s/g, "").slice(24), 10);
  }
  function who(name, key) { return name || keyShort(key); }
  function clock(date) { return date.toLocaleTimeString([], { hour12: false }); }
  function fmtMs(ms) { return ms ? clock(new Date(ms)) : "—"; }
  function fmtNum(v) {
    if (v === null || v === undefined || v === "") return "—";
    if (typeof v === "number" && !Number.isInteger(v)) return String(Math.round(v * 1e6) / 1e6);
    return String(v);
  }
  function status(text, cls) { return el("span", text, "st " + cls); }
  function consensusName(c) {
    return { pos: "Proof of Stake", pow: "Proof of Work", poa: "Proof of Authority" }[c] || c;
  }

  function copyButton(value) {
    const b = el("button", "copy", "copy");
    b.type = "button";
    b.title = "Copy to clipboard";
    b.addEventListener("click", (ev) => {
      ev.stopPropagation();
      const done = () => { b.textContent = "copied"; b.classList.add("done");
        setTimeout(() => { b.textContent = "copy"; b.classList.remove("done"); }, 1200); };
      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(value).then(done, () => {});
      } else {
        const t = el("textarea"); t.value = value; document.body.append(t); t.select();
        try { document.execCommand("copy"); done(); } catch (e) { /* ignore */ }
        t.remove();
      }
    });
    return b;
  }
  function hashCell(value, n) {
    const td = el("td");
    if (!value) { td.textContent = "—"; return td; }
    const s = el("span", short(value, n || 16), "hash");
    s.title = value;
    td.append(s, copyButton(value));
    return td;
  }

  function fillKv(dl, rows) {
    dl.replaceChildren();
    for (const [k, v, mono] of rows) {
      const dd = el("dd");
      if (v instanceof Node) dd.append(v);
      else dd.textContent = v === undefined || v === null || v === "" ? "—" : String(v);
      if (mono) dd.className = "mono";
      dl.append(el("dt", k), dd);
    }
  }

  async function api(path, options) {
    const r = await fetch(path, Object.assign({ headers: { "Content-Type": "application/json" }, cache: "no-store" }, options || {}));
    let data = {};
    try { data = await r.json(); } catch (e) { data = {}; }
    return { status: r.status, data };
  }

  // ------------------------------------------------------------ activity log

  function logEvent(kind, cls, message, when) {
    state.events.unshift({ t: when || new Date(), kind, cls, message, fresh: true });
    state.events.length = Math.min(state.events.length, MAX_EVENTS);
  }

  function renderEvents() {
    const ol = $("activity");
    ol.replaceChildren();
    if (!state.events.length) { ol.append(el("li", "No events observed yet.", "empty")); return; }
    for (const e of state.events) {
      const li = el("li");
      if (e.fresh) { li.className = "fresh"; e.fresh = false; }
      li.append(el("span", clock(e.t), "t"), el("span", e.kind, "k " + e.cls), el("span", e.message, "m"));
      ol.append(li);
    }
  }

  function deriveEvents(prev, cur) {
    const n = cur.node, net = cur.network;
    if (!prev) {
      logEvent("MONITOR", "net", `attached to ${n.name} (${n.consensus.toUpperCase()}), height ${n.height}, ` +
        `${net.peers.filter((p) => p.connected).length} connected peer(s)`);
      return;
    }
    const pn = prev.node;

    // signalling
    const ps = pn.signalling ? pn.signalling.state : null, cs = n.signalling ? n.signalling.state : null;
    if (ps !== cs && cs) {
      const cls = cs === "joined" ? "ok" : (cs === "connecting" || cs === "connected" ? "net" : "bad");
      logEvent("SIGNAL", cls, `signalling ${ps || "—"} → ${cs}${n.room ? " (room " + n.room + ")" : ""}`);
    }

    // peers
    const before = new Map(prev.network.peers.map((p) => [p.node_id, p]));
    const after = new Map(net.peers.map((p) => [p.node_id, p]));
    for (const [id, p] of after) {
      const old = before.get(id);
      if (!old) logEvent("PEER", "net", `peer ${p.name} discovered at ${p.host}:${p.port}`);
      if (p.connected && (!old || !old.connected)) {
        const dir = [p.inbound ? "in" : "", p.outbound ? "out" : ""].filter(Boolean).join("+");
        logEvent("P2P", "ok", `direct link established with ${p.name}${dir ? " (" + dir + ")" : ""}`);
      }
      if (!p.connected && old && old.connected) logEvent("P2P", "bad", `link to ${p.name} lost`);
    }
    for (const [id, p] of before) {
      if (!after.has(id)) logEvent("PEER", "bad", `peer ${p.name} removed (left the room / unreachable)`);
    }

    // transactions: new pending, pending -> confirmed
    const prevTx = new Map(prev.transactions.map((t) => [t.id, t]));
    for (const t of cur.transactions.slice().reverse()) {
      const old = prevTx.get(t.id);
      const desc = `${short(t.id, 8)} ${who(t.sender_name, t.sender)} → ${t.type === "transfer" ? who(t.receiver_name, t.receiver) : t.receiver} ${fmtNum(t.amount)}`;
      if (!old && t.status === "pending") logEvent("TX", "tx", `validated, added to mempool: ${desc}`);
      if (t.status === "confirmed" && old && old.status === "pending") {
        logEvent("TX", "ok", `confirmed in block #${t.block_height}: ${short(t.id, 8)}`);
      }
      if (!old && t.status === "confirmed" && t.block_height > pn.height - 1 && t.type !== "genesis") {
        logEvent("TX", "ok", `confirmed in block #${t.block_height}: ${short(t.id, 8)}`);
      }
    }

    // blocks
    if (n.height > pn.height && n.genesis_hash === pn.genesis_hash) {
      const fresh = cur.blocks.filter((b) => b.height >= pn.height).sort((a, b) => a.height - b.height);
      for (const b of fresh) {
        const mine = b.creator && b.creator === n.public_key;
        const role = n.consensus === "pow" ? "miner" : "validator";
        const extra = n.consensus === "pos" && b.staked_amt ? `, stake ${b.staked_amt}/${b.total_stake}` : "";
        logEvent("BLOCK", "blk", mine
          ? `block #${b.height} created by this node (${b.tx_count} tx${extra}), broadcast to peers`
          : `block #${b.height} received and validated — ${role} ${who(b.creator_name, b.creator)}, ${b.tx_count} tx${extra}`);
      }
    } else if (n.genesis_hash && n.genesis_hash !== pn.genesis_hash) {
      logEvent("CHAIN", "blk", `chain synchronised from peers: height ${n.height}`);
    } else if (n.height < pn.height) {
      logEvent("CHAIN", "warn", `chain replaced by fork choice: height ${pn.height} → ${n.height}`);
    }

    // PoS stakes
    if (n.consensus === "pos") {
      const pk = new Map((pn.current_stakers || []).map((s) => [s.staker, s]));
      for (const s of n.current_stakers || []) {
        if (!pk.has(s.staker)) logEvent("STAKE", "pos", `stake registered: ${who(s.name, s.staker)} ${s.amount}`);
      }
      const prevSlashed = (pn.slashed_blocks || []).length, curSlashed = (n.slashed_blocks || []).length;
      if (curSlashed > prevSlashed) logEvent("SLASH", "bad", `double signing evidence: ${curSlashed - prevSlashed} block(s) slashed`);
    }
    if (n.consensus === "poa" && (n.pending_authority_updates || 0) > (pn.pending_authority_updates || 0)) {
      logEvent("AUTH", "pos", "admin-signed authority update received (pending activation)");
    }
  }

  function trackRejections(node) {
    const fresh = [];
    for (const r of node.recent_rejections || []) {
      const key = `${r.ts}|${r.what}|${r.reason}`;
      if (state.seenRejections.has(key)) continue;
      state.seenRejections.add(key);
      state.rejectedTotal += 1;
      if (r.what === "new_block" || r.what === "chain") state.invalidBlocks += 1;
      if (r.what === "new_tx" || r.what === "mempool transaction") state.invalidTxs += 1;
      fresh.push(r);
    }
    return fresh;
  }

  // --------------------------------------------------------------- rendering

  function renderHeader(cur) {
    const n = cur.node, net = cur.network;
    const connected = net.peers.filter((p) => p.connected).length;
    document.title = `${n.name} · ${n.consensus.toUpperCase()} · Network Monitor · Node Dashboard`;
    $("meta-consensus").textContent = n.consensus.toUpperCase();
    $("meta-node").textContent = n.name;
    $("meta-room").textContent = n.room || "—";
    $("meta-online").textContent = n.state === "running" ? "ONLINE" : n.state.toUpperCase();
    $("meta-online-dot").className = "dot " + (n.state === "running" ? "ok" : "warn");
    $("meta-net").textContent = connected ? `Network connected (${connected})` : "No P2P peers";
    $("meta-net-dot").className = "dot " + (connected ? "ok" : "warn");
  }

  function metric(label, value, sub) {
    const d = el("div", null, "metric");
    d.append(el("div", label, "metric-label"), el("div", value, "metric-value"), el("div", sub || " ", "metric-sub"));
    return d;
  }

  function renderMetrics(cur, stats) {
    const n = cur.node, net = cur.network;
    const connected = net.peers.filter((p) => p.connected).length;
    const cells = [
      metric("Block height", n.height, n.height ? `tip ${short(cur.blocks[0] && cur.blocks[0].hash, 10)}` : "no chain"),
      metric("Transactions", stats.confirmed, "confirmed on chain"),
      metric("Pending", n.mempool_size, "in mempool"),
      metric("Peers", `${connected}/${net.peers.length}`, "connected / known"),
      metric("Balance", fmtNum(n.balance), "spendable"),
    ];
    if (n.consensus === "pos") {
      cells.push(metric("Stake", n.staked_amt || 0, n.staker ? "this node, this epoch" : "not a staker"));
      cells.push(metric("Validators", stats.validators, `total stake ${stats.totalStake}`));
    } else if (n.consensus === "poa") {
      cells.push(metric("Authorities", (n.authorities || []).length, n.miner ? "this node is one" : "this node is not one"));
    } else {
      cells.push(metric("Difficulty", n.difficulty, n.miner ? "this node mines" : "not mining"));
    }
    cells.push(metric("Rejected", state.rejectedTotal + state.localRejected.length, "invalid data, this session"));
    const box = $("metrics");
    const old = Array.from(box.querySelectorAll(".metric-value")).map((e) => e.textContent);
    box.replaceChildren(...cells);
    box.querySelectorAll(".metric-value").forEach((e, i) => {
      if (old.length && old[i] !== undefined && old[i] !== e.textContent) {
        e.classList.add("changed");
        setTimeout(() => e.classList.remove("changed"), 900);
      }
    });
  }

  function renderNode(cur) {
    const n = cur.node, net = cur.network;
    const connected = net.peers.filter((p) => p.connected).length;
    $("node-id-short").textContent = short(n.node_id, 13);
    const sig = n.signalling;
    const rows = [
      ["Name", n.name],
      ["Node ID", n.node_id, true],
      ["Address", n.host, true],
      ["P2P port", n.port, true],
      ["Web port", location.port || (location.protocol === "https:" ? "443" : "80"), true],
      ["Consensus", consensusName(n.consensus)],
      ["Status", status(n.state === "running" ? "ONLINE" : n.state.toUpperCase(), n.state === "running" ? "ok" : "warn")],
      ["Role", status(n.role.toUpperCase(), n.malicious ? "bad" : "ok")],
      ["Network", status(connected ? "CONNECTED" : "ISOLATED", connected ? "ok" : "warn")],
      ["Signalling", sig ? status(sig.state.toUpperCase(), sig.state === "joined" ? "ok" : (sig.state === "closed" ? "off" : "warn")) : "not configured"],
      ["Room", n.room, true],
      ["Peers", `${connected} connected, ${net.connections.inbound} in / ${net.connections.outbound} out`],
      ["Public key", keyShort(n.public_key), true],
    ];
    if (n.consensus === "pos") rows.push(["Validator", status(n.staked_amt > 0 ? "YES (staked)" : (n.staker ? "ELIGIBLE" : "NO"), n.staked_amt > 0 ? "ok" : "off")]);
    if (n.consensus === "poa") rows.push(["Authority", status(n.miner ? "YES" : "NO", n.miner ? "ok" : "off")]);
    if (n.consensus === "pow") rows.push(["Miner", status(n.miner ? "YES" : "NO", n.miner ? "ok" : "off")]);
    fillKv($("node-kv"), rows);
  }

  function renderConsensus(cur, stats) {
    const n = cur.node;
    const last = cur.blocks[0];
    $("consensus-caption").textContent = consensusName(n.consensus);
    const bar = $("epoch-bar"), note = $("epoch-note");
    let rows;
    if (n.consensus === "pos") {
      const since = n.seconds_since_epoch_start, dur = n.epoch_time;
      const open = since <= dur * 5 / 6;
      rows = [
        ["Algorithm", "Proof of Stake (stake-weighted lottery)"],
        ["Epoch duration", `${dur} s`, true],
        ["Epoch elapsed", `${Math.min(since, 999).toFixed(1)} s`, true],
        ["Registration", status(open ? "OPEN" : "CLOSED", open ? "ok" : "warn")],
        ["Validators", `${stats.validators} staked this epoch`],
        ["Total stake", stats.totalStake, true],
        ["Stakers", (n.current_stakers || []).map((s) => `${who(s.name, s.staker)}:${s.amount}`).join("  ") || "—", true],
        ["This node", status(n.staked_amt > 0 ? `VALIDATOR (stake ${n.staked_amt})` : (n.staker ? "STAKER, NOT STAKED" : "OBSERVER"),
          n.staked_amt > 0 ? "ok" : "off")],
        ["Last block by", last && last.height > 0 ? `${who(last.creator_name, last.creator)} · stake ${last.staked_amt}/${last.total_stake}` : "—"],
        ["Slashed blocks", (n.slashed_blocks || []).length, true],
      ];
      bar.classList.remove("hidden");
      $("epoch-fill").style.width = `${Math.min(100, (since / dur) * 100)}%`;
      note.textContent = "Bar: epoch progress. Dashed line: stake registration closes at 5/6 of the epoch.";
    } else if (n.consensus === "poa") {
      rows = [
        ["Algorithm", "Proof of Authority (rotating signer slots)"],
        ["Round time", `${n.round_time} s`, true],
        ["Authorities", (n.authorities || []).map((a) => a.name || short(a.node_id, 8)).join(", ")],
        ["Admin", n.is_admin ? "this node" : short(n.admin_node_id, 13), true],
        ["This node", status(n.miner ? "AUTHORITY" : "OBSERVER", n.miner ? "ok" : "off")],
        ["Pending updates", n.pending_authority_updates || 0, true],
        ["Last block by", last && last.height > 0 ? who(last.creator_name, last.creator) : "—"],
      ];
      bar.classList.add("hidden"); note.textContent = "";
    } else {
      rows = [
        ["Algorithm", "Proof of Work (sha256)"],
        ["Difficulty", `${n.difficulty} leading hex zeros`, true],
        ["This node", status(n.miner ? "MINER" : "OBSERVER", n.miner ? "ok" : "off")],
        ["Last block by", last && last.height > 0 ? who(last.creator_name, last.creator) : "—"],
        ["Last nonce", last && last.height > 0 ? last.nonce : "—", true],
      ];
      bar.classList.add("hidden"); note.textContent = "";
    }
    fillKv($("consensus-kv"), rows);
    $("creator-head").textContent = n.consensus === "pow" ? "Miner" : "Validator";
  }

  function renderTopology(cur) {
    const n = cur.node, net = cur.network;
    const root = $("topology");
    root.replaceChildren();
    const W = 640, H = 270;
    const peers = net.peers.slice().sort((a, b) => a.name.localeCompare(b.name));
    const nodes = [{ self: true, name: n.name, consensus: n.consensus, malicious: n.malicious, connected: true,
      in_room: !!(n.signalling && n.signalling.joined), host: n.host, port: n.port }].concat(peers);
    const count = nodes.length;
    const boxW = Math.max(78, Math.min(120, (W - 20) / count - 12)), boxH = 44;
    const y = 190;
    const slot = W / count;
    nodes.forEach((d, i) => { d.x = slot * i + slot / 2; d.y = y; });
    const self = nodes[0];
    const hasSig = !!n.signalling;
    const sig = { x: W / 2, y: 36 };

    const edges = svg("g", {});
    root.append(edges);
    if (hasSig) {
      for (const d of nodes) {
        if (d.in_room) edges.append(svg("line", { x1: sig.x, y1: sig.y + 16, x2: d.x, y2: d.y - boxH / 2, class: "edge-sig" }));
      }
    }
    // P2P links: the node only knows its own links, so edges start at this node
    for (const d of peers) {
      const midY = y + 58;
      const path = `M ${self.x} ${self.y + boxH / 2} C ${self.x} ${midY}, ${d.x} ${midY}, ${d.x} ${d.y + boxH / 2}`;
      edges.append(svg("path", { d: path, fill: "none", class: d.connected ? "edge-p2p" : "edge-down" }));
      const dir = d.connected ? [d.inbound ? "in" : "", d.outbound ? "out" : ""].filter(Boolean).join("+") || "p2p" : "down";
      edges.append(svg("text", { x: (self.x + d.x) / 2, y: midY + 10, "text-anchor": "middle", class: "lbl" }, dir));
    }

    if (hasSig) {
      const g = svg("g", { class: "sig" });
      g.append(svg("rect", { x: sig.x - 90, y: sig.y - 16, width: 180, height: 32, rx: 3 }));
      g.append(svg("text", { x: sig.x, y: sig.y - 1, "text-anchor": "middle" }, "Signalling server"));
      g.append(svg("text", { x: sig.x, y: sig.y + 11, "text-anchor": "middle", class: "sub" },
        `room ${n.room || "—"} · ${n.signalling.state}`));
      root.append(g);
    }

    for (const d of nodes) {
      const cls = ["node", d.self ? "self" : "", d.malicious ? "mal" : "", d.connected ? "" : "offline"].join(" ");
      const g = svg("g", { class: cls });
      const t = svg("title", {}, `${d.name} ${d.host}:${d.port}`);
      g.append(t);
      g.append(svg("rect", { x: d.x - boxW / 2, y: d.y - boxH / 2, width: boxW, height: boxH, rx: 3 }));
      g.append(svg("circle", { cx: d.x - boxW / 2 + 9, cy: d.y - 8, r: 3.5,
        class: d.malicious ? "st-bad" : (d.connected ? "st-ok" : "st-off") }));
      const label = d.name.length > 12 ? d.name.slice(0, 11) + "…" : d.name;
      g.append(svg("text", { x: d.x - boxW / 2 + 17, y: d.y - 4 }, label + (d.self ? " (this)" : "")));
      g.append(svg("text", { x: d.x - boxW / 2 + 8, y: d.y + 12, class: "sub" },
        `${(d.consensus || "?").toUpperCase()} · ${d.malicious ? "MALICIOUS" : "honest"}`));
      root.append(g);
    }
    const inRoom = nodes.filter((d) => d.in_room).length;
    $("topo-caption").textContent = `${count} node(s) · ${peers.filter((p) => p.connected).length} direct link(s) · ${inRoom} in room`;
  }

  function renderSecurity(cur, stats, freshRejections) {
    const n = cur.node, net = cur.network;
    const malicious = (n.malicious ? 1 : 0) + net.peers.filter((p) => p.malicious).length;
    const honest = 1 + net.peers.length - malicious;
    let genesisRow;
    if (state.roomGenesis && n.genesis_hash) {
      const match = state.roomGenesis === n.genesis_hash;
      genesisRow = status(match ? "MATCHES ROOM" : "MISMATCH", match ? "ok" : "bad");
    } else genesisRow = n.genesis_hash ? short(n.genesis_hash, 12) : "—";
    const rows = [
      ["Honest nodes", honest, true],
      ["Malicious nodes", malicious ? status(String(malicious), "bad") : "0", true],
      ["Rejected data", state.rejectedTotal + state.localRejected.length, true],
      ["Invalid blocks", state.invalidBlocks ? status(String(state.invalidBlocks), "bad") : "0", true],
      ["Invalid txs", state.invalidTxs + state.localRejected.length, true],
      ["Chain validation", status(n.has_chain ? "PASS" : "NO CHAIN", n.has_chain ? "ok" : "warn")],
      ["Genesis", genesisRow],
    ];
    if (n.consensus === "pos") rows.push(["Slashed blocks", (n.slashed_blocks || []).length ? status(String(n.slashed_blocks.length), "bad") : "0", true]);
    fillKv($("security-kv"), rows);
    $("security-kv").querySelectorAll("dt")[5].title =
      "Every block in the local chain was accepted by the node's validator (signatures, balances, consensus rules). Invalid data is rejected and counted above.";

    const list = $("rejections");
    const all = (n.recent_rejections || []).slice().reverse();
    list.replaceChildren();
    if (!all.length && !state.localRejected.length) list.append(el("li", "none", "empty"));
    for (const r of state.localRejected.slice(0, 5)) {
      const li = el("li"); li.append(el("span", "web submission ", "what"), document.createTextNode(r.reason)); list.append(li);
    }
    for (const r of all) {
      const li = el("li");
      li.append(el("span", `${clock(new Date(r.ts * 1000))} ${r.what} `, "what"), document.createTextNode(r.reason));
      list.append(li);
    }
    for (const r of freshRejections) {
      logEvent("REJECT", "bad", `${r.what}: ${r.reason}`, new Date(r.ts * 1000));
    }
  }

  function computeStats(cur) {
    const n = cur.node, blocks = cur.blocks;
    const confirmed = blocks.reduce((s, b) => s + b.tx_count, 0);
    const stakers = n.current_stakers || [];
    const nonGenesis = blocks.filter((b) => b.height > 0).sort((a, b) => a.height - b.height);
    let avgInterval = null;
    if (nonGenesis.length >= 2) {
      const span = nonGenesis[nonGenesis.length - 1].ts - nonGenesis[0].ts;
      avgInterval = span / (nonGenesis.length - 1) / 1000;
    }
    return {
      confirmed,
      validators: stakers.length,
      totalStake: stakers.reduce((s, x) => s + x.amount, 0),
      avgInterval,
      nonGenesis,
    };
  }

  function renderStats(cur, stats) {
    const n = cur.node, net = cur.network;
    const connected = net.peers.filter((p) => p.connected).length;
    const pendingLocal = cur.transactions.filter((t) => t.status === "pending").length;
    const rows = [
      ["Block height", n.height, true],
      ["Total transactions", stats.confirmed + n.mempool_size, true],
      ["Confirmed", stats.confirmed, true],
      ["Pending", pendingLocal, true],
      ["Rejected", state.rejectedTotal + state.localRejected.length, true],
      ["Connected peers", connected, true],
      ["Active nodes", 1 + connected, true],
      ["Room members", n.signalling ? n.signalling.members + (n.signalling.joined ? 1 : 0) : "—", true],
      ["Avg block interval", stats.avgInterval === null ? "—" : `${stats.avgInterval.toFixed(1)} s`, true],
    ];
    if (n.consensus === "pos") rows.push(["Validators", stats.validators, true], ["Total stake", stats.totalStake, true]);
    if (n.consensus === "poa") rows.push(["Authorities", (n.authorities || []).length, true]);
    fillKv($("stats-kv"), rows);
    renderChart(stats.nonGenesis.slice(-40));
  }

  function renderChart(blocks) {
    const root = $("tx-chart");
    root.replaceChildren();
    const W = 400, H = 90, pad = 14;
    root.append(svg("line", { x1: 0, y1: H - pad, x2: W, y2: H - pad }));
    if (!blocks.length) {
      root.append(svg("text", { x: W / 2, y: H / 2, "text-anchor": "middle" }, "no blocks after genesis yet"));
      $("chart-range").textContent = "";
      return;
    }
    const max = Math.max(1, ...blocks.map((b) => b.tx_count));
    const bw = Math.min(24, W / blocks.length);
    blocks.forEach((b, i) => {
      const h = ((H - pad - 12) * b.tx_count) / max;
      const r = svg("rect", { x: i * bw + 1, y: H - pad - h, width: Math.max(1, bw - 2), height: h });
      r.append(svg("title", {}, `block #${b.height}: ${b.tx_count} tx`));
      root.append(r);
    });
    root.append(svg("text", { x: 2, y: 9 }, `max ${max}`));
    root.append(svg("text", { x: 2, y: H - 2 }, `#${blocks[0].height}`));
    root.append(svg("text", { x: Math.min(W - 2, blocks.length * bw), y: H - 2, "text-anchor": "end" }, `#${blocks[blocks.length - 1].height}`));
    $("chart-range").textContent = `(blocks #${blocks[0].height}–#${blocks[blocks.length - 1].height})`;
  }

  function txStatusCell(s) {
    const td = el("td");
    td.append(status(s.toUpperCase(), s === "confirmed" ? "ok" : s === "pending" ? "warn" : "bad"));
    return td;
  }

  function txRow(t) {
    const tr = el("tr");
    tr.append(hashCell(t.id, 13));
    const snd = el("td", who(t.sender_name, t.sender)); snd.title = t.sender || "";
    const rcv = el("td", t.type === "transfer" || t.type === "genesis" ? who(t.receiver_name, t.receiver) : t.receiver);
    rcv.title = t.receiver || "";
    tr.append(snd, rcv, el("td", fmtNum(t.amount), "num mono"), el("td", t.type), txStatusCell(t.status),
      el("td", t.block_height === null || t.block_height === undefined ? "—" : "#" + t.block_height, "num mono"),
      el("td", t.ts ? clock(new Date(t.ts * 1000)) : "—", "mono"));
    return tr;
  }

  function renderTransactions(cur) {
    const tb = $("transactions");
    tb.replaceChildren();
    const rows = state.localRejected.map((r) => ({ id: r.id, sender: null, sender_name: "this node", receiver: r.receiver,
      receiver_name: r.receiver, amount: r.amount, type: "transfer", status: "rejected", block_height: null, ts: r.ts }))
      .concat(cur.transactions);
    if (!rows.length) { tb.append(emptyRow(8, "No transactions yet")); return; }
    for (const t of rows) tb.append(txRow(t));
  }

  function emptyRow(cols, text) {
    const tr = el("tr"); const td = el("td", text, "empty"); td.colSpan = cols; tr.append(td); return tr;
  }

  function renderBlocks(cur) {
    const tb = $("blocks");
    tb.replaceChildren();
    if (!cur.blocks.length) { tb.append(emptyRow(8, "No blocks yet")); return; }
    for (const b of cur.blocks) {
      const open = state.expanded.has(b.hash);
      const tr = el("tr", null, "expandable" + (open ? " open" : ""));
      tr.append(el("td", open ? "▾" : "▸", "caret"), el("td", "#" + b.height, "mono"), hashCell(b.hash, 16), hashCell(b.prev_hash, 12),
        el("td", fmtMs(b.ts), "mono"));
      const cr = el("td", b.height === 0 ? `${who(b.creator_name, b.creator)} (genesis)` : who(b.creator_name, b.creator));
      cr.title = b.creator || "";
      tr.append(cr, el("td", b.tx_count, "num mono"));
      const st = el("td");
      st.append(b.slashed ? status("SLASHED", "bad") : status("VALIDATED", "ok"));
      tr.append(st);
      tr.addEventListener("click", () => toggleBlock(b));
      tb.append(tr);
      if (open) tb.append(detailRow(b));
    }
  }

  function detailRow(b) {
    const tr = el("tr", null, "detail");
    const td = el("td"); td.colSpan = 8; tr.append(td);
    const d = state.blockDetails.get(b.hash);
    if (!d) { td.append(el("span", "loading…", "muted")); return tr; }
    const dl = el("dl", null, "detail-grid");
    const rows = [["Hash", d.hash], ["Previous hash", d.prev_hash || "— (genesis)"], ["Block id", d.id],
      ["Timestamp", d.ts ? new Date(d.ts).toISOString() : "—"],
      [cur_consensus() === "pow" ? "Miner" : "Validator", d.creator ? `${who(d.creator_name, d.creator)} (key ${keyShort(d.creator)})` : "—"]];
    if (d.staked_amt !== undefined) rows.push(["Stake", `${d.staked_amt} of ${d.total_stake} (${(d.stakers || []).length} staker(s))`]);
    if (d.nonce !== undefined) rows.push(["Nonce", d.nonce]);
    if (d.admin_update_seq) rows.push(["Authority update", `seq ${d.admin_update_seq}`]);
    for (const [k, v] of rows) { dl.append(el("dt", k)); const dd = el("dd", v, "mono"); dl.append(dd); }
    td.append(dl);
    const table = el("table", null, "tbl compact");
    const head = el("thead"); const hr = el("tr");
    for (const h of ["Transaction ID", "Sender", "Receiver", "Amount", "Type", "Status", "Block", "Timestamp"]) hr.append(el("th", h));
    head.append(hr);
    const body = el("tbody");
    for (const t of d.transactions) body.append(txRow(t));
    table.append(head, body);
    td.append(table);
    return tr;
  }

  function cur_consensus() { return state.prev ? state.prev.node.consensus : "pos"; }

  async function toggleBlock(b) {
    if (state.expanded.has(b.hash)) { state.expanded.delete(b.hash); renderBlocks(state.prev); return; }
    state.expanded.add(b.hash);
    renderBlocks(state.prev);
    if (!state.blockDetails.has(b.hash)) {
      try {
        const { status: code, data } = await api(`/api/blocks/${b.height}`);
        if (code === 200 && data.hash === b.hash) state.blockDetails.set(b.hash, data);
      } catch (e) { /* retried on next expand */ }
      if (state.prev) renderBlocks(state.prev);
    }
  }

  function renderPeerNames(net) {
    const dl = $("peer-names"); dl.replaceChildren();
    for (const p of net.peers) { const o = el("option"); o.value = p.name; dl.append(o); }
  }

  // ----------------------------------------------------------------- polling

  async function refreshRooms() {
    try {
      const { status: code, data } = await api("/api/rooms");
      const tb = $("rooms");
      tb.replaceChildren();
      if (code !== 200) { tb.append(emptyRow(3, "signalling unavailable")); return; }
      const rooms = data.rooms || [];
      if (!rooms.length) tb.append(emptyRow(3, data.signalling ? "no rooms" : "signalling not configured"));
      const mine = state.prev && state.prev.node.room;
      state.roomGenesis = null;
      for (const r of rooms) {
        if (r.room === mine) state.roomGenesis = r.genesis;
        const tr = el("tr");
        tr.append(el("td", r.room + (r.room === mine ? "  (current)" : ""), "mono"), el("td", r.consensus), el("td", r.members, "num mono"));
        tb.append(tr);
      }
    } catch (e) { /* keep last rendering */ }
  }

  async function poll() {
    try {
      const [node, network, blocks, txs] = await Promise.all([
        api("/api/node"), api("/api/network"), api("/api/blocks?limit=500"), api("/api/transactions?limit=100")]);
      if ([node, network, blocks, txs].some((r) => r.status !== 200)) throw new Error("bad status");
      const cur = { node: node.data, network: network.data, blocks: blocks.data.blocks, transactions: txs.data.transactions };
      if (state.prev && state.prev.node.genesis_hash !== cur.node.genesis_hash) state.blockDetails.clear();
      if (state.failing) logEvent("MONITOR", "ok", "API reachable again");
      state.failing = false;
      $("banner").classList.add("hidden");

      deriveEvents(state.prev, cur);
      const freshRejections = trackRejections(cur.node);
      const stats = computeStats(cur);
      renderHeader(cur);
      renderMetrics(cur, stats);
      renderTopology(cur);
      renderNode(cur);
      renderConsensus(cur, stats);
      renderSecurity(cur, stats, freshRejections);
      renderStats(cur, stats);
      renderBlocks(cur);
      renderTransactions(cur);
      renderPeerNames(cur.network);
      $("stake-box").classList.toggle("hidden", !(cur.node.consensus === "pos" && cur.node.staker));
      state.prev = cur;
      state.lastOk = Date.now();
      renderEvents();
      if (state.pollCount % 5 === 0) refreshRooms();
      state.pollCount += 1;
    } catch (e) {
      if (!state.failing) { logEvent("MONITOR", "bad", "API unreachable, retrying"); renderEvents(); }
      state.failing = true;
      $("banner").classList.remove("hidden");
    } finally {
      setTimeout(poll, POLL_MS);   // single chained timer: polls never overlap
    }
  }

  function tickLiveLabel() {
    const dot = $("live-dot"), label = $("live-label"), upd = $("updated");
    if (!state.lastOk) { upd.textContent = "waiting for first update"; return; }
    const age = Math.round((Date.now() - state.lastOk) / 1000);
    upd.textContent = `updated ${age}s ago`;
    const stale = state.failing || age > 6;
    dot.className = "dot " + (stale ? "bad" : "ok");
    label.textContent = stale ? "STALE" : "LIVE";
  }

  // ----------------------------------------------------------------- actions

  function show(target, ok, message) { target.textContent = message; target.className = "result " + (ok ? "ok" : "bad"); }

  $("tx-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const receiver = $("tx-receiver").value.trim();
    const amount = Number($("tx-amount").value);
    try {
      const { status: code, data } = await api("/api/transactions", { method: "POST", body: JSON.stringify({ receiver, amount }) });
      if (code === 201) {
        show($("tx-result"), true, `accepted ${short(data.transaction.id, 13)} — validated, pending`);
        logEvent("SUBMIT", "tx", `submitted from dashboard: ${receiver} ${fmtNum(amount)} → accepted`);
      } else {
        show($("tx-result"), false, `rejected: ${data.error || code}`);
        state.localRejected.unshift({ id: "(not assigned)", receiver, amount, reason: data.error || String(code), ts: Date.now() / 1000 });
        state.localRejected.length = Math.min(state.localRejected.length, 20);
        logEvent("REJECT", "bad", `dashboard submission rejected by validation: ${data.error || code}`);
      }
    } catch (e) { show($("tx-result"), false, "node unreachable"); }
    renderEvents();
  });

  $("stake-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const { status: code, data } = await api("/api/stake", { method: "POST",
        body: JSON.stringify({ amount: parseInt($("stake-amount").value, 10) }) });
      if (code === 201) {
        show($("stake-result"), true, `staked ${data.stake.amount}; block creation at epoch end`);
        logEvent("SUBMIT", "pos", `stake of ${data.stake.amount} announced from dashboard`);
      } else show($("stake-result"), false, data.error || String(code));
    } catch (e) { show($("stake-result"), false, "node unreachable"); }
    renderEvents();
  });

  $("room-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const action = ev.submitter && ev.submitter.dataset.action === "join" ? "join" : "create";
    try {
      const { status: code, data } = await api(`/api/rooms/${action}`, { method: "POST",
        body: JSON.stringify({ room: $("room-name").value }) });
      show($("room-result"), code === 200, code === 200 ? `${action === "join" ? "joined" : "created"} room ${data.room}` : `${data.code || ""} ${data.error || code}`.trim());
      if (code === 200) logEvent("SIGNAL", "ok", `${action === "join" ? "joined" : "created"} room ${data.room} from dashboard`);
    } catch (e) { show($("room-result"), false, "node unreachable"); }
    renderEvents();
    refreshRooms();
  });

  document.querySelectorAll(".tab").forEach((t) => t.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((x) => x.classList.toggle("active", x === t));
    $("tab-blocks").classList.toggle("hidden", t.dataset.tab !== "blocks");
    $("tab-txs").classList.toggle("hidden", t.dataset.tab !== "txs");
  }));

  poll();
  setInterval(tickLiveLabel, 1000);
})();
