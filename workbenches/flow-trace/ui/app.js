// 智能体流转轨迹工作台页面。纯 ES 模块，所有动态内容只用 textContent / createElement，
// 不拼接 HTML，避免把记录里的文本当成标记执行。

const $ = (id) => document.getElementById(id);

// 页面挂载在 /api(或 /server/<id>/api)/projects/<pid>/agents/<aid>/workbenches/<wid>/ui/index.html。
// apiBase 是路径里 /projects/ 之前的部分；优先从页面 SDK 拿，拿不到就自己解析。
const MOUNT_RE = /\/projects\/([^/]+)\/agents\/([^/]+)\/workbenches\/([^/]+)\/ui\//;
const mount = MOUNT_RE.exec(location.pathname);
const projectId = mount ? mount[1] : "";

let apiBase = "";
if (mount) apiBase = location.pathname.slice(0, location.pathname.indexOf("/projects/"));
try {
  const SDK_MODULE = "../sdk/workbench.js";
  const sdk = await import(SDK_MODULE);
  // 只接受「同源绝对路径」：apiBase 会被直接拼进 fetch 的 URL，
  // 若采信 SDK 返回的绝对地址（含 "//host" 或 "https://…"），页面就会把登录 Cookie
  // 之外的请求发往第三方源。以单个 "/" 开头且不是 "//" 才认为可信。
  if (typeof sdk.apiBase === "string" && /^\/(?!\/)/.test(sdk.apiBase)) apiBase = sdk.apiBase;
} catch {
  // 老服务器没有页面 SDK：退回自己解析出的 apiBase。
}

// fetch 超时封装：10 秒无响应即放弃（AbortController），避免网络卡住时 flowBusy 永远卡死。
const FETCH_TIMEOUT_MS = 10000;
async function fetchWithTimeout(url, init, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...(init || {}), signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

// 工作台自己的 API：页面在 ui/ 下，../api 即工作台的 api/ 挂载点。
async function api(route, body) {
  const response = await fetchWithTimeout("../api" + route, {
    method: body === undefined ? "GET" : "POST",
    headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  }, FETCH_TIMEOUT_MS);
  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error("工作台服务返回了无法解析的内容（HTTP " + response.status + "）");
  }
  if (!response.ok) throw new Error((data && data.error) || "请求失败（HTTP " + response.status + "）");
  return data;
}

// 服务器自身的 API（登录 Cookie 同源可用）：智能体与会话列表。
async function serverApi(path) {
  const response = await fetchWithTimeout(apiBase + path, undefined, FETCH_TIMEOUT_MS);
  if (!response.ok) throw new Error("服务器接口失败（HTTP " + response.status + "）: " + path);
  return response.json();
}

// ---- 会话清单 -------------------------------------------------------------

// sessionId -> { agentId, title, source, lastActiveAt }
const sessions = new Map();
// 会话列表接口的分页上限：单个智能体超过这个条数时，超出部分不会参与自动关注与回填。
const SESSION_PAGE_LIMIT = 1000;

async function refreshSessions() {
  if (!projectId) return [];
  const agentsRes = await serverApi("/projects/" + encodeURIComponent(projectId) + "/agents");
  const agents = Array.isArray(agentsRes.agents) ? agentsRes.agents : [];
  // 先构建新 Map，全部请求结束后再整体替换。
  // 旧实现先 sessions.clear() 再并发拉取：任一智能体的会话列表请求失败，
  // 它名下的会话就会全部消失；随后 watchAll() 不再提交这些会话，
  // 服务端会把它们当成「不在本次请求中」而在淘汰时优先剔除。
  const next = new Map();
  const capped = [];
  await Promise.all(
    agents.map(async (agent) => {
      const agentId = String(agent.agentId || "");
      if (!agentId) return;
      try {
        const res = await serverApi(
          "/projects/" + encodeURIComponent(projectId) + "/agents/" + encodeURIComponent(agentId) + "/sessions?limit=" + SESSION_PAGE_LIMIT,
        );
        const list = Array.isArray(res.sessions) ? res.sessions : [];
        if (list.length >= SESSION_PAGE_LIMIT) capped.push(agent.name || agentId);
        for (const s of list) {
          if (s && typeof s.sessionId === "string") {
            next.set(s.sessionId, {
              agentId,
              agentName: agent.name || agentId,
              title: typeof s.title === "string" && s.title ? s.title : "",
              source: typeof s.source === "string" ? s.source : "",
              lastActiveAt: typeof s.lastActiveAt === "string" ? s.lastActiveAt : "",
            });
          }
        }
      } catch {
        // 单个智能体的会话列表读不到就跳过，不影响整体（已有会话也不会被清掉）。
      }
    }),
  );
  sessions.clear();
  for (const [k, v] of next) sessions.set(k, v);
  if (capped.length > 0) {
    $("notice").hidden = false;
    $("notice").textContent =
      "有 " + capped.length + " 个智能体的会话数已达 " + SESSION_PAGE_LIMIT +
      " 条上限（" + capped.join("、") + "），更早的会话不会被自动关注与回填。";
  }
  return agents;
}

// ---- 历史回填 -------------------------------------------------------------
// host.subscribe 不重放历史：对已关注会话各拉一次尾部记录交给工作台 /ingest 折叠入账
// （服务端做形状校验 + origin 白名单 + 每会话节流 + 持久化指纹去重，重复回填不会重复计数）。
const BACKFILL_SESSIONS = 30;
// tailLimit 是「轮次」而非记录条数（1 轮可能展开多条记录），10 轮即可覆盖最近记录，避免冷启动拉太多。
const BACKFILL_TAIL = 10;
// sessionId -> 回填时该会话的订阅时刻 subAt：订阅被服务端回收重建（subAt 变化）后允许二次回填，补记录缺口。
const backfilled = new Map();
// 点过「忘记」的会话不再自动回填。
const forgotten = new Set();
// 服务端限制（来自 /status.limits），用于计算每会话回填配额与本地视图上限。
let serverLimits = { maxEvents: 4000, ingestMax: 500 };
async function backfill(watchedList) {
  if (!projectId) return;
  const watched = Array.isArray(watchedList) ? watchedList : view.watched;
  const subAtOf = new Map();
  for (const w of watched)
    if (w && typeof w.sessionId === "string")
      subAtOf.set(w.sessionId, typeof w.subAt === "string" ? w.subAt : "");
  // 升序（先最旧的会话）：服务端事件缓冲是 FIFO 淘汰最旧，这样自然保留最新会话的历史。
  const targets = watched
    .filter(
      (w) =>
        w &&
        typeof w.sessionId === "string" &&
        !forgotten.has(w.sessionId) &&
        backfilled.get(w.sessionId) !== subAtOf.get(w.sessionId),
    )
    .map((w) => ({
      sessionId: w.sessionId,
      lastActiveAt: (sessions.get(w.sessionId) || {}).lastActiveAt || "",
    }))
    .sort((a, b) => (a.lastActiveAt < b.lastActiveAt ? -1 : 1))
    .slice(0, BACKFILL_SESSIONS);
  if (targets.length === 0) return;
  // 每会话配额：回填总量不超过服务端事件缓冲上限，避免把实时事件挤掉。
  const quota = Math.max(
    1,
    Math.min(serverLimits.ingestMax, Math.floor(serverLimits.maxEvents / targets.length)),
  );
  for (const t of targets) {
    const subAt = subAtOf.get(t.sessionId) || "";
    try {
      const res = await serverApi(
        "/sessions/" + encodeURIComponent(t.sessionId) + "/messages?tailLimit=" + BACKFILL_TAIL,
      );
      const records = Array.isArray(res.messages) ? res.messages : [];
      if (records.length === 0) {
        backfilled.set(t.sessionId, subAt);
        continue;
      }
      // /messages 旧→新：只取最新 quota 条提交。
      const r = await api("/ingest", { sessionId: t.sessionId, records: records.slice(-quota) });
      if (r && r.throttled) continue; // 被节流：不标记，下轮再试
      backfilled.set(t.sessionId, subAt);
    } catch {
      // 单个会话回填失败不影响其他会话。
    }
  }
}

async function watchAll() {
  const list = [];
  for (const [sessionId, info] of sessions)
    list.push({ sessionId, agentId: info.agentId, lastActiveAt: info.lastActiveAt || undefined });
  if (list.length === 0) return { added: 0 };
  return api("/watch", { sessions: list });
}

// ---- 展示辅助 -------------------------------------------------------------

const KIND_LABEL = {
  task_begin: "任务开始",
  task_end: "任务完成",
  prompt: "提问",
  delegate: "派发子智能体",
  link: "派发链接",
  artifact: "产物写入",
  artifact_result: "产物结果",
};
const KIND_TONE = {
  task_begin: "neutral",
  task_end: "success",
  prompt: "neutral",
  delegate: "attention",
  link: "attention",
  artifact: "attention",
  artifact_result: "success",
};

const shortId = (id) => (id.length > 10 ? id.slice(0, 8) + "…" : id);
const fmtTime = (iso) => {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso || "";
  const p = (n) => String(n).padStart(2, "0");
  return (
    d.getFullYear() + "-" + p(d.getMonth() + 1) + "-" + p(d.getDate()) +
    " " + p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds())
  );
};
const el = (tag, cls, text) => {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text;
  return node;
};
function badge(text, tone) {
  const b = el("span", "wb-badge", text);
  b.dataset.tone = tone || "neutral";
  return b;
}

function showError(message) {
  const box = $("error");
  box.textContent = message || "出错了";
  box.hidden = false;
}
function hideError() {
  $("error").hidden = true;
}

// ---- 流转图 ---------------------------------------------------------------

const SVG_NS = "http://www.w3.org/2000/svg";
const svgEl = (tag, attrs, text) => {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs || {})) node.setAttribute(k, v);
  if (text !== undefined) node.textContent = text;
  return node;
};

function renderGraph(flow, agentNames) {
  const host = $("graph");
  host.textContent = "";
  const watched = Array.isArray(flow.watched) ? flow.watched : [];
  const events = Array.isArray(flow.events) ? flow.events : [];
  const links = Array.isArray(flow.links) ? flow.links : [];

  // 节点 = 关注的会话 ∪ 事件/链接里出现过的会话。
  const nodes = new Map(); // sessionId -> { id, agentId, title }
  const ensure = (sessionId, agentId, title) => {
    if (!sessionId) return null;
    let n = nodes.get(sessionId);
    if (!n) {
      n = { id: sessionId, agentId: agentId || "未知智能体", title: title || "" };
      nodes.set(sessionId, n);
    } else {
      if (agentId && n.agentId === "未知智能体") n.agentId = agentId;
      if (title && !n.title) n.title = title;
    }
    return n;
  };
  for (const w of watched) {
    const info = sessions.get(w.sessionId);
    ensure(w.sessionId, w.agentId, info ? info.title : "");
  }
  for (const e of events) ensure(e.sessionId, e.agentId, "");
  // 不为链接端点单独建节点：边只在两端都有节点时才绘制，避免悬空的幽灵节点。
  if (nodes.size === 0) {
    host.appendChild(el("p", "wb-muted", "还没有记录。点击「关注全部会话」后，任务与产物流转会显示在这里。"));
    return;
  }

  // 列 = 智能体。
  const columns = [];
  const colIndex = new Map();
  const agents = Array.isArray(flow.agents) ? flow.agents : [];
  for (const a of agents) {
    const id = String(a.agentId || "");
    if (id && !colIndex.has(id)) {
      colIndex.set(id, columns.length);
      columns.push(id);
    }
  }
  for (const n of nodes.values()) {
    if (!colIndex.has(n.agentId)) {
      colIndex.set(n.agentId, columns.length);
      columns.push(n.agentId);
    }
  }

  const COL_W = 210;
  const ROW_H = 62;
  const MARGIN_X = 20;
  const HEADER_H = 34;
  const rowsPerCol = columns.map(() => 0);
  for (const n of nodes.values()) rowsPerCol[colIndex.get(n.agentId)] += 1;
  const maxRows = Math.max(1, ...rowsPerCol);
  const width = Math.max(560, columns.length * COL_W + MARGIN_X * 2);
  const height = Math.max(140, maxRows * ROW_H + HEADER_H + 16);

  const svg = svgEl("svg", {
    viewBox: "0 0 " + width + " " + height,
    width: "100%",
    role: "img",
    "aria-label": "智能体之间的会话派发流转图",
  });
  svg.style.color = "var(--wb-fg, #1f2328)";

  const defs = svgEl("defs");
  const marker = svgEl("marker", {
    id: "ft-arrow",
    viewBox: "0 0 10 10",
    refX: "9",
    refY: "5",
    markerWidth: "7",
    markerHeight: "7",
    orient: "auto-start-reverse",
  });
  marker.appendChild(svgEl("path", { d: "M 0 1 L 9 5 L 0 9 z", fill: "var(--wb-muted, #8b949e)" }));
  defs.appendChild(marker);
  svg.appendChild(defs);

  // 列标题。
  columns.forEach((agentId, ci) => {
    svg.appendChild(
      svgEl("text", { x: MARGIN_X + ci * COL_W + COL_W / 2, y: 20, "text-anchor": "middle", class: "graph-col" },
        agentNames.get(agentId) || agentId),
    );
  });

  // 节点坐标。
  const pos = new Map(); // sessionId -> {x, y}
  const seen = new Map(); // agentId -> count
  for (const n of nodes.values()) {
    const ci = colIndex.get(n.agentId);
    const ri = seen.get(n.agentId) || 0;
    seen.set(n.agentId, ri + 1);
    pos.set(n.id, { x: MARGIN_X + ci * COL_W + COL_W / 2, y: HEADER_H + 14 + ri * ROW_H });
  }

  // 产物计数（每个会话写过多少产物）。
  const artifactCount = new Map();
  for (const e of events) {
    if (e.kind === "artifact") artifactCount.set(e.sessionId, (artifactCount.get(e.sessionId) || 0) + 1);
  }

  // 边（派发链接），只画两端都有节点的。
  for (const l of links) {
    const a = pos.get(l.from);
    const b = pos.get(l.to);
    if (!a || !b) continue;
    const dy = Math.max(18, Math.abs(b.y - a.y) / 2);
    svg.appendChild(
      svgEl("path", {
        d: "M " + a.x + " " + a.y + " C " + a.x + " " + (a.y + dy) + ", " + b.x + " " + (b.y - dy) + ", " + (b.x - 8) + " " + b.y,
        fill: "none",
        stroke: "var(--wb-muted, #8b949e)",
        "stroke-width": "1.4",
        "marker-end": "url(#ft-arrow)",
      }),
    );
  }

  // 节点。
  const watchedSet = new Set(watched.map((w) => w.sessionId));
  for (const n of nodes.values()) {
    const p = pos.get(n.id);
    const g = svgEl("g", { class: "graph-node" });
    const watchedHere = watchedSet.has(n.id);
    g.appendChild(
      svgEl("circle", {
        cx: p.x,
        cy: p.y,
        r: "9",
        fill: watchedHere ? "var(--wb-accent, #2f81f7)" : "var(--wb-surface, #ffffff)",
        stroke: "var(--wb-accent, #2f81f7)",
        "stroke-width": "1.6",
      }),
    );
    const count = artifactCount.get(n.id) || 0;
    const label = (n.title || shortId(n.id)) + (count > 0 ? " · " + count + " 产物" : "");
    g.appendChild(
      svgEl("text", { x: p.x, y: p.y + 24, "text-anchor": "middle", class: "graph-label" },
        label.length > 24 ? label.slice(0, 23) + "…" : label),
    );
    const title = document.createElementNS(SVG_NS, "title");
    title.textContent = n.id + "（" + n.agentId + "）" + (n.title ? "：" + n.title : "");
    g.appendChild(title);
    svg.appendChild(g);
  }

  host.appendChild(svg);
}

// ---- 关注列表 / 产物台账 / 时间线 ------------------------------------------

function renderWatched(flow) {
  const list = $("watched");
  list.textContent = "";
  const watched = Array.isArray(flow.watched) ? flow.watched : [];
  if (watched.length === 0) {
    list.appendChild(el("li", "wb-muted", "暂无关注的会话。"));
    return;
  }
  for (const w of watched) {
    const li = el("li", "chip");
    const info = sessions.get(w.sessionId);
    li.appendChild(
      el("span", "chip-text", (info && info.title ? info.title : shortId(w.sessionId)) + "（" + shortId(w.sessionId) + "）"),
    );
    const btn = el("button", "chip-x", "取消关注");
    btn.type = "button";
    btn.addEventListener("click", async () => {
      try {
        await api("/unwatch", { sessionId: w.sessionId });
        await loadFlow();
      } catch (err) {
        showError(err.message);
      }
    });
    li.appendChild(btn);
    const forgetBtn = el("button", "chip-x", "忘记");
    forgetBtn.type = "button";
    forgetBtn.title = "删除该会话的全部记录并停止关注";
    forgetBtn.addEventListener("click", async () => {
      if (!window.confirm("忘记该会话？将删除它的全部事件与派发链接，并停止关注。")) return;
      try {
        await api("/forget", { sessionId: w.sessionId });
        forgotten.add(w.sessionId);
        backfilled.delete(w.sessionId);
        resetView();
        await loadFlow();
      } catch (err) {
        showError(err.message);
      }
    });
    li.appendChild(forgetBtn);
    list.appendChild(li);
  }
}

function renderArtifacts(events) {
  const tbody = $("artifacts").querySelector("tbody");
  tbody.textContent = "";
  const results = new Map(); // callId -> artifact_result 事件
  for (const e of events) if (e.kind === "artifact_result" && e.callId) results.set(e.callId, e);
  const rows = [];
  for (const e of events) {
    if (e.kind !== "artifact") continue;
    const res = e.callId ? results.get(e.callId) : undefined;
    rows.push({ e, res });
  }
  rows.sort((a, b) => (a.e.ts < b.e.ts ? 1 : -1));
  if (rows.length === 0) {
    const tr = el("tr");
    const td = el("td", "wb-muted", "暂无产物记录。");
    td.colSpan = 6;
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }
  for (const { e, res } of rows.slice(0, 300)) {
    const tr = el("tr");
    tr.appendChild(el("td", "mono", fmtTime(e.ts)));
    tr.appendChild(el("td", "mono path", e.path || "（未知路径）"));
    tr.appendChild(el("td", "", e.tool || ""));
    tr.appendChild(el("td", "", e.agentId || ""));
    tr.appendChild(el("td", "mono", shortId(e.sessionId || "")));
    const statusTd = el("td");
    if (res) {
      const ok = /success|ok|完成/i.test(res.status || "");
      const bad = /error|fail|失败/i.test(res.status || "");
      statusTd.appendChild(badge(res.status || "已返回", bad ? "danger" : ok ? "success" : "neutral"));
    } else {
      statusTd.appendChild(el("span", "wb-muted", "待结果"));
    }
    tr.appendChild(statusTd);
    tbody.appendChild(tr);
  }
}

function eventLine(e) {
  switch (e.kind) {
    case "task_begin":
      return "任务开始" + (e.text ? "：" + e.text : "");
    case "task_end":
      return "任务完成" + (e.text ? "。终稿摘要：" + e.text : "");
    case "prompt":
      return e.text || "（空提问）";
    case "delegate":
      return "派发给 " + (e.targetAgentId || "子智能体") + (e.text ? "：" + e.text : "");
    case "link":
      return "派生子会话 " + shortId(e.childSessionId || "");
    case "artifact":
      return "写入 " + (e.path || "（未知路径）") + (e.tool ? "（" + e.tool + "）" : "");
    case "artifact_result":
      return "结果 " + (e.status || "") + (e.path ? " · " + e.path : "");
    default:
      return e.text || e.kind;
  }
}

function renderTimeline(events) {
  const ol = $("timeline");
  ol.textContent = "";
  const list = events.slice(-300).reverse();
  if (list.length === 0) {
    ol.appendChild(el("li", "wb-muted", "还没有事件。"));
    return;
  }
  for (const e of list) {
    const li = el("li", "event");
    const head = el("div", "event-head");
    head.appendChild(el("time", "mono", fmtTime(e.ts)));
    const kindBadge = badge(KIND_LABEL[e.kind] || e.kind, KIND_TONE[e.kind] || "neutral");
    head.appendChild(kindBadge);
    if (e.kind === "artifact_result" && /error|fail|失败/i.test(e.status || "")) {
      kindBadge.dataset.tone = "danger";
    }
    head.appendChild(el("span", "wb-muted meta", e.agentId + " · " + shortId(e.sessionId || "")));
    li.appendChild(head);
    li.appendChild(el("p", "event-body", eventLine(e)));
    ol.appendChild(li);
  }
}

function renderStats(flow) {
  const events = Array.isArray(flow.events) ? flow.events : [];
  const agents = Array.isArray(flow.agents) ? flow.agents : [];
  const watched = Array.isArray(flow.watched) ? flow.watched : [];
  const links = Array.isArray(flow.links) ? flow.links : [];
  $("stat-agents").textContent = String(agents.length);
  $("stat-sessions").textContent = String(sessions.size);
  $("stat-watched").textContent = String(watched.length);
  $("stat-events").textContent = String(events.length);
  $("stat-artifacts").textContent = String(events.filter((e) => e.kind === "artifact").length);
  $("stat-links").textContent = String(links.length);
}

// ---- 主流程 ---------------------------------------------------------------

let agentNames = new Map();

// 增量轮询状态：view 是合并后的全量视图，lastSeq 是已收到的最大事件序号。
let lastSeq = 0;
let flowBusy = false;
let viewEpoch = "";     // 服务端状态纪元；变化（如 /clear）即整体重拉
let rendered = false;   // 是否已渲染过至少一次
let lastRenderKey = ""; // 上次渲染的内容键；无变化则跳过整页重渲染
const view = { agents: [], watched: [], links: [], events: [] };
const viewLinkKeys = new Set();
function resetView() {
  lastSeq = 0;
  viewEpoch = "";
  rendered = false;
  lastRenderKey = "";
  view.agents = [];
  view.watched = [];
  view.links = [];
  view.events = [];
  viewLinkKeys.clear();
}
function mergeFlow(flow) {
  if (typeof flow.epoch === "string") viewEpoch = flow.epoch;
  if (Array.isArray(flow.agents)) view.agents = flow.agents;
  if (Array.isArray(flow.watched)) view.watched = flow.watched;
  if (Array.isArray(flow.links)) {
    for (const l of flow.links) {
      const k = l.from + "->" + l.to;
      if (!viewLinkKeys.has(k)) {
        viewLinkKeys.add(k);
        view.links.push(l);
      }
    }
  }
  // 只在有新事件时才重建去重 Set（增量轮询常返回 0 条，避免每 3 秒扫全量）。
  if (Array.isArray(flow.events) && flow.events.length > 0) {
    const known = new Set(view.events.map((e) => e.seq));
    for (const e of flow.events) if (!known.has(e.seq)) view.events.push(e);
  }
  // 服务端 FIFO 淘汰（seq < minSeq）的事件与链接同步从本地视图移除，保持与服务端一致。
  if (typeof flow.minSeq === "number" && flow.minSeq > 1) {
    view.events = view.events.filter((e) => e.seq >= flow.minSeq);
    view.links = view.links.filter((l) => (l.seq ?? 0) >= flow.minSeq);
    viewLinkKeys.clear();
    for (const l of view.links) viewLinkKeys.add(l.from + "->" + l.to);
  }
  view.events.sort((a, b) => a.seq - b.seq);
  if (view.events.length > serverLimits.maxEvents)
    view.events.splice(0, view.events.length - serverLimits.maxEvents);
  if (typeof flow.maxSeq === "number" && flow.maxSeq > 0) lastSeq = Math.max(lastSeq, flow.maxSeq);
}

async function loadFlow() {
  if (flowBusy) return; // 上一次还在飞：跳过本次，避免请求堆积
  if (document.hidden) return; // 标签页隐藏时不轮询
  flowBusy = true;
  try {
    let flow = await api("/flow" + (lastSeq > 0 ? "?since=" + lastSeq : ""));
    let maxSeq = typeof flow.maxSeq === "number" ? flow.maxSeq : 0;
    if (
      lastSeq > 0 &&
      (maxSeq < lastSeq ||
        (typeof flow.epoch === "string" && flow.epoch !== "" && flow.epoch !== viewEpoch))
    ) {
      // 服务端状态被重置（/clear 轮换了 epoch，或 seq 回绕）：清掉本地视图后整体重拉。
      resetView();
      flow = await api("/flow");
      maxSeq = typeof flow.maxSeq === "number" ? flow.maxSeq : 0;
    }
    hideError();
    mergeFlow(flow);
    // 渲染跳过：内容键无变化且已渲染过，则跳过整页重渲染（每 3 秒轮询不空转 DOM）。
    const renderKey = [
      lastSeq,
      flow.minSeq ?? 0,
      view.watched.length,
      view.watched.map((w) => w.sessionId).join(","),
      view.events.length,
      view.links.length,
    ].join(":");
    if (rendered && renderKey === lastRenderKey) return;
    lastRenderKey = renderKey;
    rendered = true;
    renderStats(view);
    renderGraph(view, agentNames);
    renderWatched(view);
    renderArtifacts(view.events);
    renderTimeline(view.events);
  } catch (err) {
    showError(err.message);
  } finally {
    flowBusy = false;
  }
}

async function exportFile(format) {
  try {
    const response = await fetch("../api/export?format=" + encodeURIComponent(format));
    if (!response.ok) {
      let msg = "导出失败（HTTP " + response.status + "）";
      try {
        const data = JSON.parse(await response.text());
        if (data && data.error) msg = data.error;
      } catch {}
      throw new Error(msg);
    }
    const blob = await response.blob();
    const disposition = response.headers.get("content-disposition") || "";
    const m = /filename="?([^";]+)"?/.exec(disposition);
    const name = m ? m[1] : "agent-flow-trace." + (format === "md" ? "md" : format);
    const url = URL.createObjectURL(blob);
    const a = el("a");
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 5000);
  } catch (err) {
    showError(err.message);
  }
}

async function boot() {
  $("refresh").addEventListener("click", () => { refreshSessions().then(loadFlow).catch((e) => showError(e.message)); });
  $("watch-all").addEventListener("click", async () => {
    try {
      await refreshSessions();
      const res = await watchAll();
      await backfill(res && res.watched);
      $("notice").hidden = false;
      $("notice").textContent = "已关注全部会话，本次新增 " + (res.added || 0) + " 个。";
      await loadFlow();
    } catch (err) {
      showError(err.message);
    }
  });
  $("export-json").addEventListener("click", () => exportFile("json"));
  $("export-jsonl").addEventListener("click", () => exportFile("jsonl"));
  $("export-md").addEventListener("click", () => exportFile("md"));
  $("clear").addEventListener("click", async () => {
    if (!window.confirm("清空全部事件与派发链接记录？（关注列表保留，之后仍会继续记录）")) return;
    try {
      await api("/clear", {});
      // 清空后把当前关注会话标记为已回填，避免轮询时二次回填把历史重新灌回来。
      for (const w of view.watched)
        if (w && w.sessionId)
          backfilled.set(w.sessionId, typeof w.subAt === "string" ? w.subAt : "");
      // 注意：这里刻意不重置 forgotten。
      // 「忘记」是用户对单个会话的显式意图（服务端 /forget 已把它移出关注），
      // 不应被「清空记录」隐式撤销；重置它只会让页面反复去回填一个未被关注的会话，
      // 徒增被服务端拒绝的请求。要恢复某个会话，点「关注全部会话」即可。
      resetView();
      await loadFlow();
    } catch (err) {
      showError(err.message);
    }
  });

  try {
    const agents = await refreshSessions();
    for (const a of agents) {
      if (a && a.agentId) agentNames.set(String(a.agentId), a.name || String(a.agentId));
    }
    // 首次进入自动关注当前全部会话（幂等），让面板立刻有内容。
    const wres = await watchAll();
    const status = await api("/status");
    if (status && status.limits)
      serverLimits = {
        maxEvents: status.limits.maxEvents || 4000,
        ingestMax: status.limits.ingestMax || 500,
      };
    if (status && status.subscribeSupported === false) {
      $("notice").hidden = false;
      $("notice").textContent = "当前服务器不支持实时订阅：新活动不会实时记录（历史回填仍可用），升级服务器后自动恢复。";
    }
    // 订阅不重放历史：回填已关注会话的尾部记录（订阅重建后会自动二次回填）。
    await backfill(wres && wres.watched);
  } catch (err) {
    showError(err.message);
  }

  await loadFlow();
  setInterval(() => {
    void loadFlow();
  }, 3000);
  setInterval(async () => {
    if (document.hidden) return;
    try {
      await refreshSessions();
      const wres = await watchAll(); // 幂等：新出现的会话自动补关注
      await backfill(wres && wres.watched); // 新会话 / 订阅重建的会话补回填历史
      await loadFlow();
    } catch (err) {
      showError(err.message);
    }
  }, 15000);
}

boot();
