/* 多表增量装载协调器控制台 */
"use strict";

const SVG_NS = "http://www.w3.org/2000/svg";
const STATE_COLORS = {
  PENDING: "#6b7892", READY: "#39c6d6", WAITING_LOCK: "#f2b544",
  RUNNING: "#5b9dff", RETRY_WAIT: "#a78bfa", SUCCESS: "#3fce7e",
  FAILED: "#f05d6e", CANCELLED: "#6b7892", BLOCKED: "#f05d6e",
};
const STATE_LABELS = {
  PENDING: "等待依赖", READY: "等待资源", WAITING_LOCK: "等待锁",
  RUNNING: "运行中", RETRY_WAIT: "退避重试", SUCCESS: "成功",
  FAILED: "失败", CANCELLED: "已取消", BLOCKED: "下游阻塞",
};

const state = { status: null, paused: false };

const $ = (sel) => document.querySelector(sel);

function fmtTime(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString("zh-CN", { hour12: false }) +
    "." + String(d.getMilliseconds()).padStart(3, "0").slice(0, 2);
}

function toast(msg, ok = false) {
  const el = $("#toast");
  el.textContent = msg;
  el.className = "toast" + (ok ? " ok" : "");
  clearTimeout(toast._t);
  toast._t = setTimeout(() => el.classList.add("hidden"), 3500);
}

async function api(path, opts = {}) {
  try {
    const resp = await fetch(path, { method: "POST", ...opts });
    const body = await resp.json();
    if (!resp.ok) throw new Error(body.error || "请求失败");
    return body;
  } catch (err) {
    toast(err.message);
    throw err;
  }
}

/* ------------------------------------------------------------ 轮询 + SSE */
async function poll() {
  try {
    const resp = await fetch("/api/status");
    state.status = await resp.json();
    render();
  } catch (err) {
    $("#sched-state").textContent = "后端不可达";
  }
}

function connectSSE() {
  const es = new EventSource("/api/events");
  es.onmessage = () => {
    if ($("#live").checked) poll();
  };
  es.onerror = () => {
    // 浏览器会自动重连；同时保留轮询兜底
  };
}

/* ------------------------------------------------------------ DAG 布局 */
function computeLayout(tasks, topo, deps) {
  const byId = Object.fromEntries(tasks.map((t) => [t.task_id, t]));
  const layerOf = {};
  for (const id of topo) {
    const ds = deps[id] || [];
    layerOf[id] = ds.length ? Math.max(...ds.map((d) => layerOf[d])) + 1 : 0;
  }
  const layers = {};
  for (const id of topo) (layers[layerOf[id]] ||= []).push(id);

  const NODE_W = 168, NODE_H = 78, GAP_X = 34, GAP_Y = 56;
  const positions = {};
  const layerKeys = Object.keys(layers).map(Number).sort((a, b) => a - b);
  const maxRows = Math.max(...layerKeys.map((l) => layers[l].length));
  layerKeys.forEach((l) => {
    const ids = layers[l];
    const totalH = ids.length * NODE_H + (ids.length - 1) * GAP_Y;
    const offsetY = (maxRows * NODE_H + (maxRows - 1) * GAP_Y - totalH) / 2;
    ids.forEach((id, i) => {
      positions[id] = {
        x: 24 + l * (NODE_W + GAP_X),
        y: 24 + offsetY + i * (NODE_H + GAP_Y),
      };
    });
  });
  return { positions, NODE_W, NODE_H, width: 24 * 2 + (layerKeys.length) * (NODE_W + GAP_X) - GAP_X,
           height: 24 * 2 + maxRows * NODE_H + (maxRows - 1) * GAP_Y };
}

function renderDag(tasks, topo) {
  const svg = $("#dag");
  svg.innerHTML = "";
  const deps = Object.fromEntries(tasks.map((t) => [t.task_id, t.deps]));
  const byId = Object.fromEntries(tasks.map((t) => [t.task_id, t]));
  const { positions, NODE_W, NODE_H, width, height } = computeLayout(tasks, topo, deps);
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("height", height);

  // 边
  for (const t of tasks) {
    for (const dep of t.deps) {
      const a = positions[dep], b = positions[t.task_id];
      const x1 = a.x + NODE_W, y1 = a.y + NODE_H / 2;
      const x2 = b.x, y2 = b.y + NODE_H / 2;
      const mx = (x1 + x2) / 2;
      const path = document.createElementNS(SVG_NS, "path");
      path.setAttribute("d", `M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`);
      const bad = ["FAILED", "CANCELLED", "BLOCKED"].includes(byId[dep].state);
      const done = byId[dep].state === "SUCCESS";
      path.setAttribute("class", "edge" + (bad ? " edge-bad" : done ? " edge-done" : ""));
      svg.appendChild(path);
    }
  }

  // 节点
  for (const t of tasks) {
    const p = positions[t.task_id];
    const g = document.createElementNS(SVG_NS, "g");
    g.setAttribute("class", `node node-state-${t.state}`);
    g.setAttribute("transform", `translate(${p.x},${p.y})`);

    const rect = document.createElementNS(SVG_NS, "rect");
    rect.setAttribute("width", NODE_W);
    rect.setAttribute("height", NODE_H);
    rect.setAttribute("rx", 9);
    g.appendChild(rect);

    const title = document.createElementNS(SVG_NS, "title");
    title.textContent = `${t.task_id}\n表: ${t.table}\n分区: ${t.partitions.join(", ")}\n状态: ${STATE_LABELS[t.state]}`;
    g.appendChild(title);

    const name = document.createElementNS(SVG_NS, "text");
    name.setAttribute("x", 10); name.setAttribute("y", 19);
    name.textContent = t.table.length > 18 ? t.table.slice(0, 17) + "…" : t.table;
    g.appendChild(name);

    const sub = document.createElementNS(SVG_NS, "text");
    sub.setAttribute("class", "sub"); sub.setAttribute("x", 10); sub.setAttribute("y", 35);
    sub.textContent = `${t.partitions.length} 分区 · 尝试 ${t.attempts}/${t.max_attempts}`;
    g.appendChild(sub);

    // 进度条
    const bg = document.createElementNS(SVG_NS, "rect");
    bg.setAttribute("class", "node-progress-bg");
    bg.setAttribute("x", 10); bg.setAttribute("y", 46);
    bg.setAttribute("width", NODE_W - 20); bg.setAttribute("height", 6); bg.setAttribute("rx", 3);
    g.appendChild(bg);
    const bar = document.createElementNS(SVG_NS, "rect");
    bar.setAttribute("class", "node-progress");
    bar.setAttribute("x", 10); bar.setAttribute("y", 46);
    bar.setAttribute("width", (NODE_W - 20) * t.progress); bar.setAttribute("height", 6); bar.setAttribute("rx", 3);
    g.appendChild(bar);

    const st = document.createElementNS(SVG_NS, "text");
    st.setAttribute("class", "node-badge");
    st.setAttribute("x", 10); st.setAttribute("y", 70);
    st.setAttribute("fill", STATE_COLORS[t.state]);
    let label = STATE_LABELS[t.state];
    if (t.state === "RETRY_WAIT" && t.retry_in != null) label += ` ${t.retry_in}s`;
    if (t.state === "RUNNING" && t.current_partition != null)
      label += ` 分区${t.current_partition + 1}/${t.partitions.length}`;
    st.textContent = label;
    g.appendChild(st);

    svg.appendChild(g);
  }
}

/* ------------------------------------------------------------ 资源 */
function barClass(pct) {
  if (pct >= 1) return "full";
  if (pct >= 0.8) return "warn";
  return "";
}

function renderResources(res) {
  const slotPct = res.used_slots / res.total_slots;
  const memPct = res.used_mem / res.total_mem;
  const allocs = res.allocations.map(
    (a) => `<span>${a.task_id} · s${a.slots}/m${a.mem}</span>`
  ).join("");
  $("#resources").innerHTML = `
    <div class="res-row">
      <div class="res-label"><span>执行槽位</span><b>${res.used_slots} / ${res.total_slots}</b></div>
      <div class="bar ${barClass(slotPct)}"><i style="width:${slotPct * 100}%"></i></div>
    </div>
    <div class="res-row">
      <div class="res-label"><span>内存预算</span><b>${res.used_mem} / ${res.total_mem}</b></div>
      <div class="bar ${barClass(memPct)}"><i style="width:${memPct * 100}%"></i></div>
    </div>
    <div class="alloc-list">${allocs ? "占用中：" + allocs : '<span class="empty-note">当前无运行中任务</span>'}</div>
  `;
}

/* ------------------------------------------------------------ 锁 */
function renderLocks(locks) {
  if (!locks.holders.length && !locks.waiters.length) {
    $("#locks").innerHTML = '<p class="empty-note">无分区锁占用，任务可自由并行</p>';
    return;
  }
  const waiterByKey = {};
  for (const w of locks.waiters) {
    for (const k of w.keys) (waiterByKey[k] ||= []).push(w);
  }
  const html = locks.holders.map((h) => {
    const waiters = (waiterByKey[h.key] || []).map(
      (w) => `<div class="lock-waiter">⏳ ${w.task_id} 等待 <em>${w.wait_seconds}s</em></div>`
    ).join("");
    return `<div class="lock-block">
      <div class="lock-key"><span>🔒 ${h.key}</span><span class="lock-owner">${h.task_id}</span></div>
      ${waiters}
    </div>`;
  }).join("");
  $("#locks").innerHTML = html || '<p class="empty-note">无锁</p>';
}

/* ------------------------------------------------------------ 任务表 */
function taskRow(t) {
  const pct = Math.round(t.progress * 100);
  const cp = t.checkpoint >= 0
    ? `<span class="cp">#${t.checkpoint} ${t.partitions[t.checkpoint] ?? ""}</span>`
    : '<span class="cp-none">无（未完成分区）</span>';
  const wait = t.wait
    ? `<span title="${escapeHtml(JSON.stringify(t.wait))}">${waitText(t.wait)}</span>`
    : '<span class="cp-none">—</span>';
  const err = t.retry_in != null
    ? `<span class="retry-in">${t.retry_in}s 后重试</span>`
    : t.last_error ? `<span title="${escapeHtml(t.last_error)}">${escapeHtml(t.last_error)}</span>`
    : '<span class="cp-none">—</span>';
  return `<tr>
    <td><div class="task-name">${t.task_id}</div>
        <div class="task-parts">${t.table} · s${t.slots}/m${t.mem} · 超时${t.timeout}s</div></td>
    <td><span class="state-tag st-${t.state}">${STATE_LABELS[t.state] || t.state}</span></td>
    <td><span class="bar mini-bar"><i style="width:${pct}%"></i></span><span class="pct">${pct}%</span></td>
    <td>${t.attempts}/${t.max_attempts}</td>
    <td>${cp}</td>
    <td class="wait-cell">${wait}</td>
    <td class="err-cell">${err}</td>
    <td><div class="op-group">${actionButtons(t)}</div></td>
  </tr>`;
}

function waitText(wait) {
  if (wait.type === "resource") return `资源不足（需 s${wait.slots}/m${wait.mem}）`;
  if (wait.type === "lock") return `等锁：被 ${wait.blocked_by} 占用`;
  if (wait.type === "dependency") return `上游未完成：${wait.upstream.join(",")}`;
  return "—";
}

function actionButtons(t) {
  const btns = [];
  const running = ["PENDING", "READY", "WAITING_LOCK", "RUNNING", "RETRY_WAIT", "BLOCKED"].includes(t.state);
  if (running) btns.push(`<button class="btn btn-mini btn-danger" data-act="cancel" data-id="${t.task_id}">取消</button>`);
  if (t.state === "RUNNING") btns.push(`<button class="btn btn-mini btn-warn" data-act="inject" data-id="${t.task_id}">注入故障</button>`);
  if (["FAILED", "CANCELLED"].includes(t.state))
    btns.push(`<button class="btn btn-mini" data-act="retry" data-id="${t.task_id}">人工补齐</button>`);
  return btns.join("") || '<span class="cp-none">—</span>';
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

/* ------------------------------------------------------------ 事件 */
function renderEvents(events) {
  const html = events.slice().reverse().map((e) => `
    <div class="ev t-${e.type}">
      <span class="ev-time">${fmtTime(e.ts)}</span>
      <span class="ev-type">${e.type}</span>
      <span class="ev-task">${e.task_id || ""}</span>
      <span class="ev-msg">${escapeHtml(eventMessage(e))}</span>
    </div>`).join("");
  $("#event-log").innerHTML = html;
}

function eventMessage(e) {
  const p = e.payload || {};
  const map = {
    TASK_STARTED: `第 ${p.attempt} 次尝试，从分区 #${p.resume_from} 续跑，超时 ${p.timeout}s`,
    PARTITION_STARTED: `开始分区 #${p.index} ${p.partition}`,
    PARTITION_DONE: `完成分区 #${p.index} ${p.partition}（检查点=${p.checkpoint}）`,
    TASK_SUCCESS: `装载成功，共 ${p.partitions} 个分区，尝试 ${p.attempts} 次`,
    TASK_ERROR: `${p.kind === "timeout" ? "超时" : "错误"}: ${p.error}（第 ${p.attempt}/${p.max_attempts} 次，${p.will_retry ? "将重试" : "重试耗尽"}，检查点=${p.checkpoint}）`,
    TASK_RETRY: `${p.retry_in}s 后发起第 ${p.next_attempt} 次尝试，从检查点 #${p.checkpoint} 续跑`,
    TASK_FAILED: `重试耗尽：${p.error}`,
    TASK_BLOCKED: `上游 ${p.upstream?.join(",")} 未成功，等待补齐`,
    TASK_UNBLOCKED: p.note,
    TASK_CANCELLED: `已取消：${p.reason || p.table || ""}`,
    TASK_CANCEL_REQUESTED: p.note,
    TASK_RETRY_REQUESTED: `${p.note}（检查点=${p.checkpoint}）`,
    FAILURE_INJECTED: p.manual ? "手动注入故障" : `自动注入 ${p.kind}（第 ${p.attempt} 次尝试）`,
    TIMEOUT_SIGNALLED: `超过 ${p.timeout}s，触发超时（第 ${p.attempt} 次尝试）`,
    SCHEDULER_STARTED: `调度器启动（代次 ${p.generation}）`,
    SCHEDULER_PAUSED: p.note,
    SCHEDULER_CRASH: p.note,
    SCHEDULER_RESTARTED: `${p.note}（代次 ${p.generation}）`,
    SCHEDULER_RECOVERED: `${p.note}（代次 ${p.generation}）`,
    SCHEDULER_RESET: `状态已重置（代次 ${p.generation}）`,
    SCHEDULER_ERROR: p.error,
  };
  return map[e.type] || JSON.stringify(p);
}

/* ------------------------------------------------------------ 总渲染 */
function render() {
  const s = state.status;
  if (!s) return;
  const sc = s.scheduler;

  const pill = $("#sched-state");
  if (sc.stopping) {
    pill.textContent = "已停止/崩溃";
    pill.className = "pill pill-stopped";
  } else if (sc.running) {
    pill.textContent = "运行中";
    pill.className = "pill pill-running";
  } else {
    pill.textContent = "已暂停";
    pill.className = "pill pill-paused";
  }
  $("#sched-gen").textContent = `代次 #${sc.generation}`;
  $("#btn-start").disabled = sc.running || sc.stopping;
  $("#btn-pause").disabled = !sc.running;

  $("#legend").innerHTML = Object.entries(STATE_LABELS).map(
    ([k, v]) => `<span><i style="background:${STATE_COLORS[k]}"></i>${v}</span>`
  ).join("");

  renderDag(s.tasks, sc.topo);
  renderResources(s.resources);
  renderLocks(s.locks);
  $("#task-table tbody").innerHTML = s.tasks.map(taskRow).join("");
  renderEvents(s.events);
}

/* ------------------------------------------------------------ 交互 */
document.addEventListener("click", async (ev) => {
  const btn = ev.target.closest("button[data-act]");
  if (!btn) return;
  const { act, id } = btn.dataset;
  const label = { cancel: "取消", inject: "注入故障", retry: "人工补齐" }[act];
  if (!confirm(`确认对任务 ${id} 执行「${label}」？`)) return;
  try {
    await api(`/api/tasks/${id}/${act}`);
    toast(`已发送：${id} ${label}`, true);
    poll();
  } catch { /* toast 已提示 */ }
});

$("#btn-start").onclick = async () => { await api("/api/scheduler/start").then(poll); toast("调度器继续", true); };
$("#btn-pause").onclick = async () => { await api("/api/scheduler/pause").then(poll); toast("调度器已暂停（运行中的任务会跑完）"); };
$("#btn-restart").onclick = async () => {
  if (!confirm("模拟调度器进程崩溃，然后从磁盘快照重启？")) return;
  await api("/api/scheduler/restart").then(poll);
  toast("已从快照恢复重启", true);
};
$("#btn-reset").onclick = async () => {
  if (!confirm("清空所有状态并按配置重新装载？快照与事件日志都会被清除。")) return;
  await api("/api/scheduler/reset").then(poll);
  toast("已重新装载", true);
};

poll();
setInterval(poll, 1000);
connectSSE();
