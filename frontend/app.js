import { API, APIError } from "./api.js";
import { graph, template } from "./dag.js";

const $ = (selector) => document.querySelector(selector);
const main = $("#main");
const states = {
  QUEUED: "排队中",
  RUNNING: "运行中",
  STOPPING: "停止中",
  SUCCEEDED: "已完成",
  FAILED: "失败",
  CANCELLED: "已取消",
  PENDING: "等待",
  SKIPPED: "跳过",
  online: "在线",
  offline: "离线",
  draining: "排空中",
};
const terminal = new Set(["SUCCEEDED", "FAILED", "CANCELLED"]);
let api = null,
  version = "0.2.0",
  epoch = 0,
  refreshing = false,
  connected = false;
let route = "tasks",
  taskId = "",
  rows = [],
  counts = {},
  workers = [],
  detail = null,
  events = [];
let cursor = null,
  cursors = [],
  nextCursor = null,
  filterState = "",
  filterPool = "";
let tab = "progress",
  attemptId = null;
let draft = template(),
  selected = draft.steps[0].id,
  validation = "",
  submitIdentity = null;
const retryKeys = new Map();
let submitting = false;

// All business data is rendered with textContent; no HTML interpolation.
function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === "class") node.className = value;
    else if (key === "onclick" || key === "onchange")
      node.addEventListener(key.slice(2), value);
    else if (["value", "checked", "disabled", "hidden"].includes(key))
      node[key] = value;
    else node.setAttribute(key, value);
  }
  children
    .flat(Infinity)
    .filter((v) => v !== null && v !== undefined)
    .forEach((child) =>
      node.append(
        child instanceof Node ? child : document.createTextNode(String(child)),
      ),
    );
  return node;
}
const button = (label, action, cls = "", disabled = false) =>
  h(
    "button",
    { type: "button", class: cls, disabled, onclick: run(action) },
    label,
  );
function run(action) {
  return async (event) => {
    const node = event?.currentTarget;
    if (node instanceof HTMLButtonElement) node.disabled = true;
    try {
      await action(event);
    } catch (error) {
      notify(error.message);
    } finally {
      if (node instanceof HTMLButtonElement && node.isConnected)
        node.disabled = false;
    }
  };
}
function notify(message, success = false) {
  const node = $("#notice");
  node.textContent = message;
  node.className = success ? "success" : "";
  node.hidden = false;
}
function needAPI() {
  if (!api || !connected) throw new Error("请先连接管控节点 API");
  return api;
}
function badge(state) {
  return h(
    "span",
    { class: `badge ${Object.hasOwn(states, state) ? state : ""}` },
    states[state] || state,
  );
}
function date(value) {
  return value
    ? new Date(value * 1000).toLocaleString("zh-CN", { hour12: false })
    : "—";
}
function seconds(value) {
  return Number.isFinite(value) ? `${value.toFixed(1)} s` : "—";
}
function progress(value, state) {
  const fraction =
    state === "SUCCEEDED"
      ? 1
      : Math.min(1, Math.max(0, Number(value?.fraction) || 0));
  const fill = h("span");
  fill.style.width = `${fraction * 100}%`;
  return h(
    "div",
    {},
    h(
      "div",
      {
        class: `bar ${state}`,
        role: "progressbar",
        "aria-valuenow": Math.round(fraction * 100),
        "aria-valuemin": 0,
        "aria-valuemax": 100,
        "aria-label": "执行进度",
      },
      fill,
    ),
    h("small", { class: "muted" }, `${Math.round(fraction * 100)}%`),
  );
}
function empty(title, text) {
  return h("div", { class: "empty" }, h("strong", {}, title), h("p", {}, text));
}
function panel(title, content, extra) {
  return h(
    "section",
    { class: "panel" },
    h("div", { class: "panel-head" }, h("h3", {}, title), extra),
    h("div", { class: "panel-body" }, content),
  );
}
function heading(title, description, actions) {
  return h(
    "div",
    { class: "page-heading" },
    h(
      "div",
      {},
      h("div", { class: "eyebrow" }, "DATAFLOWCORE / CONTROL PLANE"),
      h("h1", {}, title),
      h("p", {}, description),
    ),
    actions,
  );
}
function kv(values) {
  return h(
    "dl",
    { class: "key-values" },
    values.flatMap(([key, value]) => [
      h("dt", {}, key),
      h("dd", {}, value ?? "—"),
    ]),
  );
}
function json(value, cls = "") {
  return h(
    "pre",
    { class: cls },
    typeof value === "string" ? value : JSON.stringify(value, null, 2),
  );
}
function download(value, filename) {
  const url = URL.createObjectURL(
    new Blob([JSON.stringify(value, null, 2)], { type: "application/json" }),
  );
  const link = h("a", { href: url, download: filename });
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
async function confirm(title, text) {
  $("#confirm-title").textContent = title;
  $("#confirm-text").textContent = text;
  const dialog = $("#confirm-dialog");
  dialog.showModal();
  return new Promise((resolve) => {
    const finish = (value) => {
      dialog.close();
      $("#confirm-yes").onclick = null;
      $("#confirm-no").onclick = null;
      dialog.oncancel = null;
      resolve(value);
    };
    $("#confirm-yes").onclick = () => finish(true);
    $("#confirm-no").onclick = () => finish(false);
    dialog.oncancel = (event) => {
      event.preventDefault();
      finish(false);
    };
  });
}
function connectionUI() {
  $("#connection-label").textContent = connected
    ? "API 已连接"
    : "配置 API 连接";
  $("#connection-dot").className = "live-dot" + (connected ? "" : " offline");
}
function resetData() {
  rows = [];
  workers = [];
  counts = {};
  detail = null;
  events = [];
  cursor = null;
  cursors = [];
  nextCursor = null;
}
function openConnection() {
  $("#api-url").value =
    api?.base ||
    localStorage.getItem("dataflow-api-base") ||
    window.DATAFLOW_CONFIG?.apiBase ||
    "/api";
  $("#api-token").value = "";
  $("#connect-error").textContent = "";
  $("#connect-dialog").showModal();
}
$("#connection").onclick = openConnection;
$("#close-connect").onclick = () => $("#connect-dialog").close();
$("#disconnect").onclick = () => {
  epoch++;
  connected = false;
  api = null;
  resetData();
  connectionUI();
  $("#connect-dialog").close();
  $("#refresh-status").textContent = "已断开";
  if (route !== "dag") render();
};
$("#connect-form").onsubmit = async (event) => {
  event.preventDefault();
  const submit = event.submitter;
  submit.disabled = true;
  const connectionEpoch = ++epoch;
  try {
    const candidate = new API(
      $("#api-url").value,
      $("#api-token").value.trim(),
    );
    const overview = await candidate.request("/v1/overview");
    if (epoch !== connectionEpoch) return;
    api = candidate;
    connected = true;
    version = overview.version;
    resetData();
    counts = overview.task_counts;
    localStorage.setItem("dataflow-api-base", api.base);
    $("#api-token").value = "";
    connectionUI();
    $("#connect-dialog").close();
    $("#notice").hidden = true;
    if (route !== "dag") render();
    await refresh();
  } catch (error) {
    $("#connect-error").textContent = error.message;
  } finally {
    submit.disabled = false;
  }
};
$("#refresh").onclick = run(() => refresh(true));

async function refresh(manual = false) {
  if (!connected || refreshing || (document.hidden && !manual)) return;
  const activeEpoch = epoch,
    activeRoute = route,
    activeId = taskId;
  refreshing = true;
  try {
    const client = api;
    if (activeRoute === "tasks") {
      const query = new URLSearchParams({ limit: "30", summary: "1" });
      if (cursor) query.set("cursor", cursor);
      if (filterState) query.set("state", filterState);
      if (filterPool) query.set("pool", filterPool);
      const [overview, page] = await Promise.all([
        client.request("/v1/overview"),
        client.request(`/v1/tasks?${query}`),
      ]);
      const status = page.tasks.length
        ? await client.post("/v1/tasks/status", {
            task_ids: page.tasks.map((t) => t.id),
          })
        : { tasks: [] };
      if (epoch !== activeEpoch) return;
      const byId = new Map(status.tasks.map((t) => [t.id, t]));
      counts = overview.task_counts;
      rows = page.tasks.map((t) => ({ ...t, ...byId.get(t.id) }));
      nextCursor = page.next_cursor;
    } else if (activeRoute === "workers") {
      const result = await client.request("/v1/workers");
      if (epoch !== activeEpoch) return;
      workers = result.workers;
    } else if (activeRoute === "detail") {
      const [task, history] = await Promise.all([
        client.request(`/v1/tasks/${encodeURIComponent(activeId)}`),
        client.request(`/v1/tasks/${encodeURIComponent(activeId)}/events`),
      ]);
      if (epoch !== activeEpoch) return;
      detail = task;
      events = history.events;
    }
    if (epoch !== activeEpoch) return;
    $("#refresh-status").textContent =
      `${version} · ${new Date().toLocaleTimeString("zh-CN", { hour12: false })} 已更新`;
    const editingFilter =
      route === "tasks" &&
      document.activeElement?.matches(".toolbar input") &&
      document.activeElement.value.trim() !== filterPool;
    if (route !== "dag" && !editingFilter) render();
  } catch (error) {
    if (epoch !== activeEpoch) return;
    $("#refresh-status").textContent = "更新失败 · 显示上次数据";
    notify(error.message);
    if (error instanceof APIError && error.status === 401) {
      connected = false;
      connectionUI();
      notify("管理令牌失效，请重新配置 API 连接。当前展示的是上次获取的数据。");
    }
  } finally {
    refreshing = false;
    if (connected && epoch !== activeEpoch) void refresh();
  }
}
function parseRoute() {
  const parts = location.hash.slice(1).split("/");
  route =
    parts[0] === "task" && parts[1]
      ? "detail"
      : ["tasks", "dag", "workers"].includes(parts[0])
        ? parts[0]
        : "tasks";
  taskId = route === "detail" ? parts[1] : "";
  detail = null;
  events = [];
  attemptId = null;
  tab = "progress";
  epoch++;
  render();
  void refresh();
}
window.addEventListener("hashchange", () => {
  if (route === "dag") {
    try {
      syncDraft();
    } catch (error) {
      notify(error.message);
    }
  }
  parseRoute();
});
setInterval(
  () => {
    if (route !== "dag") void refresh();
  },
  Math.max(2000, Number(window.DATAFLOW_CONFIG?.pollInterval) || 5000),
);

function renderTasks() {
  const total = Object.values(counts).reduce((a, b) => a + b, 0);
  const stat = (label, value, foot, icon) =>
    h(
      "div",
      { class: "stat" },
      h(
        "div",
        { class: "stat-label" },
        label,
        h("span", { class: "stat-icon" }, icon),
      ),
      h("div", { class: "stat-value" }, value),
      h("div", { class: "stat-foot" }, foot),
    );
  const filters = h(
    "div",
    { class: "toolbar" },
    h(
      "select",
      {
        "aria-label": "状态筛选",
        onchange: run((event) => {
          filterState = event.target.value;
          cursor = null;
          cursors = [];
          epoch++;
          return refresh();
        }),
      },
      h("option", { value: "" }, "全部状态"),
      Object.entries(states)
        .filter(([key]) =>
          ["QUEUED", "RUNNING", "STOPPING", ...terminal].includes(key),
        )
        .map(([key, label]) => h("option", { value: key }, label)),
    ),
    h("input", {
      "aria-label": "执行池筛选",
      placeholder: "执行池，如 default",
      value: filterPool,
      onchange: run((event) => {
        filterPool = event.target.value.trim();
        cursor = null;
        cursors = [];
        epoch++;
        return refresh();
      }),
    }),
  );
  filters.querySelector("select").value = filterState;
  const table = h(
    "table",
    {},
    h(
      "thead",
      {},
      h(
        "tr",
        {},
        ["任务 / ID", "状态", "执行进度", "执行池", "尝试", "创建时间", ""].map(
          (label) => h("th", {}, label),
        ),
      ),
    ),
    h(
      "tbody",
      {},
      rows.map((task) =>
        h(
          "tr",
          {},
          h(
            "td",
            {},
            h("a", { href: `#task/${task.id}`, class: "task-name" }, task.name),
            h("span", { class: "mono" }, task.id.slice(0, 14)),
          ),
          h("td", {}, badge(task.state)),
          h("td", {}, progress(task.progress, task.state)),
          h("td", {}, task.pool),
          h("td", {}, `${task.attempt_count} 次`),
          h("td", {}, date(task.created_at)),
          h(
            "td",
            {},
            h(
              "a",
              {
                href: `#task/${task.id}`,
                "aria-label": `查看任务 ${task.name}`,
              },
              "查看 →",
            ),
          ),
        ),
      ),
    ),
  );
  return [
    heading(
      "任务中心",
      "从文件提交到执行完成，观察每一次任务状态与执行进度。",
      button(
        "＋ 创建任务",
        () => {
          location.hash = "dag";
        },
        "primary",
      ),
    ),
    h(
      "div",
      { class: "stats" },
      stat("全部任务", total, "持久化任务记录", "▤"),
      stat(
        "正在执行",
        (counts.RUNNING || 0) + (counts.STOPPING || 0),
        `${counts.QUEUED || 0} 个任务等待调度`,
        "◷",
      ),
      stat("成功完成", counts.SUCCEEDED || 0, "完整文件处理成功", "✓"),
      stat("失败任务", counts.FAILED || 0, "可查看原因并整任务重试", "↗"),
    ),
    h(
      "section",
      { class: "panel" },
      h("div", { class: "panel-head" }, h("h3", {}, "任务列表"), filters),
      rows.length
        ? h("div", { class: "table-scroll" }, table)
        : empty(
            connected ? "暂无任务" : "连接 API 后查看任务",
            connected
              ? "调整筛选条件，或创建一个文件处理任务。"
              : "点击左下角连接设置，输入管控节点地址和管理令牌。",
          ),
      h(
        "div",
        { class: "table-footer" },
        h(
          "span",
          {},
          `第 ${cursors.length + 1} 页 · 当前 ${rows.length} 条 · 每 5 秒更新`,
        ),
        h(
          "div",
          { class: "actions" },
          button(
            "上一页",
            () => {
              cursor = cursors.pop() ?? null;
              epoch++;
              return refresh();
            },
            "ghost",
            !cursors.length,
          ),
          button(
            "下一页",
            () => {
              cursors.push(cursor);
              cursor = nextCursor;
              epoch++;
              return refresh();
            },
            "ghost",
            !nextCursor,
          ),
        ),
      ),
    ),
    h(
      "p",
      { class: "hint" },
      "任务失败或执行器丢失后，会按照重试策略从头执行文件；任务进度按 DAG 节点等权计算。",
    ),
  ];
}

async function cancelTask() {
  const client = needAPI(),
    id = detail.id;
  if (
    !(await confirm(
      "停止任务",
      "停止当前文件任务及其正在执行的节点。运行中的任务会先进入停止中，执行器确认退出后变为已取消。",
    ))
  )
    return;
  await client.post(`/v1/tasks/${id}/cancel`);
  notify("已发送停止请求", true);
  await refresh();
}
async function retryTask() {
  const client = needAPI(),
    id = detail.id;
  if (
    !(await confirm(
      "从头重试任务",
      "创建一个新任务，使用原文件和完整 DAG 从头执行；原任务记录保留。",
    ))
  )
    return;
  if (!retryKeys.has(id)) retryKeys.set(id, crypto.randomUUID());
  const result = await client.post(
    `/v1/tasks/${id}/retry`,
    {},
    retryKeys.get(id),
  );
  retryKeys.delete(id);
  notify("已创建重试任务", true);
  location.hash = `task/${result.id}`;
}
function renderDetail() {
  if (!detail)
    return [
      heading("任务详情", taskId),
      empty(
        connected ? "正在获取任务…" : "请先连接 API",
        "可在连接设置中配置管控节点。",
      ),
    ];
  const task = detail,
    attempts = task.attempts || [];
  const attempt = attempts.find((a) => a.id === attemptId) || attempts.at(-1),
    p = attempt?.progress || {};
  const current = attempts.at(-1);
  const selector = h(
    "select",
    {
      "aria-label": "查看尝试",
      onchange: (event) => {
        attemptId = event.target.value;
        render();
      },
    },
    attempts.map((a) =>
      h(
        "option",
        { value: a.id },
        `第 ${a.number} 次 · ${states[a.state] || a.state}`,
      ),
    ),
  );
  if (attempt) selector.value = attempt.id;
  let body;
  if (tab === "progress")
    body = h(
      "div",
      {},
      graph(task.spec.steps, { progress: p.steps }),
      h(
        "p",
        { class: "hint" },
        "DAG 在一个执行器内执行；同层节点可使用真实线程并行。",
      ),
      h(
        "div",
        { class: "step-list" },
        task.spec.steps.map((s) => {
          const state = p.steps?.[s.id] || {};
          return h(
            "div",
            { class: "step-row" },
            badge(state.state || "PENDING"),
            h(
              "div",
              {},
              h("strong", {}, s.id),
              h("p", { class: "hint" }, state.message || s.callable),
            ),
            h("span", { class: "muted" }, seconds(state.duration_seconds)),
            state.total
              ? h("small", {}, `${state.completed || 0} / ${state.total}`)
              : null,
          );
        }),
      ),
    );
  else if (tab === "logs")
    body = h(
      "div",
      {},
      h(
        "p",
        { class: "hint" },
        "执行器上报的日志末尾（最多约 16 KiB），可能延迟一个心跳周期；不是完整日志归档。",
      ),
      json(p.log_tail || "暂无日志", "log"),
    );
  else if (tab === "events")
    body = h(
      "div",
      {},
      h("p", { class: "hint" }, "最近 200 条持久化事件，按时间顺序展示。"),
      events.length
        ? events.map((event) =>
            h(
              "div",
              { class: "event" },
              h("span", { class: "muted" }, date(event.created_at)),
              h("strong", {}, event.kind),
              h("code", {}, JSON.stringify(event.payload)),
            ),
          )
        : empty("暂无事件", "任务事件会在状态变化时记录。"),
    );
  else if (tab === "result")
    body = h(
      "div",
      {},
      attempt?.error ? h("p", { class: "error-text" }, attempt.error) : null,
      json(attempt?.result || task.result || { message: "本次尝试尚无结果" }),
    );
  else body = json(task.spec);
  const tabs = [
    ["progress", "DAG / 进度"],
    ["logs", "日志"],
    ["events", "事件"],
    ["result", "结果"],
    ["spec", "任务配置"],
  ];
  return [
    h("a", { href: "#tasks", class: "back" }, "← 返回任务列表"),
    heading(
      task.spec.name,
      task.id,
      h(
        "div",
        { class: "actions" },
        button("复制到编排器", () => {
          draft = structuredClone(task.spec);
          selected = draft.steps[0].id;
          validation = "";
          submitIdentity = null;
          location.hash = "dag";
        }),
        button(
          "从头重试",
          retryTask,
          "",
          !["FAILED", "CANCELLED"].includes(task.state) || !connected,
        ),
        button(
          "停止任务",
          cancelTask,
          "danger ghost",
          !["QUEUED", "RUNNING"].includes(task.state) || !connected,
        ),
      ),
    ),
    task.error ? h("p", { class: "error-text" }, task.error) : null,
    h(
      "div",
      { class: "detail-grid" },
      h(
        "div",
        {},
        panel(
          "执行概况",
          h(
            "div",
            {},
            h(
              "div",
              { class: "title-meta" },
              badge(task.state),
              h(
                "span",
                { class: "muted" },
                `当前尝试 ${task.attempt_count} / ${task.spec.max_attempts}`,
              ),
            ),
            progress(current?.progress, task.state),
          ),
          selector,
        ),
        h(
          "section",
          { class: "panel" },
          h(
            "div",
            { class: "tabs" },
            tabs.map(([id, label]) =>
              button(
                label,
                () => {
                  tab = id;
                  render();
                },
                tab === id ? "active" : "",
              ),
            ),
          ),
          h("div", { class: "panel-body" }, body),
        ),
      ),
      h(
        "div",
        {},
        panel(
          "任务信息",
          kv([
            ["输入文件", task.spec.input_path],
            ["执行池", task.spec.pool],
            ["线程 / CPU", `${task.spec.dag_workers} / ${task.spec.cpu}`],
            ["内存声明", `${task.spec.memory_mb} MiB`],
            ["运行版本", task.spec.runtime_version],
            ["超时", `${task.spec.timeout} 秒`],
            ["创建时间", date(task.created_at)],
            ["更新时间", date(task.updated_at)],
          ]),
        ),
        panel(
          "本次尝试",
          kv([
            ["尝试 ID", attempt?.id],
            ["执行器会话", attempt?.worker_session],
            ["状态", attempt ? badge(attempt.state) : "等待分配"],
            ["开始时间", date(attempt?.started_at)],
            ["结束时间", date(attempt?.finished_at)],
            ["已用时间", seconds(p.elapsed_seconds)],
            ["错误", attempt?.error],
          ]),
        ),
        button("导出任务配置", () => download(task.spec, "task-spec.json")),
      ),
    ),
  ];
}

function renderWorkers() {
  return [
    heading(
      "执行器",
      "观察执行器心跳与声明容量；排空后完成已有任务，停止接收新任务。",
    ),
    h(
      "div",
      { class: "worker-cards" },
      workers.map((worker) =>
        h(
          "section",
          { class: "panel worker-card" },
          h(
            "div",
            { class: "panel-head" },
            h("h3", {}, worker.name),
            badge(
              !worker.online
                ? "offline"
                : worker.draining
                  ? "draining"
                  : "online",
            ),
          ),
          h(
            "div",
            { class: "panel-body" },
            kv([
              ["会话 ID", worker.session_id],
              ["执行池", worker.pool],
              ["运行版本", worker.runtime_version],
              ["并发槽位", worker.slots],
              ["CPU / 内存", `${worker.cpu} / ${worker.memory_mb} MiB`],
              ["最近心跳", date(worker.last_seen)],
            ]),
            h(
              "div",
              { class: "actions" },
              button(
                "排空执行器",
                async () => {
                  const client = needAPI();
                  if (
                    !(await confirm(
                      "排空执行器",
                      `${worker.name} 将继续完成当前任务，停止接收新任务。恢复接单需替换或重启为新会话。`,
                    ))
                  )
                    return;
                  await client.post(
                    `/v1/workers/${encodeURIComponent(worker.session_id)}/drain`,
                  );
                  notify("已请求排空执行器", true);
                  await refresh();
                },
                "ghost",
                !!worker.draining || !worker.online || !connected,
              ),
            ),
          ),
        ),
      ),
    ),
    !workers.length
      ? panel(
          "执行器列表",
          empty(
            connected ? "暂无执行器" : "连接 API 后查看执行器",
            "启动执行器后会自动注册并发送心跳。",
          ),
        )
      : null,
    h(
      "p",
      { class: "hint" },
      "CPU 与内存为调度声明，实际限制由 Pod 资源配置控制。旧会话保留用于审计，心跳过期显示离线。",
    ),
  ];
}

function objectJSON(value, label) {
  const result = JSON.parse(value);
  if (!result || typeof result !== "object" || Array.isArray(result))
    throw new Error(`${label}必须是 JSON 对象`);
  return result;
}
function syncDraft() {
  if (!$("#dag-form")) return;
  const copy = structuredClone(draft);
  $("#dag-form")
    .querySelectorAll("[data-field]")
    .forEach((node) => {
      const key = node.dataset.field;
      copy[key] = node.type === "number" ? Number(node.value) : node.value;
    });
  copy.parameters = objectJSON($("#task-parameters").value, "全局参数");
  if ($("#input-checksum").value.trim())
    copy.input_sha256 = $("#input-checksum").value.trim();
  else delete copy.input_sha256;
  const step = copy.steps.find((s) => s.id === selected);
  if (step && $("#node-id")) {
    const id = $("#node-id").value.trim();
    if (
      !/^[A-Za-z0-9_-]{1,100}$/.test(id) ||
      copy.steps.some((s) => s !== step && s.id === id)
    )
      throw new Error(
        "节点 ID 必须唯一，使用 1–100 位字母、数字、下划线或连字符",
      );
    step.callable = $("#node-callable").value.trim();
    step.parameters = objectJSON($("#node-parameters").value, "节点参数");
    step.depends_on = Array.from(
      $("#dependencies").querySelectorAll("input:checked"),
      (node) => node.value,
    );
    if (id !== step.id) {
      const old = step.id;
      step.id = id;
      copy.steps.forEach((s) => {
        s.depends_on = (s.depends_on || []).map((dep) =>
          dep === old ? id : dep,
        );
      });
    }
    selected = id;
  }
  if (JSON.stringify(copy) !== JSON.stringify(draft)) validation = "";
  draft = copy;
}
function formField(key, label, type = "text", span = false) {
  return h(
    "label",
    { class: span ? "span-2" : "" },
    label,
    h("input", {
      "data-field": key,
      "aria-label": label,
      value: draft[key],
      type,
      ...(type === "number"
        ? {
            step: key === "timeout" || key === "retry_delay" ? "any" : "1",
            min: key === "retry_delay" ? "0" : "1",
          }
        : {}),
    }),
  );
}
async function validateDraft() {
  syncDraft();
  const sent = JSON.stringify(draft),
    editEpoch = epoch;
  const result = await needAPI().post("/v1/dags/validate", draft);
  if (epoch !== editEpoch || route !== "dag")
    throw new Error("页面已切换，请重新校验编排");
  syncDraft();
  if (JSON.stringify(draft) !== sent)
    throw new Error("编排在校验期间已更改，请重新校验");
  validation = `校验通过 · ${result.spec.steps.length} 个节点 · ${result.layers.length} 个依赖层。文件存在性、算子安装及实际资源在提交/运行时检查。`;
  render();
  return result.spec;
}
async function importDraft(file) {
  if (!file) return;
  if (file.size > 1000000) throw new Error("配置文件不能超过 1 MB");
  const result = await needAPI().post(
    "/v1/dags/validate",
    JSON.parse(await file.text()),
  );
  draft = result.spec;
  selected = draft.steps[0].id;
  validation = "配置已导入并通过 API 校验";
  submitIdentity = null;
  render();
}
async function submitDraft() {
  if (submitting) return;
  submitting = true;
  try {
    const spec = await validateDraft(),
      payload = JSON.stringify(spec);
    if (submitIdentity?.payload !== payload)
      submitIdentity = { payload, key: crypto.randomUUID() };
    const result = await needAPI().post("/v1/tasks", spec, submitIdentity.key);
    // An acknowledged operation is complete; a later explicit submission is new work.
    submitIdentity = null;
    notify("任务已提交，等待执行器调度", true);
    location.hash = `task/${result.id}`;
  } finally {
    submitting = false;
    const submitButton = $("#submit-dag");
    if (submitButton) submitButton.disabled = false;
  }
}

function renderDAG() {
  const selectedStep =
    draft.steps.find((s) => s.id === selected) || draft.steps[0];
  selected = selectedStep.id;
  const importInput = h("input", {
    type: "file",
    accept: ".json,application/json",
    hidden: true,
    onchange: run((event) => importDraft(event.target.files[0])),
  });
  const fields = h(
    "div",
    { class: "form-grid" },
    formField("name", "任务名称"),
    formField("input_path", "共享文件绝对路径"),
    formField("pool", "执行池"),
    formField("runtime_version", "运行版本"),
    formField("dag_workers", "DAG 线程数", "number"),
    formField("cpu", "CPU 声明", "number"),
    formField("memory_mb", "内存声明 MiB", "number"),
    formField("timeout", "任务超时 秒", "number"),
  );
  const extra = h(
    "details",
    {},
    h("summary", {}, "重试策略、校验和与全局参数"),
    h(
      "div",
      { class: "form-grid" },
      formField("max_attempts", "最多尝试次数", "number"),
      formField("retry_delay", "重试间隔 秒", "number"),
      h(
        "label",
        { class: "span-2" },
        "输入 SHA256（可选）",
        h("input", { id: "input-checksum", value: draft.input_sha256 || "" }),
      ),
      h(
        "label",
        { class: "span-2" },
        "全局参数 JSON",
        h(
          "textarea",
          { id: "task-parameters", rows: 5 },
          JSON.stringify(draft.parameters || {}, null, 2),
        ),
      ),
    ),
  );
  const dependencies = h(
    "div",
    { id: "dependencies", class: "dependencies" },
    draft.steps
      .filter((s) => s.id !== selected)
      .map((s) =>
        h(
          "label",
          {},
          h("input", {
            type: "checkbox",
            value: s.id,
            checked: (selectedStep.depends_on || []).includes(s.id),
          }),
          s.id,
        ),
      ),
  );
  const selectNode = (id) => {
    syncDraft();
    selected = id;
    render();
  };
  const nodeEditor = panel(
    "节点配置",
    h(
      "div",
      {},
      h(
        "label",
        {},
        "节点 ID",
        h("input", { id: "node-id", value: selectedStep.id }),
      ),
      h(
        "label",
        {},
        "算子 module:symbol",
        h("input", { id: "node-callable", value: selectedStep.callable }),
      ),
      h("label", {}, "上游依赖", dependencies),
      h(
        "label",
        {},
        "算子配置参数 JSON（提交时填写）",
        h(
          "textarea",
          { id: "node-parameters", rows: 6 },
          JSON.stringify(selectedStep.parameters || {}, null, 2),
        ),
      ),
      h(
        "p",
        { class: "hint" },
        "算子通过 context.parameters 获取配置，通过 inputs 获取上游输出。节点配置覆盖同名任务级参数；提交后配置随任务保存，重试沿用。",
      ),
      h(
        "div",
        { class: "actions" },
        button(
          "删除节点",
          () => {
            syncDraft();
            draft.steps = draft.steps.filter((s) => s.id !== selected);
            draft.steps.forEach((s) => {
              s.depends_on = (s.depends_on || []).filter(
                (id) => id !== selected,
              );
            });
            selected = draft.steps[0].id;
            validation = "";
            render();
          },
          "danger ghost",
          draft.steps.length === 1,
        ),
        button(
          "应用",
          () => {
            syncDraft();
            render();
          },
          "primary",
        ),
      ),
    ),
  );
  nodeEditor.classList.add("node-editor");
  const dagPanel = h(
    "section",
    { class: "panel" },
    h(
      "div",
      { class: "panel-head" },
      h("h3", {}, "DAG 依赖图"),
      button(
        "＋ 添加节点",
        () => {
          syncDraft();
          let number = draft.steps.length + 1;
          while (draft.steps.some((s) => s.id === `step_${number}`)) number++;
          const step = {
            id: `step_${number}`,
            callable: "dataflowcore.operators:delay",
            depends_on: [selected],
            parameters: { seconds: 1 },
          };
          draft.steps.push(step);
          selected = step.id;
          validation = "";
          render();
        },
        "",
        draft.steps.length >= 1000,
      ),
    ),
    graph(draft.steps, { selected }),
    h(
      "div",
      { class: "node-chips" },
      draft.steps.map((s) =>
        button(s.id, () => selectNode(s.id), s.id === selected ? "active" : ""),
      ),
    ),
  );
  // Use a guarded callback so malformed JSON never discards edits during selection.
  dagPanel.querySelector(".graph-scroll").replaceWith(
    graph(draft.steps, {
      selected,
      onSelect: (id) => {
        try {
          selectNode(id);
        } catch (error) {
          notify(error.message);
        }
      },
    }),
  );
  const editor = h(
    "div",
    { id: "dag-form" },
    panel(
      "任务配置",
      h(
        "div",
        {},
        fields,
        extra,
        h(
          "p",
          { class: "hint" },
          "输入文件须已存在于所有 Pod 共享的目录中；此处提交文件路径。算子代码需预先安装到执行器镜像。",
        ),
      ),
    ),
    h(
      "div",
      { class: "editor-grid" },
      h(
        "div",
        {},
        dagPanel,
        h(
          "p",
          { class: "hint" },
          "点击节点编辑；勾选上游依赖创建连接。API 会拒绝环、未知依赖与重复节点。",
        ),
      ),
      nodeEditor,
    ),
  );
  editor.addEventListener("input", () => {
    validation = "";
    $("#validation")?.remove();
  });
  const submitButton = button("提交任务", submitDraft, "primary", submitting);
  submitButton.id = "submit-dag";
  return [
    heading(
      "DAG 编排",
      "配置文件任务、算子参数与依赖关系，校验后交由管控节点调度。",
      h(
        "div",
        { class: "actions" },
        button("导入 JSON", () => {
          needAPI();
          importInput.click();
        }),
        button("导出 JSON", () => {
          syncDraft();
          download(draft, "task-spec.json");
        }),
        button("校验 DAG", validateDraft),
        submitButton,
      ),
    ),
    importInput,
    validation
      ? h(
          "div",
          { id: "validation", class: "validation", role: "status" },
          validation,
        )
      : null,
    editor,
    h(
      "div",
      { class: "actions" },
      button("恢复示例", async () => {
        if (
          await confirm(
            "恢复示例 DAG",
            "当前编排内容将被示例替换。可先导出 JSON 保存。",
          )
        ) {
          draft = template(version);
          selected = draft.steps[0].id;
          validation = "";
          submitIdentity = null;
          render();
        }
      }),
      button("保存草稿", () => {
        syncDraft();
        localStorage.setItem("dataflow-dag-draft", JSON.stringify(draft));
        notify("草稿已保存在此浏览器，不含管理令牌", true);
      }),
      button("加载草稿", () => {
        const saved = localStorage.getItem("dataflow-dag-draft");
        if (!saved) throw new Error("此浏览器没有已保存的草稿");
        return importDraft(new Blob([saved]));
      }),
    ),
  ];
}
function render() {
  const titles = {
    tasks: "任务中心",
    workers: "执行器",
    dag: "DAG 编排",
    detail: "任务详情",
  };
  $("#breadcrumb").textContent = titles[route];
  document
    .querySelectorAll("[data-nav]")
    .forEach((node) =>
      node.classList.toggle(
        "active",
        node.dataset.nav === (route === "detail" ? "tasks" : route),
      ),
    );
  main.replaceChildren(
    ...(route === "dag"
      ? renderDAG()
      : route === "workers"
        ? renderWorkers()
        : route === "detail"
          ? renderDetail()
          : renderTasks()
    ).filter(Boolean),
  );
}
parseRoute();
