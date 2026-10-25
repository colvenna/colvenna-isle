"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const SCOPES = ["datasets:manage", "ingest:write", "ingest:replay", "data:write", "query:read", "query:manage",
    "pipeline:run", "pipeline:manage", "runtime:manage", "monitor:read", "keys:manage", "audit:read", "retention:manage",
    "backup:manage"];
  const AUDIT_ACTIONS = [
    "KEY_BOOTSTRAPPED", "KEY_CREATED", "KEY_ROTATED", "KEY_REVOKED", "DATASET_REGISTERED",
    "PIPELINE_CREATED", "PIPELINE_DRAFT_REPLACED", "PIPELINE_ARTIFACT_ADMITTED", "PIPELINE_VERSION_PUBLISHED",
    "PIPELINE_ACTIVATED", "PIPELINE_SCHEDULE_CHANGED", "ENVIRONMENT_CREATED", "ENVIRONMENT_BUILD_ADMITTED",
    "RUN_CREATED", "RUN_CANCEL_REQUESTED", "RUN_RETRIED", "TRIAL_CREATED", "INGESTION_REPLAY_INTENT",
    "RETENTION_POLICY_CHANGED", "RETENTION_HOLD_PLACED", "RETENTION_HOLD_RELEASED",
    "RETENTION_COLLECTION_REQUESTED", "RETENTION_COLLECTION_COMPLETED", "QUOTA_POLICY_CHANGED",
    "GENERATION_PURGED", "BACKUP_CREATED", "SPACE_RESTORED",
  ];
  const RETENTION_FIELDS = ["raw_seconds", "replay_grace_seconds", "upload_seconds", "staging_seconds",
    "pipeline_version_seconds", "release_keep", "release_seconds", "failed_build_seconds"];
  const TABS = ["overview", "keys", "pipelines", "environments", "audit", "trace", "retention", "quotas"];
  const SECTIONS = [["writer", "写者"], ["queue", "接入队列"], ["dlq", "死信队列"], ["jobs", "作业"],
    ["queries", "查询"], ["scheduler", "调度器"]];
  const LAYERS = ["ods", "dwd", "dws", "ads"];
  const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_.-]{0,127}$/;
  const RELATIVE = /^(?!\/)(?!.*(?:^|\/)\.\.(?:\/|$))(?!.*\\)[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*$/;
  const DATASET = /^(ods|dwd|dws|ads)\.[A-Za-z_][A-Za-z0-9_]*$/;
  const RUN_TERMINAL = new Set(["PUBLISHED", "FAILED", "CANCELLED", "VALIDATED"]);
  // Known reasons a QUEUED run has not started; any other server code is shown verbatim.
  const WAITING = {
    PIPELINE_RUN_ACTIVE: "同一流水线已有运行进行中",
    SPACE_RUN_QUOTA: "空间活动运行数已达配额",
    WRITER_AUTHORITY_UNAVAILABLE: "写者不可用，恢复后再绑定根输入",
    WRITER_RESULT_UNKNOWN: "写者结果未确认，将按原请求核对",
  };
  const ENVIRONMENT_ACTIVE = new Set(["RESOLVING", "BUILDING", "VERIFYING", "UNKNOWN"]);
  const COLLECTION_ACTIVE = new Set(["QUEUED", "RUNNING"]);
  const TRACEABLE = new Set(["run", "ingestion", "query", "release"]);
  const POLL_MS = 5000;
  const state = {
    context: null, epoch: 0, view: 0, tab: null, requests: new Set(), timers: new Map(), pending: null,
    token: null, keys: [], rotating: null, confirming: null,
    pipeline: null, creating: false, files: [], file: null, versions: [], selection: 0, runs: { run: null, trial: null },
    environment: null, audit: { next: null, page: 1 }, trace: null,
    retention: null, retentionRevision: null, collection: null,
  };

  class ApiError extends Error {
    constructor(status, code, message) {
      super(message || "请求失败");
      this.status = status;
      this.code = code || "REQUEST_FAILED";
    }
  }
  class LocalError extends Error {}
  class StaleError extends Error {}

  // -- DOM helpers: every value is text; no response is parsed as HTML. --

  function node(tag, className) {
    const value = document.createElement(tag);
    if (className) value.className = className;
    return value;
  }

  function text(tag, value, className) {
    const result = node(tag, className);
    result.textContent = value;
    return result;
  }

  function display(value) {
    if (value === null || value === undefined || value === "") return "—";
    if (typeof value === "object") return JSON.stringify(value);
    return String(value);
  }

  function badge(status) {
    const value = text("span", display(status), "status-badge");
    value.dataset.status = String(status);
    return value;
  }

  function button(label, handler, className) {
    const value = text("button", label, className);
    value.type = "button";
    value.addEventListener("click", safely(handler));
    return value;
  }

  function row(...children) {
    const value = node("div", "meta");
    value.append(...children);
    return value;
  }

  function heading(label) {
    return text("h3", label, "subheading");
  }

  function observedText(at, age) {
    const suffix = age !== null && age !== undefined ? " · 距今 " + age + " 秒" : "";
    return (at === null || at === undefined ? "观测时间：未观测" : "观测 " + at) + suffix;
  }

  function pairs(values, omit = []) {
    const entries = Object.entries(values && typeof values === "object" ? values : {})
      .filter(([name]) => !omit.includes(name));
    if (!entries.length) return text("p", "无附加字段。", "small muted");
    const list = node("dl", "pairs");
    for (const [name, value] of entries) {
      const detail = text("dd", display(value));
      if (value === null || value === undefined) detail.className = "null";
      list.append(text("dt", name), detail);
    }
    return list;
  }

  function fill(target, value) {
    if (value === "") return;
    if (value instanceof Node) {
      target.append(value);
    } else if (Array.isArray(value)) {
      target.append(...value);
    } else {
      target.textContent = display(value);
      if (value === null || value === undefined) target.classList.add("null");
    }
  }

  // columns: [label, (row) => text | Node | Node[]]; after() may append a sub-row.
  function table(columns, rows, empty = "暂无数据。", after = null) {
    if (!rows.length) return text("p", empty, "small muted");
    const value = node("table");
    const head = value.createTHead().insertRow();
    for (const [label] of columns) head.append(text("th", label));
    const body = value.createTBody();
    for (const item of rows) {
      const tr = body.insertRow();
      for (const [, read] of columns) fill(tr.insertCell(), read(item));
      if (after) after(item, body, columns.length);
    }
    return value;
  }

  function markStale(target, message = "最近一次读取未确认，以下为之前的观测。") {
    for (const marker of target.querySelectorAll(":scope > .stale-marker")) marker.remove();
    target.prepend(text("p", message, "reason stale-marker"));
  }

  function notice(message, error = false) {
    $("notice").textContent = message;
    $("notice").classList.toggle("error", error);
    $("notice").hidden = !message;
  }

  // -- Connection, paths and the single API wrapper. --

  function current(context) {
    return Boolean(context) && state.context === context;
  }

  function live(context, view) {
    return current(context) && state.view === view;
  }

  function enc(value) {
    return encodeURIComponent(value);
  }

  function spacePath(context = state.context) {
    return "/v1/spaces/" + enc(context.space);
  }

  function adminPath(context = state.context) {
    return "/v1/admin/spaces/" + enc(context.space);
  }

  function pipelinePath(id, context = state.context) {
    return spacePath(context) + "/pipelines/" + enc(id);
  }

  function query(values) {
    const parts = Object.entries(values)
      .filter(([, value]) => value !== undefined && value !== null && value !== "")
      .map(([name, value]) => enc(name) + "=" + enc(value));
    return parts.length ? "?" + parts.join("&") : "";
  }

  function quoted(revision) {
    return '"' + revision + '"';
  }

  function idempotencyKey() {
    return "console." + crypto.randomUUID();
  }

  async function api(path, options = {}, context = state.context) {
    if (!context) throw new LocalError("请先连接数据空间。");
    const target = new URL(path, location.origin);
    const prefixes = [spacePath(context) + "/", adminPath(context) + "/"];
    if (target.origin !== location.origin || path.includes("#") || target.pathname !== path.split("?")[0]
        || !prefixes.some((prefix) => path.startsWith(prefix))) {
      throw new LocalError("请求不属于当前数据空间。");
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 30000);
    state.requests.add(controller);
    const headers = { Authorization: "Bearer " + context.key, ...(options.headers || {}) };
    if (options.body !== undefined) headers["Content-Type"] = "application/json";
    try {
      const response = await fetch(path, {
        method: options.method || "GET", headers,
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
        signal: controller.signal, cache: "no-store", credentials: "omit",
        redirect: "error", referrerPolicy: "no-referrer",
      });
      let value;
      try {
        value = await response.json();
      } catch (error) {
        if (response.ok) throw error;
        value = {};
      }
      if (!current(context)) throw new StaleError("CONNECTION_CHANGED");
      if (!response.ok) {
        const body = value && typeof value === "object" ? value : {};
        throw new ApiError(response.status, body.code || body.error, body.message);
      }
      if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("响应不是 JSON 对象");
      return value;
    } finally {
      clearTimeout(timer);
      state.requests.delete(controller);
    }
  }

  // A transport failure, timeout or 5xx leaves the server outcome unconfirmed.
  function unconfirmed(error) {
    if (error instanceof ApiError) return error.status >= 500;
    return !(error instanceof LocalError || error instanceof StaleError);
  }

  function describe(error) {
    if (error instanceof ApiError) {
      const detail = error.code + " · " + error.message;
      if (error.status === 401 || error.status === 403) return detail + " · 服务端拒绝（权限或空间不匹配）";
      if (error.status === 412) return "修订冲突：已被其他人修改，请重新加载 · " + detail;
      if (error.status >= 500) return "请求未确认 · HTTP " + error.status + " · " + detail;
      return detail;
    }
    if (error instanceof LocalError) return error.message;
    return "请求未确认（网络中断、超时或响应无法解析）。";
  }

  function report(error, context = state.context, view = null) {
    if (error instanceof StaleError || !current(context) || (view !== null && view !== state.view)) return;
    notice(describe(error) + (state.pending && unconfirmed(error) ? "\n原请求身份已保留，请使用“核对原请求”。" : ""), true);
  }

  // -- Mutations keep their exact identity until the server confirms an outcome. --

  function request(context, label, method, path, body, headers = {}, replay = true) {
    return { context, label, method, path, body, headers, replay, attempts: 0, lastError: null, done: null };
  }

  async function send(spec) {
    if (state.pending && state.pending !== spec) {
      throw new LocalError("请先核对原请求：" + state.pending.label + "。核对前不发送新的变更请求。");
    }
    spec.attempts++;
    try {
      const value = await api(spec.path, spec, spec.context);
      if (state.pending === spec) {
        state.pending = null;
        renderPending();
        notice("原请求已确认 · " + spec.label);
      }
      return value;
    } catch (error) {
      if (!current(spec.context) || error instanceof StaleError) throw error;
      if (state.pending === spec || (spec.replay && unconfirmed(error))) {
        spec.lastError = error;
        state.pending = spec;
        renderPending();
      }
      throw error;
    }
  }

  // done() continues a multi-step flow, so a confirmed retry resumes where it stopped.
  async function step(spec) {
    const value = await send(spec);
    return spec.done ? spec.done(value) : value;
  }

  function renderPending() {
    const spec = state.pending;
    $("pending").hidden = !spec;
    $("pending-detail").replaceChildren();
    if (!spec) return;
    $("pending-detail").append(text("strong", "请求未确认 · " + spec.label), text("code", spec.method + " " + spec.path));
    for (const name of ["Idempotency-Key", "If-Match"]) {
      if (spec.headers[name]) $("pending-detail").append(text("code", name + " " + spec.headers[name]));
    }
    if (spec.lastError) {
      const conflict = spec.attempts > 1 && spec.lastError instanceof ApiError && spec.lastError.status === 412;
      $("pending-detail").append(text("span", "最近结果：" + describe(spec.lastError)
        + (conflict ? "。原请求可能已经生效，请重新加载核对。" : ""), "small"));
    }
    $("pending-detail").append(text("span", "“核对原请求”重发完全相同的请求与身份，不生成新的身份。", "small muted"));
  }

  async function retryPending() {
    const spec = state.pending;
    if (!spec) return;
    $("pending-retry").disabled = true;
    try {
      await step(spec);
    } catch (error) {
      report(error, spec.context);
    } finally {
      if (current(spec.context)) $("pending-retry").disabled = false;
    }
  }

  function dropPending() {
    const spec = state.pending;
    if (!spec) return;
    state.pending = null;
    renderPending();
    notice("已放弃核对 · " + spec.label + "。该请求的服务端结果仍未确认。", true);
  }

  // Runs synchronously (so submit handlers can preventDefault) and reports any rejection.
  function safely(handler) {
    return (...values) => {
      try {
        const result = handler(...values);
        if (result && typeof result.then === "function") result.catch((error) => report(error));
      } catch (error) {
        report(error);
      }
    };
  }

  // -- Timers and tab navigation. --

  function schedule(name, handler, delay) {
    clearTimeout(state.timers.get(name));
    state.timers.set(name, setTimeout(safely(handler), delay));
  }

  function stopTimer(name) {
    clearTimeout(state.timers.get(name));
    state.timers.delete(name);
  }

  function stopTimers() {
    for (const timer of state.timers.values()) clearTimeout(timer);
    state.timers.clear();
  }

  function tabFromHash() {
    const name = location.hash.slice(1);
    return TABS.includes(name) ? name : "overview";
  }

  const LOADERS = {
    overview: () => loadOverview(),
    keys: () => loadKeys(),
    pipelines: () => enterPipelines(),
    environments: () => Promise.all([loadEnvironments(), state.environment ? observeEnvironment() : null]),
    audit: () => loadAudit(null, 1),
    trace: () => null,
    retention: () => Promise.all([loadRetention(true), state.collection ? observeCollection() : null]),
    quotas: () => Promise.all([loadQuotas(), loadCosts()]),
  };

  function show(tab) {
    if (!state.context) return;
    stopTimers();
    clearToken();
    state.confirming = null;
    state.view++;
    state.tab = tab;
    notice("");
    for (const link of document.querySelectorAll("[data-tab]")) {
      if (link.dataset.tab === tab) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    }
    for (const panel of document.querySelectorAll("[data-panel]")) panel.hidden = panel.dataset.panel !== tab;
    return LOADERS[tab]();
  }

  // -- Overview. --

  async function loadOverview() {
    stopTimer("overview");
    const context = state.context;
    const view = state.view;
    const [status, metrics] = await Promise.allSettled([
      api(adminPath(context) + "/status", {}, context),
      api(adminPath(context) + "/metrics" + query({ window_seconds: 3600 }), {}, context),
    ]);
    if (!live(context, view)) return;
    if (status.status === "fulfilled") {
      renderStatus(status.value);
    } else {
      for (const id of ["overview-meta", "overview-cards", "overview-datasets"]) $(id).replaceChildren();
      $("overview-cards").append(text("p", "概览尚未取得。", "small muted"));
      report(status.reason, context, view);
    }
    if (metrics.status === "fulfilled") {
      renderMetrics(metrics.value);
    } else {
      $("metrics-meta").replaceChildren();
      $("overview-metrics").replaceChildren(text("p", "指标尚未取得。", "small muted"));
      if (status.status === "fulfilled") report(metrics.reason, context, view);
    }
    if ($("overview-auto").checked) schedule("overview", loadOverview, 15000);
  }

  function observationCard(label, name, value) {
    const card = node("article", "obs-card");
    card.dataset.section = name;
    const title = text("h3", label + " ");
    title.append(text("span", name, "kind"));
    const head = node("div", "obs-head");
    head.append(title, badge(value.status));
    card.append(head, text("div", observedText(value.observed_at, value.age_seconds), "small observed"),
      text("div", "来源 " + display(value.source), "small muted"));
    if (value.reason) card.append(text("div", "原因 " + value.reason, "reason"));
    card.append(pairs(value.metrics));
    return card;
  }

  function renderStatus(value) {
    $("overview-meta").replaceChildren(text("span", "空间 " + display(value.space_id)),
      text("span", "generation " + display(value.generation)), text("span", "观测 " + display(value.observed_at)));
    const cards = [];
    for (const [name, label] of SECTIONS) {
      if (value[name]) cards.push(observationCard(label, name, value[name]));
    }
    for (const layer of LAYERS) {
      if (value.layers && value.layers[layer]) cards.push(observationCard("分层", "layers." + layer, value.layers[layer]));
    }
    $("overview-cards").replaceChildren(...cards);
    $("overview-datasets").replaceChildren(table([
      ["数据集", (item) => text("code", item.dataset)], ["层", (item) => item.layer], ["写入方", (item) => item.write_owner],
      ["状态", (item) => badge(item.status)], ["提交水位", (item) => item.commit_sequence],
      ["读取水位", (item) => item.snapshot_sequence], ["Release ID", (item) => item.release_id],
      ["发布时间", (item) => item.published_at],
      ["观测时间", (item) => item.observed_at === null ? "未观测" : item.observed_at],
    ], value.datasets || [], "暂无数据集。"));
  }

  function renderMetrics(value) {
    $("metrics-meta").replaceChildren(text("span", "窗口 " + display(value.window_seconds) + " 秒"),
      text("span", "观测 " + display(value.observed_at)));
    $("overview-metrics").replaceChildren(table([
      ["指标", (item) => text("code", item.name)],
      ["值", (item) => item.status === "UNKNOWN" ? "UNKNOWN · " + display(item.reason)
        : item.value === null ? "—" : item.value + " " + item.unit],
      ["状态", (item) => badge(item.status)], ["样本", (item) => item.samples],
      ["窗口（秒）", (item) => item.window_seconds],
      ["观测时间", (item) => item.observed_at === null ? "未观测" : item.observed_at], ["来源", (item) => item.source],
    ], value.series || [], "暂无指标。"));
  }

  // -- API Keys. The one-time token lives only in state.token and #token-value. --

  function showToken(value) {
    state.token = value.token;
    $("token-value").value = value.token;
    $("token-key").replaceChildren(text("code", value.key.key_id), text("span", value.key.name), badge(value.key.status),
      text("span", "到期 " + value.key.expires_at));
    $("token-panel").hidden = false;
    $("token-value").focus();
    $("token-value").select();
  }

  function clearToken() {
    state.token = null;
    $("token-value").value = "";
    $("token-key").replaceChildren();
    $("token-panel").hidden = true;
  }

  async function copyToken() {
    const token = state.token;
    if (!token) return;
    if (navigator.clipboard && navigator.clipboard.writeText) {
      try {
        await navigator.clipboard.writeText(token);
        notice("令牌已复制到剪贴板。");
        return;
      } catch (error) {
        // Fall back to a selected field the user copies manually.
      }
    }
    $("token-value").focus();
    $("token-value").select();
    notice("浏览器未允许写入剪贴板，令牌已选中，请手动复制。", true);
  }

  async function loadKeys() {
    const context = state.context;
    const view = state.view;
    try {
      const value = await api(adminPath(context) + "/api-keys", {}, context);
      if (!live(context, view)) return;
      state.keys = value.keys || [];
      renderKeys();
    } catch (error) {
      if (!live(context, view)) return;
      state.keys = [];
      $("keys-table").replaceChildren(text("p", "列表尚未取得。", "small muted"));
      report(error, context, view);
    }
  }

  function refreshKeys() {
    clearToken();
    return loadKeys();
  }

  function keyActions(key) {
    if (key.status !== "ACTIVE") return "";
    if (state.confirming === key.key_id) {
      return [button("确认吊销", () => revokeKey(key), "small-button danger"),
        button("取消", () => { state.confirming = null; renderKeys(); }, "small-button quiet")];
    }
    const revoke = button("吊销", () => { state.confirming = key.key_id; renderKeys(); }, "small-button danger");
    // The service issues one replacement per key; a rotated key can only be revoked.
    return key.replacement_key_id ? [revoke] : [button("轮换", () => startRotate(key), "small-button"), revoke];
  }

  function renderKeys() {
    $("keys-table").replaceChildren(table([
      ["key_id", (key) => text("code", key.key_id)], ["名称", (key) => key.name], ["指纹", (key) => key.fingerprint],
      ["权限范围", (key) => (key.scopes || []).join("\n")], ["状态", (key) => badge(key.status)],
      ["创建时间", (key) => key.created_at], ["到期时间", (key) => key.expires_at],
      ["最近使用", (key) => key.last_used_at === null || key.last_used_at === undefined ? "未观测" : key.last_used_at],
      ["替换关系", (key) => [key.replaces_key_id && "替换 " + key.replaces_key_id,
        key.replacement_key_id && "已轮换为 " + key.replacement_key_id].filter(Boolean).join("\n") || null],
      ["", keyActions],
    ], state.keys, "暂无 API Key。"));
  }

  function scopeBoxes() {
    return [...$("key-scopes").querySelectorAll("input[type=checkbox]")];
  }

  function startRotate(key) {
    clearToken();
    state.rotating = key.key_id;
    state.confirming = null;
    $("key-form-title").textContent = "轮换 " + key.key_id;
    $("key-submit").textContent = "轮换";
    $("key-rotate-cancel").hidden = false;
    $("key-name").value = key.name;
    $("key-expires").value = "";
    for (const box of scopeBoxes()) box.checked = (key.scopes || []).includes(box.value);
    renderKeys();
    $("key-name").focus();
  }

  function resetKeyForm() {
    state.rotating = null;
    $("key-form").reset();
    $("key-form-title").textContent = "创建 API Key";
    $("key-submit").textContent = "创建";
    $("key-rotate-cancel").hidden = true;
  }

  // datetime-local has no zone; the console sends it as UTC.
  function utcStamp(value) {
    if (!value) return undefined;
    const match = /^(\d{4}-\d\d-\d\d)T(\d\d:\d\d)(?::(\d\d)(?:\.\d+)?)?$/.exec(value);
    if (!match) throw new LocalError("时间格式无效：" + value);
    return match[1] + "T" + match[2] + ":" + (match[3] || "00") + "Z";
  }

  function keyRequest() {
    const name = $("key-name").value.trim();
    const scopes = scopeBoxes().filter((box) => box.checked).map((box) => box.value);
    const expires = utcStamp($("key-expires").value);
    if (!name) throw new LocalError("请输入名称。");
    if (!scopes.length) throw new LocalError("至少选择一个权限范围。");
    if (!expires) throw new LocalError("请填写到期时间（UTC）。");
    return { name, scopes, expires_at: expires };
  }

  async function submitKey(event) {
    event.preventDefault();
    const context = state.context;
    const view = state.view;
    const body = keyRequest();
    const rotating = state.rotating;
    const verb = rotating ? "轮换" : "创建";
    const path = adminPath(context) + "/api-keys" + (rotating ? "/" + enc(rotating) + "/rotate" : "");
    clearToken();
    $("key-submit").disabled = true;
    try {
      // Key issuance has no idempotency identity, so an unconfirmed reply is never resent.
      const value = await send(request(context, verb + " API Key", "POST", path, body, {}, false));
      if (!value.key || typeof value.token !== "string" || !value.token) throw new LocalError("回执缺少 Key 或令牌。");
      if (!live(context, view)) {
        notice("已" + verb + " " + value.key.key_id + "，但页面已切换，令牌未显示。如需使用请再次轮换，否则请吊销该 Key。", true);
        return;
      }
      showToken(value);
      resetKeyForm();
      notice("已" + verb + " · " + value.key.key_id + "。令牌只显示这一次。");
      await loadKeys();
    } catch (error) {
      if (!current(context)) return;
      if (unconfirmed(error)) {
        notice("请求未确认：" + verb + " API Key 没有幂等身份，页面不会自动重发。请刷新列表核对名称为“"
          + body.name + "”的 Key；若已生成，令牌无法再次取得，请吊销它。", true);
        return;
      }
      report(error, context);
    } finally {
      if (current(context)) $("key-submit").disabled = false;
    }
  }

  async function revokeKey(key) {
    const context = state.context;
    state.confirming = null;
    const spec = request(context, "吊销 " + key.key_id, "POST",
      adminPath(context) + "/api-keys/" + enc(key.key_id) + "/revoke", {});
    spec.done = (value) => {
      notice("吊销请求已确认 · " + value.key_id + " · 状态 " + value.status);
      return refreshKeys();
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  // -- Pipelines: list, metadata, draft editor and DAG preview. --

  async function enterPipelines() {
    renderSummary();
    await loadPipelines();
    if (state.pipeline) {
      await Promise.all([loadVersions(), loadRuns(), state.runs.run ? observeRun("run") : null,
        state.runs.trial ? observeRun("trial") : null]);
    }
  }

  async function loadPipelines() {
    const context = state.context;
    const view = state.view;
    try {
      const value = await api(spacePath(context) + "/pipelines", {}, context);
      if (!live(context, view)) return;
      renderPipelineList(value.pipelines || []);
    } catch (error) {
      if (!live(context, view)) return;
      $("pipeline-list").replaceChildren(text("p", "列表尚未取得。", "small muted"));
      report(error, context, view);
    }
  }

  function renderPipelineList(pipelines) {
    const list = $("pipeline-list");
    list.replaceChildren();
    if (!pipelines.length) list.append(text("p", "暂无流水线。", "small muted"));
    for (const item of pipelines) {
      const entry = node("button", "list-item");
      entry.type = "button";
      entry.dataset.pipeline = item.pipeline_id;
      entry.setAttribute("aria-current", String(Boolean(state.pipeline) && state.pipeline.pipeline_id === item.pipeline_id));
      const detail = node("span", "small");
      detail.append(badge(item.status), text("span", item.tasks + " 个任务", "muted"),
        text("span", "激活 " + (item.active_version || "未激活"), "muted"));
      entry.append(text("code", item.pipeline_id), text("span", item.name, "small"), detail);
      entry.addEventListener("click", safely(() => openPipeline(item.pipeline_id)));
      list.append(entry);
    }
  }

  async function openPipeline(id) {
    const context = state.context;
    const view = state.view;
    const selection = ++state.selection;
    try {
      const value = await api(pipelinePath(id, context), {}, context);
      if (!live(context, view) || selection !== state.selection) return;
      if (value.pipeline_id !== id) throw new LocalError("流水线身份不一致。");
      setPipeline(value);
      for (const entry of $("pipeline-list").children) {
        if (entry.dataset.pipeline) entry.setAttribute("aria-current", String(entry.dataset.pipeline === id));
      }
      await Promise.all([loadVersions(), loadRuns()]);
    } catch (error) {
      if (live(context, view) && selection === state.selection) report(error, context, view);
    }
  }

  function resetPipelineViews() {
    stopTimer("run");
    stopTimer("trial");
    state.runs = { run: null, trial: null };
    state.versions = [];
    state.file = null;
    for (const id of ["validation", "publish-result", "file-meta", "versions", "runs", "run-result", "run-detail",
      "trial-result", "trial-detail", "trial-preview", "run-version", "trial-version", "trial-task"]) {
      $(id).replaceChildren();
    }
    $("file-editor").hidden = true;
    $("run-view").hidden = true;
    $("trial-view").hidden = true;
  }

  function setPipeline(value) {
    resetPipelineViews();
    state.pipeline = value;
    state.creating = false;
    state.files = (value.artifacts || []).map((artifact) => ({
      path: artifact.path, upload_id: artifact.upload_id, sha256: artifact.sha256, bytes: artifact.bytes,
      encoding: null, content: null, original: null, isNew: false,
    }));
    $("definition").value = value.definition ? JSON.stringify(value.definition, null, 2) : "";
    showPipelineCards(true);
    renderSummary();
    renderFiles();
    renderDag();
    fillSchedule();
    fillTrialTasks();
  }

  function newPipeline() {
    state.selection++;
    resetPipelineViews();
    state.pipeline = null;
    state.creating = true;
    state.files = [];
    $("definition").value = JSON.stringify({
      spec_version: 1, pipeline_id: "", name: "", schedule: { owner: "none" },
      max_active_runs: 1, coalesce_pending: true, tasks: [],
    }, null, 2);
    showPipelineCards(false);
    for (const entry of $("pipeline-list").children) entry.setAttribute("aria-current", "false");
    renderSummary();
    renderFiles();
    renderDag();
    $("definition").focus();
  }

  function showPipelineCards(existing) {
    $("pipeline-editor").hidden = false;
    $("dag-card").hidden = false;
    for (const id of ["versions-card", "schedule-form", "runs-card", "trial-card"]) $(id).hidden = !existing;
    $("pipeline-save").textContent = existing ? "保存草稿" : "创建流水线";
    $("pipeline-validate").disabled = !existing;
    $("pipeline-publish").disabled = !existing;
    $("pipeline-reload").disabled = !existing;
  }

  function renderSummary() {
    const pipeline = state.pipeline;
    if (state.creating) {
      $("pipeline-summary").replaceChildren(text("span", "新建流水线：填写定义 JSON，新增至少一个代码文件后创建。", "muted"));
      return;
    }
    if (!pipeline) {
      $("pipeline-summary").replaceChildren(text("span", "选择或新建流水线。", "muted"));
      return;
    }
    const schedule = pipeline.schedule || {};
    $("pipeline-summary").replaceChildren(text("code", pipeline.pipeline_id), badge(pipeline.status),
      text("span", "revision " + pipeline.revision), text("span", "draft_revision " + pipeline.draft_revision),
      text("span", "激活版本 " + (pipeline.active_version || "未激活")),
      text("span", "调度 " + [schedule.owner || pipeline.schedule_owner, schedule.expression, schedule.timezone,
        schedule.state].filter(Boolean).join(" · ")));
  }

  function changed(file) {
    return file.isNew || (file.content !== null && file.content !== file.original);
  }

  function fileState(file) {
    if (file.isNew) return "新文件";
    if (file.encoding === "unavailable") return "不可内联编辑";
    if (file.content === null) return "未载入";
    return changed(file) ? "已修改" : "已载入";
  }

  function renderFiles() {
    $("file-list").replaceChildren(table([
      ["路径", (file) => text("code", file.path)], ["字节", (file) => file.isNew ? "待计算" : file.bytes],
      ["SHA-256", (file) => file.isNew ? "待计算" : file.sha256],
      ["upload_id", (file) => file.isNew ? "待上传" : file.upload_id],
      ["状态", (file) => text("span", fileState(file), "file-state")],
      ["", (file) => [button(file.isNew || file.content !== null ? "编辑"
        : file.encoding === "unavailable" ? "查看" : "载入", () => openFile(file), "small-button"),
      button("移除", () => removeFile(file), "small-button quiet")]],
    ], state.files, "尚无代码文件。"));
  }

  async function openFile(file) {
    if (file.isNew || file.content !== null || file.encoding === "unavailable") {
      selectFile(file);
      return;
    }
    const context = state.context;
    const view = state.view;
    const pipeline = state.pipeline;
    try {
      const value = await api(pipelinePath(pipeline.pipeline_id, context) + "/draft/artifacts"
        + query({ path: file.path }), {}, context);
      if (!live(context, view) || state.pipeline !== pipeline) return;
      if (value.path !== file.path || value.upload_id !== file.upload_id || value.sha256 !== file.sha256) {
        throw new LocalError("服务端草稿文件已变化，请重新加载流水线。");
      }
      file.encoding = value.encoding === "utf-8" && typeof value.content === "string" ? "utf-8" : "unavailable";
      if (file.encoding === "utf-8") {
        file.content = value.content;
        file.original = value.content;
      }
      renderFiles();
      selectFile(file);
    } catch (error) {
      report(error, context, view);
    }
  }

  function selectFile(file) {
    state.file = file;
    $("file-editor").hidden = false;
    const editable = file.isNew || (file.encoding !== "unavailable" && file.content !== null);
    const meta = [text("code", file.path)];
    if (!file.isNew) {
      meta.push(text("span", file.bytes + " 字节"), text("code", "sha256 " + file.sha256), text("code", file.upload_id));
    }
    if (!editable) {
      meta.push(text("span", "服务端未提供 UTF-8 文本（非文本或超过 768 KiB），只显示 hash 和大小，不可编辑。", "reason"));
    }
    $("file-meta").replaceChildren(...meta);
    $("file-content").value = editable ? file.content : "";
    $("file-content").disabled = !editable;
  }

  function editFile() {
    if (!state.file || $("file-content").disabled) return;
    state.file.content = $("file-content").value;
    renderFiles();
  }

  function addFile() {
    if (!state.pipeline && !state.creating) return;
    const path = $("file-new-path").value.trim();
    if (path.length > 512 || !RELATIVE.test(path) || path.split("/")[0] === ".colvenna") {
      notice("文件路径须为相对路径（字母、数字、_ . -，以 / 分隔），且不能使用 .colvenna/。", true);
      return;
    }
    if (state.files.some((file) => file.path === path || file.path.startsWith(path + "/")
        || path.startsWith(file.path + "/"))) {
      notice("文件路径已存在或与现有路径冲突。", true);
      return;
    }
    const file = { path, upload_id: null, sha256: null, bytes: null, encoding: "utf-8", content: "",
      original: null, isNew: true };
    state.files.push(file);
    $("file-new-path").value = "";
    renderFiles();
    selectFile(file);
    $("file-content").focus();
  }

  function removeFile(file) {
    state.files = state.files.filter((item) => item !== file);
    if (state.file === file) {
      state.file = null;
      $("file-editor").hidden = true;
      $("file-meta").replaceChildren();
    }
    renderFiles();
  }

  function parseDefinition() {
    let value;
    try {
      value = JSON.parse($("definition").value);
    } catch (error) {
      throw new LocalError("定义不是有效的 JSON：" + error.message);
    }
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new LocalError("定义必须是 JSON 对象。");
    if (typeof value.pipeline_id !== "string" || !IDENTIFIER.test(value.pipeline_id)) {
      throw new LocalError("定义中的 pipeline_id 缺失或格式无效。");
    }
    return value;
  }

  async function sha256(value) {
    const bytes = new TextEncoder().encode(value);
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    return { sha256: [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join(""), bytes: bytes.length };
  }

  async function prepare(files) {
    const items = [];
    for (const file of files) {
      const content = file.content;
      if (typeof content !== "string" || !content.length) throw new LocalError("文件内容为空：" + file.path);
      if (content.length > 786432) throw new LocalError("浏览器内联上传最多 786432 个字符：" + file.path);
      const digest = await sha256(content);
      if (digest.bytes > 10485760) throw new LocalError("文件超过 10 MiB：" + file.path);
      items.push({ file, bytes: digest.bytes, body: { path: file.path, sha256: digest.sha256, content } });
    }
    return items;
  }

  function uploadChain(context, pipelineId, items, index, finish) {
    if (index >= items.length) return finish();
    const item = items[index];
    const spec = request(context, "上传文件 " + item.body.path, "POST",
      pipelinePath(pipelineId, context) + "/artifacts", item.body);
    spec.done = (receipt) => {
      if (receipt.path !== item.body.path || receipt.sha256 !== item.body.sha256 || receipt.bytes !== item.bytes
          || !receipt.upload_id) {
        throw new LocalError("上传回执与原文件不一致：" + item.body.path);
      }
      Object.assign(item.file, { upload_id: receipt.upload_id, sha256: receipt.sha256, bytes: receipt.bytes,
        original: item.body.content, encoding: "utf-8", isNew: false });
      if (item.file.content === null) item.file.content = item.body.content;
      return uploadChain(context, pipelineId, items, index + 1, finish);
    };
    return step(spec);
  }

  function putDraft(context, pipelineId, revision, definition, files, label) {
    const artifacts = files.map((file) => ({ path: file.path, upload_id: file.upload_id, sha256: file.sha256,
      bytes: file.bytes }));
    if (artifacts.some((artifact) => !artifact.upload_id || !artifact.sha256 || !artifact.bytes)) {
      throw new LocalError("仍有文件未上传，草稿未保存。");
    }
    const spec = request(context, label, "PUT", pipelinePath(pipelineId, context) + "/draft",
      { definition, artifacts }, { "If-Match": quoted(revision) });
    spec.done = async (receipt) => {
      if (receipt.pipeline_id !== pipelineId) throw new LocalError("草稿回执与原流水线不一致。");
      notice("草稿已保存 · " + receipt.pipeline_id + " · revision " + receipt.revision + " · " + receipt.status);
      await openPipeline(pipelineId);
      await loadPipelines();
    };
    return step(spec);
  }

  function createPipeline(context, definition, files, items) {
    const id = definition.pipeline_id;
    // Documented flow: placeholder upload IDs, inline uploads, then a conditional draft replacement.
    const body = { definition, artifacts: items.map((item) => ({ path: item.body.path, upload_id: "pending",
      sha256: item.body.sha256, bytes: item.bytes })) };
    const spec = request(context, "创建流水线 " + id, "POST", spacePath(context) + "/pipelines", body);
    spec.done = (receipt) => {
      if (receipt.pipeline_id !== id || !receipt.revision) throw new LocalError("创建回执与原请求不一致。");
      notice("流水线已创建 · " + id + " · 正在上传代码文件…");
      return uploadChain(context, id, items, 0,
        () => putDraft(context, id, receipt.revision, definition, files, "替换占位上传 " + id));
    };
    return step(spec);
  }

  function setPipelineBusy(busy) {
    $("pipeline-save").disabled = busy;
    $("pipeline-validate").disabled = busy || !state.pipeline;
    $("pipeline-publish").disabled = busy || !state.pipeline;
  }

  async function savePipeline() {
    const context = state.context;
    if (!context || (!state.pipeline && !state.creating)) return;
    setPipelineBusy(true);
    try {
      const definition = parseDefinition();
      const files = [...state.files];
      if (!files.length) throw new LocalError("草稿至少需要一个代码文件。");
      if (state.creating) {
        await createPipeline(context, definition, files, await prepare(files));
        return;
      }
      const pipeline = state.pipeline;
      if (definition.pipeline_id !== pipeline.pipeline_id) {
        throw new LocalError("定义中的 pipeline_id 与当前流水线不一致。");
      }
      const items = await prepare(files.filter(changed));
      await uploadChain(context, pipeline.pipeline_id, items, 0, () => putDraft(context, pipeline.pipeline_id,
        pipeline.revision, definition, files, "保存草稿 " + pipeline.pipeline_id));
    } catch (error) {
      report(error, context);
    } finally {
      if (current(context)) setPipelineBusy(false);
    }
  }

  async function validateDraft() {
    const context = state.context;
    const view = state.view;
    const pipeline = state.pipeline;
    if (!pipeline) return;
    $("validation").replaceChildren(text("span", "正在校验 draft_revision " + pipeline.draft_revision + " …", "muted"));
    try {
      const value = await api(pipelinePath(pipeline.pipeline_id, context) + "/validate",
        { method: "POST", body: { draft_revision: pipeline.draft_revision } }, context);
      if (!live(context, view) || state.pipeline !== pipeline) return;
      const diagnostics = value.diagnostics || [];
      $("validation").replaceChildren(text("div", "校验结果 valid=" + value.valid + " · draft_revision "
        + pipeline.draft_revision + " · " + diagnostics.length + " 条诊断", value.valid ? "small" : "reason"),
      table([["路径", (item) => item.path], ["代码", (item) => item.code], ["说明", (item) => item.message]],
        diagnostics, "无诊断。"));
    } catch (error) {
      if (!live(context, view)) return;
      $("validation").replaceChildren();
      report(error, context, view);
    }
  }

  async function publishDraft() {
    const context = state.context;
    const pipeline = state.pipeline;
    if (!pipeline) return;
    const spec = request(context, "发布版本 · draft_revision " + pipeline.draft_revision, "POST",
      pipelinePath(pipeline.pipeline_id, context) + "/versions", { draft_revision: pipeline.draft_revision },
      { "Idempotency-Key": idempotencyKey() });
    spec.done = async (value) => {
      if (state.pipeline !== pipeline) return;
      $("publish-result").replaceChildren(row(text("span", "已发布版本"), text("code", value.pipeline_version),
        badge(value.status)));
      await loadVersions();
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  async function loadVersions() {
    const context = state.context;
    const view = state.view;
    const pipeline = state.pipeline;
    if (!pipeline) return;
    try {
      const value = await api(pipelinePath(pipeline.pipeline_id, context) + "/versions", {}, context);
      if (!live(context, view) || state.pipeline !== pipeline) return;
      state.versions = value.versions || [];
      renderVersions();
      fillVersionSelects();
    } catch (error) {
      if (!live(context, view) || state.pipeline !== pipeline) return;
      $("versions").replaceChildren(text("p", "版本尚未取得。", "small muted"));
      report(error, context, view);
    }
  }

  function renderVersions() {
    $("versions").replaceChildren(table([
      ["版本", (version) => text("code", version.pipeline_version)],
      ["激活", (version) => version.active ? "是（当前激活）" : "否"], ["draft_revision", (version) => version.draft_revision],
      ["创建者", (version) => version.created_by], ["创建时间", (version) => version.created_at],
      ["代码文件", (version) => Object.entries(version.code_hashes || {}).map(([path, hash]) => path + " " + hash).join("\n")],
      ["", (version) => version.active ? "" : button("激活", () => activate(version.pipeline_version), "small-button")],
    ], state.versions, "尚无已发布版本。"));
  }

  function fillVersionSelects() {
    const active = state.versions.find((version) => version.active);
    for (const id of ["run-version", "trial-version"]) {
      const select = $(id);
      const previous = select.value;
      select.replaceChildren();
      for (const version of state.versions) {
        const option = text("option", version.pipeline_version + (version.active ? "（激活）" : ""));
        option.value = version.pipeline_version;
        select.append(option);
      }
      if (state.versions.some((version) => version.pipeline_version === previous)) select.value = previous;
      else if (active) select.value = active.pipeline_version;
    }
  }

  function applyHeader(value) {
    for (const name of ["revision", "draft_revision", "status", "active_version", "schedule_owner", "schedule"]) {
      if (value[name] !== undefined) state.pipeline[name] = value[name];
    }
    renderSummary();
    fillSchedule();
  }

  // Header-only refresh: adopt the new revision only while the draft is unchanged.
  async function refreshSummary() {
    const context = state.context;
    const view = state.view;
    const pipeline = state.pipeline;
    if (!pipeline) return;
    try {
      const value = await api(pipelinePath(pipeline.pipeline_id, context), {}, context);
      if (!live(context, view) || state.pipeline !== pipeline) return;
      if (value.draft_revision !== pipeline.draft_revision) {
        notice("服务端草稿已被其他人修改，请重新加载。", true);
        return;
      }
      applyHeader(value);
    } catch (error) {
      report(error, context, view);
    }
  }

  async function activate(version) {
    const context = state.context;
    const pipeline = state.pipeline;
    if (!pipeline) return;
    const spec = request(context, "激活版本 " + version, "POST", pipelinePath(pipeline.pipeline_id, context) + "/activate",
      { pipeline_version: version }, { "If-Match": quoted(pipeline.revision) });
    spec.done = async (value) => {
      if (state.pipeline !== pipeline) return;
      if (value.draft_revision !== pipeline.draft_revision) {
        notice("已激活，但服务端草稿已变化，请重新加载。", true);
        return;
      }
      applyHeader(value);
      notice("激活已确认 · " + display(value.active_version) + " · revision " + value.revision);
      await loadVersions();
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  function fillSchedule() {
    const schedule = (state.pipeline && state.pipeline.schedule) || {};
    $("schedule-owner").value = schedule.owner || "none";
    $("schedule-expression").value = schedule.expression || "";
    $("schedule-timezone").value = schedule.timezone || "";
    $("schedule-state").value = schedule.state || "PAUSED";
    $("schedule-pause").disabled = !state.pipeline || !state.pipeline.schedule;
  }

  function scheduleBody(source, stateValue) {
    const body = { owner: source.owner };
    if (source.expression) body.expression = source.expression;
    if (source.timezone) body.timezone = source.timezone;
    body.state = stateValue;
    return body;
  }

  async function saveSchedule(body) {
    const context = state.context;
    const pipeline = state.pipeline;
    if (!pipeline) return;
    const spec = request(context, "更新调度 · " + body.state, "PUT", pipelinePath(pipeline.pipeline_id, context) + "/schedule",
      body, { "If-Match": quoted(pipeline.revision) });
    spec.done = async (value) => {
      if (state.pipeline !== pipeline) return;
      pipeline.revision = value.revision;
      pipeline.schedule = value.schedule;
      notice("调度已更新 · " + value.schedule.state + " · revision " + value.revision);
      renderSummary();
      fillSchedule();
      await refreshSummary();
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  function submitSchedule(event) {
    event.preventDefault();
    return saveSchedule(scheduleBody({ owner: $("schedule-owner").value, expression: $("schedule-expression").value.trim(),
      timezone: $("schedule-timezone").value.trim() }, $("schedule-state").value));
  }

  function pauseSchedule() {
    if (!state.pipeline || !state.pipeline.schedule) return;
    return saveSchedule(scheduleBody(state.pipeline.schedule, "PAUSED"));
  }

  function dependencies(task) {
    const values = new Set(Array.isArray(task.depends_on) ? task.depends_on.map(String) : []);
    const inputs = task.inputs && typeof task.inputs === "object" ? task.inputs : {};
    for (const binding of Object.values(inputs)) {
      if (binding && typeof binding.task === "string") values.add(binding.task);
    }
    return [...values];
  }

  // Longest-path layering; a cycle is reported instead of hiding tasks.
  function layering(tasks) {
    const byId = new Map(tasks.map((task) => [String(task.task_id), task]));
    const depth = new Map();
    const visiting = new Set();
    let cyclic = false;
    const visit = (id) => {
      if (depth.has(id)) return depth.get(id);
      if (visiting.has(id)) {
        cyclic = true;
        return 0;
      }
      visiting.add(id);
      let level = 0;
      for (const dependency of dependencies(byId.get(id))) {
        if (byId.has(dependency)) level = Math.max(level, visit(dependency) + 1);
      }
      visiting.delete(id);
      depth.set(id, level);
      return level;
    };
    for (const id of byId.keys()) visit(id);
    const layers = [];
    for (const [id, level] of depth) (layers[level] = layers[level] || []).push(byId.get(id));
    return { layers: layers.filter(Boolean), cyclic, known: byId };
  }

  function bindingText(binding) {
    if (!binding || typeof binding !== "object") return display(binding);
    const source = "dataset" in binding ? "dataset " + display(binding.dataset)
      : "task" in binding ? "task " + display(binding.task)
        : "state" in binding ? "state " + display(binding.state) : JSON.stringify(binding);
    return source + " · " + display(binding.selection);
  }

  function taskCard(task, known) {
    const card = node("article", "dag-task");
    card.dataset.task = String(task.task_id);
    const processing = task.processing && typeof task.processing === "object" ? task.processing.mode : undefined;
    card.append(text("code", display(task.task_id)),
      text("span", [task.executor, task.language, processing, task.resource_profile].map(display).join(" · ")));
    const depends = dependencies(task);
    if (depends.length) {
      const unknown = depends.filter((name) => !known.has(name));
      card.append(text("span", "依赖 " + depends.join(", ") + (unknown.length ? "（未知：" + unknown.join(", ") + "）" : ""),
        unknown.length ? "reason" : "muted"));
    }
    const inputs = node("ul");
    const bindings = task.inputs && typeof task.inputs === "object" ? task.inputs : {};
    for (const [alias, binding] of Object.entries(bindings)) inputs.append(text("li", alias + " ← " + bindingText(binding)));
    const output = task.output && typeof task.output === "object" ? task.output : {};
    card.append(text("span", "输入", "muted"), inputs,
      text("span", "输出 " + display(output.dataset) + " · " + display(output.write_mode)));
    return card;
  }

  function renderDag() {
    let definition;
    try {
      definition = JSON.parse($("definition").value);
    } catch (error) {
      $("dag").replaceChildren(text("p", "定义 JSON 无法解析，暂无预览。", "small muted"));
      $("parameters").replaceChildren();
      return;
    }
    const tasks = definition && Array.isArray(definition.tasks)
      ? definition.tasks.filter((task) => task && typeof task === "object") : [];
    const { layers, cyclic, known } = layering(tasks);
    const columns = layers.map((layer, index) => {
      const column = node("section", "dag-layer");
      column.append(text("h3", "第 " + (index + 1) + " 层"), ...layer.map((task) => taskCard(task, known)));
      return column;
    });
    $("dag").replaceChildren(...columns);
    if (!tasks.length) $("dag").append(text("p", "定义中暂无任务。", "small muted"));
    if (cyclic) $("dag").prepend(text("p", "存在循环依赖，服务端校验会拒绝该定义。", "reason"));
    const parameters = definition && definition.parameters && typeof definition.parameters === "object"
      ? Object.entries(definition.parameters) : [];
    $("parameters").replaceChildren(table([
      ["名称", ([name]) => text("code", name)], ["类型", ([, value]) => value && value.type],
      ["默认值", ([, value]) => value && value.default], ["最小值", ([, value]) => value && value.minimum],
      ["最大值", ([, value]) => value && value.maximum],
    ], parameters, "无参数。"));
  }

  function fillTrialTasks() {
    const select = $("trial-task");
    select.replaceChildren();
    const definition = state.pipeline && state.pipeline.definition;
    for (const task of definition && Array.isArray(definition.tasks) ? definition.tasks : []) {
      const option = text("option", task.task_id + " · " + task.executor);
      option.value = task.task_id;
      select.append(option);
    }
  }

  // -- Runs and trial runs share one observer per slot ("run" / "trial"). --

  async function loadRuns() {
    const context = state.context;
    const view = state.view;
    const pipeline = state.pipeline;
    if (!pipeline) return;
    try {
      const value = await api(pipelinePath(pipeline.pipeline_id, context) + "/runs" + query({ limit: 20 }), {}, context);
      if (!live(context, view) || state.pipeline !== pipeline) return;
      renderRuns(value.runs || [], pipeline.pipeline_id);
    } catch (error) {
      if (!live(context, view) || state.pipeline !== pipeline) return;
      $("runs").replaceChildren(text("p", "运行记录尚未取得。", "small muted"));
      report(error, context, view);
    }
  }

  function renderRuns(runs, pipelineId) {
    $("runs").replaceChildren(table([
      ["run_id", (item) => text("code", item.run_id)], ["模式", (item) => item.mode], ["状态", (item) => badge(item.status)],
      ["触发", (item) => item.trigger], ["版本", (item) => item.pipeline_version], ["创建", (item) => item.created_at],
      ["结束", (item) => item.finished_at], ["错误", (item) => item.error_code], ["试运行任务", (item) => item.task_id],
      ["", (item) => button("查看", () => watch(item.mode === "trial" ? "trial" : "run", pipelineId, item.run_id),
        "small-button")],
    ], runs, "暂无运行记录。"));
  }

  function jsonField(id, label, kind) {
    const raw = $(id).value.trim();
    if (!raw) return undefined;
    let value;
    try {
      value = JSON.parse(raw);
    } catch (error) {
      throw new LocalError(label + "不是有效的 JSON。");
    }
    const valid = kind === "array" ? Array.isArray(value) : value && typeof value === "object" && !Array.isArray(value);
    if (!valid) throw new LocalError(label + (kind === "array" ? "须为 JSON 数组。" : "须为 JSON 对象。"));
    return value;
  }

  async function startRun() {
    const context = state.context;
    const pipeline = state.pipeline;
    if (!pipeline) return;
    const version = $("run-version").value;
    if (!version) throw new LocalError("请先发布并选择版本。");
    const mode = $("run-mode").value;
    const body = { pipeline_version: version, mode };
    const parameters = jsonField("run-parameters", "参数", "object");
    if (parameters) body.parameters = parameters;
    if (mode === "backfill") {
      const partitions = jsonField("run-partitions", "回填分区", "array");
      if (!partitions || !partitions.length) throw new LocalError("回填需要非空的 JSON 分区数组。");
      body.partitions = partitions;
    }
    const id = pipeline.pipeline_id;
    const spec = request(context, "启动运行 · " + version + " · " + mode, "POST", pipelinePath(id, context) + "/runs",
      body, { "Idempotency-Key": idempotencyKey() });
    spec.done = (receipt) => {
      notice("运行已受理 · " + receipt.run_id + " · " + receipt.status);
      return Promise.all([watch("run", id, receipt.run_id), loadRuns()]);
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  async function startTrial() {
    const context = state.context;
    const pipeline = state.pipeline;
    if (!pipeline) return;
    const version = $("trial-version").value;
    const task = $("trial-task").value;
    const rows = Number($("trial-rows").value);
    if (!version) throw new LocalError("请先发布并选择版本。");
    if (!task) throw new LocalError("请选择任务。");
    if (!Number.isInteger(rows) || rows < 1 || rows > 10000) throw new LocalError("样本行数须为 1–10000 的整数。");
    const body = { pipeline_version: version, task_id: task, sample_rows: rows };
    const parameters = jsonField("trial-parameters", "参数", "object");
    if (parameters) body.parameters = parameters;
    const id = pipeline.pipeline_id;
    const spec = request(context, "启动试运行 · " + task, "POST", pipelinePath(id, context) + "/trial-runs", body,
      { "Idempotency-Key": idempotencyKey() });
    spec.done = (receipt) => {
      notice("试运行已受理 · " + receipt.run_id + " · " + receipt.status);
      return Promise.all([watch("trial", id, receipt.run_id), loadRuns()]);
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  function watch(slot, pipelineId, runId) {
    if (!state.pipeline || state.pipeline.pipeline_id !== pipelineId) return null;
    stopTimer(slot);
    state.runs[slot] = { pipelineId, runId };
    $(slot + "-view").hidden = false;
    $(slot + "-result").replaceChildren();
    if (slot === "trial") $("trial-preview").replaceChildren();
    $(slot + "-detail").replaceChildren(text("p", "正在读取 " + runId + " …", "small muted"));
    return observeRun(slot);
  }

  function runPath(watched, context) {
    return pipelinePath(watched.pipelineId, context) + "/runs/" + enc(watched.runId);
  }

  async function observeRun(slot) {
    stopTimer(slot);
    const watched = state.runs[slot];
    const context = state.context;
    const view = state.view;
    if (!watched || !context) return;
    try {
      const value = await api(runPath(watched, context), {}, context);
      if (!live(context, view) || state.runs[slot] !== watched) return;
      if (value.run_id !== watched.runId) throw new LocalError("运行身份不一致。");
      renderRun(slot, value);
      if (!RUN_TERMINAL.has(value.status)) {
        watched.active = true;
        schedule(slot, () => observeRun(slot), POLL_MS);
      } else if (watched.active) {
        watched.active = false;
        await loadRuns();  // The list row is a separate observation; refresh it once the run settles.
      }
    } catch (error) {
      if (!live(context, view) || state.runs[slot] !== watched) return;
      markStale($(slot + "-detail"));
      report(error, context, view);
      if (unconfirmed(error)) schedule(slot, () => observeRun(slot), POLL_MS);
    }
  }

  function progressText(task) {
    const progress = task.progress;
    if (!progress) {
      return task.imported_chunks === undefined ? null : task.imported_chunks + "/" + display(task.total_chunks);
    }
    // No reported total (a writer-executed task publishes without chunk copies): say so, never guess.
    const chunks = progress.total_chunks === null || progress.total_chunks === undefined ? "分块数未报告"
      : progress.imported_chunks + "/" + progress.total_chunks;
    return progress.state + " · " + chunks + " · 观测 " + progress.observed_at
      + (progress.error_code ? " · " + progress.error_code : "");
  }

  // A skipped job published nothing in this run; its Release ID is the task's existing release.
  function taskState(task) {
    if (!task.skipped) return badge(task.status);
    const cell = node("span");
    cell.append(badge(task.status), text("span", " 跳过 · " + task.skipped + "（沿用已有 release）", "small muted"));
    return cell;
  }

  const TASK_COLUMNS = [
    ["任务", (task) => text("code", task.task_id)], ["状态", taskState], ["执行器", (task) => task.executor],
    ["尝试", (task) => task.attempt_number], ["Release ID", (task) => task.release_id], ["错误", (task) => task.error_code],
    ["暂时性代码", (task) => task.transient_code], ["导入进度", progressText], ["观测时间", (task) => task.observed_at],
  ];
  const ATTEMPT_COLUMNS = [
    ["attempt_id", (attempt) => text("code", attempt.attempt_id)], ["状态", (attempt) => badge(attempt.status)],
    ["dispatch_id", (attempt) => attempt.dispatch_id], ["execution_id", (attempt) => attempt.execution_id],
    ["eligibility_epoch", (attempt) => attempt.eligibility_epoch], ["代码", (attempt) => attempt.code],
    ["派发", (attempt) => attempt.dispatched_at], ["结束", (attempt) => attempt.ended_at],
  ];

  function attemptRows(task, body, width) {
    if (!Array.isArray(task.attempts) || !task.attempts.length) return;
    const subrow = body.insertRow();
    subrow.className = "subrow";
    const cell = subrow.insertCell();
    cell.colSpan = width;
    cell.append(text("div", "尝试记录 · " + task.task_id, "small muted"), table(ATTEMPT_COLUMNS, task.attempts));
  }

  function trialBlock(value) {
    const trial = value.trial;
    const banner = text("div", "试运行不发布正式数据（publication: " + (trial ? display(trial.publication) : "未返回") + "）",
      "banner");
    if (!trial) return [banner];
    const validation = trial.validation || {};
    return [banner, row(text("span", "任务 " + trial.task_id), text("span", "样本行数 " + trial.sample_rows),
      text("span", "输出校验"), badge(validation.status), text("span", "error_code " + display(validation.error_code)),
      text("span", "row_count " + display(validation.row_count)), text("span", "files " + display(validation.files)),
      text("span", "bytes " + display(validation.bytes)), text("span", observedText(validation.observed_at)))];
  }

  function renderRun(slot, value) {
    const meta = row(text("code", value.run_id), badge(value.status), text("span", "模式 " + display(value.mode)),
      text("span", "触发 " + display(value.trigger)), text("span", "版本 " + value.pipeline_version),
      text("span", "创建 " + display(value.created_at)), text("span", "结束 " + display(value.finished_at)),
      text("span", "观测 " + value.observed_at), text("span", "projection_version " + value.projection_version));
    if (value.error_code) meta.append(text("span", "错误 " + value.error_code, "reason"));
    if (value.waiting) {
      meta.append(text("span", "等待 " + value.waiting.code + (WAITING[value.waiting.code] ? "（"
        + WAITING[value.waiting.code] + "）" : "") + " · 自 " + value.waiting.since, "reason"));
    }
    if (value.cancel_requested) meta.append(text("span", "已请求取消", "reason"));
    const parts = [meta];
    if (slot === "trial" || value.mode === "trial") parts.push(...trialBlock(value));
    if (value.root_inputs) parts.push(heading("根输入"), pairs(value.root_inputs));
    if (value.parameters) parts.push(heading("参数"), pairs(value.parameters));
    if (value.partitions) parts.push(heading("分区"), text("pre", JSON.stringify(value.partitions, null, 2)));
    parts.push(heading("任务"), table(TASK_COLUMNS, value.tasks || [], "暂无任务。", attemptRows));
    $(slot + "-detail").replaceChildren(...parts);
    $(slot + "-cancel").disabled = RUN_TERMINAL.has(value.status);
    if (slot === "run") $("run-retry").disabled = value.status !== "FAILED";
    else $("trial-preview-button").disabled = value.status !== "VALIDATED";
  }

  async function cancelRun(slot) {
    const context = state.context;
    const watched = state.runs[slot];
    if (!watched) return;
    const spec = request(context, "取消运行 " + watched.runId, "POST", runPath(watched, context) + "/cancel", {});
    spec.done = (value) => {
      if (value.run_id !== watched.runId) throw new LocalError("取消回执与原运行不一致。");
      if (state.runs[slot] === watched) {
        $(slot + "-result").replaceChildren(row(text("span", "取消请求返回状态"), badge(value.status), text("code", value.run_id)));
      }
      notice("取消请求已提交 · 服务端返回 " + value.status);
      return observeRun(slot);
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  async function retryRun() {
    const context = state.context;
    const watched = state.runs.run;
    if (!watched) return;
    const spec = request(context, "重试运行 " + watched.runId, "POST", runPath(watched, context) + "/retry", {},
      { "Idempotency-Key": idempotencyKey() });
    spec.done = (receipt) => {
      if (receipt.run_id !== watched.runId) throw new LocalError("重试回执与原运行不一致。");
      notice("重试已受理 · " + receipt.run_id + " · " + receipt.status);
      if (state.runs.run === watched) $("run-result").replaceChildren(row(text("span", "重试回执"), badge(receipt.status)));
      return Promise.all([observeRun("run"), loadRuns()]);
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  async function preview() {
    const context = state.context;
    const view = state.view;
    const watched = state.runs.trial;
    if (!watched) return;
    $("trial-preview").replaceChildren(text("p", "正在读取样本…", "small muted"));
    try {
      const value = await api(runPath(watched, context) + "/preview" + query({ rows: 20 }), {}, context);
      if (!live(context, view) || state.runs.trial !== watched) return;
      renderPreview(value, watched);
    } catch (error) {
      if (!live(context, view) || state.runs.trial !== watched) return;
      if (error instanceof ApiError && error.status === 403) {
        $("trial-preview").replaceChildren(text("p", "需要 query:read 权限查看样本数据 · " + error.code + " · "
          + error.message, "denied"));
        notice("需要 query:read 权限查看样本数据。", true);
        return;
      }
      $("trial-preview").replaceChildren();
      report(error, context, view);
    }
  }

  function renderPreview(value, watched) {
    const columns = value.columns;
    const rows = value.rows;
    if (value.run_id !== watched.runId || !Array.isArray(columns) || !Array.isArray(rows)
        || rows.some((item) => !Array.isArray(item) || item.length !== columns.length
          || item.some((cell) => cell !== null && typeof cell === "object"))) {
      throw new LocalError("样本结构与合同不一致，未显示。");
    }
    const grid = node("table");
    const head = grid.createTHead().insertRow();
    for (const column of columns) {
      const cell = node("th");
      cell.append(text("span", display(column.name)), text("small", display(column.type)));
      head.append(cell);
    }
    const body = grid.createTBody();
    for (const item of rows) {
      const tr = body.insertRow();
      for (const datum of item) {
        const cell = tr.insertCell();
        cell.textContent = datum === null ? "NULL" : String(datum);
        if (datum === null) cell.className = "null";
      }
    }
    $("trial-preview").replaceChildren(row(text("span", "样本 · 任务 " + value.task_id), badge(value.publication),
      text("span", "row_count " + value.row_count), text("span", "本次显示 " + rows.length + " 行"),
      text("span", value.truncated ? "已截断" : "未截断"), text("span", "观测 " + value.observed_at)), grid);
  }

  // -- Runtime environments. --

  async function loadEnvironments() {
    const context = state.context;
    const view = state.view;
    try {
      const value = await api(spacePath(context) + "/runtime-environments", {}, context);
      if (!live(context, view)) return;
      $("environments").replaceChildren(table([
        ["环境", (item) => text("code", item.environment_id)], ["版本", (item) => item.version],
        ["状态", (item) => badge(item.status)], ["build_id", (item) => item.build_id],
        ["创建时间", (item) => item.created_at], ["创建者", (item) => item.created_by],
        ["", (item) => button("查看", () => openEnvironment(item.environment_id, item.version), "small-button")],
      ], value.environments || [], "暂无运行环境。"));
    } catch (error) {
      if (!live(context, view)) return;
      $("environments").replaceChildren(text("p", "列表尚未取得。", "small muted"));
      report(error, context, view);
    }
  }

  function environmentPath(environment, context) {
    return spacePath(context) + "/runtime-environments/" + enc(environment.id) + "/versions/" + enc(environment.version);
  }

  function openEnvironment(id, version) {
    stopTimer("environment");
    state.environment = { id, version };
    $("environment-card").hidden = false;
    $("environment-build").disabled = true;
    $("environment-detail").replaceChildren(text("p", "正在读取 " + id + " / " + version + " …", "small muted"));
    return observeEnvironment();
  }

  async function observeEnvironment() {
    stopTimer("environment");
    const context = state.context;
    const view = state.view;
    const environment = state.environment;
    if (!environment || !context) return;
    try {
      const value = await api(environmentPath(environment, context), {}, context);
      if (!live(context, view) || state.environment !== environment) return;
      if (value.environment_id !== environment.id || value.version !== environment.version) {
        throw new LocalError("环境身份不一致。");
      }
      renderEnvironment(value);
      if (ENVIRONMENT_ACTIVE.has(value.status) || (value.status === "DRAFT" && value.build_id)) {
        environment.active = true;
        schedule("environment", observeEnvironment, POLL_MS);
      } else if (environment.active) {
        environment.active = false;
        await loadEnvironments();
      }
    } catch (error) {
      if (!live(context, view) || state.environment !== environment) return;
      markStale($("environment-detail"));
      report(error, context, view);
      if (unconfirmed(error)) schedule("environment", observeEnvironment, POLL_MS);
    }
  }

  function renderEnvironment(value) {
    const parts = [row(text("code", value.environment_id + " / " + value.version), badge(value.status),
      text("span", "build_id " + display(value.build_id)), text("span", "codebuild_id " + display(value.codebuild_id)),
      text("span", "创建 " + display(value.created_at)))];
    if (value.status === "UNKNOWN") {
      parts.push(text("p", "服务端无法确认构建结果（UNKNOWN）。页面继续核对，不派发新的构建。", "reason"));
    }
    if (value.failure) {
      parts.push(heading("失败信息"), pairs({ phase: value.failure.phase, aws_status: value.failure.aws_status,
        code: value.failure.code }), text("pre", display(value.failure.message)));
    }
    parts.push(heading("镜像与依赖锁"), pairs({ image_digest: value.image_digest,
      dependency_lock_sha256: value.dependency_lock_sha256 }));
    for (const [name, label] of [["dependency_lock", "依赖锁文件"], ["provenance", "来源证明"], ["runtime", "运行时"]]) {
      if (value[name]) parts.push(heading(label), pairs(value[name]));
    }
    if (Array.isArray(value.executor_bindings) && value.executor_bindings.length) {
      parts.push(heading("执行器绑定"), table([
        ["executor", (item) => item.executor], ["profile", (item) => item.profile],
        ["ARN", (item) => item.function_arn || item.task_definition_arn], ["image", (item) => item.image],
        ["lambda_mode", (item) => item.lambda_mode],
      ], value.executor_bindings));
    }
    if (value.dependency_source) {
      parts.push(heading("依赖来源 · " + value.dependency_source.format), text("pre", value.dependency_source.content));
    }
    $("environment-detail").replaceChildren(...parts);
    $("environment-build").disabled = value.status !== "DRAFT" || Boolean(value.build_id);
  }

  async function createEnvironment(event) {
    event.preventDefault();
    const context = state.context;
    const id = $("environment-id").value.trim();
    const version = $("environment-version").value.trim();
    const content = $("environment-requirements").value;
    if (!IDENTIFIER.test(id) || !IDENTIFIER.test(version)) throw new LocalError("环境 ID 和版本须为标识符（字母或 _ 开头）。");
    if (!content.trim() || content.length > 65536) throw new LocalError("依赖内容须为 1–65536 个字符。");
    const body = { environment_id: id, version, python_version: "3.12", platform: "linux_arm64",
      build_profile: "binary_wheels", dependency_source: { format: "requirements", content } };
    const spec = request(context, "创建环境 " + id + " / " + version, "POST", spacePath(context) + "/runtime-environments", body);
    spec.done = async (receipt) => {
      notice("环境版本已创建 · " + receipt.environment_id + " / " + receipt.version + " · " + receipt.status);
      $("environment-form").reset();
      await Promise.all([openEnvironment(receipt.environment_id, receipt.version), loadEnvironments()]);
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  async function buildEnvironment() {
    const context = state.context;
    const environment = state.environment;
    if (!environment) return;
    const spec = request(context, "构建环境 " + environment.id + " / " + environment.version, "POST",
      environmentPath(environment, context) + "/build", {}, { "Idempotency-Key": idempotencyKey() });
    spec.done = (receipt) => {
      notice("构建已受理 · " + receipt.build_id + " · " + receipt.status);
      if (state.environment !== environment) return null;
      return Promise.all([observeEnvironment(), loadEnvironments()]);
    };
    $("environment-build").disabled = true;
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  // -- Audit. --

  function actorText(actor) {
    if (!actor) return null;
    return [actor.kind, actor.fingerprint && "指纹 " + actor.fingerprint, actor.principal_id,
      actor.key_id && "key " + actor.key_id].filter(Boolean).join(" · ");
  }

  function targetText(target) {
    if (!target) return null;
    return [target.kind + "/" + target.id, target.pipeline_id && "pipeline " + target.pipeline_id,
      target.dataset && "dataset " + target.dataset].filter(Boolean).join(" · ");
  }

  async function loadAudit(token, page) {
    const context = state.context;
    const view = state.view;
    const action = $("audit-action").value;
    try {
      const value = await api(adminPath(context) + "/audit" + query({ limit: 50, action, page_token: token }), {}, context);
      if (!live(context, view)) return;
      const records = value.records || [];
      state.audit = { next: value.next_page_token || null, page };
      $("audit-table").replaceChildren(table([
        ["时间", (item) => item.at], ["操作", (item) => item.action], ["操作者", (item) => actorText(item.actor)],
        ["目标", (item) => targetText(item.target)], ["结果", (item) => badge(item.outcome)],
        ["request_id", (item) => item.request_id],
        ["详情", (item) => Object.entries(item.details || {}).map(([name, detail]) => name + "=" + display(detail)).join("\n")],
      ], records, "没有符合条件的审计记录。"));
      $("audit-page").textContent = "第 " + page + " 页 · 本页 " + records.length + " 条" + (action ? " · " + action : "");
      $("audit-next").disabled = !state.audit.next;
      $("audit-first").disabled = page === 1;
    } catch (error) {
      if (!live(context, view)) return;
      $("audit-table").replaceChildren(text("p", "审计记录尚未取得。", "small muted"));
      $("audit-page").textContent = "";
      $("audit-next").disabled = true;
      report(error, context, view);
    }
  }

  // -- Trace. --

  function submitTrace(event) {
    event.preventDefault();
    return traceRequest({ kind: $("trace-kind").value, id: $("trace-id").value.trim(),
      pipeline_id: $("trace-pipeline").value.trim() });
  }

  async function traceRequest(params) {
    const context = state.context;
    const view = state.view;
    if (!IDENTIFIER.test(params.id)) throw new LocalError("ID 须为标识符（字母或 _ 开头，最多 128 个字符）。");
    if (params.kind === "run" && !IDENTIFIER.test(params.pipeline_id || "")) {
      throw new LocalError("追踪运行需要有效的 pipeline_id。");
    }
    $("trace-kind").value = params.kind;
    $("trace-id").value = params.id;
    $("trace-pipeline").value = params.pipeline_id || "";
    try {
      const value = await api(adminPath(context) + "/trace" + query({ kind: params.kind, id: params.id,
        pipeline_id: params.kind === "run" ? params.pipeline_id : undefined }), {}, context);
      if (!live(context, view)) return;
      state.trace = { ...params };
      renderTrace(value);
    } catch (error) {
      if (!live(context, view)) return;
      $("trace-meta").replaceChildren();
      $("trace-nodes").replaceChildren();
      report(error, context, view);
    }
  }

  function traceLink(link, item) {
    const label = link.kind + " · " + link.id;
    if (!TRACEABLE.has(link.kind) || !IDENTIFIER.test(link.id)) return text("span", label, "link-text");
    const attributes = item.attributes || {};
    const pipeline = link.kind !== "run" ? undefined
      : typeof attributes.pipeline_id === "string" ? attributes.pipeline_id : state.trace && state.trace.pipeline_id;
    const target = button(label, () => traceRequest({ kind: link.kind, id: link.id, pipeline_id: pipeline }), "small-button");
    target.dataset.kind = link.kind;
    if (link.kind === "run" && !pipeline) {
      target.disabled = true;
      target.title = "追踪运行需要 pipeline_id";
    }
    return target;
  }

  function traceNode(item) {
    const card = node("article", "trace-node");
    card.dataset.kind = item.kind;
    card.append(row(text("span", item.kind, "kind"), text("code", item.id), badge(item.status),
      text("span", "权威 " + item.authority), text("span", observedText(item.observed_at))));
    if (item.conflict) {
      card.classList.add("has-conflict");
      card.append(text("div", "投影与权威记录冲突 · projection " + display(item.conflict.projection) + " · authority "
        + display(item.conflict.authority), "conflict"));
    }
    const attributes = item.attributes || {};
    card.append(pairs(attributes, ["sql"]));
    if (typeof attributes.sql === "string") card.append(heading("原始 SQL（audit:read）"), text("pre", attributes.sql));
    if (Array.isArray(item.links) && item.links.length) {
      const links = node("div", "links");
      links.append(text("span", "关联", "muted"), ...item.links.map((link) => traceLink(link, item)));
      card.append(links);
    }
    return card;
  }

  function renderTrace(value) {
    const nodes = value.nodes || [];
    $("trace-meta").replaceChildren(text("span", value.kind, "kind"), text("code", value.id), badge(value.status),
      text("span", "观测 " + value.observed_at), text("span", nodes.length + " 个节点"));
    $("trace-nodes").replaceChildren(...nodes.map(traceNode));
    if (!nodes.length) $("trace-nodes").append(text("p", "没有节点。", "small muted"));
  }

  // -- Retention. The form keeps the revision it was filled from as its If-Match. --

  async function loadRetention(refill) {
    const context = state.context;
    const view = state.view;
    try {
      const value = await api(adminPath(context) + "/retention", {}, context);
      if (!live(context, view)) return;
      state.retention = value;
      renderRetention(value);
      if (refill) fillRetentionForm(value);
    } catch (error) {
      if (!live(context, view)) return;
      for (const id of ["retention-meta", "retention-policy", "holds", "collections", "retention-categories"]) {
        $(id).replaceChildren();
      }
      $("retention-meta").append(text("span", "保留状态尚未取得。", "muted"));
      report(error, context, view);
    }
  }

  function renderRetention(value) {
    $("retention-meta").replaceChildren(text("span", "revision " + value.revision), badge(value.state),
      text("span", "generation " + value.generation), text("span", "观测 " + value.observed_at),
      text("span", value.probe_minimum ? "probe 空间：最小 60 秒" : "产品最小值生效"));
    $("retention-policy").replaceChildren(table([
      ["策略", ([name]) => text("code", name)],
      ["值", ([name, amount]) => amount + (name === "release_keep" ? " 个" : " 秒")],
    ], Object.entries(value.policy || {}), "无策略。"));
    $("retention-categories").replaceChildren(table([
      ["分类", (item) => text("code", item.category)], ["规则", (item) => item.rule], ["回收方", (item) => item.collector],
    ], value.categories || [], "无分类。"));
    $("holds").replaceChildren(table([
      ["hold_id", (hold) => text("code", hold.hold_id)], ["目标", (hold) => targetText(hold.target)],
      ["原因", (hold) => hold.reason], ["创建", (hold) => hold.created_at], ["到期", (hold) => hold.expires_at],
      ["状态", (hold) => badge(hold.status)], ["创建者", (hold) => hold.created_by],
      ["", (hold) => hold.status === "ACTIVE" ? button("解除", () => releaseHold(hold.hold_id), "small-button") : ""],
    ], value.holds || [], "暂无保留锁定。"));
    $("collections").replaceChildren(table([
      ["collection_id", (item) => text("code", item.collection_id)], ["状态", (item) => badge(item.status)],
      ["演练", (item) => item.dry_run ? "是" : "否"], ["请求时间", (item) => item.requested_at],
      ["结束", (item) => item.finished_at], ["回收对象", (item) => item.collected_objects],
      ["回收字节", (item) => item.collected_bytes], ["错误", (item) => item.error_code],
      ["", (item) => button("查看", () => openCollection(item.collection_id), "small-button")],
    ], value.collections || [], "暂无回收记录。"));
  }

  function fillRetentionForm(value) {
    state.retentionRevision = value.revision;
    $("retention-state").value = value.state;
    for (const name of RETENTION_FIELDS) {
      const amount = value.policy ? value.policy[name] : undefined;
      $("retention-" + name).value = amount === undefined ? "" : String(amount);
    }
  }

  async function saveRetention(event) {
    event.preventDefault();
    const context = state.context;
    const revision = state.retentionRevision;
    if (!revision) throw new LocalError("请先载入保留策略。");
    const policy = {};
    for (const name of RETENTION_FIELDS) {
      const raw = $("retention-" + name).value.trim();
      const amount = Number(raw);
      if (!raw || !Number.isInteger(amount) || amount < 1) throw new LocalError(name + " 须为正整数。");
      policy[name] = amount;
    }
    const spec = request(context, "更新保留策略", "PUT", adminPath(context) + "/retention",
      { state: $("retention-state").value, policy }, { "If-Match": quoted(revision) });
    spec.done = (value) => {
      state.retention = value;
      renderRetention(value);
      fillRetentionForm(value);
      notice("保留策略已更新 · revision " + value.revision + " · " + value.state);
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  async function placeHold(event) {
    event.preventDefault();
    const context = state.context;
    const kind = $("hold-kind").value;
    const id = $("hold-id").value.trim();
    const pipeline = $("hold-pipeline").value.trim();
    const dataset = $("hold-dataset").value.trim();
    const reason = $("hold-reason").value.trim();
    const expires = utcStamp($("hold-expires").value);
    if (!IDENTIFIER.test(id) || (pipeline && !IDENTIFIER.test(pipeline))) throw new LocalError("目标 ID 和 pipeline_id 须为标识符。");
    if (dataset && !DATASET.test(dataset)) throw new LocalError("数据集须为 层.名称，例如 dwd.orders。");
    if (!reason) throw new LocalError("请填写原因。");
    if (!expires) throw new LocalError("请填写到期时间（UTC）。");
    const target = { kind, id };
    if (pipeline) target.pipeline_id = pipeline;
    if (dataset) target.dataset = dataset;
    try {
      // A hold has no idempotency identity; an unconfirmed reply is reconciled from the list.
      const value = await send(request(context, "放置保留锁定", "POST", adminPath(context) + "/retention/holds",
        { target, reason, expires_at: expires }, {}, false));
      notice("保留锁定已放置 · " + value.hold_id + " · " + value.status);
      $("hold-form").reset();
      await loadRetention(false);
    } catch (error) {
      if (!current(context)) return;
      if (unconfirmed(error)) {
        notice("请求未确认：放置保留锁定没有幂等身份，页面不会自动重发。请刷新列表核对。", true);
        return;
      }
      report(error, context);
    }
  }

  async function releaseHold(holdId) {
    const context = state.context;
    const spec = request(context, "解除保留锁定 " + holdId, "POST",
      adminPath(context) + "/retention/holds/" + enc(holdId) + "/release", {});
    spec.done = (value) => {
      notice("解除请求已确认 · " + value.hold_id + " · " + value.status);
      return loadRetention(false);
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  async function requestCollection(event) {
    event.preventDefault();
    const context = state.context;
    const body = { dry_run: $("collection-dry-run").checked };
    const spec = request(context, body.dry_run ? "请求回收演练" : "请求回收", "POST",
      adminPath(context) + "/retention/collections", body, { "Idempotency-Key": idempotencyKey() });
    spec.done = (receipt) => {
      notice("回收已受理 · " + receipt.collection_id + " · " + receipt.status);
      return Promise.all([openCollection(receipt.collection_id), loadRetention(false)]);
    };
    try {
      await step(spec);
    } catch (error) {
      report(error, context);
    }
  }

  function openCollection(id) {
    stopTimer("collection");
    state.collection = { id };
    $("collection-view").hidden = false;
    $("collection-detail").replaceChildren(text("p", "正在读取 " + id + " …", "small muted"));
    return observeCollection();
  }

  async function observeCollection() {
    stopTimer("collection");
    const context = state.context;
    const view = state.view;
    const collection = state.collection;
    if (!collection || !context) return;
    try {
      const value = await api(adminPath(context) + "/retention/collections/" + enc(collection.id), {}, context);
      if (!live(context, view) || state.collection !== collection) return;
      if (value.collection_id !== collection.id) throw new LocalError("回收身份不一致。");
      renderCollection(value);
      if (COLLECTION_ACTIVE.has(value.status)) {
        collection.active = true;
        schedule("collection", observeCollection, POLL_MS);
      } else if (collection.active) {
        collection.active = false;
        await loadRetention(false);
      }
    } catch (error) {
      if (!live(context, view) || state.collection !== collection) return;
      markStale($("collection-detail"));
      report(error, context, view);
      if (unconfirmed(error)) schedule("collection", observeCollection, POLL_MS);
    }
  }

  function renderCollection(value) {
    const meta = row(text("code", value.collection_id), badge(value.status),
      text("span", value.dry_run ? "演练（不删除）" : "实际回收"), text("span", "请求 " + display(value.requested_at)),
      text("span", "开始 " + display(value.started_at)), text("span", "结束 " + display(value.finished_at)));
    if (value.error_code) meta.append(text("span", "错误 " + value.error_code, "reason"));
    if (value.failure) {
      meta.append(text("span", "失败调用 " + value.failure.operation + " · " + display(value.failure.code), "reason"));
    }
    const summary = Object.entries(value.summary || {});
    const metrics = ["candidates", "collected", "retained", "bytes"];
    for (const [, values] of summary) {
      for (const name of Object.keys(values || {})) if (!metrics.includes(name)) metrics.push(name);
    }
    const parts = [meta, heading("汇总"), table([["分类", ([category]) => text("code", category)],
      ...metrics.map((name) => [name, ([, values]) => (values || {})[name]])], summary, "无汇总。"),
    heading("删除回执"), value.receipt ? pairs(value.receipt) : text("p", "尚无回执。", "small muted"),
    heading("对象"), table([
      ["分类", (item) => item.category], ["key", (item) => item.key], ["version_id", (item) => item.version_id],
      ["字节", (item) => item.bytes], ["动作", (item) => badge(item.action)], ["原因", (item) => item.reason],
    ], value.objects || [], "无对象。")];
    if (value.truncated) parts.push(text("p", "对象列表已截断。", "small muted"));
    $("collection-detail").replaceChildren(...parts);
  }

  // -- Quotas and costs. --

  async function loadQuotas() {
    const context = state.context;
    const view = state.view;
    try {
      const value = await api(adminPath(context) + "/quotas", {}, context);
      if (!live(context, view)) return;
      $("quota-meta").replaceChildren(text("span", "generation " + value.generation), text("span", "观测 " + value.observed_at));
      $("quota-policy").replaceChildren(heading("配额策略"), pairs(value.policy));
      $("quota-usage").replaceChildren(table([
        ["名称", (item) => text("code", item.name)], ["上限", (item) => item.limit], ["已用", (item) => item.used],
        ["单位", (item) => item.unit], ["状态", (item) => badge(item.status)],
        ["观测时间", (item) => item.observed_at === null ? "未观测" : item.observed_at],
        ["执行方式", (item) => item.enforcement], ["来源", (item) => item.source], ["代码", (item) => item.code],
      ], value.usage || [], "暂无配额观测。"));
    } catch (error) {
      if (!live(context, view)) return;
      for (const id of ["quota-meta", "quota-policy", "quota-usage"]) $(id).replaceChildren();
      $("quota-meta").append(text("span", "配额尚未取得。", "muted"));
      report(error, context, view);
    }
  }

  async function loadCosts(event) {
    if (event) event.preventDefault();
    const context = state.context;
    const view = state.view;
    const since = utcStamp($("cost-since").value);
    const until = utcStamp($("cost-until").value);
    try {
      const value = await api(adminPath(context) + "/costs" + query({ since, until }), {}, context);
      if (!live(context, view)) return;
      renderCosts(value);
    } catch (error) {
      if (!live(context, view)) return;
      for (const id of ["cost-meta", "cost-totals", "cost-lines", "cost-excluded"]) $(id).replaceChildren();
      $("cost-meta").append(text("span", "成本尚未取得。", "muted"));
      report(error, context, view);
    }
  }

  function renderCosts(value) {
    const prices = value.prices || {};
    $("cost-meta").replaceChildren(badge(value.status),
      text("span", value.status === "UNKNOWN" ? "区域价目表不可用，成本未知。" :
        "估算：实测用量 × 公开单价（" + display(prices.retrieved) + "）"), text("span", "basis " + value.basis),
      text("span", display(prices.region) + " · " + display(prices.currency)),
      text("span", "区间 " + value.since + " — " + value.until), text("span", "观测 " + value.observed_at));
    $("cost-totals").replaceChildren(heading("合计（USD）"), table([
      ["分类", ([category]) => text("code", category)], ["估算 USD", ([, amount]) => amount],
    ], Object.entries(value.totals || {}), "无合计。"));
    $("cost-lines").replaceChildren(heading("明细"), table([
      ["分类", (item) => item.category], ["工作负载", (item) => item.workload ? item.workload.kind + " · " + item.workload.id : null],
      ["用量", (item) => item.usage], ["单位", (item) => item.unit], ["单价 USD", (item) => item.unit_price_usd],
      ["估算 USD", (item) => item.estimated_usd],
      ["观测时间", (item) => item.observed_at === null ? "未观测" : item.observed_at], ["来源", (item) => item.source],
      ["属性", (item) => Object.entries(item.attributes || {}).map(([name, detail]) => name + "=" + display(detail)).join("\n")],
    ], value.lines || [], "无成本明细。"));
    if (value.truncated) $("cost-lines").append(text("p", "明细已截断。", "small muted"));
    const excluded = node("ul", "plain");
    for (const item of value.excluded || []) excluded.append(text("li", item));
    const sources = node("ul", "plain");
    for (const item of prices.sources || []) sources.append(text("li", item));
    const unmeasured = value.unmeasured || [];
    $("cost-excluded").replaceChildren(
      heading("未能计量（结果为 PARTIAL 的原因）"), unmeasured.length ? table([
        ["类型", (item) => item.kind], ["标识", (item) => text("code", item.id)], ["原因", (item) => item.reason],
      ], unmeasured) : text("p", "无。", "small muted"),
      heading("按设计未计入"),
      (value.excluded || []).length ? excluded : text("p", "无。", "small muted"), heading("价格来源"), sources);
  }

  // -- Connect / disconnect. Disconnect clears memory, requests, timers and the DOM. --

  function resetFields(root) {
    for (const field of root.querySelectorAll("input, textarea, select")) {
      if (field.type === "checkbox" || field.type === "radio") field.checked = field.defaultChecked;
      else if (field.tagName === "SELECT") for (const option of field.options) option.selected = option.defaultSelected;
      else field.value = field.defaultValue;
      field.disabled = false;
    }
  }

  function disconnect() {
    stopTimers();
    state.context = null;
    state.epoch++;
    state.view++;
    state.selection++;
    for (const controller of state.requests) controller.abort();
    Object.assign(state, {
      tab: null, pending: null, token: null, keys: [], rotating: null, confirming: null,
      pipeline: null, creating: false, files: [], file: null, versions: [], runs: { run: null, trial: null },
      environment: null, audit: { next: null, page: 1 }, trace: null,
      retention: null, retentionRevision: null, collection: null,
    });
    clearToken();
    for (const target of document.querySelectorAll("[data-dynamic]")) target.replaceChildren();
    for (const target of document.querySelectorAll("[data-detail]")) target.hidden = true;
    resetFields($("workspace"));
    resetKeyForm();
    $("run-partitions").disabled = true;
    $("pipeline-reload").disabled = true;
    $("pending-retry").disabled = false;
    for (const link of document.querySelectorAll("[data-tab]")) link.removeAttribute("aria-current");
    for (const panel of document.querySelectorAll("[data-panel]")) panel.hidden = true;
    renderPending();
    $("api-key").value = "";
    $("workspace").hidden = true;
    $("connection").hidden = false;
    $("connection-status").textContent = "未连接";
    $("disconnect").disabled = true;
    notice("");
  }

  function connect(event) {
    event.preventDefault();
    const space = $("space").value.trim();
    const key = $("api-key").value.trim();
    if (!IDENTIFIER.test(space) || !key || key.length > 512) {
      $("api-key").value = "";
      notice("请输入有效的数据空间 ID（字母或 _ 开头）和 API Key。", true);
      return;
    }
    disconnect();
    state.context = { space, key, epoch: state.epoch };
    $("connection").hidden = true;
    $("workspace").hidden = false;
    $("disconnect").disabled = false;
    $("connection-status").textContent = space;
    return show(tabFromHash());
  }

  function init() {
    for (const scope of SCOPES) {
      const label = node("label");
      const box = node("input");
      box.type = "checkbox";
      box.value = scope;
      label.append(box, scope);
      $("key-scopes").append(label);
    }
    for (const action of AUDIT_ACTIONS) {
      const option = text("option", action);
      option.value = action;
      $("audit-action").append(option);
    }
    for (const name of RETENTION_FIELDS) {
      const label = text("label", name);
      const input = node("input");
      input.id = "retention-" + name;
      input.type = "number";
      input.min = name === "release_keep" ? "1" : "60";
      input.step = "1";
      input.required = true;
      label.append(input);
      $("retention-fields").append(label);
    }
    const on = (id, type, handler) => $(id).addEventListener(type, safely(handler));
    on("connect-form", "submit", connect);
    on("disconnect", "click", disconnect);
    window.addEventListener("hashchange", safely(() => show(tabFromHash())));
    on("pending-retry", "click", retryPending);
    on("pending-drop", "click", dropPending);
    on("overview-refresh", "click", loadOverview);
    on("overview-auto", "change", () => {
      if ($("overview-auto").checked) schedule("overview", loadOverview, 15000);
      else stopTimer("overview");
    });
    on("keys-refresh", "click", refreshKeys);
    on("key-form", "submit", submitKey);
    on("key-rotate-cancel", "click", resetKeyForm);
    on("token-copy", "click", copyToken);
    on("token-hide", "click", clearToken);
    on("pipelines-refresh", "click", loadPipelines);
    on("pipeline-new", "click", newPipeline);
    on("pipeline-reload", "click", () => state.pipeline && openPipeline(state.pipeline.pipeline_id));
    on("definition", "input", renderDag);
    on("file-add", "click", addFile);
    $("file-new-path").addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.isComposing) {
        event.preventDefault();
        addFile();
      }
    });
    on("file-content", "input", editFile);
    on("pipeline-save", "click", savePipeline);
    on("pipeline-validate", "click", validateDraft);
    on("pipeline-publish", "click", publishDraft);
    on("versions-refresh", "click", loadVersions);
    on("schedule-form", "submit", submitSchedule);
    on("schedule-pause", "click", pauseSchedule);
    on("runs-refresh", "click", loadRuns);
    on("run-mode", "change", () => {
      $("run-partitions").disabled = $("run-mode").value !== "backfill";
    });
    on("run-start", "click", startRun);
    on("run-refresh", "click", () => observeRun("run"));
    on("run-cancel", "click", () => cancelRun("run"));
    on("run-retry", "click", retryRun);
    on("trial-start", "click", startTrial);
    on("trial-refresh", "click", () => observeRun("trial"));
    on("trial-cancel", "click", () => cancelRun("trial"));
    on("trial-preview-button", "click", preview);
    on("environments-refresh", "click", loadEnvironments);
    on("environment-refresh", "click", observeEnvironment);
    on("environment-build", "click", buildEnvironment);
    on("environment-form", "submit", createEnvironment);
    on("audit-form", "submit", (event) => {
      event.preventDefault();
      return loadAudit(null, 1);
    });
    on("audit-first", "click", () => loadAudit(null, 1));
    on("audit-next", "click", () => state.audit.next && loadAudit(state.audit.next, state.audit.page + 1));
    on("trace-form", "submit", submitTrace);
    on("retention-refresh", "click", () => loadRetention(true));
    on("retention-form", "submit", saveRetention);
    on("hold-form", "submit", placeHold);
    on("collection-form", "submit", requestCollection);
    on("collection-refresh", "click", observeCollection);
    on("quotas-refresh", "click", () => Promise.all([loadQuotas(), loadCosts()]));
    on("cost-form", "submit", loadCosts);
    window.addEventListener("pagehide", disconnect);
  }

  init();
})();
