import { test, expect } from "@playwright/test";
import path from "node:path";
import { createHash } from "node:crypto";
import { readdir, stat } from "node:fs/promises";
const input = path.join(process.env.DATAFLOW_CONSOLE_TEST_ROOT, "input.txt");
const base = "http://127.0.0.1:8086";
const headers = { Authorization: `Bearer ${"a".repeat(32)}` };

async function expectRuntime(request, runtime) {
  // The browser fixture starts controller and workers with the same interpreter.
  const expected = (await (await request.get(base + "/version")).json()).runtime;
  expect(runtime.gil_enabled).toBe(expected.gil_enabled);
  expect(runtime.execution_model).toBe(expected.execution_model);
}

async function connect(page) {
  await page.goto("/");
  await page.getByRole("button", { name: /配置 API 连接/ }).click();
  await page.getByLabel("API 地址").fill(base);
  await page.getByRole("button", { name: "连接", exact: true }).click();
    await expect(
      page.getByRole("button", { name: /API 已连接/ }),
    ).toBeVisible();
}
async function importSpec(page, spec) {
  await page.getByRole("link", { name: "DAG 编排" }).click();
  await page.locator("#dag-import").setInputFiles({
    name: "spec.json",
    mimeType: "application/json",
    buffer: Buffer.from(JSON.stringify(spec)),
  });
}
async function submit(page) {
  await page.getByRole("button", { name: "提交任务" }).click();
  await expect(page).toHaveURL(/#task\//);
  return page.url().split("#task/")[1];
}
function spec(name, steps, parameters = {}) {
  return {
    name,
    input_path: input,
    steps,
    parameters,
    retry_delay: 0,
    timeout: 30,
  };
}

// These tests issue real browser CORS requests; no mock control-plane responses.
test("token-free connection, cursor pagination, filters, DAG edits, cycle rejection and business results", async ({
  page,
  request,
}) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await connect(page);
  await expect(page.locator("tbody tr")).toHaveCount(30);
  await page.getByRole("button", { name: "下一页" }).click();
  await expect(page.locator("tbody tr")).toHaveCount(5);
  await page.getByRole("button", { name: "上一页" }).click();
  await expect(page.locator("tbody tr")).toHaveCount(30);
  await page.getByLabel("执行池筛选").fill("default");
  await page.getByLabel("执行池筛选").press("Tab");
  await expect(page.getByText("暂无任务", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "创建任务" }).click();
  await page.getByLabel("共享文件绝对路径").fill(input);
  await page.getByLabel("任务名称").fill("浏览器 DAG 编辑验证");
  await page.getByRole("button", { name: "编辑节点 read" }).click();
  await page
    .locator("#dependencies")
    .getByLabel("write", { exact: true })
    .check();
  await page.getByRole("button", { name: "校验 DAG", exact: true }).click();
  await expect(page.locator("#notice")).toContainText("cycle");
  await page
    .locator("#dependencies")
    .getByLabel("write", { exact: true })
    .uncheck();
  await page.getByRole("button", { name: "添加节点" }).click();
  await page.locator("#node-id").fill("finish");
  await page.locator("#node-parameters").fill('{"seconds":0.1}');
  await page.getByRole("button", { name: "应用", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "编辑节点 finish" }),
  ).toBeVisible();
  const download = page.waitForEvent("download");
  await page.getByRole("button", { name: "导出 JSON" }).click();
  expect((await download).suggestedFilename()).toBe("task-spec.json");
  await page.getByRole("button", { name: "保存草稿" }).click();
  expect(await page.evaluate(() => JSON.stringify(localStorage))).not.toContain(
    "a".repeat(32),
  );
  const id = await submit(page);
  await expect(page.locator(".title-meta .badge")).toHaveText("已完成");
  const task = await (
    await request.get(`${base}/v1/tasks/${id}`, { headers })
  ).json();
  await expect(page.getByRole("button", { name: "停止任务", exact: true })).toBeDisabled();
  await expect(page.locator("#task-stop-hint")).toContainText("已完成");
  expect(task.result.steps.count.words).toBe(400);
  expect(task.spec.steps.at(-1).id).toBe("finish");
  await expectRuntime(request, task.result.runtime);
  await page.getByRole("button", { name: "事件", exact: true }).click();
  await expect(page.locator(".event")).not.toHaveCount(0);
  await page.getByRole("button", { name: "结果", exact: true }).click();
  await expect(page.locator("pre")).toContainText("gil_enabled");
  await page.getByRole("button", { name: "复制到编排器" }).click();
  await expect(page.getByLabel("任务名称")).toHaveValue("浏览器 DAG 编辑验证");
  const module = "dataflowcore.examples.ingestion:";
  await importSpec(
    page,
    spec(
      "文档入库 / 六节点业务",
      [
        { id: "parse", callable: module + "parse" },
        { id: "chunk", callable: module + "chunk", depends_on: ["parse"] },
        { id: "stats", callable: module + "statistics", depends_on: ["parse"] },
        { id: "embed", callable: module + "embed", depends_on: ["chunk"] },
        {
          id: "index",
          callable: module + "index",
          depends_on: ["embed", "stats"],
        },
        { id: "receipt", callable: module + "receipt", depends_on: ["index"] },
      ],
      { receipt_delay: 1 },
    ),
  );
  await expect(page.getByLabel("任务名称")).toHaveValue(
    "文档入库 / 六节点业务",
  );
  await page.locator("#task-parameters").evaluate((node) => {
    node.closest("details").open = true;
  });
  await page
    .locator("#task-parameters")
    .fill('{"receipt_delay":1,"chunk_size":128,"overlap":16}');
  await page
    .getByRole("button", { name: "编辑节点 chunk", exact: true })
    .click();
  await page.locator("#node-parameters").fill('{"chunk_size":256}');
  // Submission applies the current node configuration without requiring an extra Apply click.
  const business = await submit(page);
  await expect(page.locator(".title-meta .badge")).toHaveText("已完成");
  const final = await (
    await request.get(`${base}/v1/tasks/${business}`, { headers })
  ).json();
  expect(final.spec.parameters.chunk_size).toBe(128);
  expect(
    final.spec.steps.find((step) => step.id === "chunk").parameters.chunk_size,
  ).toBe(256);
  const expectedChunks =
    1 +
    Math.ceil(
      Math.max(0, final.result.steps.parse.characters - 256) / (256 - 16),
    );
  expect(final.result.steps.index.chunks).toBe(expectedChunks);
  expect(final.attempt_count).toBe(1);
  await page.screenshot({
    path: "test-results/business-detail.png",
    fullPage: true,
  });
  await page.getByRole("link", { name: "任务中心" }).click();
  await expect(page.locator("tbody")).toContainText("文档入库 / 六节点业务");
  await page.screenshot({ path: "test-results/tasks.png", fullPage: true });
  expect(errors).toEqual([]);
  await page.reload();
  await expect(
    page.getByRole("button", { name: /配置 API 连接/ }),
  ).toBeVisible();
});

test("local upload through /api preserves edits, confirms lost response and executes the real file", async ({
  page, request,
}) => {
  await page.addInitScript(() => {
    Object.defineProperty(Crypto.prototype, "randomUUID", { value: undefined });
  });
  await page.goto("/#dag");
  await page.getByRole("button", { name: /配置 API 连接/ }).click();
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.getByRole("button", { name: /API 已连接/ })).toBeVisible();
  await importSpec(page, spec("浏览器上传文件业务", [
    { id: "parse", callable: "dataflowcore.examples.ingestion:parse" },
    { id: "stats", callable: "dataflowcore.examples.ingestion:statistics", depends_on: ["parse"] },
  ]));
  await expect(page.getByLabel("任务名称")).toHaveValue("浏览器上传文件业务");
  await page.locator("#task-parameters").evaluate((node) => { node.closest("details").open = true; });
  await page.locator("#task-parameters").fill("{");
  await page.getByLabel("本地输入文件").setInputFiles({
    name: "超过上限.txt", mimeType: "text/plain", buffer: Buffer.alloc(4 * 1024 * 1024),
  });
  await page.getByRole("button", { name: "上传文件", exact: true }).click();
  await expect(page.locator("#upload-status")).toContainText("超过上传上限");
  await expect(page.getByRole("button", { name: "提交任务" })).toBeDisabled();
  await expect(page.getByLabel("共享文件绝对路径")).toHaveValue(input);
  await expect(page.locator("#task-parameters")).toHaveValue("{");
  await page.getByRole("button", { name: "清除所选文件" }).click();

  const payload = Buffer.from("上传文档 DataFlow hello world\n".repeat(100) + " ".repeat(2 * 1024 * 1024));
  let uploaded, uploadKey, posts = 0;
  await page.route(/\/api\/v1\/files\?filename=/, async (route) => {
    posts++;
    uploadKey = route.request().headers()["idempotency-key"];
    const response = await route.fetch();
    expect(response.status()).toBe(201);
    uploaded = await response.json();
    await page.getByLabel("共享文件绝对路径").focus();
    await route.abort("connectionfailed");
  }, { times: 1 });
  await page.getByLabel("本地输入文件").setInputFiles({
    name: "前端上传 文档.txt", mimeType: "text/plain", buffer: payload,
  });
  await page.getByRole("button", { name: "上传文件", exact: true }).click();
  await expect(page.locator("#upload-status")).toContainText("上传完成");
  await page.getByLabel("任务名称").click();
  expect(posts).toBe(1);
  expect(uploaded.input_sha256).toBe(createHash("sha256").update(payload).digest("hex"));
  expect(uploaded.input_path.startsWith(path.join(path.dirname(input), "console-inputs") + path.sep)).toBe(true);
  await expect(page.getByLabel("共享文件绝对路径")).toHaveValue(uploaded.input_path);
  await expect(page.locator("#input-checksum")).toHaveValue(uploaded.input_sha256);
  await expect(page.locator("#task-parameters")).toHaveValue("{");
  await expect(page.getByRole("progressbar", { name: "文件上传进度" })).toHaveJSProperty("value", 100);
  const recovered = await request.get(base + "/v1/files", {
    headers: { ...headers, "Idempotency-Key": uploadKey },
  });
  expect(await recovered.json()).toEqual(uploaded);
  await page.screenshot({ path: "test-results/upload-ready.png", fullPage: true });
  await page.locator("#task-parameters").fill('{"business":"前端上传"}');
  const id = await submit(page);
  await expect(page.locator(".title-meta .badge")).toHaveText("已完成");
  const finished = await (await request.get(base + "/v1/tasks/" + id, { headers })).json();
  expect(finished.spec.input_path).toBe(uploaded.input_path);
  expect(finished.spec.input_sha256).toBe(uploaded.input_sha256);
  expect(finished.spec.parameters.business).toBe("前端上传");
  expect(finished.result.steps.stats.words).toBe(400);
  await expectRuntime(request, finished.result.runtime);
});

test("per-operator resource editing, IO bypass and resource-wait cancellation", async ({ page, request }) => {
  const holder = await (await request.post(base + "/v1/tasks", {
    headers: { ...headers, "Idempotency-Key": `holder-${Date.now()}` },
    data: spec("CPU 预算占用", [{id: "hold", callable: "dataflowcore.operators:delay",
      workload: "cpu", resources: {cpu: 4, memory_mb: 128}, parameters: {seconds: 30}}]),
  })).json();
  try {
    await expect.poll(async () => {
      const task = await (await request.get(base + `/v1/tasks/${holder.id}`, {headers})).json();
      return task.attempts.at(-1)?.progress.steps?.hold.state;
    }).toBe("RUNNING");
    await connect(page);
    await page.getByRole("link", {name: "DAG 编排"}).click();
    await expect(page.locator('[data-field="cpu"], [data-field="memory_mb"], [data-field="dag_workers"], [data-field="io_workers"]')).toHaveCount(0);
    await page.getByLabel("任务名称").fill("算子预算浏览器验证");
    await page.getByLabel("共享文件绝对路径").fill(input);
    await page.getByLabel("算子类型", {exact: true}).selectOption("io");
    await page.getByLabel("算子 CPU", {exact: true}).fill("0");
    await page.getByLabel("算子内存 MiB", {exact: true}).fill("64");
    await page.getByRole("button", {name: "编辑节点 count", exact: true}).click();
    await page.getByLabel("算子类型", {exact: true}).selectOption("cpu");
    await page.getByLabel("算子 CPU", {exact: true}).fill("4");
    await page.getByLabel("算子内存 MiB", {exact: true}).fill("128");
    await page.getByRole("button", {name: "编辑节点 read", exact: true}).click();
    await expect(page.getByLabel("算子 CPU", {exact: true})).toHaveValue("0");
    await page.screenshot({path: "test-results/operator-editor.png", fullPage: true});
    const tid = await submit(page);
    await expect(page.locator(".step-row").filter({hasText: "count"})).toContainText("等待资源");
    await expect(page.locator(".step-row").filter({hasText: "count"})).toContainText("等待 CPU 预算");
    const task = await (await request.get(base + `/v1/tasks/${tid}`, {headers})).json();
    expect(task.scheduling).toBe("operator");
    expect(task.placement.cpu).toBe(4);
    for (const key of ["cpu", "memory_mb", "dag_workers", "io_workers", "scheduling"])
      expect(task.spec).not.toHaveProperty(key);
    expect(task.spec.steps.find(s => s.id === "read").resources).toEqual({cpu: 0, memory_mb: 64});
    expect(task.spec.steps.find(s => s.id === "count").resources).toEqual({cpu: 4, memory_mb: 128});
    expect(task.attempts.at(-1).progress.steps.read.state).toBe("SUCCEEDED");
    expect(task.attempts.at(-1).progress.steps.count.wait_reasons).toContain("cpu");
    await page.getByRole("button", {name: "停止任务", exact: true}).click();
    await page.locator("#confirm-yes").click();
    await expect(page.locator(".title-meta .badge")).toHaveText("已取消");
  } finally {
    const cancelled = await request.post(base + `/v1/tasks/${holder.id}/cancel`, {headers, data: {}});
    expect(cancelled.ok()).toBeTruthy();
    await expect.poll(async () => (await (await request.get(base + `/v1/tasks/${holder.id}`, {headers})).json()).state).toMatch(/CANCELLED|SUCCEEDED/);
  }
});

test("lost submit response is idempotent; cancellation, retry, failures and worker drain", async ({
  page,
  request,
}) => {
  await connect(page);
  const name = "长任务取消与重试";
  await importSpec(
    page,
    spec(name, [
      {
        id: "wait",
        callable: "dataflowcore.operators:delay",
        parameters: { seconds: 15 },
      },
    ]),
  );
  await expect(page.getByLabel("任务名称")).toHaveValue(name);
  let first = true,
    firstKey;
  await page.route("**/v1/tasks", async (route) => {
    if (route.request().method() === "POST" && first) {
      first = false;
      firstKey = route.request().headers()["idempotency-key"];
      await route.fetch();
      await route.abort("failed");
    } else await route.continue();
  });
  await page.getByRole("button", { name: "提交任务" }).click();
  await expect(page.locator("#notice")).toContainText("操作可能已被服务器接收");
  let secondKey;
  page.on("request", (req) => {
    if (req.url() === `${base}/v1/tasks` && req.method() === "POST")
      secondKey = req.headers()["idempotency-key"];
  });
  const id = await submit(page);
  expect(secondKey).toBe(firstKey);
  await expect(page.locator(".title-meta .badge")).toHaveText("运行中");
  await expect(page.getByRole("button", { name: "停止任务", exact: true })).toBeEnabled();
  await expect(page.locator("#task-stop-hint")).toContainText("运行中的任务可以停止");
  await page.getByRole("button", { name: "停止任务" }).click();
  await page.locator("#confirm-yes").click();
  await expect(page.locator(".title-meta .badge")).toHaveText("已取消");
  // A confirmed submission followed by an intentional new submission creates new work.
  await page.getByRole("link", { name: "DAG 编排", exact: true }).click();
  const intentional = await submit(page);
  expect(intentional).not.toBe(id);
  await page.getByRole("button", { name: "停止任务" }).click();
  await page.locator("#confirm-yes").click();
  await expect(page.locator(".title-meta .badge")).toHaveText("已取消");
  await page.evaluate((id) => {
    location.hash = `task/${id}`;
  }, id);
  await expect(page.locator(".title-meta .badge")).toHaveText("已取消");
  await page.getByRole("button", { name: "从头重试" }).click();
  await page.locator("#confirm-yes").click();
  await expect(page).not.toHaveURL(new RegExp(id));
  const firstRetry = page.url().split("#task/")[1];
  await page.getByRole("button", { name: "停止任务" }).click();
  await page.locator("#confirm-yes").click();
  await expect(page.locator(".title-meta .badge")).toHaveText("已取消");
  // Separate successful retry requests for the original task must also create new identities.
  await page.evaluate((id) => {
    location.hash = `task/${id}`;
  }, id);
  await expect(page.locator(".title-meta .badge")).toHaveText("已取消");
  await page.getByRole("button", { name: "从头重试" }).click();
  await page.locator("#confirm-yes").click();
  await expect(page).not.toHaveURL(new RegExp(id));
  expect(page.url().split("#task/")[1]).not.toBe(firstRetry);
  await page.getByRole("button", { name: "停止任务" }).click();
  await page.locator("#confirm-yes").click();
  await expect(page.locator(".title-meta .badge")).toHaveText("已取消");
  await importSpec(
    page,
    spec('<img src=x onerror="alert(1)">', [
      { id: "fail", callable: "dataflowcore.operators:fail" },
    ]),
  );
  await expect(page.getByLabel("任务名称")).toHaveValue(
    '<img src=x onerror="alert(1)">',
  );
  const failed = await submit(page);
  await expect(page.locator(".title-meta .badge")).toHaveText("失败");
  await expect(page.getByRole("button", { name: "停止任务", exact: true })).toBeDisabled();
  await expect(page.locator("#task-stop-hint")).toContainText("失败");
  await page.getByRole("button", { name: "日志", exact: true }).click();
  await expect(page.locator(".log")).toContainText("example permanent failure");
  expect(await page.locator("main img").count()).toBe(0);
  const result = await (
    await request.get(`${base}/v1/tasks/${failed}`, { headers })
  ).json();
  expect(result.attempt_count).toBe(1);
  await page.getByRole("link", { name: "执行器", exact: true }).click();
  await expect(page.getByText("在线", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "排空执行器" }).click();
  await page.locator("#confirm-yes").click();
  await expect(page.getByText("排空中", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "排空执行器" })).toBeDisabled();
});

test("mobile editor is usable, API outage preserves edits and denied gateway stops polling", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/#dag");
  await page.getByLabel("任务名称").fill("离线编辑保留");
  await page.locator("#node-parameters").fill("{");
  await page.getByRole("button", { name: /配置 API 连接/ }).click();
  await page.getByLabel("API 地址").fill(base);
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.getByRole("button", { name: /API 已连接/ })).toBeVisible();
  await expect(page.getByLabel("任务名称")).toHaveValue("离线编辑保留");
  await expect(page.locator("#node-parameters")).toHaveValue("{");
  await page.locator("#node-parameters").fill("{}");
  await page.route("**/v1/dags/validate", (route) => route.abort());
  await page.getByRole("button", { name: "校验 DAG" }).click();
  await expect(page.locator("#notice")).toContainText("无法连接 API");
  await expect(page.getByLabel("任务名称")).toHaveValue("离线编辑保留");
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth),
  ).toBeLessThanOrEqual(390);
  await page.screenshot({
    path: "test-results/mobile-editor.png",
    fullPage: true,
  });
  await page.getByRole("link", { name: "任务中心" }).click();
  await page.route("**/v1/overview", (route) =>
    route.fulfill({
      status: 401,
      contentType: "application/json",
      body: '{"error":"authentication required"}',
      headers: { "Access-Control-Allow-Origin": "http://127.0.0.1:8000" },
    }),
  );
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.locator("#notice")).toContainText("API 入口拒绝访问");
  await expect(
    page.getByRole("button", { name: /配置 API 连接/ }),
  ).toBeVisible();
});

test("development server default /api connects and validates through the real proxy", async ({
  page,
}) => {
  await page.goto("/");
  await page.getByRole("button", { name: /配置 API 连接/ }).click();
  await expect(page.getByLabel("API 地址")).toHaveValue("/api");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.getByRole("button", { name: /API 已连接/ })).toBeVisible();
  await page.getByRole("link", { name: "DAG 编排" }).click();
  await page.getByLabel("共享文件绝对路径").fill(input);
  const validation = page.waitForResponse((response) =>
    response.url().includes("/api/v1/dags/validate") && response.request().method() === "POST",
  );
  await page.getByRole("button", { name: "校验 DAG" }).click();
  expect((await validation).status()).toBe(200);
});

test("stop acknowledgement survives an older in-flight refresh", async ({ page, request }) => {
  await connect(page);
  const response = await request.post(base + "/v1/tasks", {
    headers: { ...headers, "Idempotency-Key": "stop-refresh-" + Date.now() },
    data: { ...spec("停止排队任务与刷新竞态", [
      { id: "read", callable: "dataflowcore.operators:read_text" },
    ]), pool: "no-executor" },
  });
  expect(response.ok()).toBe(true);
  const task = await response.json();
  await page.evaluate((id) => { location.hash = `task/${id}`; }, task.id);
  await expect(page.locator(".title-meta .badge")).toHaveText("排队中");
  await expect(page.getByRole("button", { name: "停止任务", exact: true })).toBeEnabled();
  let markHeld, release;
  const held = new Promise((resolve) => { markHeld = resolve; });
  const pause = new Promise((resolve) => { release = resolve; });
  await page.route(`**/v1/tasks/${task.id}`, async (route) => {
    const snapshot = await route.fetch();
    const body = await snapshot.body();
    markHeld();
    await pause;
    await route.fulfill({ response: snapshot, body });
  }, { times: 1 });
  try {
    await page.getByRole("button", { name: "刷新", exact: true }).click();
    await held;
    await page.getByRole("button", { name: "停止任务", exact: true }).click();
    const acknowledgement = page.waitForResponse((result) =>
      result.url().endsWith(`/v1/tasks/${task.id}/cancel`) && result.request().method() === "POST",
    );
    await page.locator("#confirm-yes").click();
    expect((await (await acknowledgement).json()).state).toBe("CANCELLED");
    // The accepted state must appear even while an older GET is still pending.
    await expect(page.locator(".title-meta .badge")).toHaveText("已取消", { timeout: 3000 });
    await expect(page.getByRole("button", { name: "停止任务", exact: true })).toBeDisabled();
    const stale = page.waitForResponse((result) =>
      result.url().endsWith(`/v1/tasks/${task.id}`) && result.request().method() === "GET",
    );
    release();
    expect((await (await stale).json()).state).toBe("QUEUED");
    await expect(page.getByRole("button", { name: "刷新", exact: true })).toBeEnabled();
    await expect(page.locator(".title-meta .badge")).toHaveText("已取消");
    await expect(page.locator("#task-stop-hint")).toContainText("已取消");
  } finally {
    release();
  }
});

test("stop controls recover after dismissing confirmation and reconnecting", async ({ page, request }) => {
  await connect(page);
  const response = await request.post(base + "/v1/tasks", {
    headers: { ...headers, "Idempotency-Key": "stop-reconnect-" + Date.now() },
    data: { ...spec("排队任务停止权限恢复", [
      { id: "read", callable: "dataflowcore.operators:read_text" },
    ]), pool: "no-executor" },
  });
  expect(response.ok()).toBe(true);
  const task = await response.json();
  await page.evaluate((id) => { location.hash = `task/${id}`; }, task.id);
  await expect(page.locator(".title-meta .badge")).toHaveText("排队中");
  let cancels = 0;
  page.on("request", (event) => {
    if (event.method() === "POST" && event.url().endsWith(`/v1/tasks/${task.id}/cancel`)) cancels++;
  });
  await page.getByRole("button", { name: "停止任务", exact: true }).click();
  await expect(page.locator("#stop-task")).toBeDisabled();
  await expect(page.locator("#task-stop-hint")).toContainText("正在确认");
  await page.locator("#confirm-no").click();
  await expect(page.getByRole("button", { name: "停止任务", exact: true })).toBeEnabled();
  expect(cancels).toBe(0);
  expect((await (await request.get(`${base}/v1/tasks/${task.id}`, { headers })).json()).state).toBe("QUEUED");
  // An optional product gateway may reject an API connection independently of this component.
  await page.route(`**/v1/tasks/${task.id}`, route => route.fulfill({status: 401,
    contentType: "application/json", body: '{"error":"gateway denied"}',
    headers: {"Access-Control-Allow-Origin": "http://127.0.0.1:8000"}}), {times: 1});
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.locator("#notice")).toContainText("API 入口拒绝访问");
  await expect(page.locator("#stop-task")).toBeDisabled();
  await expect(page.locator("#task-stop-hint")).toContainText("重新连接");
  await page.getByRole("button", { name: /配置 API 连接/ }).click();
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.getByRole("button", { name: "停止任务", exact: true })).toBeEnabled();
  await page.getByRole("button", { name: "停止任务", exact: true }).click();
  await page.locator("#confirm-yes").click();
  await expect(page.locator(".title-meta .badge")).toHaveText("已取消");
  await expect(page.locator("#task-stop-hint")).toContainText("已取消");
  expect(cancels).toBe(1);
});

test("cancel a slow real upload and keep the task draft", async ({ page, request }) => {
  await connect(page);
  await page.getByRole("link", { name: "DAG 编排" }).click();
  await page.getByLabel("任务名称").fill("取消上传保留的草稿");
  const session = await page.context().newCDPSession(page);
  await session.send("Network.enable");
  await session.send("Network.emulateNetworkConditions", {
    offline: false, latency: 0, downloadThroughput: -1, uploadThroughput: 16 * 1024,
  });
  let key;
  page.on("request", (event) => {
    if (event.method() === "POST" && event.url().includes("/v1/files?filename="))
      key = event.headers()["idempotency-key"];
  });
  await page.getByLabel("本地输入文件").setInputFiles({
    name: "取消上传.txt", mimeType: "text/plain", buffer: Buffer.alloc(2 * 1024 * 1024, 0x61),
  });
  await page.getByRole("button", { name: "上传文件", exact: true }).click();
  const limits = await (await request.get(base + "/v1/files/limits", { headers })).json();
  const staging = path.join(limits.upload_dir, ".staging");
  await expect.poll(async () => {
    const dirs = await readdir(staging);
    if (!dirs.length) return 0;
    return (await stat(path.join(staging, dirs[0], "source", "取消上传.txt")).catch(() => ({ size: 0 }))).size;
  }).toBeGreaterThan(0);
  const aborted = page.waitForEvent("requestfailed", {
    predicate: (event) => event.method() === "POST" && event.url().includes("/v1/files?filename="),
  });
  await page.getByRole("button", { name: "取消上传", exact: true }).click();
  await aborted;
  await expect(page.locator("#upload-status")).toContainText("已取消");
  await expect(page.getByLabel("任务名称")).toHaveValue("取消上传保留的草稿");
  await expect.poll(() => readdir(staging)).toEqual([]);
  expect((await request.get(base + "/v1/files", {
    headers: { ...headers, "Idempotency-Key": key },
  })).status()).toBe(404);
  await expect(page.getByRole("button", { name: "提交任务" })).toBeDisabled();
  await page.getByRole("button", { name: "清除所选文件" }).click();
  await expect(page.getByRole("button", { name: "提交任务" })).toBeEnabled();
  // Keep the wire throttled until cancellation has reached the server. Removing
  // throttling immediately after the click can flush queued bytes before abort.
  await session.detach();
});

test("internal console controls work without credentials and explain queue placement", async ({page, request}) => {
  const created = await request.post(base + "/v1/tasks", {headers: {"Idempotency-Key": "internal-queue"},
    data: {...spec("内部排队诊断", [{id: "read", callable: "dataflowcore.operators:read_text"}]), pool: "missing-pool"}});
  expect(created.ok()).toBeTruthy();
  const task = await created.json();
  await connect(page);
  await expect(page.getByLabel("管理令牌")).toHaveCount(0);
  await page.evaluate(id => {location.hash = `task/${id}`;}, task.id);
  await expect(page.locator("#queue-reason")).toContainText("执行池");
  await expect(page.getByRole("button", {name: "停止任务", exact: true})).toBeEnabled();
  await page.getByRole("button", {name: "停止任务", exact: true}).click();
  await page.locator("#confirm-yes").click();
  await expect(page.locator(".title-meta .badge")).toHaveText("已取消");
  await page.getByRole("link", {name: "执行器", exact: true}).click();
  await expect(page.locator("#main")).toContainText("文件运行 / 空闲槽位");
  await expect(page.locator("#main")).toContainText("当前资源预留");
  await expect(page.locator("#main")).not.toContainText("当前无资源快照");
});

test("operations pauses admission and reports quiescence", async ({page, request}) => {
  await connect(page);
  await page.getByRole("link", {name: "运维", exact: true}).click();
  await expect(page.getByRole("button", {name: "暂停任务接入", exact: true})).toBeEnabled();
  try {
    await page.getByRole("button", {name: "暂停任务接入", exact: true}).click();
    await page.locator("#confirm-yes").click();
    await expect(page.getByRole("button", {name: "恢复任务接入", exact: true})).toBeEnabled();
    await expect(page.locator("#main")).toContainText("已暂停且执行与上传归零");
    const refused = await request.post(base + "/v1/tasks", {headers: {...headers, "Idempotency-Key": "paused-new"},
      data: spec("暂停时不能提交", [{id: "read", callable: "dataflowcore.operators:read_text"}])});
    expect(refused.status()).toBe(503);
    expect(refused.headers()["retry-after"]).toBe("5");
    await page.getByRole("link", {name: "DAG 编排", exact: true}).click();
    await expect(page.getByRole("button", {name: "提交任务", exact: true})).toBeDisabled();
    await expect(page.locator("#admission-status")).toContainText("维护暂停");
    await page.getByRole("link", {name: "运维", exact: true}).click();
    await page.screenshot({path: "test-results/production-operations.png", fullPage: true});
    await page.getByRole("button", {name: "恢复任务接入", exact: true}).click();
    await page.locator("#confirm-yes").click();
    await expect(page.getByRole("button", {name: "暂停任务接入", exact: true})).toBeEnabled();
  } finally {
    await request.post(base + "/v1/control", {headers, data: {paused: false}});
  }
});
