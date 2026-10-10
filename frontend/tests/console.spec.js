import { test, expect } from "@playwright/test";
import path from "node:path";
import { createHash } from "node:crypto";
import { readdir, stat } from "node:fs/promises";
const input = path.resolve("../.e2e/console/input.txt");
const base = "http://127.0.0.1:8086";
const headers = { Authorization: `Bearer ${"a".repeat(32)}` };

async function connect(page, token = "a".repeat(32)) {
  await page.goto("/");
  await page.getByRole("button", { name: /配置 API 连接/ }).click();
  await page.getByLabel("API 地址").fill(base);
  await page.getByLabel("管理令牌").fill(token);
  await page.getByRole("button", { name: "连接", exact: true }).click();
  if (token === "a".repeat(32))
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
test("auth, cursor pagination, filters, DAG edits, cycle rejection and business results", async ({
  page,
  request,
}) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await connect(page, "wrong");
  await expect(page.getByRole("alert")).toContainText(
    "authentication required",
  );
  await page.getByLabel("管理令牌").fill("a".repeat(32));
  await page.getByRole("button", { name: "连接", exact: true }).click();
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
  expect(task.result.steps.count.words).toBe(400);
  expect(task.spec.steps.at(-1).id).toBe("finish");
  expect(task.result.runtime.gil_enabled).toBe(false);
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
  await page.getByLabel("管理令牌").fill("a".repeat(32));
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
  expect(finished.result.runtime.gil_enabled).toBe(false);
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

test("mobile editor is usable, API outage preserves edits and auth expiry stops polling", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/#dag");
  await page.getByLabel("任务名称").fill("离线编辑保留");
  await page.locator("#node-parameters").fill("{");
  await page.getByRole("button", { name: /配置 API 连接/ }).click();
  await page.getByLabel("API 地址").fill(base);
  await page.getByLabel("管理令牌").fill("a".repeat(32));
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
  await expect(page.locator("#notice")).toContainText("管理令牌失效");
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
  await page.getByLabel("管理令牌").fill("wrong");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("authentication required");
  await page.getByLabel("管理令牌").fill("a".repeat(32));
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
  const staging = path.join(path.dirname(input), "uploads", ".staging");
  await expect.poll(async () => {
    const dirs = await readdir(staging);
    if (!dirs.length) return 0;
    return (await stat(path.join(staging, dirs[0], "source", "取消上传.txt")).catch(() => ({ size: 0 }))).size;
  }).toBeGreaterThan(0);
  await page.getByRole("button", { name: "取消上传", exact: true }).click();
  await session.send("Network.emulateNetworkConditions", {
    offline: false, latency: 0, downloadThroughput: -1, uploadThroughput: -1,
  });
  await session.detach();
  await expect(page.locator("#upload-status")).toContainText("已取消");
  await expect(page.getByLabel("任务名称")).toHaveValue("取消上传保留的草稿");
  await expect.poll(() => readdir(staging)).toEqual([]);
  expect((await request.get(base + "/v1/files", {
    headers: { ...headers, "Idempotency-Key": key },
  })).status()).toBe(404);
  await expect(page.getByRole("button", { name: "提交任务" })).toBeDisabled();
  await page.getByRole("button", { name: "清除所选文件" }).click();
  await expect(page.getByRole("button", { name: "提交任务" })).toBeEnabled();
});
