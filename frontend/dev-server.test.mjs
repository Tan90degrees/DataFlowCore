import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFile, spawn } from "node:child_process";
import { once } from "node:events";
import http from "node:http";
import { promisify } from "node:util";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { createDevServer } from "./dev-server.mjs";

const command = fileURLToPath(new URL("dev-server.mjs", import.meta.url));
const execute = promisify(execFile);

async function listen(t, server) {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve) => {
    server.close(resolve);
    server.closeAllConnections();
  }));
  return `http://127.0.0.1:${server.address().port}`;
}

test("serves actual browser assets, HEAD and excludes project/private files", async (t) => {
  const base = await listen(t, createDevServer());
  const page = await fetch(base);
  assert.equal(page.status, 200);
  assert.match(await page.text(), /DataFlowCore/);
  assert.equal(page.headers.get("cache-control"), "no-store");
  const module = await fetch(base + "/app.js");
  assert.match(module.headers.get("content-type"), /javascript/);
  assert.match(await module.text(), /context.parameters/);
  const head = await fetch(base + "/styles.css", { method: "HEAD" });
  assert.equal(head.status, 200);
  assert.equal(await head.text(), "");
  for (const path of ["/.env", "/package.json", "/dev-server.mjs", "/tests/serve.py"])
    assert.equal((await fetch(base + path)).status, 404);
  assert.equal((await fetch(base, { method: "POST" })).status, 405);
});

test("streams API body, auth, idempotency key and query with upstream prefix", async (t) => {
  const upstream = await listen(t, http.createServer(async (req, res) => {
    let body = "";
    for await (const data of req) body += data;
    res.writeHead(202, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ path: req.url, method: req.method, headers: req.headers, body }));
  }));
  const base = await listen(t, createDevServer(upstream + "/control"));
  const response = await fetch(base + "/api/v1/tasks?summary=1", {
    method: "POST",
    headers: {
      Authorization: "Bearer test-admin",
      "Idempotency-Key": "dev-proxy-test",
      "Content-Type": "application/json",
      Origin: "http://remote-console:8000",
    },
    body: '{"name":"中文文件"}',
  });
  assert.equal(response.status, 202);
  const value = await response.json();
  assert.equal(value.path, "/control/v1/tasks?summary=1");
  assert.equal(value.method, "POST");
  assert.equal(value.headers.authorization, "Bearer test-admin");
  assert.equal(value.headers["idempotency-key"], "dev-proxy-test");
  assert.equal(value.headers.origin, undefined);
  assert.equal(value.body, '{"name":"中文文件"}');
});

test("preserves backend authentication failures", async (t) => {
  const upstream = await listen(t, http.createServer((req, res) => {
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end('{"error":"authentication required"}');
  }));
  const base = await listen(t, createDevServer(upstream));
  const response = await fetch(base + "/api/v1/overview");
  assert.equal(response.status, 401);
  assert.equal((await response.json()).error, "authentication required");
});

test("streams a binary file larger than the JSON budget without changing its bytes", async (t) => {
  const expected = Buffer.alloc(2 * 1024 * 1024, 0xa8);
  const upstream = await listen(t, http.createServer(async (req, res) => {
    const digest = createHash("sha256");
    let size = 0;
    for await (const block of req) { size += block.length; digest.update(block); }
    res.writeHead(201, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ size, hash: digest.digest("hex"), type: req.headers["content-type"],
      key: req.headers["idempotency-key"], path: req.url }));
  }));
  const base = await listen(t, createDevServer(upstream));
  const path = "/v1/files?filename=" + encodeURIComponent("中文文件.pdf");
  const response = await fetch(base + "/api" + path, {
    method: "POST", body: expected,
    headers: { "Content-Type": "application/octet-stream", "Idempotency-Key": "binary-upload" },
  });
  assert.equal(response.status, 201);
  assert.deepEqual(await response.json(), {
    size: expected.length, hash: createHash("sha256").update(expected).digest("hex"),
    type: "application/octet-stream", key: "binary-upload", path,
  });
});

test("unavailable API returns actionable JSON while the frontend remains available", async (t) => {
  const unused = http.createServer().listen(0, "127.0.0.1");
  await once(unused, "listening");
  const port = unused.address().port;
  await new Promise((resolve) => unused.close(resolve));
  const base = await listen(t, createDevServer(`http://127.0.0.1:${port}`));
  const response = await fetch(base + "/api/readyz");
  assert.equal(response.status, 502);
  assert.match((await response.json()).error, /--api-upstream/);
  assert.equal((await fetch(base)).status, 200);
});

test("CLI reports invalid configuration and port conflicts with nonzero exit", async (t) => {
  for (const args of [["--port", "0"], ["--api-upstream", "file:///tmp"]]) {
    await assert.rejects(execute(process.execPath, [command, ...args]), (error) => {
      assert.equal(error.code, 1);
      assert.match(error.stderr, /前端启动失败/);
      return true;
    });
  }
  const occupied = http.createServer();
  await listen(t, occupied);
  await assert.rejects(execute(process.execPath, [command, "--port", String(occupied.address().port)]),
    (error) => {
      assert.equal(error.code, 1);
      assert.match(error.stderr, /已被占用/);
      return true;
    });
});

test("CLI stays running without Python and closes on SIGTERM", { timeout: 10000 }, async (t) => {
  const reservation = http.createServer();
  await listen(t, reservation);
  const port = reservation.address().port;
  await new Promise((resolve) => reservation.close(resolve));
  const child = spawn(process.execPath, [command, "--port", String(port)]);
  t.after(() => { if (child.exitCode === null) child.kill("SIGKILL"); });
  let output = "";
  await new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("exit", () => reject(new Error("dev server exited before listening")));
    child.stdout.on("data", (chunk) => {
      output += chunk;
      if (output.includes("API 代理")) resolve();
    });
  });
  assert.match(output, /0\.0\.0\.0/);
  assert.equal(child.exitCode, null);
  assert.equal((await fetch(`http://127.0.0.1:${port}`)).status, 200);
  const exit = once(child, "exit");
  child.kill("SIGTERM");
  assert.deepEqual(await exit, [0, null]);
});
