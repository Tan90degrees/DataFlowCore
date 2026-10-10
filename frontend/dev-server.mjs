import { readFile } from "node:fs/promises";
import http from "node:http";
import https from "node:https";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const assets = new Map([
  ["index.html", "text/html; charset=utf-8"],
  ["styles.css", "text/css; charset=utf-8"],
  ...["app.js", "api.js", "dag.js", "config.js"].map((name) => [
    name,
    "text/javascript; charset=utf-8",
  ]),
]);
const hopHeaders = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailer", "transfer-encoding", "upgrade",
]);

function headersWithoutHop(headers) {
  const blocked = new Set(hopHeaders);
  for (const name of (headers.connection || "").split(","))
    blocked.add(name.trim().toLowerCase());
  return Object.fromEntries(
    Object.entries(headers).filter(([name]) => !blocked.has(name.toLowerCase())),
  );
}

export function createDevServer(apiUpstream = "http://127.0.0.1:8080") {
  const upstream = new URL(apiUpstream);
  if (
    !["http:", "https:"].includes(upstream.protocol) || upstream.username ||
    upstream.password || upstream.search || upstream.hash
  ) throw new Error("API upstream 必须是无凭据、查询参数和片段的 http(s) 地址");

  const server = http.createServer(async (request, response) => {
    response.setHeader("Cache-Control", "no-store");
    response.setHeader("X-Content-Type-Options", "nosniff");
    const path = request.url.split("?", 1)[0];
    if (path === "/api" || path.startsWith("/api/")) {
      const headers = headersWithoutHop(request.headers);
      headers.host = upstream.host;
      delete headers.origin;
      const transport = upstream.protocol === "https:" ? https : http;
      const proxy = transport.request({
        protocol: upstream.protocol,
        hostname: upstream.hostname.replace(/^\[|\]$/g, ""),
        port: upstream.port,
        path: upstream.pathname.replace(/\/$/, "") +
          (request.url.slice(4).startsWith("?") ? "/" + request.url.slice(4) : request.url.slice(4) || "/"),
        method: request.method,
        headers,
      }, (incoming) => {
        response.writeHead(incoming.statusCode, headersWithoutHop(incoming.headers));
        incoming.on("error", () => response.destroy());
        incoming.pipe(response);
      });
      proxy.setTimeout(path === "/api/v1/files" && request.method === "POST" ? 86400000 : 30000,
        () => proxy.destroy(new Error("API upstream timeout")));
      proxy.on("error", () => {
        if (response.headersSent) return response.destroy();
        response.writeHead(502, { "Content-Type": "application/json; charset=utf-8" });
        response.end(JSON.stringify({
          error: "开发代理无法连接管控 API：请启动管控服务，并检查 --api-upstream 地址。",
        }));
      });
      request.on("aborted", () => proxy.destroy());
      response.on("close", () => proxy.destroy());
      request.pipe(proxy);
      return;
    }
    if (!["GET", "HEAD"].includes(request.method)) {
      response.writeHead(405, { Allow: "GET, HEAD" });
      response.end("Method not allowed");
      return;
    }
    const name = path === "/" ? "index.html" : path.slice(1);
    if (!assets.has(name)) {
      response.writeHead(404);
      response.end("Not found");
      return;
    }
    try {
      const body = await readFile(new URL(name, import.meta.url));
      response.writeHead(200, {
        "Content-Type": assets.get(name),
        "Content-Length": body.length,
        "Referrer-Policy": "no-referrer",
      });
      response.end(request.method === "HEAD" ? undefined : body);
    } catch {
      response.writeHead(500);
      response.end("Cannot read frontend asset");
    }
  });
  // Upload deadlines belong to the control API; preserve its configurable total timeout.
  server.requestTimeout = 86400000;
  return server;
}

function main() {
  const { values } = parseArgs({ options: {
    host: { type: "string", default: process.env.DATAFLOW_DEV_HOST || "0.0.0.0" },
    port: { type: "string", default: process.env.DATAFLOW_DEV_PORT || "8000" },
    "api-upstream": {
      type: "string", default: process.env.DATAFLOW_API_UPSTREAM || "http://127.0.0.1:8080",
    },
    help: { type: "boolean", default: false },
  } });
  if (values.help) {
    console.log("npm run dev -- --host 0.0.0.0 --port 8000 --api-upstream http://127.0.0.1:8080");
    return;
  }
  const port = Number(values.port);
  if (!/^\d+$/.test(values.port) || port < 1 || port > 65535)
    throw new Error("--port 必须是 1..65535 的整数");
  const server = createDevServer(values["api-upstream"]);
  server.on("error", (error) => {
    console.error(error.code === "EADDRINUSE"
      ? `端口 ${port} 已被占用，请使用 --port 指定其他端口。`
      : `前端启动失败：${error.message}`);
    process.exitCode = 1;
  });
  server.listen(port, values.host, () => {
    const host = values.host === "0.0.0.0" ? "127.0.0.1" : values.host;
    console.log(`DataFlowCore 前端已启动：http://${host.includes(":") ? `[${host}]` : host}:${port}`);
    console.log(`监听地址：${values.host}:${port}；远程浏览器请使用服务器 IP。`);
    console.log(`API 代理：/api → ${values["api-upstream"]}`);
    console.log("在页面配置 API 连接时使用 /api 和管理令牌。按 Ctrl+C 停止。仅用于开发。 ");
  });
  for (const signal of ["SIGINT", "SIGTERM"])
    process.on(signal, () => {
      server.close();
      server.closeAllConnections();
    });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { main(); }
  catch (error) { console.error(`前端启动失败：${error.message}`); process.exitCode = 1; }
}
