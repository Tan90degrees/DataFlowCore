export class APIError extends Error {
  constructor(message, status = 0) {
    super(message);
    this.status = status;
  }
}

export function normalizeBase(value) {
  const base = value.trim().replace(/\/+$/, "");
  if (base.startsWith("/") && !base.startsWith("//") && !/[?#]/.test(base))
    return base;
  const url = new URL(base);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error(
      "API 地址必须为 http(s) 地址或 /api 路径，不得含凭据或查询参数",
    );
  }
  return url.href.replace(/\/+$/, "");
}

export class API {
  constructor(base, token) {
    this.base = normalizeBase(base);
    this.token = token;
  }
  async request(path, { method = "GET", body, key } = {}) {
    const headers = { Authorization: `Bearer ${this.token}` };
    if (body !== undefined) headers["Content-Type"] = "application/json";
    if (key) headers["Idempotency-Key"] = key;
    let response;
    try {
      response = await fetch(this.base + path, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(10000),
        cache: "no-store",
        credentials: "omit",
        redirect: "error",
      });
    } catch {
      throw new APIError(
        "无法连接 API：请检查地址、网络、HTTPS 与跨域配置。操作可能已被服务器接收，可再次提交确认。",
      );
    }
    const result = await response.json().catch(() => ({}));
    if (!response.ok)
      throw new APIError(
        result.error || `HTTP ${response.status}`,
        response.status,
      );
    return result;
  }
  post(path, body = {}, key) {
    return this.request(path, { method: "POST", body, key });
  }
}
