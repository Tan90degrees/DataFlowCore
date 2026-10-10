export class APIError extends Error {
  constructor(message, status = 0) {
    super(message);
    this.status = status;
  }
}

// getRandomValues also works on remote HTTP development origins.
export function requestKey() {
  return Array.from(crypto.getRandomValues(new Uint8Array(16)),
    (value) => value.toString(16).padStart(2, "0")).join("");
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
  upload(file, { key, signal, onProgress = () => {}, timeout = 330000 }) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      const abort = () => xhr.abort();
      const finish = (error, value) => {
        signal?.removeEventListener("abort", abort);
        if (error) reject(error);
        else resolve(value);
      };
      xhr.upload.onprogress = (event) =>
        onProgress(event.lengthComputable && event.total > 0 ? event.loaded / event.total : 0);
      xhr.open("POST", this.base + "/v1/files?filename=" + encodeURIComponent(file.name));
      xhr.setRequestHeader("Authorization", `Bearer ${this.token}`);
      xhr.setRequestHeader("Content-Type", "application/octet-stream");
      xhr.setRequestHeader("Idempotency-Key", key);
      xhr.responseType = "json";
      xhr.timeout = timeout;
      xhr.withCredentials = false;
      xhr.onload = () => {
        const result = xhr.response;
        if (xhr.status < 200 || xhr.status >= 300)
          finish(new APIError(result?.error || `HTTP ${xhr.status}`, xhr.status));
        else if (!result?.input_path || !result?.input_sha256)
          finish(new APIError("上传响应无效，可重试确认文件是否已保存。"));
        else finish(null, result);
      };
      xhr.onerror = () => finish(new APIError("上传连接中断，可重试确认文件是否已保存。"));
      xhr.ontimeout = () => finish(new APIError("上传超时，可重试整个文件。"));
      xhr.onabort = () => finish(new APIError("上传已取消。", -1));
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) {
        finish(new APIError("上传已取消。", -1));
        return;
      }
      xhr.send(file);
    });
  }
}
