// Lightweight FindLabs API client with retries and rate limiting
// Uses env vars:
// - FINDLABS_API_BASE: base URL (default https://api.find.xyz)
// - FINDLABS_API_KEY:  API key/secret (not logged)
// - FINDLABS_API_KEY_HEADER: header name for the key (default Authorization)
// - FINDLABS_RATE_LIMIT_RPS: max requests per second (default 5)
// - FINDLABS_RETRY_LIMIT: max retry attempts on 429/5xx (default 3)
import fetch from "node-fetch";

const baseUrl = (process.env.FINDLABS_API_BASE || "https://api.find.xyz").replace(/\/$/, "");
const apiKey = process.env.FINDLABS_API_KEY;
const apiKeyHeader = process.env.FINDLABS_API_KEY_HEADER || "Authorization";
const requestsPerSecond = Number(process.env.FINDLABS_RATE_LIMIT_RPS || 5);
const retryLimit = Number(process.env.FINDLABS_RETRY_LIMIT || 3);
const minIntervalMs = requestsPerSecond > 0 ? Math.ceil(1000 / requestsPerSecond) : 0;

let lastRequestAt = 0;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rateLimit() {
  if (!minIntervalMs) return;
  const now = Date.now();
  const waitFor = lastRequestAt + minIntervalMs - now;
  if (waitFor > 0) {
    await sleep(waitFor);
  }
  lastRequestAt = Date.now();
}

function buildHeaders(extraHeaders = {}) {
  const headers = {
    Accept: "application/json",
    ...extraHeaders
  };

  if (apiKey) {
    const headerName = apiKeyHeader;
    const isAuthHeader = headerName.toLowerCase() === "authorization";
    const value = isAuthHeader && !apiKey.toLowerCase().startsWith("bearer ")
      ? `Bearer ${apiKey}`
      : apiKey;
    headers[headerName] = value;
  }

  return headers;
}

export async function findlabsRequest(path, options = {}) {
  const {
    method = "GET",
    headers = {},
    query = undefined,
    body = undefined,
    timeoutMs = 15000
  } = options;

  if (!path) throw new Error("findlabsRequest: path is required");

  const search = query
    ? "?" +
      Object.entries(query)
        .filter(([, v]) => v !== undefined && v !== null)
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join("&")
    : "";

  const url = `${baseUrl}${path.startsWith("/") ? "" : "/"}${path}${search}`;

  const finalHeaders = buildHeaders(headers);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    let attempt = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      await rateLimit();
      attempt += 1;

      try {
        const response = await fetch(url, {
          method,
          headers: finalHeaders,
          body: body && typeof body === "object" && !(body instanceof Buffer)
            ? JSON.stringify(body)
            : body,
          signal: controller.signal
        });

        if (response.ok) {
          const text = await response.text();
          if (!text) return null;
          try {
            return JSON.parse(text);
          } catch {
            return text;
          }
        }

        const isRetriable = response.status === 429 || response.status >= 500;
        if (!isRetriable || attempt > retryLimit) {
          const errText = await response.text();
          const msg = `FindLabs request failed (${response.status} ${response.statusText}): ${errText}`;
          throw new Error(msg);
        }

        // Respect Retry-After when present
        const retryAfterHeader = response.headers.get("retry-after");
        const retryAfterMs = retryAfterHeader ? Number(retryAfterHeader) * 1000 : 0;
        const backoffMs = retryAfterMs || Math.min(15000, 500 * Math.pow(2, attempt - 1));
        await sleep(backoffMs);
      } catch (err) {
        const isAbort = err.name === "AbortError";
        const isNetwork = err.type === "system" || err.code === "ECONNRESET" || err.code === "ETIMEDOUT";
        if ((isAbort || isNetwork) && attempt <= retryLimit) {
          const backoffMs = Math.min(15000, 500 * Math.pow(2, attempt - 1));
          await sleep(backoffMs);
          continue;
        }
        throw err;
      }
    }
  } finally {
    clearTimeout(timer);
  }
}

export function makeFindlabsClient(defaults = {}) {
  return {
    async get(path, opts = {}) {
      return findlabsRequest(path, { ...defaults, ...opts, method: "GET" });
    },
    async post(path, body, opts = {}) {
      const headers = { "Content-Type": "application/json", ...(opts.headers || {}) };
      return findlabsRequest(path, { ...defaults, ...opts, method: "POST", headers, body });
    },
    async request(path, opts = {}) {
      return findlabsRequest(path, { ...defaults, ...opts });
    }
  };
}

// Default singleton client with env-based config
export const findlabsClient = makeFindlabsClient();

