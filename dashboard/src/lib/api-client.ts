const API_BASE = import.meta.env.VITE_API_BASE ?? "";

const CSRF_COOKIE = "flexiq_csrf";
const CSRF_HEADER = "X-CSRF-Token";

export class ApiError extends Error {
  readonly status: number;
  readonly body: unknown;

  constructor(message: string, status: number, body: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.body = body;
  }
}

type Query = Record<string, string | number | boolean | null | undefined>;

function buildUrl(path: string, params?: Query): string {
  const url = `${API_BASE}${path}`;
  if (!params) return url;
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined || value === "") continue;
    search.set(key, String(value));
  }
  const query = search.toString();
  return query ? `${url}?${query}` : url;
}

/**
 * Read a cookie by name from ``document.cookie``. Returns ``undefined`` if
 * the cookie isn't set or we're running in an environment without
 * ``document`` (e.g. unit tests via jsdom may omit it).
 */
export function readCookie(name: string): string | undefined {
  if (typeof document === "undefined") return undefined;
  for (const part of document.cookie.split(";")) {
    const [k, v] = part.trim().split("=");
    if (k === name) return v;
  }
  return undefined;
}

function withCsrf(headers: Record<string, string>): Record<string, string> {
  const csrf = readCookie(CSRF_COOKIE);
  return csrf ? { ...headers, [CSRF_HEADER]: csrf } : headers;
}

async function parse<T>(response: Response): Promise<T> {
  const contentType = response.headers.get("content-type") ?? "";
  const payload: unknown = contentType.includes("application/json")
    ? await response.json()
    : await response.text();

  if (!response.ok) {
    const message =
      typeof payload === "object" && payload && "error" in payload
        ? String((payload as { error: unknown }).error)
        : `Request failed with status ${response.status}`;
    throw new ApiError(message, response.status, payload);
  }
  return payload as T;
}

export interface RequestOptions {
  signal?: AbortSignal;
  params?: Query;
  headers?: Record<string, string>;
}

export const api = {
  async get<T>(path: string, options: RequestOptions = {}): Promise<T> {
    const response = await fetch(buildUrl(path, options.params), {
      method: "GET",
      signal: options.signal,
      credentials: "same-origin",
      headers: { Accept: "application/json", ...options.headers },
    });
    return parse<T>(response);
  },

  async post<T>(path: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    const response = await fetch(buildUrl(path, options.params), {
      method: "POST",
      signal: options.signal,
      credentials: "same-origin",
      headers: withCsrf({
        Accept: "application/json",
        ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
        ...options.headers,
      }),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    return parse<T>(response);
  },

  async put<T>(path: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    const response = await fetch(buildUrl(path, options.params), {
      method: "PUT",
      signal: options.signal,
      credentials: "same-origin",
      headers: withCsrf({
        Accept: "application/json",
        ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
        ...options.headers,
      }),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    return parse<T>(response);
  },

  async delete<T>(path: string, options: RequestOptions = {}): Promise<T> {
    const response = await fetch(buildUrl(path, options.params), {
      method: "DELETE",
      signal: options.signal,
      credentials: "same-origin",
      headers: withCsrf({ Accept: "application/json", ...options.headers }),
    });
    return parse<T>(response);
  },
};
