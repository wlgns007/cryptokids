const METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]; 

function mergeHeaders(defaults, overrides) {
  const merged = new Headers();
  for (const [key, value] of Object.entries(defaults || {})) {
    if (value === undefined || value === null) continue;
    merged.set(key, value);
  }
  for (const [key, value] of Object.entries(overrides || {})) {
    if (value === undefined || value === null) continue;
    merged.set(key, value);
  }
  return merged;
}

async function parseBody(response) {
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    const text = await response.text();
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch (error) {
      const parseError = new Error("Failed to parse JSON response");
      parseError.status = response.status;
      parseError.cause = error;
      throw parseError;
    }
  }
  return await response.text();
}

async function request(baseURL, defaultHeaders, method, path, { headers, body } = {}) {
  if (!METHODS.includes(method.toUpperCase())) {
    throw new Error(`Unsupported HTTP method: ${method}`);
  }
  const url = new URL(path, baseURL).toString();
  const init = { method, headers: mergeHeaders(defaultHeaders, headers) };
  if (body !== undefined) {
    init.body = typeof body === "string" ? body : JSON.stringify(body);
    if (!init.headers.has("content-type")) {
      init.headers.set("content-type", "application/json");
    }
  }

  const response = await fetch(url, init);
  const parsed = await parseBody(response);
  if (!response.ok) {
    const error = new Error(`Request failed: ${response.status} ${response.statusText}`);
    error.status = response.status;
    error.body = parsed;
    throw error;
  }
  return {
    status: response.status,
    headers: response.headers,
    body: parsed,
  };
}

export function createHttpClient({ baseURL, headers } = {}) {
  if (!baseURL) {
    throw new Error("baseURL is required to create an HTTP client");
  }
  const client = {};
  for (const method of METHODS) {
    const lower = method.toLowerCase();
    client[lower] = (path, options = {}) => request(baseURL, headers, method, path, options);
  }
  return client;
}
