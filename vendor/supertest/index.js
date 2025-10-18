import { once } from 'node:events';

function normalizeHeaders(headers) {
  const result = {};
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined || value === null) continue;
    result[key.toLowerCase()] = String(value);
  }
  return result;
}

class TestRequest {
  constructor(app, method, path) {
    this.app = app;
    this.method = method;
    this.path = path;
    this.headers = {};
    this.payload = null;
    this._promise = null;
  }

  set(name, value) {
    if (name) {
      this.headers[name.toLowerCase()] = value;
    }
    return this;
  }

  send(body) {
    if (body === undefined || body === null) {
      this.payload = null;
      return this;
    }
    if (typeof body === 'object') {
      this.payload = JSON.stringify(body);
      if (!this.headers['content-type']) {
        this.headers['content-type'] = 'application/json';
      }
    } else {
      this.payload = String(body);
    }
    return this;
  }

  async _execute() {
    if (this._promise) {
      return this._promise;
    }
    this._promise = (async () => {
      const server = this.app.listen(0, '127.0.0.1');
      try {
        await once(server, 'listening');
        const { port } = server.address();
        const url = new URL(this.path, `http://127.0.0.1:${port}`);
        const headers = normalizeHeaders(this.headers);
        const response = await fetch(url, {
          method: this.method,
          headers,
          body: this.payload,
        });
        const text = await response.text();
        const contentType = response.headers.get('content-type') || '';
        let body = text;
        if (contentType.includes('application/json')) {
          try {
            body = text.length ? JSON.parse(text) : null;
          } catch {
            body = null;
          }
        }
        const headerEntries = {};
        for (const [key, value] of response.headers.entries()) {
          headerEntries[key.toLowerCase()] = value;
        }
        return {
          status: response.status,
          ok: response.ok,
          headers: headerEntries,
          text,
          body,
        };
      } finally {
        server.close();
      }
    })();
    return this._promise;
  }

  then(onFulfilled, onRejected) {
    return this._execute().then(onFulfilled, onRejected);
  }

  catch(onRejected) {
    return this._execute().catch(onRejected);
  }

  async expect(status, body) {
    const res = await this._execute();
    if (res.status !== status) {
      throw new Error(`Expected status ${status} but received ${res.status}`);
    }
    if (body !== undefined) {
      if (typeof body === 'object') {
        const actual = res.body;
        const expected = body;
        if (actual === null || typeof actual !== 'object') {
          throw new Error('Expected JSON body for comparison');
        }
        const keys = Object.keys(expected);
        for (const key of keys) {
          if (JSON.stringify(actual[key]) !== JSON.stringify(expected[key])) {
            throw new Error(`Expected body field ${key} to match`);
          }
        }
      } else if (res.text !== String(body)) {
        throw new Error('Response text did not match expected value');
      }
    }
    return res;
  }
}

class TestAgent {
  constructor(app) {
    this.app = app;
  }

  get(path) {
    return new TestRequest(this.app, 'GET', path);
  }

  post(path) {
    return new TestRequest(this.app, 'POST', path);
  }

  put(path) {
    return new TestRequest(this.app, 'PUT', path);
  }
}

export default function request(app) {
  return new TestAgent(app);
}

export { request };
