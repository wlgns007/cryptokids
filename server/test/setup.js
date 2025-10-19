import fs from 'node:fs';
import path from 'node:path';
import { once } from 'node:events';
import { createHttpClient } from './helpers/httpClient.js';

const TMP_ROOT = path.join(process.cwd(), 'server', 'tmp');
fs.mkdirSync(TMP_ROOT, { recursive: true });

let contextPromise = null;
let contextValue = null;

async function bootstrap() {
  const tmpDir = fs.mkdtempSync(path.join(TMP_ROOT, 'db-'));
  const dbPath = path.join(tmpDir, 'test.db');

  process.env.NODE_ENV = 'test';
  process.env.DB_PATH = dbPath;
  if (!process.env.MASTER_ADMIN_KEY) {
    process.env.MASTER_ADMIN_KEY = 'Murasaki';
  }

  const { default: db, ensureKidsFamilyScope } = await import('../db.js');
  ensureKidsFamilyScope(db);
  const { app } = await import('../index.js');

  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  const baseURL = `http://127.0.0.1:${port}`;

  const close = async () => {
    await new Promise((resolve) => server.close(resolve));
    await fs.promises.rm(tmpDir, { recursive: true, force: true });
    contextPromise = null;
    contextValue = null;
  };

  const createClient = (headers = {}) => createHttpClient({ baseURL, headers });

  return {
    db,
    app,
    server,
    baseURL,
    createClient,
    dbPath,
    tmpDir,
    close,
  };
}

export async function createTestContext() {
  if (contextValue) {
    return contextValue;
  }
  if (!contextPromise) {
    contextPromise = bootstrap().then((ctx) => {
      contextValue = ctx;
      return ctx;
    });
  }
  return contextPromise;
}

export async function getActiveContext() {
  if (contextValue) {
    return contextValue;
  }
  if (contextPromise) {
    contextValue = await contextPromise;
    return contextValue;
  }
  return null;
}
