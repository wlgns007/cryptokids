import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

process.env.NODE_ENV = 'test';
const TEST_DB = path.join(process.cwd(), 'data', `test-default-family-${randomUUID()}.db`);
process.env.DB_PATH = TEST_DB;
process.env.MASTER_ADMIN_KEY = 'Murasaki';

const fetch = globalThis.fetch;

const { app } = await import('../index.js');
import db from '../db.js';

const DEFAULT_FAMILY_ID = 'default';

function insertFamily({ id = `fam-${randomUUID()}`, name = 'Family', adminKey = `Key-${randomUUID()}` } = {}) {
  const now = new Date().toISOString();
  db.prepare(
    `INSERT INTO family (id, name, email, admin_key, status, created_at, updated_at)
     VALUES (?, ?, NULL, ?, 'active', ?, ?)`
  ).run(id, name, adminKey, now, now);
  return { id, name, adminKey };
}

async function withServer(t, handler) {
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() =>
    new Promise((resolve) => {
      server.close(resolve);
    })
  );
  const { port } = server.address();
  const baseUrl = `http://127.0.0.1:${port}`;
  await handler(baseUrl);
}

test.after(() => {
  fs.rmSync(TEST_DB, { force: true });
});

function listHasDefault(families) {
  return families.some((family) => (family?.id || '').toLowerCase() === DEFAULT_FAMILY_ID);
}

test('default family is hidden from listings and rejected by scope', async (t) => {
  const otherFamily = insertFamily({ id: 'FamOne', name: 'Fam One', adminKey: 'FamOneKey' });

  await withServer(t, async (baseUrl) => {
    const masterHeaders = { 'X-ADMIN-KEY': 'Murasaki' };

    const listRes = await fetch(new URL('/api/admin/families', baseUrl), { headers: masterHeaders });
    assert.equal(listRes.status, 200);
    const families = await listRes.json();
    assert.ok(Array.isArray(families), 'families payload should be an array');
    assert.equal(listHasDefault(families), false, 'default family should not appear in listings');
    assert.ok(
      families.some((family) => family.id === otherFamily.id),
      'non-default families should still appear'
    );

    const defaultRes = await fetch(new URL('/api/admin/families?id=default', baseUrl), { headers: masterHeaders });
    assert.equal(defaultRes.status, 400);
    const defaultBody = await defaultRes.json();
    assert.deepEqual(defaultBody, { error: 'default family is reserved' });

    const scopedRes = await fetch(new URL('/api/admin/members', baseUrl), {
      headers: { ...masterHeaders, 'X-Act-As-Family': 'default' }
    });
    assert.equal(scopedRes.status, 400);
    const scopedBody = await scopedRes.json();
    assert.deepEqual(scopedBody, { error: 'default family is reserved' });
  });
});
