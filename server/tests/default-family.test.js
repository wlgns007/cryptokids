import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

import { getFirstActiveFamilyId, buildMasterCookie } from './testUtils.js';

process.env.NODE_ENV = 'test';
const TEST_DB = path.join(process.cwd(), 'data', `test-default-family-${randomUUID()}.db`);
process.env.DB_PATH = TEST_DB;
process.env.MASTER_ADMIN_KEY = 'Murasaki';

const fetch = globalThis.fetch;

const { app } = await import('../index.js');
import db from '../db.js';

const DEFAULT_FAMILY_ID = 'default';
const MASTER_COOKIE = buildMasterCookie();

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
    const listRes = await fetch(new URL('/api/admin/families', baseUrl), {
      headers: { cookie: MASTER_COOKIE },
    });
    assert.equal(listRes.status, 200);
    const families = await listRes.json();
    assert.ok(Array.isArray(families), 'families payload should be an array');
    assert.equal(listHasDefault(families), false, 'default family should not appear in listings');
    assert.ok(
      families.some((family) => family.id === otherFamily.id),
      'non-default families should still appear'
    );

    const defaultRes = await fetch(new URL('/api/admin/families?id=default', baseUrl), {
      headers: { cookie: MASTER_COOKIE },
    });
    assert.equal(defaultRes.status, 400);
    const defaultBody = await defaultRes.json();
    assert.deepEqual(defaultBody, { error: 'default family is reserved' });
  });
});

test('scoped endpoints require family scope', async (t) => {
  insertFamily({ name: 'Scoped Fam', adminKey: 'ScopedKey' });

  await withServer(t, async (baseUrl) => {
    const res1 = await fetch(new URL('/api/admin/members', baseUrl), {
      headers: { cookie: MASTER_COOKIE },
    });
    assert.equal(res1.status, 400);
    const body1 = await res1.json();
    assert.equal(body1.error, 'Missing family scope (x-family)');

    const famId = await getFirstActiveFamilyId(baseUrl, process.env.MASTER_ADMIN_KEY);
    const res2 = await fetch(new URL('/api/admin/members', baseUrl), {
      headers: {
        cookie: MASTER_COOKIE,
        'x-family': famId,
      },
    });
    assert.equal(res2.status, 200);
    const rows = await res2.json();
    assert.ok(Array.isArray(rows));
  });
});

test('activity returns empty list for now', async (t) => {
  insertFamily({ name: 'Activity Fam', adminKey: 'ActivityKey' });

  await withServer(t, async (baseUrl) => {
    const famId = await getFirstActiveFamilyId(baseUrl, process.env.MASTER_ADMIN_KEY);
    const res = await fetch(new URL('/api/admin/activity?limit=50', baseUrl), {
      headers: {
        cookie: MASTER_COOKIE,
        'x-family': famId,
      },
    });
    assert.equal(res.status, 200);
    const rows = await res.json();
    assert.deepEqual(rows, []);
  });
});
