import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import request from 'supertest';

process.env.NODE_ENV = 'test';
const TEST_DB = path.join(process.cwd(), 'data', `test-admin-members-${randomUUID()}.db`);
process.env.DB_PATH = TEST_DB;
process.env.MASTER_ADMIN_KEY = 'Murasaki';

const { app } = await import('../index.js');
import db from '../db.js';

const NOW_ISO = new Date().toISOString();

function ensureKidsTable() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS kids (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      addr TEXT,
      pk TEXT,
      family_id TEXT
    );
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_kids_family_id ON kids(family_id)');
}

function resetDatabase() {
  db.exec(`
    DELETE FROM kids;
    DELETE FROM family_admin;
    DELETE FROM family;
    DELETE FROM master_admin;
  `);
}

function upsertFamily({ id, name, adminKey, email = `${id}@example.com`, status = 'active' }) {
  db.prepare(
    `INSERT INTO family (id, name, email, status, admin_key, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET
       name = excluded.name,
       email = excluded.email,
       status = excluded.status,
       admin_key = excluded.admin_key,
       updated_at = excluded.updated_at`
  ).run(id, name, email, status, adminKey, NOW_ISO, NOW_ISO);

  db.prepare(
    `INSERT INTO family_admin (id, family_id, admin_key, family_role, created_at, updated_at)
     VALUES (?, ?, ?, 'owner', ?, ?)
     ON CONFLICT(family_id) DO UPDATE SET
       admin_key = excluded.admin_key,
       updated_at = excluded.updated_at`
  ).run(id, id, adminKey, NOW_ISO, NOW_ISO);
}

function insertKid({ id, familyId, name = 'Kid', addr = `${id}@wallet`, pk = `${id}-pk` }) {
  db.prepare(
    `INSERT INTO kids (id, name, addr, pk, family_id)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET
       name = excluded.name,
       addr = excluded.addr,
       pk = excluded.pk,
       family_id = excluded.family_id`
  ).run(id, name, addr, pk, familyId);
}

ensureKidsTable();

const families = {
  alpha: { id: 'fam-alpha', adminKey: 'alpha-key', name: 'Alpha Family' },
  beta: { id: 'fam-beta', adminKey: 'beta-key', name: 'Beta Family' },
};

function seedData() {
  resetDatabase();
  upsertFamily(families.alpha);
  upsertFamily(families.beta);
  db.prepare('INSERT OR IGNORE INTO master_admin (id, admin_key, created_at) VALUES (?, ?, ?)').run(
    'master-id',
    'Murasaki',
    NOW_ISO
  );
  insertKid({ id: 'kid-a', familyId: families.alpha.id, name: 'Alice' });
  insertKid({ id: 'kid-b', familyId: families.beta.id, name: 'Bob' });
}

seedData();

test.after(() => {
  try {
    fs.rmSync(TEST_DB, { force: true });
  } catch {
    // ignore cleanup errors
  }
});

test.beforeEach(() => {
  seedData();
});

test('master admin can list members for any family', async () => {
  const res = await request(app)
    .get(`/api/admin/families/${families.alpha.id}/members`)
    .set('x-admin-key', 'Murasaki');

  assert.equal(res.status, 200);
  assert.ok(Array.isArray(res.body?.members));
  assert.equal(res.body.members.length, 1);
  assert.equal(res.body.members[0].id, 'kid-a');
  assert.equal(res.body.members[0].family_id, families.alpha.id);
});

test('family admin can list members for their own family', async () => {
  const res = await request(app)
    .get(`/api/admin/families/${families.alpha.id}/members`)
    .set('x-admin-key', families.alpha.adminKey);

  assert.equal(res.status, 200);
  assert.equal(res.body.members.length, 1);
  assert.equal(res.body.members[0].id, 'kid-a');
});

test('family admin forbidden from other families', async () => {
  const res = await request(app)
    .get(`/api/admin/families/${families.beta.id}/members`)
    .set('x-admin-key', families.alpha.adminKey);

  assert.equal(res.status, 403);
  assert.deepEqual(res.body, { error: 'forbidden_family_scope' });
});

test('unknown admin key returns 401', async () => {
  const res = await request(app)
    .get(`/api/admin/families/${families.alpha.id}/members`)
    .set('x-admin-key', 'bad-key');

  assert.equal(res.status, 401);
  assert.deepEqual(res.body, { error: 'invalid_admin_key' });
});

test('missing family id returns 404', async () => {
  const res = await request(app)
    .get(`/api/admin/families/${randomUUID()}/members`)
    .set('x-admin-key', 'Murasaki');

  assert.equal(res.status, 404);
  assert.deepEqual(res.body, { error: 'family_not_found' });
});

test('unexpected database error surfaces as 500', async (t) => {
  const restore = mock.method(db, 'prepare', () => {
    throw new Error('boom');
  });
  t.after(() => restore.mock.restore());

  const res = await request(app)
    .get(`/api/admin/families/${families.alpha.id}/members`)
    .set('x-admin-key', 'Murasaki');

  assert.equal(res.status, 500);
  assert.deepEqual(res.body, { error: 'server_error', detail: 'members_query_failed' });
});
