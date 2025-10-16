import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

process.env.NODE_ENV = 'test';
const TEST_DB = path.join(process.cwd(), 'data', `test-admin-management-${randomUUID()}.db`);
process.env.DB_PATH = TEST_DB;
process.env.MASTER_ADMIN_KEY = 'Murasaki';

const fetch = globalThis.fetch;

const { app } = await import('../index.js');
import db from '../db.js';

function ensureDefaultFamily() {
  const now = new Date().toISOString();
  db.prepare(
    `INSERT INTO family (id, name, email, status, admin_key, created_at, updated_at)
     VALUES (?, ?, ?, 'system', ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET
       name = excluded.name,
       email = excluded.email,
       status = excluded.status,
       admin_key = excluded.admin_key,
       updated_at = excluded.updated_at`
  ).run('default', 'Master Templates', 'default@example.com', 'Murasaki', now, now);
}

function ensureSupportTables() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS holds (
      id TEXT PRIMARY KEY,
      family_id TEXT,
      reward_id TEXT,
      status TEXT,
      created_at INTEGER,
      updated_at INTEGER
    );
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_holds_family ON holds(family_id)');

  db.exec(`
    CREATE TABLE IF NOT EXISTS history (
      id TEXT PRIMARY KEY,
      family_id TEXT,
      description TEXT,
      created_at INTEGER
    );
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_history_family ON history(family_id)');
}

ensureSupportTables();
ensureDefaultFamily();

test.after(() => {
  fs.rmSync(TEST_DB, { force: true });
});

function resetTables() {
  db.exec(`
    DELETE FROM task;
    DELETE FROM reward;
    DELETE FROM member;
    DELETE FROM holds;
    DELETE FROM history;
    DELETE FROM master_task;
    DELETE FROM family WHERE id <> 'default';
  `);
  ensureDefaultFamily();
}

function createFamily({
  id = `fam-${randomUUID()}`,
  name = 'Test Family',
  adminKey = `Key-${randomUUID()}`,
  status = 'active',
  email = `${randomUUID()}@example.com`
} = {}) {
  const now = new Date().toISOString();
  db.prepare(
    `INSERT INTO family (id, name, email, status, admin_key, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`
  ).run(id, name, email, status, adminKey, now, now);
  return { id, name, adminKey };
}

function insertTask({
  id = `task-${randomUUID()}`,
  familyId,
  title = 'Chore',
  status = 'active',
  points = 10,
  masterTaskId = null,
  updated = Date.now(),
  created = Date.now()
}) {
  db.prepare(
    `INSERT INTO task (id, family_id, title, description, icon, points, status, master_task_id, created_at, updated_at)
     VALUES (?, ?, ?, '', NULL, ?, ?, ?, ?, ?)`
  ).run(id, familyId, title, points, status, masterTaskId, created, updated);
  return id;
}

function insertReward({
  id = `reward-${randomUUID()}`,
  familyId,
  name = 'Reward',
  cost = 5,
  status = 'active',
  created = Date.now()
}) {
  db.prepare(
    `INSERT INTO reward (id, family_id, name, cost, description, image_url, youtube_url, status, tags, campaign_id, source, created_at, updated_at)
     VALUES (?, ?, ?, ?, '', '', NULL, ?, NULL, NULL, NULL, ?, ?)`
  ).run(id, familyId, name, cost, status, created, created);
  return id;
}

function insertMember({
  id = `kid-${randomUUID()}`,
  familyId,
  name = 'Kid Tester',
  status = 'active',
  created = Date.now()
}) {
  db.prepare(
    `INSERT INTO member (id, family_id, name, status, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?)`
  ).run(id, familyId, name, status, created, created);
  return id;
}

function withServer(t, handler) {
  const server = app.listen(0, '127.0.0.1');
  return once(server, 'listening').then(() => {
    t.after(() =>
      new Promise((resolve) => {
        server.close(resolve);
      })
    );
    const { port } = server.address();
    const baseUrl = `http://127.0.0.1:${port}`;
    return handler(baseUrl);
  });
}

test.beforeEach(() => {
  resetTables();
});

const MASTER_HEADERS = Object.freeze({ 'X-ADMIN-KEY': 'Murasaki' });

test('admin earn templates respects active and inactive modes', async (t) => {
  const family = createFamily();
  const masterTaskId = `mt-${randomUUID()}`;
  const timestamp = Date.now();
  db.prepare(
    `INSERT INTO master_task (id, title, description, base_points, icon, youtube_url, status, created_at, updated_at)
     VALUES (?, ?, '', 25, NULL, NULL, 'active', ?, ?)`
  ).run(masterTaskId, 'Template Row', timestamp, timestamp);

  const activeId = insertTask({ familyId: family.id, title: 'Make Bed', status: 'active', points: 15, masterTaskId, created: timestamp, updated: timestamp });
  insertTask({ familyId: family.id, title: 'Dishes', status: 'inactive', points: 5, created: timestamp - 5000, updated: timestamp - 5000 });

  await withServer(t, async (baseUrl) => {
    const qsActive = new URLSearchParams({ family_id: family.id, mode: 'active' }).toString();
    const activeRes = await fetch(`${baseUrl}/api/admin/earn-templates?${qsActive}`, {
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': family.id }
    });
    assert.equal(activeRes.status, 200, 'active request should succeed');
    const activeRows = await activeRes.json();
    assert.ok(Array.isArray(activeRows));
    assert.equal(activeRows.length, 1);
    assert.equal(activeRows[0].id, activeId);
    assert.equal(activeRows[0].status, 'active');
    assert.equal(activeRows[0].master_task_id, masterTaskId);

    const qsInactive = new URLSearchParams({ family_id: family.id, mode: 'inactive' }).toString();
    const inactiveRes = await fetch(`${baseUrl}/api/admin/earn-templates?${qsInactive}`, {
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': family.id }
    });
    assert.equal(inactiveRes.status, 200, 'inactive request should succeed');
    const inactiveRows = await inactiveRes.json();
    assert.ok(Array.isArray(inactiveRows));
    assert.equal(inactiveRows.length, 1);
    assert.equal(inactiveRows[0].status, 'inactive');
  });
});

test('DELETE /api/tasks/:id removes scoped task only', async (t) => {
  const familyA = createFamily({ id: 'fam-a', name: 'Family A', adminKey: 'Key-A' });
  const familyB = createFamily({ id: 'fam-b', name: 'Family B', adminKey: 'Key-B' });
  const taskA = insertTask({ familyId: familyA.id, title: 'Laundry' });
  const taskB = insertTask({ familyId: familyB.id, title: 'Trash' });

  await withServer(t, async (baseUrl) => {
    const deleteWrong = await fetch(`${baseUrl}/api/tasks/${taskB}`, {
      method: 'DELETE',
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': familyA.id }
    });
    assert.equal(deleteWrong.status, 404, 'task outside scope should not delete');

    const deleteRight = await fetch(`${baseUrl}/api/tasks/${taskA}`, {
      method: 'DELETE',
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': familyA.id }
    });
    assert.equal(deleteRight.status, 200);
    const body = await deleteRight.json();
    assert.deepEqual(body, { ok: true });
    const remainingA = db.prepare('SELECT id FROM task WHERE id = ?').get(taskA);
    assert.equal(remainingA, undefined);

    const deleteOther = await fetch(`${baseUrl}/api/tasks/${taskB}`, {
      method: 'DELETE',
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': familyB.id }
    });
    assert.equal(deleteOther.status, 200);
  });

  const remainingB = db.prepare('SELECT id FROM task WHERE id = ?').get(taskB);
  assert.equal(remainingB, undefined);
});

test('DELETE /api/families/:id cascades dependents', async (t) => {
  const family = createFamily({ id: 'fam-del', name: 'Delete Me', adminKey: 'Key-Del' });
  const memberId = insertMember({ familyId: family.id, name: 'Kiddo' });
  const taskId = insertTask({ familyId: family.id, title: 'Homework' });
  const rewardId = insertReward({ familyId: family.id, name: 'Ice Cream' });
  const now = Date.now();
  db.prepare(`INSERT INTO holds (id, family_id, reward_id, status, created_at, updated_at) VALUES (?, ?, ?, 'pending', ?, ?)`)
    .run(`hold-${randomUUID()}`, family.id, rewardId, now, now);
  db.prepare(`INSERT INTO history (id, family_id, description, created_at) VALUES (?, ?, ?, ?)`)
    .run(`hist-${randomUUID()}`, family.id, 'Earned points', now);

  await withServer(t, async (baseUrl) => {
    const res = await fetch(`${baseUrl}/api/families/${family.id}`, {
      method: 'DELETE',
      headers: MASTER_HEADERS
    });
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.deepEqual(body, { ok: true });
  });

  assert.equal(db.prepare('SELECT id FROM family WHERE id = ?').get(family.id), undefined);
  assert.equal(db.prepare('SELECT id FROM member WHERE id = ?').get(memberId), undefined);
  assert.equal(db.prepare('SELECT id FROM task WHERE id = ?').get(taskId), undefined);
  assert.equal(db.prepare('SELECT id FROM reward WHERE id = ?').get(rewardId), undefined);
  const holdRow = db.prepare('SELECT id FROM holds WHERE family_id = ?').get(family.id);
  assert.equal(holdRow, undefined);
  const historyRow = db.prepare('SELECT id FROM history WHERE family_id = ?').get(family.id);
  assert.equal(historyRow, undefined);
});
