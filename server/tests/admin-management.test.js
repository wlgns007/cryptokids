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

  db.exec(`
    CREATE TABLE IF NOT EXISTS member_task (
      member_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      PRIMARY KEY (member_id, task_id)
    );
  `);
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
    DELETE FROM ledger;
    DELETE FROM member_task;
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
    const qsActive = new URLSearchParams({ familyId: family.id, status: 'active' }).toString();
    const activeRes = await fetch(`${baseUrl}/api/admin/list-earn-templates?${qsActive}`, {
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': family.id }
    });
    assert.equal(activeRes.status, 200, 'active request should succeed');
    const activeRows = await activeRes.json();
    assert.ok(Array.isArray(activeRows));
    assert.equal(activeRows.length, 1);
    assert.equal(activeRows[0].id, activeId);
    assert.equal(activeRows[0].status, 'active');
    assert.equal(activeRows[0].master_task_id, masterTaskId);

    const qsInactive = new URLSearchParams({ familyId: family.id, status: 'inactive' }).toString();
    const inactiveRes = await fetch(`${baseUrl}/api/admin/list-earn-templates?${qsInactive}`, {
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

test('DELETE /api/admin/families/:id?hard=true removes all scoped data', async (t) => {
  const family = createFamily({ id: 'fam-hard', name: 'Hard Delete', adminKey: 'Key-Hard' });
  const memberId = insertMember({ familyId: family.id, name: 'Hard Kid' });
  const taskId = insertTask({ familyId: family.id, title: 'Hard Task' });

  db.prepare(`INSERT INTO member_task (member_id, task_id) VALUES (?, ?)`)
    .run(memberId, taskId);

  const ledgerId = `ledger-${randomUUID()}`;
  const now = Date.now();
  db.prepare(
    `INSERT INTO ledger (id, user_id, verb, amount, balance_after, status, created_at, updated_at, family_id)
     VALUES (?, ?, 'earn', ?, ?, 'posted', ?, ?, ?)`
  ).run(ledgerId, memberId, 25, 25, now, now, family.id);

  await withServer(t, async (baseUrl) => {
    const res = await fetch(`${baseUrl}/api/admin/families/${family.id}?hard=true`, {
      method: 'DELETE',
      headers: MASTER_HEADERS
    });
    assert.equal(res.status, 200, 'hard delete should succeed');
    const body = await res.json();
    assert.deepEqual(body, {
      removed: {
        family: 1,
        members: 1,
        tasks: 1,
        ledger: 1,
        member_task: 1
      }
    });
  });

  const familyCount = db.prepare('SELECT COUNT(*) AS count FROM family WHERE id = ?').get(family.id).count;
  assert.equal(familyCount, 0, 'family row should be deleted');
  const memberCount = db.prepare('SELECT COUNT(*) AS count FROM member WHERE family_id = ?').get(family.id).count;
  assert.equal(memberCount, 0, 'members should be deleted');
  const taskCount = db.prepare('SELECT COUNT(*) AS count FROM task WHERE family_id = ?').get(family.id).count;
  assert.equal(taskCount, 0, 'tasks should be deleted');
  const ledgerColumns = db.prepare("PRAGMA table_info('ledger')").all().map((col) => col.name);
  const ledgerMemberColumn = ledgerColumns.includes('member_id') ? 'member_id' : 'user_id';
  const ledgerCount = db
    .prepare(
      `SELECT COUNT(*) AS count FROM ledger WHERE family_id = ? OR ${ledgerMemberColumn} IN (SELECT id FROM member WHERE family_id = ?)`
    )
    .get(family.id, family.id).count;
  assert.equal(ledgerCount, 0, 'ledger entries should be deleted');
  const memberTaskCols = db.prepare("PRAGMA table_info('member_task')").all().map((col) => col.name);
  const memberTaskMemberColumn = memberTaskCols.includes('member_id')
    ? 'member_id'
    : memberTaskCols.includes('memberId')
      ? 'memberId'
      : 'member_id';
  const memberTaskCount = db
    .prepare(`SELECT COUNT(*) AS count FROM member_task WHERE ${memberTaskMemberColumn} = ?`)
    .get(memberId).count;
  assert.equal(memberTaskCount, 0, 'member_task rows should be deleted');
});

test('adopting a master task surfaces in the active template list', async (t) => {
  const family = createFamily({ id: 'fam-adopt', name: 'Adopt Clan', adminKey: 'Key-Adopt' });
  const masterTaskId = `mt-${randomUUID()}`;
  const timestamp = Date.now();
  db.prepare(
    `INSERT INTO master_task (id, title, description, base_points, icon, youtube_url, status, created_at, updated_at)
     VALUES (?, ?, '', 15, NULL, NULL, 'active', ?, ?)`
  ).run(masterTaskId, 'Master Clean Room', timestamp, timestamp);

  await withServer(t, async (baseUrl) => {
    const adoptParams = new URLSearchParams({ familyId: family.id }).toString();
    const adoptRes = await fetch(`${baseUrl}/api/admin/templates/${masterTaskId}/adopt?${adoptParams}`, {
      method: 'POST',
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': family.id }
    });
    assert.ok([200, 201].includes(adoptRes.status), 'adoption should succeed');
    const adoptBody = await adoptRes.json();
    assert.ok(adoptBody.taskId, 'response should include taskId');

    const listParams = new URLSearchParams({ familyId: family.id, status: 'active' }).toString();
    const listRes = await fetch(`${baseUrl}/api/admin/list-earn-templates?${listParams}`, {
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': family.id }
    });
    assert.equal(listRes.status, 200, 'active listing should succeed');
    const rows = await listRes.json();
    assert.ok(Array.isArray(rows));
    assert.ok(rows.some((row) => row.id === adoptBody.taskId && row.status === 'active'));
  });
});

test('task status toggles between active and inactive lists', async (t) => {
  const family = createFamily({ id: 'fam-toggle', name: 'Toggle Crew', adminKey: 'Key-Toggle' });
  const taskId = insertTask({ familyId: family.id, title: 'Practice Piano', status: 'active' });

  await withServer(t, async (baseUrl) => {
    const patchParams = new URLSearchParams({ familyId: family.id }).toString();
    const deactivateRes = await fetch(`${baseUrl}/api/admin/tasks/${taskId}/deactivate?${patchParams}`, {
      method: 'PATCH',
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': family.id }
    });
    assert.equal(deactivateRes.status, 200, 'deactivate should succeed');

    const activeParams = new URLSearchParams({ familyId: family.id, status: 'active' }).toString();
    const activeRes = await fetch(`${baseUrl}/api/admin/list-earn-templates?${activeParams}`, {
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': family.id }
    });
    const activeRows = await activeRes.json();
    assert.ok(Array.isArray(activeRows));
    assert.equal(activeRows.length, 0, 'task should disappear from active list');

    const inactiveParams = new URLSearchParams({ familyId: family.id, status: 'inactive' }).toString();
    const inactiveRes = await fetch(`${baseUrl}/api/admin/list-earn-templates?${inactiveParams}`, {
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': family.id }
    });
    const inactiveRows = await inactiveRes.json();
    assert.ok(Array.isArray(inactiveRows));
    assert.equal(inactiveRows.length, 1, 'task should appear in inactive list');
    assert.equal(inactiveRows[0].id, taskId);
    assert.equal(inactiveRows[0].status, 'inactive');

    const reactivateRes = await fetch(`${baseUrl}/api/admin/tasks/${taskId}/reactivate?${patchParams}`, {
      method: 'PATCH',
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': family.id }
    });
    assert.equal(reactivateRes.status, 200, 'reactivate should succeed');

    const activeAfter = await fetch(`${baseUrl}/api/admin/list-earn-templates?${activeParams}`, {
      headers: { ...MASTER_HEADERS, 'X-Act-As-Family': family.id }
    });
    const activeAfterRows = await activeAfter.json();
    assert.ok(Array.isArray(activeAfterRows));
    assert.equal(activeAfterRows.length, 1, 'task should return to active list');
    assert.equal(activeAfterRows[0].id, taskId);
    assert.equal(activeAfterRows[0].status, 'active');
  });
});

test('GET /api/admin/families respects status filters', async (t) => {
  const activeFamily = createFamily({ id: 'fam-active', name: 'Active Fam', adminKey: 'Key-Active', status: 'active' });
  const inactiveFamily = createFamily({ id: 'fam-inactive', name: 'Inactive Fam', adminKey: 'Key-Inactive', status: 'inactive' });

  await withServer(t, async (baseUrl) => {
    const inactiveRes = await fetch(`${baseUrl}/api/admin/families?status=inactive`, {
      headers: MASTER_HEADERS
    });
    assert.equal(inactiveRes.status, 200, 'inactive query should succeed');
    const inactiveRows = await inactiveRes.json();
    assert.ok(Array.isArray(inactiveRows));
    assert.ok(inactiveRows.every((row) => row.status === 'inactive'));
    assert.ok(inactiveRows.some((row) => row.id === inactiveFamily.id));
    assert.ok(!inactiveRows.some((row) => row.id === activeFamily.id));

    const activeRes = await fetch(`${baseUrl}/api/admin/families?status=active`, {
      headers: MASTER_HEADERS
    });
    assert.equal(activeRes.status, 200, 'active query should succeed');
    const activeRows = await activeRes.json();
    assert.ok(Array.isArray(activeRows));
    assert.ok(activeRows.some((row) => row.id === activeFamily.id));
    assert.ok(!activeRows.some((row) => row.id === inactiveFamily.id));
  });
});

test('resolve-member handles zero, single, and multiple matches', async (t) => {
  const family = createFamily({ id: 'fam-resolve', name: 'Resolver Clan', adminKey: 'Key-Resolve' });
  const uniqueId = insertMember({ id: 'kid-unique', familyId: family.id, name: 'Unique Kid' });
  insertMember({ id: 'kid-alex-a', familyId: family.id, name: 'Alex Example' });
  insertMember({ id: 'kid-alex-b', familyId: family.id, name: 'Alex Example' });

  await withServer(t, async (baseUrl) => {
    const scopeParam = new URLSearchParams({ familyId: family.id }).toString();
    const noneRes = await fetch(`${baseUrl}/api/admin/resolve-member?q=Nope&${scopeParam}`, {
      headers: MASTER_HEADERS
    });
    assert.equal(noneRes.status, 200, 'missing members should still succeed');
    const none = await noneRes.json();
    assert.ok(Array.isArray(none));
    assert.equal(none.length, 0);

    const idRes = await fetch(`${baseUrl}/api/admin/resolve-member?q=${uniqueId}&${scopeParam}`, {
      headers: MASTER_HEADERS
    });
    assert.equal(idRes.status, 200, 'ID lookup should succeed');
    const idMatches = await idRes.json();
    assert.ok(Array.isArray(idMatches));
    assert.equal(idMatches.length, 1);
    assert.equal(idMatches[0].id, uniqueId);

    const manyRes = await fetch(`${baseUrl}/api/admin/resolve-member?q=${encodeURIComponent('Alex Example')}&${scopeParam}`, {
      headers: MASTER_HEADERS
    });
    assert.equal(manyRes.status, 200, 'name lookup should succeed');
    const manyMatches = await manyRes.json();
    assert.ok(Array.isArray(manyMatches));
    assert.equal(manyMatches.length, 2);
    assert.ok(manyMatches.every((entry) => entry.name === 'Alex Example'));
  });
});
