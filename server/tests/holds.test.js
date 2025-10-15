import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

process.env.NODE_ENV = 'test';
const TEST_DB = path.join(process.cwd(), 'data', `test-holds-${randomUUID()}.db`);
process.env.DB_PATH = TEST_DB;
process.env.CK_REFUND_WINDOW_DAYS = '30';
const DEFAULT_FAMILY_ID = 'default';

const fetch = globalThis.fetch;

const {
  app,
  applyLedger,
  __resetRefundRateLimiter
} = await import('../index.js');
import db from '../db.js';

function ensureDefaultFamily(key = 'Mamapapa') {
  const now = new Date().toISOString();
  db.prepare(
    `INSERT INTO family (id, name, email, status, admin_key, created_at, updated_at)
     VALUES (?, ?, ?, 'active', ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET
       name = excluded.name,
       email = excluded.email,
       status = excluded.status,
       admin_key = excluded.admin_key,
       updated_at = excluded.updated_at`
  ).run(DEFAULT_FAMILY_ID, 'Default Family', 'default@example.com', key, now, now);
}

ensureDefaultFamily();

test.after(() => {
  fs.rmSync(TEST_DB, { force: true });
});

function resetDatabase() {
  db.exec('PRAGMA foreign_keys = OFF;');
  db.exec(`
    DELETE FROM ledger;
    DELETE FROM hold;
    DELETE FROM reward;
    DELETE FROM member;
    DELETE FROM spend_request;
    DELETE FROM consumed_tokens;
  `);
  db.exec('PRAGMA foreign_keys = ON;');
  __resetRefundRateLimiter();
  ensureDefaultFamily();
}

function insertMember(id, name = id, familyId = DEFAULT_FAMILY_ID) {
  const now = Date.now();
  db.prepare(
    "INSERT INTO member (id, family_id, name, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)\n       ON CONFLICT(id) DO UPDATE SET\n         family_id=excluded.family_id,\n         name=excluded.name,\n         status=excluded.status,\n         updated_at=excluded.updated_at"
  ).run(id, familyId, name, 'active', now, now);
}

test('hold endpoints include state hints and honor admin gating', async (t) => {
  resetDatabase();
  insertMember('kid', 'Kid Tester');
  applyLedger({
    userId: 'kid',
    delta: 150,
    action: 'earn_seed',
    note: 'seed',
    actor: 'tester',
    verb: 'earn',
    familyId: DEFAULT_FAMILY_ID
  });

  const rewardId = 'lego-set';
  const now = Date.now();
  db.prepare(
    'INSERT INTO reward (id, family_id, name, cost, description, image_url, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)' 
  ).run(rewardId, DEFAULT_FAMILY_ID, 'Lego Set', 50, 'Blocks', '', 'active', now, now);

  const server = app.listen(0);
  await once(server, 'listening');
  t.after(async () => {
    await new Promise(resolve => {
      server.close(resolve);
    });
  });

  const { port } = server.address();
  const base = `http://127.0.0.1:${port}`;

  const reserveRes = await fetch(`${base}/api/holds`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId: 'kid', itemId: rewardId })
  });
  assert.equal(reserveRes.status, 201);
  const reserveBody = await reserveRes.json();
  assert.equal(reserveBody.ok, true);
  assert.equal(reserveBody.verb, 'hold.reserve');
  assert.ok(reserveBody.txId, 'reserve response should include txId');
  assert.ok(reserveBody.hints, 'reserve response should include hints');
  assert.equal(reserveBody.balance, 150);
  assert.equal(reserveBody.hints.balance, 150);
  assert.equal(reserveBody.hints.pending_hold_count, 1);
  assert.equal(reserveBody.hints.active_hold_id, reserveBody.holdId);
  assert.equal(reserveBody.hints.hold_status, 'pending');
  assert.equal(reserveBody.hints.max_redeem_for_reward, 100);
  assert.equal(reserveBody.hints.features?.refunds, true);

  const approveRes = await fetch(`${base}/api/holds/${reserveBody.holdId}/approve`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-admin-key': 'Mamapapa'
    },
    body: JSON.stringify({ token: reserveBody.token })
  });
  assert.equal(approveRes.status, 200);
  const approveBody = await approveRes.json();
  assert.equal(approveBody.ok, true);
  assert.equal(approveBody.verb, 'hold.redeem');
  assert.equal(approveBody.balance, 100);
  assert.equal(approveBody.hints.balance, 100);
  assert.equal(approveBody.hints.can_refund, true);
  assert.equal(approveBody.hints.max_refund, 50);
  assert.ok(Array.isArray(approveBody.hints.refundable_redeems));
  const redeemHint = approveBody.hints.refundable_redeems.find((row) => row.redeemTxId === String(approveBody.txId));
  assert.ok(redeemHint, 'redeem should appear in refundable hints');
  assert.equal(redeemHint.remaining, 50);
  assert.notEqual(approveBody.hints.hold_status, 'pending');

  const secondReserveRes = await fetch(`${base}/api/holds`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId: 'kid', itemId: rewardId })
  });
  assert.equal(secondReserveRes.status, 201);
  const secondReserveBody = await secondReserveRes.json();

  const unauthorizedRes = await fetch(`${base}/api/holds/${secondReserveBody.holdId}/approve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token: secondReserveBody.token })
  });
  assert.equal(unauthorizedRes.status, 401);
  const unauthorizedBody = await unauthorizedRes.json();
  assert.deepEqual(unauthorizedBody, { error: 'missing admin key' });

  const cancelRes = await fetch(`${base}/api/holds/${secondReserveBody.holdId}/cancel`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-admin-key': 'Mamapapa'
    }
  });
  assert.equal(cancelRes.status, 200);
  const cancelBody = await cancelRes.json();
  assert.equal(cancelBody.ok, true);
  assert.equal(cancelBody.verb, 'hold.release');
  assert.equal(cancelBody.balance, 100);
  assert.equal(cancelBody.hints.balance, 100);
  assert.equal(cancelBody.hints.pending_hold_count, 0);
  assert.equal(cancelBody.hints.active_hold_id, null);
  assert.ok(['released', 'none'].includes(cancelBody.hints.hold_status));
});
