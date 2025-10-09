import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';

process.env.NODE_ENV = 'test';
process.env.DB_PATH = ':memory:';
process.env.CK_REFUND_WINDOW_DAYS = '30';

const {
  app,
  applyLedger,
  __resetRefundRateLimiter
} = await import('../index.js');
import db from '../db.js';

function resetDatabase() {
  db.exec('PRAGMA foreign_keys = OFF;');
  db.exec(`
    DELETE FROM ledger;
    DELETE FROM ledger_tx;
    DELETE FROM ledger_postings;
    DELETE FROM balances;
    DELETE FROM accounts WHERE id != 'liability:CK';
    DELETE FROM consumed_tokens;
    DELETE FROM holds;
    DELETE FROM rewards;
  `);
  db.exec('PRAGMA foreign_keys = ON;');
  db.exec("INSERT OR IGNORE INTO accounts (id, owner_type, owner_id, token) VALUES ('liability:CK','program',NULL,'CK')");
  __resetRefundRateLimiter();
}

test('hold endpoints include state hints and honor admin gating', async (t) => {
  resetDatabase();
  applyLedger({ userId: 'kid', delta: 150, action: 'earn_seed', note: 'seed', actor: 'tester', verb: 'earn' });

  const rewardStmt = db.prepare('INSERT INTO rewards (name, price, description, image_url, active) VALUES (?, ?, ?, ?, ?)');
  const rewardResult = rewardStmt.run('Lego Set', 50, 'Blocks', '', 1);
  const rewardId = Number(rewardResult.lastInsertRowid);

  const server = app.listen(0);
  await once(server, 'listening');
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
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
  assert.deepEqual(unauthorizedBody, { error: 'UNAUTHORIZED' });

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
  assert.ok(['canceled', 'none'].includes(cancelBody.hints.hold_status));
});
