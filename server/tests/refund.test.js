import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

process.env.NODE_ENV = 'test';
const TEST_DB = path.join(process.cwd(), 'data', `test-refund-${randomUUID()}.db`);
process.env.DB_PATH = TEST_DB;
process.env.CK_REFUND_WINDOW_DAYS = '30';
const DEFAULT_FAMILY_ID = 'default';

const {
  createRefundTransaction,
  applyLedger,
  getLedgerViewForUser,
  getStateHints,
  __resetRefundRateLimiter
} = await import('../index.js');
import db from '../db.js';

function ensureDefaultFamily() {
  const now = Date.now();
  db.prepare(
    "INSERT OR IGNORE INTO family (id, name, status, created_at, updated_at) VALUES (?, ?, 'system', ?, ?)"
  ).run(DEFAULT_FAMILY_ID, 'Master Templates', now, now);
  db.prepare("UPDATE family SET name = ?, status = 'system' WHERE id = ?").run('Master Templates', DEFAULT_FAMILY_ID);
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

test('partial refunds and over-refund guard', () => {
  resetDatabase();
  insertMember('kid', 'Kid Tester');
  applyLedger({
    userId: 'kid',
    delta: 100,
    action: 'earn_seed',
    note: 'seed',
    actor: 'tester',
    verb: 'earn',
    familyId: DEFAULT_FAMILY_ID
  });
  const redeem = applyLedger({
    userId: 'kid',
    delta: -60,
    action: 'spend_redeemed',
    note: 'Ice Cream',
    actor: 'admin',
    verb: 'redeem',
    returnRow: true,
    familyId: DEFAULT_FAMILY_ID
  });
  const redeemRow = redeem.row;

  const first = createRefundTransaction({
    userId: 'kid',
    redeemTxId: String(redeemRow.id),
    amount: 20,
    reason: 'duplicate',
    notes: 'partial refund',
    actorId: 'admin1',
    familyId: DEFAULT_FAMILY_ID
  });
  assert.equal(first.balance, 60);
  assert.equal(first.remaining, 40);
  assert.equal(first.refund.refund_reason, 'duplicate');
  assert.equal(first.refund.parent_tx_id, String(redeemRow.id));

  const second = createRefundTransaction({
    userId: 'kid',
    redeemTxId: String(redeemRow.id),
    amount: 40,
    reason: 'staff_error',
    notes: 'final make-good',
    actorId: 'admin1',
    familyId: DEFAULT_FAMILY_ID
  });
  assert.equal(second.balance, 100);
  assert.equal(second.remaining, 0);

  assert.throws(
    () =>
      createRefundTransaction({
        userId: 'kid',
        redeemTxId: String(redeemRow.id),
        amount: 1,
        reason: 'duplicate',
        actorId: 'admin1',
        familyId: DEFAULT_FAMILY_ID
      }),
    /REFUND_NOT_ALLOWED/
  );

  const view = getLedgerViewForUser('kid', DEFAULT_FAMILY_ID);
  assert.ok(Array.isArray(view.redeems));
  const firstRedeem = view.redeems.find((r) => String(r.id) === String(redeemRow.id));
  assert.ok(firstRedeem, 'redeem row should exist');
  assert.equal(firstRedeem.refund_status, 'refunded');
  assert.equal(firstRedeem.remaining_refundable, 0);
  assert.equal(firstRedeem.refunded_amount, 60);
});

test('rejects mismatched users and invalid parents', () => {
  resetDatabase();
  insertMember('kid', 'Kid Tester');
  insertMember('other', 'Other Tester');
  applyLedger({
    userId: 'kid',
    delta: 80,
    action: 'earn_seed',
    note: 'seed',
    actor: 'tester',
    verb: 'earn',
    familyId: DEFAULT_FAMILY_ID
  });
  const redeem = applyLedger({
    userId: 'kid',
    delta: -30,
    action: 'spend_redeemed',
    note: 'Toy',
    actor: 'admin',
    verb: 'redeem',
    returnRow: true,
    familyId: DEFAULT_FAMILY_ID
  });
  const redeemRow = redeem.row;

  assert.throws(
    () =>
      createRefundTransaction({
        userId: 'other',
        redeemTxId: String(redeemRow.id),
        amount: 5,
        reason: 'duplicate',
        actorId: 'admin2',
        familyId: DEFAULT_FAMILY_ID
      }),
    /USER_MISMATCH/
  );

  assert.throws(
    () =>
      createRefundTransaction({
        userId: 'kid',
        redeemTxId: 'non-existent',
        amount: 5,
        reason: 'duplicate',
        actorId: 'admin2',
        familyId: DEFAULT_FAMILY_ID
      }),
    /REDEEM_NOT_FOUND/
  );

  const earnRow = applyLedger({
    userId: 'kid',
    delta: 10,
    action: 'earn_bonus',
    note: 'bonus',
    actor: 'coach',
    verb: 'earn',
    returnRow: true,
    familyId: DEFAULT_FAMILY_ID
  }).row;

  assert.throws(
    () =>
      createRefundTransaction({
        userId: 'kid',
        redeemTxId: String(earnRow.id),
        amount: 5,
        reason: 'duplicate',
        actorId: 'admin2',
        familyId: DEFAULT_FAMILY_ID
      }),
    /NOT_REDEEM_TX/
  );
});

test('idempotency returns existing refund details', () => {
  resetDatabase();
  insertMember('kid', 'Kid Tester');
  applyLedger({
    userId: 'kid',
    delta: 50,
    action: 'earn_seed',
    note: 'seed',
    actor: 'tester',
    verb: 'earn',
    familyId: DEFAULT_FAMILY_ID
  });
  const redeem = applyLedger({
    userId: 'kid',
    delta: -20,
    action: 'spend_redeemed',
    note: 'Snack',
    actor: 'admin',
    verb: 'redeem',
    returnRow: true,
    familyId: DEFAULT_FAMILY_ID
  });
  const redeemRow = redeem.row;
  const key = 'test-idempotency';

  const first = createRefundTransaction({
    userId: 'kid',
    redeemTxId: String(redeemRow.id),
    amount: 10,
    reason: 'duplicate',
    actorId: 'admin1',
    idempotencyKey: key,
    familyId: DEFAULT_FAMILY_ID
  });
  assert.equal(first.remaining, 10);

  try {
    createRefundTransaction({
      userId: 'kid',
      redeemTxId: String(redeemRow.id),
      amount: 10,
      reason: 'duplicate',
      actorId: 'admin1',
      idempotencyKey: key,
      familyId: DEFAULT_FAMILY_ID
    });
    assert.fail('Expected idempotency conflict');
  } catch (err) {
    if (err.status !== undefined) {
      assert.equal(err.status, 409);
    }
    assert.match(err.message || '', /REFUND_EXISTS|IDEMPOTENCY_CONFLICT/);
    assert.ok(err.existing, 'existing refund should be returned');
    assert.equal(err.remaining, 10);
    assert.equal(err.existing.parent_tx_id, String(redeemRow.id));
  }
});

test('enforces refund window', () => {
  resetDatabase();
  insertMember('kid', 'Kid Tester');
  applyLedger({
    userId: 'kid',
    delta: 40,
    action: 'earn_seed',
    note: 'seed',
    actor: 'tester',
    verb: 'earn',
    familyId: DEFAULT_FAMILY_ID
  });
  const redeem = applyLedger({
    userId: 'kid',
    delta: -20,
    action: 'spend_redeemed',
    note: 'Book',
    actor: 'admin',
    verb: 'redeem',
    returnRow: true,
    familyId: DEFAULT_FAMILY_ID
  });
  const redeemRow = redeem.row;
  const thirtyOneDays = 31 * 24 * 60 * 60 * 1000;
  const past = Date.now() - thirtyOneDays;
  db.prepare('UPDATE ledger SET created_at = ?, updated_at = ? WHERE id = ?').run(past, past, redeemRow.id);

  assert.throws(
    () =>
      createRefundTransaction({
        userId: 'kid',
        redeemTxId: String(redeemRow.id),
        amount: 5,
        reason: 'duplicate',
        actorId: 'admin3',
        familyId: DEFAULT_FAMILY_ID
      }),
    /REFUND_WINDOW_EXPIRED/
  );
});

test('state hints surface refund capacity', () => {
  resetDatabase();
  insertMember('kid', 'Kid Tester');
  applyLedger({
    userId: 'kid',
    delta: 100,
    action: 'earn_seed',
    note: 'seed',
    actor: 'tester',
    verb: 'earn',
    familyId: DEFAULT_FAMILY_ID
  });
  const redeemResult = applyLedger({
    userId: 'kid',
    delta: -40,
    action: 'toy',
    note: 'Toy',
    actor: 'admin',
    verb: 'redeem',
    returnRow: true,
    familyId: DEFAULT_FAMILY_ID
  });
  const redeemRow = redeemResult.row;
  let hints = getStateHints('kid', DEFAULT_FAMILY_ID);
  assert.equal(hints.balance, 60);
  assert.equal(hints.can_refund, true);
  assert.equal(hints.max_refund, 40);

  createRefundTransaction({
    userId: 'kid',
    redeemTxId: String(redeemRow.id),
    amount: 10,
    reason: 'duplicate',
    actorId: 'admin1',
    familyId: DEFAULT_FAMILY_ID
  });

  hints = getStateHints('kid', DEFAULT_FAMILY_ID);
  assert.equal(hints.balance, 70);
  assert.equal(hints.max_refund, 30);
  assert.ok(Array.isArray(hints.refundable_redeems));
  const redeemHint = hints.refundable_redeems.find(r => r.redeemTxId === String(redeemRow.id));
  assert.ok(redeemHint, 'refund hint should include redeem');
  assert.equal(redeemHint.remaining, 30);
});
