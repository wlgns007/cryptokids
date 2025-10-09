import test from 'node:test';
import assert from 'node:assert/strict';

process.env.NODE_ENV = 'test';
process.env.DB_PATH = ':memory:';
process.env.CK_REFUND_WINDOW_DAYS = '30';

const {
  createRefundTransaction,
  applyLedger,
  getLedgerViewForUser,
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
  `);
  db.exec('PRAGMA foreign_keys = ON;');
  db.exec("INSERT OR IGNORE INTO accounts (id, owner_type, owner_id, token) VALUES ('liability:CK','program',NULL,'CK')");
  __resetRefundRateLimiter();
}

test('partial refunds and over-refund guard', () => {
  resetDatabase();
  applyLedger({ userId: 'kid', delta: 100, action: 'earn_seed', note: 'seed', actor: 'tester', verb: 'earn' });
  const redeem = applyLedger({
    userId: 'kid',
    delta: -60,
    action: 'spend_redeemed',
    note: 'Ice Cream',
    actor: 'admin',
    verb: 'redeem',
    returnRow: true
  });
  const redeemRow = redeem.row;

  const first = createRefundTransaction({
    userId: 'kid',
    redeemTxId: String(redeemRow.id),
    amount: 20,
    reason: 'duplicate',
    notes: 'partial refund',
    actorId: 'admin1'
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
    actorId: 'admin1'
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
        actorId: 'admin1'
      }),
    /REFUND_NOT_ALLOWED/
  );

  const view = getLedgerViewForUser('kid');
  assert.ok(Array.isArray(view.redeems));
  const firstRedeem = view.redeems.find((r) => String(r.id) === String(redeemRow.id));
  assert.ok(firstRedeem, 'redeem row should exist');
  assert.equal(firstRedeem.refund_status, 'refunded');
  assert.equal(firstRedeem.remaining_refundable, 0);
  assert.equal(firstRedeem.refunded_amount, 60);
});

test('rejects mismatched users and invalid parents', () => {
  resetDatabase();
  applyLedger({ userId: 'kid', delta: 80, action: 'earn_seed', note: 'seed', actor: 'tester', verb: 'earn' });
  const redeem = applyLedger({
    userId: 'kid',
    delta: -30,
    action: 'spend_redeemed',
    note: 'Toy',
    actor: 'admin',
    verb: 'redeem',
    returnRow: true
  });
  const redeemRow = redeem.row;

  assert.throws(
    () =>
      createRefundTransaction({
        userId: 'other',
        redeemTxId: String(redeemRow.id),
        amount: 5,
        reason: 'duplicate',
        actorId: 'admin2'
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
        actorId: 'admin2'
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
    returnRow: true
  }).row;

  assert.throws(
    () =>
      createRefundTransaction({
        userId: 'kid',
        redeemTxId: String(earnRow.id),
        amount: 5,
        reason: 'duplicate',
        actorId: 'admin2'
      }),
    /NOT_REDEEM_TX/
  );
});

test('idempotency returns existing refund details', () => {
  resetDatabase();
  applyLedger({ userId: 'kid', delta: 50, action: 'earn_seed', note: 'seed', actor: 'tester', verb: 'earn' });
  const redeem = applyLedger({
    userId: 'kid',
    delta: -20,
    action: 'spend_redeemed',
    note: 'Snack',
    actor: 'admin',
    verb: 'redeem',
    returnRow: true
  });
  const redeemRow = redeem.row;
  const key = 'test-idempotency';

  const first = createRefundTransaction({
    userId: 'kid',
    redeemTxId: String(redeemRow.id),
    amount: 10,
    reason: 'duplicate',
    actorId: 'admin1',
    idempotencyKey: key
  });
  assert.equal(first.remaining, 10);

  try {
    createRefundTransaction({
      userId: 'kid',
      redeemTxId: String(redeemRow.id),
      amount: 10,
      reason: 'duplicate',
      actorId: 'admin1',
      idempotencyKey: key
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
  applyLedger({ userId: 'kid', delta: 40, action: 'earn_seed', note: 'seed', actor: 'tester', verb: 'earn' });
  const redeem = applyLedger({
    userId: 'kid',
    delta: -20,
    action: 'spend_redeemed',
    note: 'Book',
    actor: 'admin',
    verb: 'redeem',
    returnRow: true
  });
  const redeemRow = redeem.row;
  const thirtyOneDays = 31 * 24 * 60 * 60 * 1000;
  const past = Date.now() - thirtyOneDays;
  db.prepare('UPDATE ledger SET at = ? WHERE id = ?').run(past, redeemRow.id);

  assert.throws(
    () =>
      createRefundTransaction({
        userId: 'kid',
        redeemTxId: String(redeemRow.id),
        amount: 5,
        reason: 'duplicate',
        actorId: 'admin3'
      }),
    /REFUND_WINDOW_EXPIRED/
  );
});
