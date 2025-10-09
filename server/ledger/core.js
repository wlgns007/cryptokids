// server/ledger/core.js  — CK-only core (modular, idempotent)
import crypto from "node:crypto";
import db from "../db.js";

export const TOKEN = "CK";
export const memberAccount = id => `member:${id}:${TOKEN}`;
const LIABILITY = `liability:${TOKEN}`;

export function ensureMemberAccount(memberId) {
  const id = memberAccount(memberId);
  db.run("INSERT OR IGNORE INTO accounts (id, owner_type, owner_id, token) VALUES (?,?,?,?)", [
    id,
    "member",
    memberId,
    TOKEN
  ]);
  return id;
}

export function postTx({ memo, sourceType, sourceId, postings, idempotencyKey }) {
  if (!Array.isArray(postings) || postings.length < 2) throw new Error("need >=2 postings");
  const sum = postings.reduce((s, p) => s + (p.delta | 0), 0);
  if (sum !== 0) throw new Error("not zero-sum");

  const key =
    idempotencyKey ||
    crypto
      .createHash("sha256")
      .update(JSON.stringify({ TOKEN, memo, sourceType, sourceId, postings }))
      .digest("hex");

  const dup = db.get("SELECT id FROM ledger_tx WHERE idempotency_key=?", [key]);
  if (dup) return dup.id;

  const runTx = db.transaction(() => {
    const txId = crypto.randomUUID();
    db.prepare(
      "INSERT INTO ledger_tx (id, token, memo, source_type, source_id, idempotency_key) VALUES (?,?,?,?,?,?)"
    ).run(txId, TOKEN, memo || null, sourceType || null, sourceId || null, key);
    const stmt = db.prepare("INSERT INTO ledger_postings (tx_id, account_id, delta) VALUES (?,?,?)");
    try {
      for (const p of postings) {
        stmt.run(txId, p.accountId, p.delta | 0);
      }
    } finally {
      stmt.free?.();
    }
    return txId;
  });

  return runTx();
}

export function earn({ memberId, amount, reason, sourceId, idempotencyKey }) {
  const delta = Number(amount) | 0;
  if (!Number.isInteger(delta) || delta <= 0) throw new Error("amount must be positive integer");
  const acct = ensureMemberAccount(memberId);
  return postTx({
    memo: reason || "earn",
    sourceType: "earn",
    sourceId,
    idempotencyKey,
    postings: [
      { accountId: acct, delta: +delta },
      { accountId: LIABILITY, delta: -delta }
    ]
  });
}

export function redeem({ memberId, amount, rewardId, idempotencyKey }) {
  const delta = Number(amount) | 0;
  if (!Number.isInteger(delta) || delta <= 0) throw new Error("amount must be positive integer");
  const acct = ensureMemberAccount(memberId);
  return postTx({
    memo: "redeem",
    sourceType: "reward_redeem",
    sourceId: rewardId,
    idempotencyKey,
    postings: [
      { accountId: acct, delta: -delta },
      { accountId: LIABILITY, delta: +delta }
    ]
  });
}

export function balanceOf(memberId) {
  const row = db.get("SELECT balance FROM account_balances WHERE account_id=?", [memberAccount(memberId)]);
  return row?.balance ?? 0;
}
