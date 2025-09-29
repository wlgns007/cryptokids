// db.js (ESM)
import Database from 'better-sqlite3';

export const db = new Database('parentshop.db');

db.exec(`
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  role TEXT CHECK(role IN ('parent','child')) NOT NULL DEFAULT 'child',
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  delta INTEGER NOT NULL,
  reason TEXT,
  ref TEXT,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS balances (
  user_id TEXT PRIMARY KEY,
  balance INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS qr_tokens (
  jti TEXT PRIMARY KEY,
  kind TEXT CHECK(kind IN ('earn','redeem','present')) NOT NULL,
  payload TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  used_at INTEGER
);
`);

export function credit(userId, amount, reason = '', ref = '') {
  const tx = db.transaction(() => {
    db.prepare(
      'INSERT INTO ledger (id,user_id,delta,reason,ref,created_at) VALUES (lower(hex(randomblob(16))),?,?,?,?,?)'
    ).run(userId, amount, reason, ref, Date.now());
    const row = db.prepare('SELECT balance FROM balances WHERE user_id=?').get(userId);
    if (!row) {
      db.prepare('INSERT INTO balances (user_id,balance) VALUES (?,?)').run(userId, amount);
    } else {
      db.prepare('UPDATE balances SET balance = balance + ? WHERE user_id=?').run(amount, userId);
    }
  });
  tx();
}

export function debit(userId, amount, reason = '', ref = '') {
  const bal = db.prepare('SELECT balance FROM balances WHERE user_id=?').get(userId)?.balance ?? 0;
  if (bal < amount) throw new Error('insufficient_balance');
  const tx = db.transaction(() => {
    db.prepare(
      'INSERT INTO ledger (id,user_id,delta,reason,ref,created_at) VALUES (lower(hex(randomblob(16))),?,?,?,?,?)'
    ).run(userId, -amount, reason, ref, Date.now());
    db.prepare('UPDATE balances SET balance = balance - ? WHERE user_id=?').run(amount, userId);
  });
  tx();
}
