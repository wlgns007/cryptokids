import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

export const DATA_DIR = path.resolve(process.cwd(), "data");
fs.mkdirSync(DATA_DIR, { recursive: true });

export const DB_PATH = process.env.DB_PATH || path.join(DATA_DIR, "cryptokids.db");

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

function execWithParams(stmt, params) {
  if (params === undefined) {
    return stmt.run();
  }
  if (Array.isArray(params)) {
    return stmt.run(...params);
  }
  return stmt.run(params);
}

function getWithParams(stmt, params) {
  if (params === undefined) {
    return stmt.get();
  }
  if (Array.isArray(params)) {
    return stmt.get(...params);
  }
  return stmt.get(params);
}

if (typeof db.run !== "function") {
  db.run = (sql, params) => execWithParams(db.prepare(sql), params);
}

if (typeof db.get !== "function") {
  db.get = (sql, params) => getWithParams(db.prepare(sql), params);
}

if (typeof db.all !== "function") {
  db.all = (sql, params) => {
    const stmt = db.prepare(sql);
    if (params === undefined) {
      return stmt.all();
    }
    if (Array.isArray(params)) {
      return stmt.all(...params);
    }
    return stmt.all(params);
  };
}

// MIGRATION: ledger core (CK-only)
db.exec(`
CREATE TABLE IF NOT EXISTS accounts (
  id TEXT PRIMARY KEY,
  owner_type TEXT NOT NULL,      -- "member" | "program"
  owner_id TEXT,
  token TEXT NOT NULL,           -- "CK"
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS ledger_tx (
  id TEXT PRIMARY KEY,
  token TEXT NOT NULL,
  memo TEXT,
  source_type TEXT,
  source_id TEXT,
  idempotency_key TEXT UNIQUE,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS ledger_postings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tx_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  delta INTEGER NOT NULL,        -- signed integer units
  FOREIGN KEY (tx_id) REFERENCES ledger_tx(id),
  FOREIGN KEY (account_id) REFERENCES accounts(id)
);
CREATE VIEW IF NOT EXISTS account_balances AS
  SELECT account_id, SUM(delta) AS balance
  FROM ledger_postings GROUP BY account_id;

INSERT OR IGNORE INTO accounts (id, owner_type, owner_id, token)
VALUES ('liability:CK','program',NULL,'CK');

CREATE INDEX IF NOT EXISTS idx_postings_account ON ledger_postings(account_id);
CREATE INDEX IF NOT EXISTS idx_tx_token ON ledger_tx(token);
`);

export default db;
