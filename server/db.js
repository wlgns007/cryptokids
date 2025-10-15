import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

export const DATA_DIR = path.resolve(process.cwd(), "data");
fs.mkdirSync(DATA_DIR, { recursive: true });

export const DB_PATH = process.env.DB_PATH || path.join(DATA_DIR, "cryptokids.db");

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

const schemaStatements = [
  `CREATE TABLE IF NOT EXISTS family (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS admin_key (
    id TEXT PRIMARY KEY,
    key_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('master','family_admin')),
    family_id TEXT,
    label TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (family_id) REFERENCES family(id)
  )`,
  "CREATE INDEX IF NOT EXISTS idx_admin_key_role ON admin_key(role)",
  "CREATE INDEX IF NOT EXISTS idx_admin_key_family ON admin_key(family_id)"
];

for (const statement of schemaStatements) {
  db.exec(statement);
}

function columnInfo(table) {
  return db.prepare(`PRAGMA table_info(${table})`).all();
}

function tableHasColumn(table, column) {
  return columnInfo(table).some((row) => row.name === column);
}

function ensureFamilyColumn(table) {
  const columns = columnInfo(table);
  if (columns.length === 0) {
    return;
  }
  const hasFamilyColumn = columns.some((row) => row.name === "family_id");
  if (!hasFamilyColumn) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN family_id TEXT`);
  }
  db.prepare(`UPDATE ${table} SET family_id = @family WHERE family_id IS NULL`).run({ family: "default" });
}

function enforceFamilyNotNull(table) {
  const columns = columnInfo(table);
  if (columns.length === 0) {
    return;
  }
  const familyColumn = columns.find((row) => row.name === "family_id");
  if (!familyColumn) {
    return;
  }
  if (familyColumn.notnull === 1) {
    return;
  }

  const hasNulls = db
    .prepare(`SELECT 1 AS found FROM ${table} WHERE family_id IS NULL LIMIT 1`)
    .get();
  if (hasNulls) {
    throw new Error(
      `Cannot enforce NOT NULL on ${table}.family_id; NULL rows remain. ` +
        `Backfill a family_id (expected 'default') and retry.`
    );
  }

  const tableSqlRow = db
    .prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name = ?")
    .get(table);
  if (!tableSqlRow?.sql) {
    return;
  }

  const patterns = [
    /"family_id"\s+TEXT(?![^,]*NOT\s+NULL)/i,
    /`family_id`\s+TEXT(?![^,]*NOT\s+NULL)/i,
    /\[family_id\]\s+TEXT(?![^,]*NOT\s+NULL)/i,
    /\bfamily_id\s+TEXT(?![^,]*NOT\s+NULL)/i
  ];

  let createSql = tableSqlRow.sql;
  let replaced = false;
  for (const pattern of patterns) {
    if (pattern.test(createSql)) {
      createSql = createSql.replace(pattern, (match) => `${match} NOT NULL`);
      replaced = true;
      break;
    }
  }

  if (!replaced) {
    throw new Error(`Unable to locate family_id column definition for table ${table}.`);
  }

  if (table === "ledger") {
    const tokenPattern = /(\"token\"|`token`|\[token\]|token)\s+TEXT\s+UNIQUE\s+NOT\s+NULL/gi;
    createSql = createSql.replace(tokenPattern, "$1 TEXT UNIQUE");
  }

  const existingObjects = db
    .prepare(
      "SELECT type, sql FROM sqlite_master WHERE tbl_name = ? AND type IN ('index','trigger') AND sql IS NOT NULL"
    )
    .all(table);

  const quote = (name) => `"${String(name).replace(/"/g, '""')}"`;
  const tempName = `${table}_new_${Date.now()}`;

  const createTablePattern = new RegExp(
    `CREATE TABLE\\s+(?:IF NOT EXISTS\\s+)?(?:"${table}"|\`${table}\`|\[${table}\]|${table})`,
    "i"
  );
  if (!createTablePattern.test(createSql)) {
    throw new Error(`Unable to rewrite CREATE TABLE statement for ${table}.`);
  }
  const tempCreateSql = createSql.replace(createTablePattern, `CREATE TABLE ${quote(tempName)}`);

  const columnList = columns.map((col) => quote(col.name)).join(", ");

  db.exec("PRAGMA foreign_keys = OFF");
  db.exec("BEGIN");
  try {
    db.exec(tempCreateSql);
    db.exec(
      `INSERT INTO ${quote(tempName)} (${columnList}) SELECT ${columnList} FROM ${quote(table)}`
    );
    db.exec(`DROP TABLE ${quote(table)}`);
    db.exec(`ALTER TABLE ${quote(tempName)} RENAME TO ${quote(table)}`);
    for (const { sql } of existingObjects) {
      db.exec(sql);
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    db.exec("PRAGMA foreign_keys = ON");
    throw err;
  }
  db.exec("PRAGMA foreign_keys = ON");
}

const scopedTables = ["member", "task", "reward", "ledger", "earn_templates"];
for (const table of scopedTables) {
  ensureFamilyColumn(table);
}

for (const table of scopedTables) {
  enforceFamilyNotNull(table);
}

const scopedIndexes = [
  { table: "member", statement: "CREATE INDEX IF NOT EXISTS idx_member_family ON member(family_id)" },
  { table: "task", statement: "CREATE INDEX IF NOT EXISTS idx_task_family ON task(family_id)" },
  { table: "reward", statement: "CREATE INDEX IF NOT EXISTS idx_reward_family ON reward(family_id)" },
  { table: "ledger", statement: "CREATE INDEX IF NOT EXISTS idx_ledger_family ON ledger(family_id, created_at)" },
  {
    table: "earn_templates",
    statement: "CREATE INDEX IF NOT EXISTS idx_earn_templates_family ON earn_templates(family_id)"
  }
];

for (const { table, statement } of scopedIndexes) {
  if (columnInfo(table).length === 0 || !tableHasColumn(table, "family_id")) {
    continue;
  }
  db.exec(statement);
}

if (tableHasColumn("member", "code")) {
  db.exec(
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_member_code_per_family ON member(family_id, code)"
  );
}

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

export default db;
