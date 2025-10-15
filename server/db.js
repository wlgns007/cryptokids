import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

// Quote SQLite identifiers safely (double-quote the whole thing)
function q(id) {
  return `"${String(id).replaceAll('"', '""')}"`;
}

export const DATA_DIR = path.resolve(process.cwd(), "data");
fs.mkdirSync(DATA_DIR, { recursive: true });

export const DB_PATH = process.env.DB_PATH || path.join(DATA_DIR, "cryptokids.db");

export const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

const schemaStatements = [
  `CREATE TABLE IF NOT EXISTS family (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    admin_key TEXT UNIQUE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`
];

for (const statement of schemaStatements) {
  db.exec(statement);
}

try {
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS "idx_family_admin_key" ON "family"(admin_key)');
} catch (err) {
  console.warn('[db] unable to ensure idx_family_admin_key', err?.message || err);
}

const familyColumns = db.prepare('PRAGMA table_info("family")').all().map((c) => c.name);
if (!familyColumns.includes('admin_key')) {
  db.exec('PRAGMA foreign_keys = OFF; BEGIN;');
  try {
    db.exec(`
      CREATE TABLE "family__new" (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        admin_key TEXT UNIQUE,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      INSERT INTO "family__new"(id, name, status, admin_key, created_at, updated_at)
      SELECT id, name, COALESCE(status, 'active'), NULL, created_at, updated_at FROM "family";
      DROP TABLE "family";
      ALTER TABLE "family__new" RENAME TO "family";
      CREATE UNIQUE INDEX IF NOT EXISTS "idx_family_admin_key" ON "family"(admin_key);
    `);
    db.exec('COMMIT; PRAGMA foreign_keys = ON;');
  } catch (e) {
    db.exec('ROLLBACK; PRAGMA foreign_keys = ON;');
    throw e;
  }
}

const ADMIN_KEY_ENV = process.env.ADMIN_KEY?.trim();
if (ADMIN_KEY_ENV) {
  try {
    db.prepare(`
      UPDATE "family"
         SET admin_key = COALESCE(admin_key, ?)
       WHERE id = 'default' AND admin_key IS NULL
    `).run(ADMIN_KEY_ENV);
  } catch (err) {
    console.warn('[db] unable to backfill default admin key', err?.message || err);
  }
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

function logInvalidFamilies(db) {
  try {
    const rows = db
      .prepare(`
      SELECT m.family_id, COUNT(*) AS cnt
      FROM "member" m
      LEFT JOIN "family" f ON f.id = m.family_id
      WHERE m.family_id IS NOT NULL AND f.id IS NULL
      GROUP BY m.family_id
    `)
      .all();
    if (rows.length) {
      console.warn("[db] Found members with invalid family_id:", rows);
    }
  } catch {}
}

function enforceFamilyNotNull(db) {
  // 0) Ensure family table + default family exists BEFORE touching member
  db.exec(`
    CREATE TABLE IF NOT EXISTS ${q("family")} (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      admin_key TEXT UNIQUE,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
  `);
  db.prepare(`
    INSERT INTO ${q("family")} (id, name, status, admin_key, created_at, updated_at)
    SELECT 'default', 'Default Family', 'active', NULL, datetime('now'), datetime('now')
    WHERE NOT EXISTS (SELECT 1 FROM ${q("family")} WHERE id = 'default')
  `).run();

  // 1) Detect if we even need to migrate
  const pragma = db.prepare(`PRAGMA table_info(${q("member")})`).all();
  if (pragma.length === 0) return;
  const hasFamilyId = pragma.some((c) => c.name === "family_id");
  const isFamilyNotNull = pragma.some((c) => c.name === "family_id" && c.notnull === 1);
  if (hasFamilyId && isFamilyNotNull) return;

  // 2) Optional: log invalid family_ids pre-migration (for observability)
  try {
    logInvalidFamilies?.(db);
  } catch {}

  // 3) Migration using recreate-table pattern.
  //    Turn off FK checks during the swap; we’ll end with a valid state.
  db.exec("PRAGMA foreign_keys = OFF;");
  db.exec("BEGIN");
  try {
    // New member schema (adjust columns if your project has more)
    db.exec(`
      CREATE TABLE ${q("member__new")} (
        id TEXT PRIMARY KEY,
        name TEXT,
        date_of_birth TEXT,
        sex TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        family_id TEXT NOT NULL DEFAULT 'default',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (family_id) REFERENCES ${q("family")}(id) ON DELETE RESTRICT
      );
    `);

    // Normalize family_id:
    //  - If old family_id is NULL or blank -> 'default'
    //  - If old family_id doesn't exist in family -> 'default'
    //  - Else keep old family_id
    db.exec(`
      INSERT INTO ${q("member__new")}
        (id, name, date_of_birth, sex, status, family_id, created_at, updated_at)
      SELECT
        m.id,
        m.name,
        m.date_of_birth,
        m.sex,
        COALESCE(m.status, 'active') AS status,
        COALESCE(
          NULLIF((
            SELECT f.id FROM ${q("family")} f WHERE f.id = m.family_id
          ), ''),
          'default'
        ) AS family_id,
        COALESCE(m.created_at, datetime('now')) AS created_at,
        COALESCE(m.updated_at, datetime('now')) AS updated_at
      FROM ${q("member")} m;
    `);

    // Swap tables
    db.exec(`DROP TABLE ${q("member")};`);
    db.exec(`ALTER TABLE ${q("member__new")} RENAME TO ${q("member")};`);

    // Indexes
    db.exec(`CREATE INDEX IF NOT EXISTS ${q("idx_member_family")} ON ${q("member")} (family_id);`);

    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  } finally {
    db.exec("PRAGMA foreign_keys = ON;");
  }
}

const scopedTables = ["member", "task", "reward", "ledger", "earn_templates"];
for (const table of scopedTables) {
  ensureFamilyColumn(table);
}

enforceFamilyNotNull(db);

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

export function resolveAdminContext(database, adminKey) {
  const key = typeof adminKey === "string" ? adminKey.trim() : "";
  if (!key) {
    return { role: "none", familyId: null };
  }

  const master = process.env.MASTER_ADMIN_KEY?.trim();
  if (master && key === master) {
    return { role: "master", familyId: null };
  }

  try {
    const row = database.prepare('SELECT id FROM "family" WHERE admin_key = ? LIMIT 1').get(key);
    if (row?.id) {
      return { role: "family", familyId: row.id };
    }
  } catch (error) {
    console.warn("[auth] resolveAdminContext failed", error?.message || error);
  }
  return { role: "none", familyId: null };
}

export default db;
