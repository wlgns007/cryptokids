import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

// Quote SQLite identifiers safely (double-quote the whole thing)
function q(id) {
  return `"${String(id).replaceAll('"', '""')}"`;
}

function hasTable(database, name) {
  if (!name) return false;
  try {
    return Boolean(
      database
        .prepare("SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ? LIMIT 1")
        .get(String(name))
    );
  } catch (error) {
    console.warn('[db] table lookup failed', { table: name, error: error?.message || error });
    return false;
  }
}

function columnMap(database, table) {
  try {
    const info = database.prepare(`PRAGMA table_info(${q(table)})`).all();
    const map = new Map();
    for (const column of info) {
      if (column?.name) {
        map.set(column.name.toLowerCase(), column.name);
      }
    }
    return map;
  } catch (error) {
    console.warn('[db] column inspection failed', { table, error: error?.message || error });
    return new Map();
  }
}

function quoteSqlLiteral(value) {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  const text = String(value);
  return `'${text.replaceAll("'", "''")}'`;
}

function determineDefaultFamilyId(database) {
  try {
    if (!hasTable(database, 'family')) {
      return null;
    }

    const active = database
      .prepare(
        `SELECT id
           FROM "family"
          WHERE COALESCE(LOWER(status), 'active') <> 'deleted'
          ORDER BY created_at
          LIMIT 1`
      )
      .get();
    if (active?.id) {
      return String(active.id);
    }

    const any = database.prepare('SELECT id FROM "family" LIMIT 1').get();
    if (any?.id) {
      return String(any.id);
    }
  } catch (error) {
    console.warn('[db] unable to determine default family id', error?.message || error);
  }
  return null;
}

function adjustKidsCreateSql(originalSql) {
  if (!originalSql || typeof originalSql !== 'string') {
    return null;
  }
  let updated = originalSql;
  if (!/family_id/i.test(updated)) {
    updated = updated.replace(/\)\s*;?\s*$/i, ', family_id TEXT NOT NULL$&');
  } else {
    updated = updated.replace(/family_id[^,)]*/i, (segment) => {
      if (/NOT\s+NULL/i.test(segment)) {
        return segment;
      }
      return `${segment} NOT NULL`;
    });
  }
  return updated;
}

function rebuildKidsTableWithFamilyScope(database, options = {}) {
  try {
    const tableRow = database
      .prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'kids' LIMIT 1`)
      .get();
    const originalSql = tableRow?.sql || null;
    const adjustedSql = adjustKidsCreateSql(originalSql);
    if (!adjustedSql || !/CREATE\s+TABLE/i.test(adjustedSql)) {
      return;
    }

    const indexes = database
      .prepare(
        `SELECT name, sql
           FROM sqlite_master
          WHERE type = 'index' AND tbl_name = 'kids' AND sql IS NOT NULL`
      )
      .all();

    const tempName = `kids__legacy_${Date.now()}`;
    const familyColumnName = options.familyColumnName || 'family_id';
    const fallbackColumnName = options.fallbackColumnName || null;
    const defaultFamilyId = options.defaultFamilyId || determineDefaultFamilyId(database) || 'default';
    const defaultLiteral = quoteSqlLiteral(defaultFamilyId);

    database.exec('PRAGMA foreign_keys=OFF; BEGIN;');
    let renamed = false;
    try {
      database.exec(`ALTER TABLE "kids" RENAME TO "${tempName}"`);
      renamed = true;
      database.exec(adjustedSql);

      const newColumns = database
        .prepare(`PRAGMA table_info(${q('kids')})`)
        .all()
        .map((col) => col.name);
      const oldColumns = database
        .prepare(`PRAGMA table_info(${q(tempName)})`)
        .all()
        .map((col) => col.name);

      const fallbackSources = [
        'family_id',
        'familyId',
        'family_uuid',
        'familyUuid',
        'family',
        'family_key',
        'familyKey',
        'family_code',
        'familyCode'
      ];

      let fallbackSource = fallbackColumnName;
      if (!fallbackSource) {
        for (const candidate of fallbackSources) {
          if (candidate === familyColumnName) continue;
          if (oldColumns.includes(candidate)) {
            fallbackSource = candidate;
            break;
          }
        }
      }

      const fallbackExpr = fallbackSource && fallbackSource !== familyColumnName
        ? `NULLIF(TRIM(${q(fallbackSource)}), '')`
        : defaultLiteral;

      const hasFamilyColumn = oldColumns.includes(familyColumnName);
      const familyExpr = hasFamilyColumn
        ? `COALESCE(NULLIF(TRIM(${q(familyColumnName)}), ''), ${fallbackExpr}, ${defaultLiteral})`
        : fallbackSource
          ? `COALESCE(NULLIF(TRIM(${q(fallbackSource)}), ''), ${defaultLiteral})`
          : defaultLiteral;

      const selectExpressions = newColumns.map((name) => {
        if (name === familyColumnName) {
          return familyExpr;
        }
        if (oldColumns.includes(name)) {
          return q(name);
        }
        return 'NULL';
      });

      const insertSql = `INSERT INTO ${q('kids')} (${newColumns.map(q).join(', ')})
        SELECT ${selectExpressions.join(', ')} FROM ${q(tempName)};`;
      database.exec(insertSql);

      for (const index of indexes) {
        if (!index?.sql) continue;
        try {
          database.exec(index.sql);
        } catch (error) {
          console.warn('[db] unable to rebuild kids index', {
            name: index?.name || 'unknown',
            error: error?.message || error,
          });
        }
      }

      database.exec(`DROP TABLE ${q(tempName)};`);
      database.exec('COMMIT; PRAGMA foreign_keys=ON;');
    } catch (error) {
      database.exec('ROLLBACK; PRAGMA foreign_keys=ON;');
      console.warn('[db] unable to rebuild kids table for family scope', error?.message || error);
      if (renamed) {
        try {
          if (hasTable(database, 'kids')) {
            database.exec('DROP TABLE "kids";');
          }
        } catch {
          // ignore
        }
        try {
          database.exec(`ALTER TABLE ${q(tempName)} RENAME TO ${q('kids')}`);
        } catch (restoreError) {
          console.warn('[db] unable to restore kids table after failed rebuild', restoreError?.message || restoreError);
        }
      }
    }
  } catch (error) {
    console.warn('[db] unable to inspect kids table for rebuild', error?.message || error);
  }
}

export function ensureKidsFamilyScope(database) {
  const dbRef = database || db;
  if (!hasTable(dbRef, 'kids')) {
    return;
  }

  let columns = columnMap(dbRef, 'kids');
  let familyColumn = columns.get('family_id');

  if (!familyColumn) {
    try {
      dbRef.exec('ALTER TABLE "kids" ADD COLUMN "family_id" TEXT');
      familyColumn = 'family_id';
    } catch (error) {
      console.warn('[db] unable to add kids.family_id', error?.message || error);
      return;
    }
  }

  columns = columnMap(dbRef, 'kids');

  const fallbackSources = ['family_uuid', 'familyuuid', 'family', 'family_key', 'familykey'];
  let sourceColumn = null;
  for (const candidate of fallbackSources) {
    if (columns.has(candidate)) {
      sourceColumn = columns.get(candidate);
      break;
    }
  }

  if (sourceColumn) {
    try {
      dbRef.exec(`
        UPDATE "kids"
           SET "family_id" = TRIM("${sourceColumn}")
         WHERE ("family_id" IS NULL OR TRIM("family_id") = '')
           AND TRIM("${sourceColumn}") <> ''
      `);
    } catch (error) {
      console.warn('[db] unable to backfill kids.family_id', {
        error: error?.message || error,
        sourceColumn,
      });
    }
  }

  const defaultFamilyId = determineDefaultFamilyId(dbRef) || 'default';
  const defaultLiteral = quoteSqlLiteral(defaultFamilyId);

  try {
    dbRef.exec(`
      UPDATE "kids"
         SET "family_id" = ${defaultLiteral}
       WHERE "family_id" IS NULL OR TRIM("family_id") = ''
    `);
  } catch (error) {
    console.warn('[db] unable to enforce kids.family_id default', error?.message || error);
  }

  let familyInfo = null;
  try {
    familyInfo = dbRef
      .prepare(`PRAGMA table_info(${q('kids')})`)
      .all()
      .find((col) => col?.name && col.name.toLowerCase() === familyColumn.toLowerCase());
  } catch (error) {
    console.warn('[db] unable to inspect kids.family_id column', error?.message || error);
  }

  const isNotNull = Number(familyInfo?.notnull ?? 0) === 1;
  if (!isNotNull) {
    rebuildKidsTableWithFamilyScope(dbRef, {
      familyColumnName: familyColumn,
      fallbackColumnName: sourceColumn,
      defaultFamilyId,
    });
  }

  try {
    dbRef.exec('CREATE INDEX IF NOT EXISTS "idx_kids_family_id" ON "kids"(family_id)');
  } catch (error) {
    console.warn('[db] unable to ensure kids.family_id index', error?.message || error);
  }
}

export const DATA_DIR = path.resolve(process.cwd(), "data");
fs.mkdirSync(DATA_DIR, { recursive: true });

export const DB_PATH = process.env.DB_PATH || path.join(DATA_DIR, "cryptokids.db");

export const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

ensureKidsFamilyScope(db);

function ensureKidsFamilyScopeForFile(filePath) {
  try {
    if (!filePath) {
      return;
    }
    const absolutePath = path.resolve(filePath);
    if (absolutePath === path.resolve(DB_PATH)) {
      return;
    }
    if (!fs.existsSync(absolutePath)) {
      return;
    }
    const stat = fs.statSync(absolutePath);
    if (!stat.isFile()) {
      return;
    }

    const connection = new Database(absolutePath);
    try {
      ensureKidsFamilyScope(connection);
    } finally {
      connection.close();
    }
  } catch (error) {
    console.warn('[db] unable to inspect legacy database for kids.family_id', {
      filePath,
      error: error?.message || error,
    });
  }
}

function discoverLegacyDbPaths() {
  const paths = new Set();

  const defaultCandidates = [
    path.join(process.cwd(), 'server', 'app.db'),
    path.join(process.cwd(), 'server', 'parentshop.db'),
    path.join(process.cwd(), 'server', 'parentshop.backup.db'),
  ];

  for (const candidate of defaultCandidates) {
    paths.add(path.resolve(candidate));
  }

  try {
    for (const entry of fs.readdirSync(DATA_DIR)) {
      if (!entry) continue;
      const lower = entry.toLowerCase();
      if (!lower.endsWith('.db') && !lower.endsWith('.sqlite')) {
        continue;
      }
      paths.add(path.resolve(DATA_DIR, entry));
    }
  } catch (error) {
    console.warn('[db] unable to inspect data directory for legacy databases', error?.message || error);
  }

  paths.delete(path.resolve(DB_PATH));
  return Array.from(paths);
}

for (const legacyPath of discoverLegacyDbPaths()) {
  ensureKidsFamilyScopeForFile(legacyPath);
}

const schemaStatements = [
  `CREATE TABLE IF NOT EXISTS family (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    admin_key TEXT UNIQUE,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
  )`,
  `CREATE TABLE IF NOT EXISTS master_task (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    base_points INTEGER NOT NULL DEFAULT 0,
    icon TEXT,
    youtube_url TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS master_reward (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    base_cost INTEGER NOT NULL DEFAULT 0,
    icon TEXT,
    youtube_url TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS master_admin (
    id TEXT PRIMARY KEY,
    admin_key TEXT UNIQUE NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  )`,
  `CREATE TABLE IF NOT EXISTS family_admin (
    id TEXT PRIMARY KEY,
    family_id TEXT NOT NULL,
    admin_key TEXT UNIQUE NOT NULL,
    family_role TEXT NOT NULL DEFAULT 'owner',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (family_id) REFERENCES family(id) ON DELETE CASCADE
  )`
];

for (const statement of schemaStatements) {
  db.exec(statement);
}

try {
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS "idx_family_admin_key" ON "family"(admin_key)');
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS "idx_family_email" ON "family"(email)');
} catch (err) {
  console.warn('[db] unable to ensure family indexes', err?.message || err);
}

try {
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS "idx_master_admin_key" ON "master_admin"(admin_key)');
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS "idx_master_admin_id" ON "master_admin"(id)');
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS "idx_family_admin_family" ON "family_admin"(family_id)');
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS "idx_family_admin_admin_key" ON "family_admin"(admin_key)');
} catch (err) {
  console.warn('[db] unable to ensure admin indexes', err?.message || err);
}

try {
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS "trg_family_admin_insert"
    AFTER INSERT ON "family"
    WHEN NEW.admin_key IS NOT NULL
    BEGIN
      INSERT INTO "family_admin"(id, family_id, admin_key, family_role, created_at, updated_at)
      VALUES (NEW.id, NEW.id, NEW.admin_key, 'owner', datetime('now'), datetime('now'))
      ON CONFLICT(family_id) DO UPDATE SET
        admin_key = excluded.admin_key,
        updated_at = datetime('now');
    END;
  `);
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS "trg_family_admin_update"
    AFTER UPDATE OF admin_key ON "family"
    BEGIN
      DELETE FROM "family_admin" WHERE family_id = NEW.id;
      INSERT INTO "family_admin"(id, family_id, admin_key, family_role, created_at, updated_at)
      SELECT NEW.id, NEW.id, NEW.admin_key, 'owner', datetime('now'), datetime('now')
      WHERE NEW.admin_key IS NOT NULL;
    END;
  `);
} catch (err) {
  console.warn('[db] unable to ensure family_admin triggers', err?.message || err);
}

try {
  db.exec(`
    INSERT INTO "family_admin"(id, family_id, admin_key, family_role, created_at, updated_at)
    SELECT f.id, f.id, f.admin_key, 'owner', COALESCE(f.created_at, datetime('now')), datetime('now')
      FROM "family" f
     WHERE f.admin_key IS NOT NULL
    ON CONFLICT(family_id) DO UPDATE SET
      admin_key = excluded.admin_key,
      updated_at = datetime('now');
  `);
} catch (err) {
  console.warn('[db] unable to backfill family_admin', err?.message || err);
}

// ---- Family table migration (email + admin_key) ----
(function migrateFamilyTable() {
  try {
    const cols = db.prepare(`PRAGMA table_info("family")`).all();
    const names = new Set(cols.map(c => c.name));
    const hasEmail = names.has('email');
    const hasPhone = names.has('phone');
    const hasAdminKey = names.has('admin_key');

    // If already correct, nothing to do
    if (hasEmail && hasAdminKey && !hasPhone) return;

    console.log('[db] migrating family table → add email/admin_key, drop phone');

    db.exec('PRAGMA foreign_keys=OFF; BEGIN;');

    // Create the new shape first — don't touch "email" yet
    db.exec(`
      CREATE TABLE IF NOT EXISTS "family__new" (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        admin_key TEXT UNIQUE,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
    `);

    // Copy over from old family if it exists
    if (cols.length > 0) {
      const selectCols = cols.map(c => c.name);
      const colList = [
        selectCols.includes('id') ? 'id' : "NULL AS id",
        selectCols.includes('name') ? 'name' : "NULL AS name",
        selectCols.includes('email') ? 'email' : "NULL AS email",
        selectCols.includes('admin_key') ? 'admin_key' : "NULL AS admin_key",
        selectCols.includes('status') ? 'status' : "'active' AS status",
        selectCols.includes('created_at') ? 'created_at' : "datetime('now') AS created_at",
        selectCols.includes('updated_at') ? 'updated_at' : "datetime('now') AS updated_at"
      ].join(', ');

      db.exec(`
        INSERT INTO "family__new"(id, name, email, admin_key, status, created_at, updated_at)
        SELECT ${colList}
        FROM "family";
      `);
      db.exec(`DROP TABLE "family";`);
    }

    // Rename and index
    db.exec(`
      ALTER TABLE "family__new" RENAME TO "family";
      CREATE UNIQUE INDEX IF NOT EXISTS "idx_family_email" ON "family"(email);
      CREATE UNIQUE INDEX IF NOT EXISTS "idx_family_admin_key" ON "family"(admin_key);
    `);

    db.exec('COMMIT; PRAGMA foreign_keys=ON;');
  } catch (e) {
    db.exec('ROLLBACK; PRAGMA foreign_keys=ON;');
    console.error('[db] family migration failed', e);
    throw e;
  }
})();

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

const MASTER_ADMIN_KEY_ENV = process.env.MASTER_ADMIN_KEY?.trim();
if (MASTER_ADMIN_KEY_ENV) {
  try {
    db.prepare(`
      INSERT INTO "master_admin"(id, admin_key, created_at)
      VALUES ('master', ?, datetime('now'))
      ON CONFLICT(id) DO UPDATE SET admin_key = excluded.admin_key
    `).run(MASTER_ADMIN_KEY_ENV);
  } catch (err) {
    console.warn('[db] unable to ensure master admin key', err?.message || err);
  }
}

function columnInfo(table) {
  return db.prepare(`PRAGMA table_info(${table})`).all();
}

function tableHasColumn(table, column) {
  return columnInfo(table).some((row) => row.name === column);
}

function ensureColumnDefinition(table, column, definition) {
  const columns = columnInfo(table);
  if (!columns.length) {
    return;
  }
  const hasColumn = columns.some((row) => row.name === column);
  if (hasColumn) {
    return;
  }
  db.exec(`ALTER TABLE ${q(table)} ADD COLUMN ${column} ${definition}`);
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

(function migrateMasterYoutube() {
  // master_task: add youtube_url (TEXT)
  let cols = db.prepare(`PRAGMA table_info("master_task")`).all().map((c) => c.name);
  if (!cols.includes("youtube_url")) {
    db.exec("PRAGMA foreign_keys=OFF; BEGIN;");
    try {
      db.exec(`
        CREATE TABLE "master_task__new" (
          id TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          description TEXT,
          base_points INTEGER NOT NULL DEFAULT 0,
          icon TEXT,
          youtube_url TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        INSERT INTO "master_task__new"
          (id,title,description,base_points,icon,youtube_url,status,created_at,updated_at)
        SELECT id,title,description,base_points,icon,NULL,status,created_at,updated_at
        FROM "master_task";
        DROP TABLE "master_task";
        ALTER TABLE "master_task__new" RENAME TO "master_task";
      `);
      db.exec("COMMIT; PRAGMA foreign_keys=ON;");
    } catch (e) {
      db.exec("ROLLBACK; PRAGMA foreign_keys=ON;");
      throw e;
    }
  }

  // master_reward: add youtube_url (TEXT)
  cols = db.prepare(`PRAGMA table_info("master_reward")`).all().map((c) => c.name);
  if (!cols.includes("youtube_url")) {
    db.exec("PRAGMA foreign_keys=OFF; BEGIN;");
    try {
      db.exec(`
        CREATE TABLE "master_reward__new" (
          id TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          description TEXT,
          base_cost INTEGER NOT NULL DEFAULT 0,
          icon TEXT,
          youtube_url TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        INSERT INTO "master_reward__new"
          (id,title,description,base_cost,icon,youtube_url,status,created_at,updated_at)
        SELECT id,title,description,base_cost,icon,NULL,status,created_at,updated_at
        FROM "master_reward";
        DROP TABLE "master_reward";
        ALTER TABLE "master_reward__new" RENAME TO "master_reward";
      `);
      db.exec("COMMIT; PRAGMA foreign_keys=ON;");
    } catch (e) {
      db.exec("ROLLBACK; PRAGMA foreign_keys=ON;");
      throw e;
    }
  }
})();

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
      email TEXT UNIQUE,
      admin_key TEXT UNIQUE,
      status TEXT NOT NULL DEFAULT 'active',
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
  `);
  db.prepare(`
    INSERT INTO ${q("family")} (id, name, email, status, admin_key, created_at, updated_at)
    SELECT 'default', 'Master Templates', NULL, 'system', NULL, datetime('now'), datetime('now')
    WHERE NOT EXISTS (SELECT 1 FROM ${q("family")} WHERE id = 'default')
  `).run();
  db.prepare(
    `UPDATE ${q("family")} SET name = 'Master Templates', status = 'system' WHERE id = 'default'`
  ).run();

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

ensureColumnDefinition("task", "master_task_id", "TEXT");
ensureColumnDefinition("reward", "master_reward_id", "TEXT");
ensureColumnDefinition("master_task", "version", "INTEGER");
ensureColumnDefinition("master_reward", "version", "INTEGER");
ensureColumnDefinition("task", "source_template_id", "TEXT");
ensureColumnDefinition("task", "source_version", "INTEGER");
ensureColumnDefinition("task", "is_customized", "INTEGER");
ensureColumnDefinition("reward", "source_template_id", "TEXT");
ensureColumnDefinition("reward", "source_version", "INTEGER");
ensureColumnDefinition("reward", "is_customized", "INTEGER");

if (columnInfo('master_task').length) {
  try {
    db.exec(`
      UPDATE master_task
         SET version = COALESCE(NULLIF(version, ''), 1)
       WHERE version IS NULL OR version = ''
    `);
  } catch (err) {
    console.warn('[db] unable to backfill master_task.version', err?.message || err);
  }
}

if (columnInfo('master_reward').length) {
  try {
    db.exec(`
      UPDATE master_reward
         SET version = COALESCE(NULLIF(version, ''), 1)
       WHERE version IS NULL OR version = ''
    `);
  } catch (err) {
    console.warn('[db] unable to backfill master_reward.version', err?.message || err);
  }
}

if (columnInfo('task').length) {
  try {
    db.exec(`
      UPDATE task
         SET source_template_id = COALESCE(NULLIF(source_template_id, ''), master_task_id),
             source_version = COALESCE(NULLIF(source_version, ''), 1),
             is_customized = COALESCE(NULLIF(is_customized, ''), 0)
       WHERE master_task_id IS NOT NULL
    `);
  } catch (err) {
    console.warn('[db] unable to backfill task source metadata', err?.message || err);
  }
}

if (columnInfo('reward').length) {
  try {
    db.exec(`
      UPDATE reward
         SET source_template_id = COALESCE(NULLIF(source_template_id, ''), master_reward_id),
             source_version = COALESCE(NULLIF(source_version, ''), 1),
             is_customized = COALESCE(NULLIF(is_customized, ''), 0)
       WHERE master_reward_id IS NOT NULL
    `);
  } catch (err) {
    console.warn('[db] unable to backfill reward source metadata', err?.message || err);
  }
}

if (columnInfo('task').length) {
  try {
    db.exec('CREATE INDEX IF NOT EXISTS idx_task_source ON task(source_template_id, is_customized)');
  } catch (err) {
    console.warn('[db] unable to ensure task source metadata index', err?.message || err);
  }
}
if (columnInfo('reward').length) {
  try {
    db.exec('CREATE INDEX IF NOT EXISTS idx_reward_source ON reward(source_template_id, is_customized)');
  } catch (err) {
    console.warn('[db] unable to ensure reward source metadata index', err?.message || err);
  }
}

function ensureMasterCascadeTriggers() {
  try {
    db.exec('DROP TRIGGER IF EXISTS trg_master_task_inactivate');
  } catch (err) {
    console.warn('[db] unable to drop trg_master_task_inactivate', err?.message || err);
  }

  const statements = [];
  if (tableHasColumn("task", "master_task_id")) {
    statements.push(`
  CREATE TRIGGER IF NOT EXISTS trg_master_task_inactivate
  AFTER UPDATE OF status ON "master_task"
  WHEN NEW.status = 'inactive'
  BEGIN
    UPDATE "task" SET status='inactive', updated_at=strftime('%s','now') WHERE master_task_id = NEW.id;
  END;`);
  }
  if (tableHasColumn("reward", "master_reward_id")) {
    statements.push(`
  CREATE TRIGGER IF NOT EXISTS trg_master_reward_inactivate
  AFTER UPDATE OF status ON "master_reward"
  WHEN NEW.status = 'inactive'
  BEGIN
    UPDATE "reward" SET status='inactive', updated_at=strftime('%s','now') WHERE master_reward_id = NEW.id;
  END;`);
  }
  if (!statements.length) return;
  db.exec(statements.join("\n"));
}

ensureMasterCascadeTriggers();

enforceFamilyNotNull(db);

// Backfill any legacy rewards/tasks missing a family scope
try {
  if (columnInfo("reward").length) {
    db.exec(`UPDATE reward SET family_id='default' WHERE family_id IS NULL;`);
  }
  if (columnInfo("task").length) {
    db.exec(`UPDATE task SET family_id='default' WHERE family_id IS NULL;`);
  }
} catch (err) {
  console.warn('[db] legacy family backfill skipped', err?.message || err);
}

const scopedIndexes = [
  { table: "member", statement: "CREATE INDEX IF NOT EXISTS idx_member_family ON member(family_id)" },
  { table: "task", statement: "CREATE INDEX IF NOT EXISTS idx_task_family ON task(family_id)" },
  { table: "reward", statement: "CREATE INDEX IF NOT EXISTS idx_reward_family ON reward(family_id)" },
  { table: "ledger", statement: "CREATE INDEX IF NOT EXISTS idx_ledger_family ON ledger(family_id, created_at)" },
  {
    table: "earn_templates",
    statement: "CREATE INDEX IF NOT EXISTS idx_earn_templates_family ON earn_templates(family_id)"
  },
  { table: "task", statement: "CREATE INDEX IF NOT EXISTS idx_task_master ON task(master_task_id)" },
  { table: "reward", statement: "CREATE INDEX IF NOT EXISTS idx_reward_master ON reward(master_reward_id)" }
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

export { ensureMasterCascadeTriggers };

function loadFamilyProfile(database, familyId) {
  if (!familyId) {
    return { familyId: null, familyKey: null, familyName: "" };
  }

  try {
    const row = database
      .prepare(`SELECT id, admin_key AS familyKey, name AS familyName FROM "family" WHERE id = ? LIMIT 1`)
      .get(familyId);
    if (!row) {
      return { familyId, familyKey: null, familyName: "" };
    }
    return {
      familyId: row.id || familyId,
      familyKey: row.familyKey || null,
      familyName: row.familyName || ""
    };
  } catch (error) {
    console.warn("[db] family profile lookup failed", error?.message || error);
    return { familyId, familyKey: null, familyName: "" };
  }
}

export function resolveAdminContext(database, adminKey) {
  const key = typeof adminKey === "string" ? adminKey.trim() : "";
  if (!key) {
    return { role: "none", familyId: null, family_id: null, familyKey: null, familyName: "" };
  }

  try {
    const masterRow = database
      .prepare(`SELECT id FROM "master_admin" WHERE admin_key = ? LIMIT 1`)
      .get(key);
    if (masterRow?.id) {
      return { role: "master", familyId: null, family_id: null, familyKey: null, familyName: "" };
    }
  } catch (err) {
    console.warn('[db] master_admin lookup failed', err?.message || err);
  }

  const MASTER = process.env.MASTER_ADMIN_KEY?.trim();
  if (MASTER && key === MASTER) {
    return { role: "master", familyId: null, family_id: null, familyKey: null, familyName: "" };
  }

  try {
    const adminRow = database
      .prepare(
        `SELECT family_id AS familyId FROM "family_admin" WHERE admin_key = ? LIMIT 1`
      )
      .get(key);
    if (adminRow?.familyId) {
      const profile = loadFamilyProfile(database, adminRow.familyId);
      return {
        role: "family",
        familyId: profile.familyId,
        family_id: profile.familyId,
        familyKey: profile.familyKey,
        familyName: profile.familyName
      };
    }
  } catch (err) {
    console.warn('[db] family_admin lookup failed', err?.message || err);
  }

  try {
    const row = database
      .prepare(`SELECT id, admin_key AS familyKey, name AS familyName FROM "family" WHERE admin_key = ? LIMIT 1`)
      .get(key);
    if (row?.id) {
      return {
        role: "family",
        familyId: row.id,
        family_id: row.id,
        familyKey: row.familyKey || null,
        familyName: row.familyName || ""
      };
    }
  } catch (err) {
    console.warn('[db] family lookup failed', err?.message || err);
  }

  return { role: "none", familyId: null, family_id: null, familyKey: null, familyName: "" };
}

export default db;
