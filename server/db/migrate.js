import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

function quoteIdent(identifier) {
  return `"${String(identifier).replaceAll('"', '""')}"`;
}

function normalizeIdent(value) {
  return String(value || "").replaceAll('"', "").replaceAll("'", "").trim();
}

export function tableInfo(db, table) {
  if (!table) return [];
  try {
    return db.prepare(`PRAGMA table_info(${quoteIdent(table)})`).all();
  } catch (error) {
    console.warn("[migrate] unable to inspect table", { table, error: error?.message || error });
    return [];
  }
}

export function hasColumn(db, table, column) {
  if (!table || !column) return false;
  return tableInfo(db, table).some((info) => info?.name === column);
}

export function addColumnIfMissing(db, table, ddl) {
  if (!table || !ddl) return false;
  const trimmed = ddl.trim();
  if (!trimmed) return false;
  const [rawName] = trimmed.split(/\s+/, 1);
  const columnName = normalizeIdent(rawName);
  if (!columnName) {
    throw new Error(`Unable to determine column name from definition: ${ddl}`);
  }
  if (hasColumn(db, table, columnName)) {
    return false;
  }
  const statement = `ALTER TABLE ${quoteIdent(table)} ADD COLUMN ${ddl}`;
  try {
    db.exec(statement);
    return true;
  } catch (error) {
    const message = error?.message || "";
    if (/duplicate column/i.test(message) || /already exists/i.test(message)) {
      return false;
    }
    throw error;
  }
}

function indexTargetFromDdl(ddl) {
  const match = ddl.match(/ON\s+([^\s(]+)\s*\(([^)]+)\)/i);
  if (!match) {
    throw new Error(`Unable to parse index target from: ${ddl}`);
  }
  const table = normalizeIdent(match[1]);
  const columns = match[2]
    .split(",")
    .map((segment) => normalizeIdent(segment))
    .filter(Boolean);
  return { table, columns };
}

function indexColumns(db, name) {
  try {
    return db
      .prepare(`PRAGMA index_info(${quoteIdent(name)})`)
      .all()
      .sort((a, b) => Number(a?.seqno ?? 0) - Number(b?.seqno ?? 0))
      .map((row) => normalizeIdent(row?.name));
  } catch (error) {
    console.warn("[migrate] unable to inspect index", { name, error: error?.message || error });
    return [];
  }
}

export function createIndexIfMissing(db, name, ddl) {
  if (!name || !ddl) return false;
  const { table, columns } = indexTargetFromDdl(ddl);
  if (!table) {
    throw new Error(`Unable to determine target table for index ${name}`);
  }
  let existing = null;
  try {
    const indexes = db.prepare(`PRAGMA index_list(${quoteIdent(table)})`).all();
    existing = indexes.find((row) => row?.name === name) || null;
  } catch (error) {
    console.warn("[migrate] unable to list indexes", { table, error: error?.message || error });
  }

  if (existing) {
    const currentColumns = indexColumns(db, name);
    const desired = columns.map(normalizeIdent);
    const sameColumns =
      currentColumns.length === desired.length &&
      currentColumns.every((value, idx) => value === desired[idx]);
    if (sameColumns) {
      return false;
    }
    try {
      db.exec(`DROP INDEX IF EXISTS ${quoteIdent(name)}`);
    } catch (error) {
      console.warn("[migrate] unable to drop existing index", { name, error: error?.message || error });
    }
  }

  try {
    db.exec(ddl);
    return true;
  } catch (error) {
    const message = error?.message || "";
    if (/already exists/i.test(message)) {
      return false;
    }
    throw error;
  }
}

function ensureSchemaMigrationsTable(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      name TEXT PRIMARY KEY,
      applied_at INTEGER NOT NULL
    );
  `);
}

function migrationsDirectory(options = {}) {
  if (options?.migrationsDir) {
    return options.migrationsDir;
  }
  const currentDir = path.dirname(fileURLToPath(import.meta.url));
  return path.join(currentDir, "migrations");
}

function listMigrationFiles(dir) {
  if (!fs.existsSync(dir)) {
    return [];
  }
  return fs
    .readdirSync(dir)
    .filter((file) => /\.(sql|cjs|js)$/i.test(file))
    .sort();
}

function loadJsMigration(filePath) {
  const loaded = require(filePath);
  if (typeof loaded === "function") {
    return loaded;
  }
  if (loaded && typeof loaded.up === "function") {
    return loaded.up;
  }
  if (loaded && typeof loaded.default === "function") {
    return loaded.default;
  }
  throw new Error(`Migration module ${filePath} does not export a function`);
}

function runSqlMigration(db, filePath) {
  const sql = fs.readFileSync(filePath, "utf8");
  const trimmed = sql.trim();
  if (!trimmed) return;
  db.exec(trimmed);
}

export function runMigrations(db, options = {}) {
  ensureSchemaMigrationsTable(db);
  const dir = migrationsDirectory(options);
  const files = listMigrationFiles(dir);

  const appliedRows = db.prepare("SELECT name FROM schema_migrations").all();
  const applied = new Set(appliedRows.map((row) => row.name));
  const appliedThisRun = [];

  for (const file of files) {
    if (applied.has(file)) {
      continue;
    }
    const fullPath = path.join(dir, file);
    const context = {
      db,
      tableInfo: (table) => tableInfo(db, table),
      hasColumn: (table, column) => hasColumn(db, table, column),
      addColumnIfMissing: (table, ddl) => addColumnIfMissing(db, table, ddl),
      createIndexIfMissing: (name, ddl) => createIndexIfMissing(db, name, ddl),
      quoteIdent,
    };

    db.exec("BEGIN");
    try {
      if (/\.sql$/i.test(file)) {
        runSqlMigration(db, fullPath);
      } else {
        const runner = loadJsMigration(fullPath);
        runner(context);
      }
      db.prepare(
        "INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)"
      ).run(file, Date.now());
      db.exec("COMMIT");
      appliedThisRun.push(file);
    } catch (error) {
      db.exec("ROLLBACK");
      throw new Error(`Migration ${file} failed: ${error?.message || error}`);
    }
  }

  return appliedThisRun;
}

export default runMigrations;
