import crypto from "node:crypto";

const DEFAULT_FAMILY_ID = "default";
const MASTER_ADMIN_KEY_RAW = process.env.MASTER_ADMIN_KEY;
const MASTER_ADMIN_KEY = (MASTER_ADMIN_KEY_RAW || "").trim();
const DEFAULT_FAMILY_ADMIN_KEY_RAW = process.env.ADMIN_KEY;
const DEFAULT_FAMILY_ADMIN_KEY = (DEFAULT_FAMILY_ADMIN_KEY_RAW || "").trim();
const isProduction =
  String(process.env.NODE_ENV || "").toLowerCase() === "production" ||
  Boolean(process.env.RENDER) ||
  Boolean(process.env.RENDER_SERVICE_ID);

console.log("[prestart] starting DB checks...");
const { default: db } = await import("../server/db.js");
console.log("[prestart] DB checks done.");

function quoteIdentifier(id) {
  return `"${String(id).replaceAll('"', '""')}"`;
}

if (isProduction) {
  console.log("[prestart] production detected; skipping Jang cleanup");
} else {
  try {
    db.exec("BEGIN");
    const familyColumns = db
      .prepare("PRAGMA table_info(\"family\")")
      .all()
      .map((column) => column.name.toLowerCase());
    const shortKeyColumns = [];
    if (familyColumns.includes("key")) shortKeyColumns.push("key");
    if (familyColumns.includes("family_key")) shortKeyColumns.push("family_key");
    if (familyColumns.includes("admin_key")) shortKeyColumns.push("admin_key");

    for (const column of shortKeyColumns) {
      db.prepare(
        `DELETE FROM "family" WHERE ${quoteIdentifier(column)} = ? COLLATE NOCASE`
      ).run("jang");
    }

    db.prepare("DELETE FROM \"family\" WHERE LOWER(name) = LOWER(?)").run("jang");
    db.exec("COMMIT");
  } catch (error) {
    db.exec("ROLLBACK");
    console.warn("[prestart] Jang cleanup skipped:", error?.message || error);
  }
}

// ---- helpers ----
function hasTable(db, name) {
  return !!db
    .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?")
    .get(name);
}
function hasColumn(db, table, col) {
  try {
    const cols = db.prepare(`PRAGMA table_info(${quoteIdentifier(table)})`).all();
    return cols.some((c) => c.name === col);
  } catch {
    return false;
  }
}
function listTables(db) {
  return db
    .prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    .all()
    .map((r) => r.name);
}
function fkMap(db, table) {
  // returns: [{table: childTable, ref: parentTable}]
  const rows = db.prepare(`PRAGMA foreign_key_list(${quoteIdentifier(table)})`).all();
  // child is "table" itself; parent is rows[].table
  return rows.map((r) => ({ child: table, parent: r.table }));
}

function topoDeleteOrder(db, familyTables) {
  // Build edges child->parent for *only* tables we will delete by family_id
  const edges = [];
  for (const t of familyTables) {
    for (const { parent } of fkMap(db, t)) {
      if (familyTables.includes(parent)) {
        edges.push([t, parent]); // delete child before parent
      }
    }
  }
  // Kahn's algorithm
  const nodes = new Set(familyTables);
  const indeg = new Map([...nodes].map((n) => [n, 0]));
  for (const [, p] of edges) indeg.set(p, (indeg.get(p) || 0) + 1);
  const q = [...[...nodes].filter((n) => (indeg.get(n) || 0) === 0)];
  const order = [];
  while (q.length) {
    const n = q.shift();
    order.push(n);
    for (const [c, p] of edges.filter((e) => e[0] === n)) {
      indeg.set(p, indeg.get(p) - 1);
      if (indeg.get(p) === 0) q.push(p);
    }
  }
  // If we didn’t cover every node (cycle or unknown), fall back to a conservative order: children first by number of FKs
  if (order.length !== nodes.size) {
    const fkCounts = Object.fromEntries(familyTables.map((t) => [t, fkMap(db, t).length]));
    return [...familyTables].sort((a, b) => fkCounts[b] - fkCounts[a]);
  }
  return order; // children → parents
}

// Delete for one family id using dependency-aware order
function deleteFamilyScopedData(db, familyId) {
  // collect only tables that have a family_id column
  const all = listTables(db);
  const familyTables = all.filter((t) => hasColumn(db, t, "family_id"));

  // Compute deletion order (children first)
  const order = topoDeleteOrder(db, familyTables);

  // Attempt safe ordered delete with FKs on
  db.exec("BEGIN");
  try {
    for (const t of order) {
      db.prepare(`DELETE FROM ${quoteIdentifier(t)} WHERE family_id = ?`).run(familyId);
    }
    db.exec("COMMIT");
    return;
  } catch (e) {
    db.exec("ROLLBACK");
    console.warn(
      "[prestart] ordered family delete failed; skipping cleanup",
      e?.message || e
    );
  }
}

function sweepOrphans(db) {
  // Example only; include tables you know may orphan
  const candidates = ["history", "holds"];
  for (const t of candidates) {
    if (!hasTable(db, t)) continue;
    // if table has member_id and members enforce family_id, remove rows whose member no longer exists
    if (hasColumn(db, t, "member_id")) {
      db.exec(`
        DELETE FROM ${quoteIdentifier(t)}
        WHERE member_id NOT IN (SELECT id FROM "member")
      `);
    }
  }
}

function tableExists(name) {
  try {
    const row = db
      .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1")
      .get(name);
    return !!row;
  } catch {
    return false;
  }
}

function columnInfo(table) {
  try {
    return db.prepare(`PRAGMA table_info(${quoteIdentifier(table)})`).all();
  } catch {
    return [];
  }
}

function columnNames(table) {
  return columnInfo(table).map((col) => col.name);
}

function dumpSchema(table) {
  try {
    const row = db
      .prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1")
      .get(table);
    console.log(`[testG1] .schema ${table}:`, row?.sql || "<missing>");
  } catch (error) {
    console.warn(`[testG1] unable to read schema for ${table}:`, error?.message || error);
  }
}

function logColumnPresence(table, column) {
  if (!tableExists(table)) {
    console.log(`[testG1] ${table} table not present; skipping column check`);
    return;
  }
  const names = columnNames(table);
  console.log(`[testG1] columns for ${table}:`, names);
  if (!names.includes(column)) {
    console.warn(`[testG1] missing expected column ${column} on ${table}`);
  }
}

function hasFamilyColumn(table) {
  return columnInfo(table).some((col) => col.name === "family_id");
}

function isUuid(value) {
  if (typeof value !== "string") {
    return false;
  }
  const normalized = value.trim();
  if (!normalized) {
    return false;
  }
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(normalized);
}

function verifyJangFamilyRecord() {
  if (!hasTable(db, "family")) {
    console.log("[prestart] skipping Jang Family verification; family table missing");
    return;
  }

  const hasDeletedAt = hasColumn(db, "family", "deleted_at");
  let row = null;
  try {
    const stmt = db.prepare(
      "SELECT id, status" + (hasDeletedAt ? ", deleted_at" : "") +
        " FROM \"family\" WHERE LOWER(name) = LOWER(?) LIMIT 1"
    );
    row = stmt.get("Jang Family");
  } catch (error) {
    console.warn("[prestart] unable to verify Jang Family record", error?.message || error);
    return;
  }

  if (!row) {
    console.warn("[prestart] Jang Family record missing");
    return;
  }

  const issues = [];
  if (!isUuid(row.id)) {
    issues.push("id");
  }
  const statusValue = typeof row.status === "string" ? row.status.trim().toLowerCase() : "";
  if (statusValue !== "active") {
    issues.push("status");
  }
  if (hasDeletedAt) {
    const deletedAt = row.deleted_at;
    if (deletedAt !== null && deletedAt !== undefined && String(deletedAt).trim() !== "") {
      issues.push("deleted_at");
    }
  }

  if (issues.length) {
    console.warn("[prestart] Jang Family verification failed", { id: row.id, status: row.status, issues });
  } else {
    console.log("[prestart] verified Jang Family record", { id: row.id, status: row.status });
  }
}

function upsertFamily({ id, name, email = null, status = "active" }) {
  const now = new Date().toISOString();
  const normalizedEmail = email ? String(email).trim().toLowerCase() || null : null;
  db.prepare(
    `INSERT INTO family (id, name, email, status, created_at, updated_at)
     VALUES (@id, @name, @email, @status, @now, @now)
     ON CONFLICT(id) DO UPDATE SET
       name = excluded.name,
       email = COALESCE(excluded.email, family.email),
       status = excluded.status,
        updated_at = excluded.updated_at`
  ).run({ id, name, email: normalizedEmail, status, now });
}

function ensureDefaultFamily() {
  const rows = db.prepare("SELECT COUNT(*) AS count FROM family").get();
  if (!rows?.count) {
    upsertFamily({ id: DEFAULT_FAMILY_ID, name: "Master Templates", status: "system" });
    return;
  }
  const existing = db.prepare("SELECT id FROM family WHERE id = ? LIMIT 1").get(DEFAULT_FAMILY_ID);
  if (!existing) {
    upsertFamily({ id: DEFAULT_FAMILY_ID, name: "Master Templates", status: "system" });
  }
}

function clearDefaultFamilyData(database = db, familyId = DEFAULT_FAMILY_ID) {
  if (!hasTable(database, "family")) return;
  // only purge if you *want* a clean default family
  deleteFamilyScopedData(database, familyId);
  sweepOrphans(database);
}

function ensureFamilyAdminKey(familyId, { preset = null, allowGenerate = true } = {}) {
  const family = db.prepare("SELECT admin_key FROM family WHERE id = ? LIMIT 1").get(familyId);
  if (!family) {
    return null;
  }
  if (family.admin_key) {
    return null;
  }
  const candidate = (preset || "").trim() || (allowGenerate ? crypto.randomBytes(24).toString("base64url") : "");
  if (!candidate) {
    return null;
  }
  try {
    db.prepare(
      "UPDATE family SET admin_key = @key, updated_at = @updated_at WHERE id = @id"
    ).run({ key: candidate, updated_at: new Date().toISOString(), id: familyId });
    return candidate;
  } catch (error) {
    const message = String(error?.message || "");
    if (message.includes("UNIQUE") && message.includes("admin_key")) {
      console.warn(`[prestart] admin key collision for family ${familyId}`, message);
      return null;
    }
    throw error;
  }
}

function normalizeIdColumn(columns, preferred, table) {
  const candidates = Array.isArray(preferred) ? preferred.slice() : [preferred];
  if (!candidates.includes("id")) candidates.push("id");
  const derived = `${table}_id`;
  if (derived && !candidates.includes(derived)) {
    candidates.push(derived);
  }
  for (const candidate of candidates) {
    if (candidate && columns.includes(candidate)) {
      return candidate;
    }
  }
  return null;
}

function duplicateTableRows({ table, fromFamily, toFamily, idColumn = "id", onRowCopy = null }) {
  if (!tableExists(table) || !hasFamilyColumn(table)) {
    return { copied: 0, map: new Map() };
  }
  const targetHasRows = db
    .prepare(`SELECT 1 FROM ${quoteIdentifier(table)} WHERE family_id = ? LIMIT 1`)
    .get(toFamily);
  if (targetHasRows) {
    return { copied: 0, map: new Map() };
  }
  const sourceRows = db
    .prepare(`SELECT * FROM ${quoteIdentifier(table)} WHERE family_id = ?`)
    .all(fromFamily);
  if (!sourceRows.length) {
    return { copied: 0, map: new Map() };
  }
  const columns = columnNames(table);
  const actualIdColumn = normalizeIdColumn(columns, idColumn, table);
  if (!actualIdColumn) {
    return { copied: 0, map: new Map() };
  }
  const mapping = new Map();
  const columnList = columns.map((col) => quoteIdentifier(col)).join(", ");
  const placeholderList = columns.map((col) => `@${col}`).join(", ");
  const insertStmt = db.prepare(
    `INSERT INTO ${quoteIdentifier(table)} (${columnList}) VALUES (${placeholderList})`
  );
  const rowsToInsert = [];
  for (const row of sourceRows) {
    const record = {};
    for (const col of columns) {
      if (col === actualIdColumn) {
        const newId = crypto.randomUUID();
        record[col] = newId;
        if (Object.prototype.hasOwnProperty.call(row, col)) {
          mapping.set(row[col], newId);
        }
      } else if (col === "family_id") {
        record[col] = toFamily;
      } else if (Object.prototype.hasOwnProperty.call(row, col)) {
        record[col] = row[col];
      } else {
        record[col] = null;
      }
    }
    if (typeof onRowCopy === "function") {
      onRowCopy({ record, original: row, mapping });
    }
    rowsToInsert.push(record);
  }
  const insertMany = db.transaction((items) => {
    for (const item of items) {
      insertStmt.run(item);
    }
  });
  insertMany(rowsToInsert);
  return { copied: rowsToInsert.length, map: mapping };
}

function duplicateLedger({ fromFamily, toFamily, memberMap, rewardMap }) {
  const table = "ledger";
  if (!tableExists(table) || !hasFamilyColumn(table)) {
    return { copied: 0, map: new Map() };
  }
  const targetHasRows = db
    .prepare(`SELECT 1 FROM ${quoteIdentifier(table)} WHERE family_id = ? LIMIT 1`)
    .get(toFamily);
  if (targetHasRows) {
    return { copied: 0, map: new Map() };
  }
  const sourceRows = db
    .prepare(`SELECT * FROM ${quoteIdentifier(table)} WHERE family_id = ?`)
    .all(fromFamily);
  if (!sourceRows.length) {
    return { copied: 0, map: new Map() };
  }
  const columns = columnNames(table);
  if (!columns.includes("id")) {
    return { copied: 0, map: new Map() };
  }
  const idMap = new Map();
  for (const row of sourceRows) {
    if (row.id) {
      idMap.set(row.id, crypto.randomUUID());
    }
  }
  const columnList = columns.map((col) => quoteIdentifier(col)).join(", ");
  const placeholderList = columns.map((col) => `@${col}`).join(", ");
  const insertStmt = db.prepare(
    `INSERT INTO ${quoteIdentifier(table)} (${columnList}) VALUES (${placeholderList})`
  );
  const rowsToInsert = [];
  for (const row of sourceRows) {
    const record = {};
    for (const col of columns) {
      let value = Object.prototype.hasOwnProperty.call(row, col) ? row[col] : null;
      if (col === "id") {
        value = row.id && idMap.has(row.id) ? idMap.get(row.id) : crypto.randomUUID();
      } else if (col === "family_id") {
        value = toFamily;
      } else if (col === "user_id" || col === "member_id") {
        if (value && memberMap?.has(value)) {
          value = memberMap.get(value);
        }
      } else if (col === "reward_id") {
        if (value && rewardMap?.has(value)) {
          value = rewardMap.get(value);
        }
      } else if (col === "parent_ledger_id") {
        if (value && idMap.has(value)) {
          value = idMap.get(value);
        }
      }
      record[col] = value;
    }
    rowsToInsert.push(record);
  }
  const insertMany = db.transaction((items) => {
    for (const item of items) {
      insertStmt.run(item);
    }
  });
  insertMany(rowsToInsert);
  return { copied: rowsToInsert.length, map: idMap };
}

function duplicateFamilyData(fromFamily, toFamily) {
  const summary = {};
  const members = duplicateTableRows({ table: "member", fromFamily, toFamily });
  summary.member = members.copied;
  const tasks = duplicateTableRows({ table: "task", fromFamily, toFamily });
  summary.task = tasks.copied;
  const rewards = duplicateTableRows({ table: "reward", fromFamily, toFamily });
  summary.reward = rewards.copied;
  const ledger = duplicateLedger({
    fromFamily,
    toFamily,
    memberMap: members.map,
    rewardMap: rewards.map
  });
  summary.ledger = ledger.copied;
  return summary;
}

dumpSchema("master_task");
dumpSchema("master_reward");
logColumnPresence("task", "master_task_id");
logColumnPresence("reward", "master_reward_id");

ensureDefaultFamily();
const defaultFamilyAdminKey = ensureFamilyAdminKey(DEFAULT_FAMILY_ID, {
  preset: DEFAULT_FAMILY_ADMIN_KEY,
  allowGenerate: true
});
if (isProduction) {
  console.log("[prestart] production detected; skipping default family cleanup");
} else {
  clearDefaultFamilyData();
}

const familyCount = db.prepare("SELECT COUNT(*) AS count FROM family").get()?.count ?? 0;
const defaultAdminPresent = !!db
  .prepare("SELECT 1 FROM family WHERE id = ? AND admin_key IS NOT NULL LIMIT 1")
  .get(DEFAULT_FAMILY_ID);

console.log(
  `[prestart] families: ${familyCount} | master key configured: ${MASTER_ADMIN_KEY ? "yes" : "no"} | default admin present: ${
    defaultAdminPresent ? "yes" : "no"
  }`
);
if (!MASTER_ADMIN_KEY) {
  console.warn("MASTER_ADMIN_KEY env var missing or blank; master admin access disabled");
}
if (defaultFamilyAdminKey) {
  console.log("[DEV ONLY] Default family admin key:", defaultFamilyAdminKey);
}

verifyJangFamilyRecord();

process.exit(0);
