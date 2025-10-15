import crypto from "node:crypto";

const DEFAULT_FAMILY_ID = "default";
const JANG_FAMILY_ID = "JangJ6494";
const JANG_FAMILY_NAME = "Jang";
const JANG_ADMIN_KEY = "Mamapapa";
const MASTER_ADMIN_KEY_RAW = process.env.MASTER_ADMIN_KEY;
const MASTER_ADMIN_KEY = (MASTER_ADMIN_KEY_RAW || "").trim();
const DEFAULT_FAMILY_ADMIN_KEY_RAW = process.env.ADMIN_KEY;
const DEFAULT_FAMILY_ADMIN_KEY = (DEFAULT_FAMILY_ADMIN_KEY_RAW || "").trim();

console.log("[prestart] starting DB checks...");
const { default: db } = await import("../server/db.js");
console.log("[prestart] DB checks done.");

function quoteIdentifier(id) {
  return `"${String(id).replaceAll('"', '""')}"`;
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

function hasFamilyColumn(table) {
  return columnInfo(table).some((col) => col.name === "family_id");
}

function upsertFamily({ id, name, status = "active" }) {
  const now = Date.now();
  db.prepare(
    `INSERT INTO family (id, name, status, created_at, updated_at)
     VALUES (@id, @name, @status, @now, @now)
     ON CONFLICT(id) DO UPDATE SET
       name = excluded.name,
       status = excluded.status,
       updated_at = excluded.updated_at`
  ).run({ id, name, status, now });
}

function ensureDefaultFamily() {
  const rows = db.prepare("SELECT COUNT(*) AS count FROM family").get();
  if (!rows?.count) {
    upsertFamily({ id: DEFAULT_FAMILY_ID, name: "Default Family" });
    return;
  }
  const existing = db.prepare("SELECT id FROM family WHERE id = ? LIMIT 1").get(DEFAULT_FAMILY_ID);
  if (!existing) {
    upsertFamily({ id: DEFAULT_FAMILY_ID, name: "Default Family" });
  }
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
      "UPDATE family SET admin_key = @key, updated_at = @now WHERE id = @id"
    ).run({ key: candidate, now: Date.now(), id: familyId });
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

ensureDefaultFamily();
const defaultFamilyAdminKey = ensureFamilyAdminKey(DEFAULT_FAMILY_ID, {
  preset: DEFAULT_FAMILY_ADMIN_KEY,
  allowGenerate: true
});
upsertFamily({ id: JANG_FAMILY_ID, name: JANG_FAMILY_NAME, status: "active" });
const seededJangKey = ensureFamilyAdminKey(JANG_FAMILY_ID, {
  preset: JANG_ADMIN_KEY,
  allowGenerate: true
});
const duplicationSummary = duplicateFamilyData(DEFAULT_FAMILY_ID, JANG_FAMILY_ID);

const familyCount = db.prepare("SELECT COUNT(*) AS count FROM family").get()?.count ?? 0;
const jangCounts = {};
for (const table of ["member", "task", "reward", "ledger"]) {
  if (tableExists(table) && hasFamilyColumn(table)) {
    const row = db
      .prepare(`SELECT COUNT(*) AS count FROM ${quoteIdentifier(table)} WHERE family_id = ?`)
      .get(JANG_FAMILY_ID);
    jangCounts[table] = row?.count ?? 0;
  }
}
const defaultAdminPresent = !!db
  .prepare("SELECT 1 FROM family WHERE id = ? AND admin_key IS NOT NULL LIMIT 1")
  .get(DEFAULT_FAMILY_ID);
const jangAdminPresent = !!db
  .prepare("SELECT 1 FROM family WHERE id = ? AND admin_key IS NOT NULL LIMIT 1")
  .get(JANG_FAMILY_ID);

console.log(
  `[prestart] families: ${familyCount} | master key configured: ${MASTER_ADMIN_KEY ? "yes" : "no"} | default admin present: ${
    defaultAdminPresent ? "yes" : "no"
  } | Jang admin present: ${jangAdminPresent ? "yes" : "no"}`
);
if (!MASTER_ADMIN_KEY) {
  console.warn("MASTER_ADMIN_KEY env var missing or blank; master admin access disabled");
}
if (defaultFamilyAdminKey) {
  console.log("[DEV ONLY] Default family admin key:", defaultFamilyAdminKey);
}
if (seededJangKey) {
  console.log("[DEV] Seeded Jang admin key");
}
for (const [table, count] of Object.entries(duplicationSummary)) {
  if (count > 0) {
    console.log(`[prestart] Copied ${count} ${table} row(s) from ${DEFAULT_FAMILY_ID} to ${JANG_FAMILY_ID}.`);
  }
}

process.exit(0);
