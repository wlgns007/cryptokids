import { db as defaultDb } from "../db.js";

function ensureDatabase(database) {
  return database || defaultDb;
}

function tableExists(database, name) {
  try {
    return Boolean(
      database
        .prepare("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name = ?")
        .get(name)
    );
  } catch (error) {
    console.warn(`[admin.db] failed to check table ${name}`, error?.message || error);
    return false;
  }
}

function tableInfo(database, table) {
  try {
    return database.prepare(`PRAGMA table_info("${table}")`).all();
  } catch (error) {
    console.warn(`[admin.db] failed to inspect table ${table}`, error?.message || error);
    return [];
  }
}

function buildColumnMap(info) {
  const map = new Map();
  for (const column of info) {
    if (column?.name) {
      map.set(column.name.toLowerCase(), column.name);
    }
  }
  return map;
}

function pickColumn(map, candidates) {
  for (const candidate of candidates) {
    const key = candidate.toLowerCase();
    if (map.has(key)) {
      return map.get(key);
    }
  }
  return null;
}

export function getFamilyById(database, familyId) {
  const db = ensureDatabase(database);
  const raw = typeof familyId === "string" ? familyId.trim() : "";
  if (!raw) {
    return undefined;
  }

  return db
    .prepare(
      `SELECT id, name, status, admin_key AS adminKey, email FROM "family" WHERE id = ? LIMIT 1`
    )
    .get(raw) || undefined;
}

function buildMemberSelect(map) {
  const nameColumn = pickColumn(map, ["name", "full_name", "fullName"]);
  const nicknameColumn = pickColumn(map, ["nickname", "preferred_name", "nick_name"]);
  const balanceColumn = pickColumn(map, ["balance", "tokens", "points"]);
  const userIdColumn = pickColumn(map, ["user_id", "userId", "member_id", "kid_id"]);
  const addrColumn = pickColumn(map, ["addr", "address"]);
  const pkColumn = pickColumn(map, ["pk", "public_key", "pub_key"]);
  const createdColumn = pickColumn(map, ["created_at", "createdAt"]);
  const updatedColumn = pickColumn(map, ["updated_at", "updatedAt"]);

  const projections = [
    nameColumn ? `"${nameColumn}" AS name` : "NULL AS name",
    nicknameColumn ? `"${nicknameColumn}" AS nickname` : "NULL AS nickname",
    balanceColumn ? `"${balanceColumn}" AS balance` : "NULL AS balance",
    userIdColumn ? `"${userIdColumn}" AS user_id` : "NULL AS user_id",
    addrColumn ? `"${addrColumn}" AS addr` : "NULL AS addr",
    pkColumn ? `"${pkColumn}" AS pk` : "NULL AS pk",
    createdColumn ? `"${createdColumn}" AS created_at` : "NULL AS created_at",
    updatedColumn ? `"${updatedColumn}" AS updated_at` : "NULL AS updated_at",
  ];

  return projections.join(", ");
}

export function listKidsByFamilyId(database, familyId) {
  const db = ensureDatabase(database);
  const raw = typeof familyId === "string" ? familyId.trim() : "";
  if (!raw) {
    return [];
  }
  const normalized = raw.toLowerCase();

  if (tableExists(db, "kids")) {
    const info = tableInfo(db, "kids");
    const columns = buildColumnMap(info);
    const familyColumn = columns.get("family_id");
    if (!familyColumn) {
      console.warn("[admin.db] kids table missing family_id column after migration");
      return [];
    }

    const selectList = buildMemberSelect(columns);
    const sql = `
      SELECT id, ${selectList}, "${familyColumn}" AS family_id
        FROM "kids"
       WHERE LOWER(TRIM("${familyColumn}")) = ?
       ORDER BY id
    `;
    return db.prepare(sql).all(normalized);
  }

  if (tableExists(db, "member")) {
    const info = tableInfo(db, "member");
    const columns = buildColumnMap(info);
    const familyColumn = pickColumn(columns, ["family_id", "familyId", "family_uuid", "familyUuid", "family"]);
    if (!familyColumn) {
      return [];
    }
    const selectList = buildMemberSelect(columns);
    const sql = `
      SELECT id, ${selectList}
        FROM "member"
       WHERE LOWER(TRIM("${familyColumn}")) = ?
       ORDER BY id
    `;
    return db.prepare(sql).all(normalized);
  }

  return [];
}

export default {
  getFamilyById,
  listKidsByFamilyId,
};
