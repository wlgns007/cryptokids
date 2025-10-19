import { db as defaultDb } from "../db.js";

function normalizeKey(raw) {
  if (raw === undefined || raw === null) {
    return null;
  }
  const key = String(raw).trim();
  return key || null;
}

function normalizeEmail(raw) {
  if (raw === undefined || raw === null) {
    return null;
  }
  const email = String(raw).trim();
  return email || null;
}

function normalizeStatus(raw) {
  if (typeof raw !== "string") {
    return "active";
  }
  const status = raw.trim();
  return status || "active";
}

function isNonDefaultId(id) {
  return id && String(id).toLowerCase() !== "default";
}

export function getActiveFamilies(database = defaultDb) {
  const db = database || defaultDb;
  const rows = db
    .prepare(
      `SELECT id,
              name,
              email,
              admin_key AS key,
              status
         FROM family
        WHERE LOWER(status) = 'active' AND id <> 'default'
        ORDER BY name COLLATE NOCASE`
    )
    .all();

  return rows
    .filter((row) => isNonDefaultId(row?.id))
    .map((row) => ({
      id: row.id,
      name: row.name ?? "",
      key: normalizeKey(row.key),
      email: normalizeEmail(row.email),
      status: normalizeStatus(row.status),
    }));
}

export default getActiveFamilies;
