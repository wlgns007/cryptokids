import { db as defaultDb } from "../db.js";

function detectShortKeyColumn(database, tableName) {
  try {
    const columns = database.prepare(`PRAGMA table_info(${tableName})`).all();
    const names = columns.map((column) => column.name.toLowerCase());
    if (names.includes("key")) return "key";
    if (names.includes("family_key")) return "family_key";
    if (names.includes("admin_key")) return "admin_key";
  } catch (error) {
    console.warn("[familyResolver] unable to inspect columns", error?.message || error);
  }
  return null;
}

const FAMILY_TABLE = "family";
const DEFAULT_SHORT_KEY_COLUMN = detectShortKeyColumn(defaultDb, FAMILY_TABLE);

export function makeFamilyResolver(database = defaultDb) {
  const shortKeyColumn =
    database === defaultDb
      ? DEFAULT_SHORT_KEY_COLUMN
      : detectShortKeyColumn(database, FAMILY_TABLE);

  const byUuid = database.prepare(`
    SELECT id,
           ${
             shortKeyColumn
               ? `${shortKeyColumn} AS short_key,`
               : "NULL AS short_key,"
           }
           name,
           status
      FROM ${FAMILY_TABLE}
     WHERE id = ?
     LIMIT 1
  `);

  const byKey = shortKeyColumn
    ? database.prepare(`
        SELECT id,
               ${shortKeyColumn} AS short_key,
               name,
               status
          FROM ${FAMILY_TABLE}
         WHERE ${shortKeyColumn} = ? COLLATE NOCASE
         LIMIT 1
      `)
    : null;

  const byName = database.prepare(`
    SELECT id,
           ${
             shortKeyColumn
               ? `${shortKeyColumn} AS short_key,`
               : 'NULL AS short_key,'
           }
           name,
           status
      FROM ${FAMILY_TABLE}
     WHERE name = ? COLLATE NOCASE
     LIMIT 2
  `);

  return function resolveFamily(input) {
    const raw = (input ?? "").trim();
    if (!raw) return null;

    let row = byUuid.get(raw);
    if (!row && byKey) {
      row = byKey.get(raw);
    }
    if (!row) {
      const rows = byName.all(raw);
      if (rows.length === 1) {
        row = rows[0];
      }
    }

    if (!row) {
      const error = new Error("Family not found");
      error.status = 404;
      throw error;
    }

    return {
      id: row.id,
      key: row.short_key ?? null,
      name: row.name,
      status: row.status,
    };
  };
}

export default makeFamilyResolver;
