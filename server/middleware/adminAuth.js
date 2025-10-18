import { readAdminKey } from "../auth.js";

function safeGet(db, sql, params) {
  try {
    return db.prepare(sql).get(...(Array.isArray(params) ? params : [params].filter((p) => p !== undefined)));
  } catch (err) {
    if (String(err?.message || "").includes("no such table")) {
      return null;
    }
    throw err;
  }
}

export function createAdminAuth(db) {
  return function adminAuth(req, _res, next) {
    try {
      const key = String(readAdminKey(req) || "").trim();
      if (!key) {
        req.auth = { role: null };
        return next();
      }

      const masterRow = safeGet(db, 'SELECT id FROM "master_admin" WHERE admin_key = ? LIMIT 1', key);
      if (masterRow) {
        req.auth = { role: "master", adminKey: key };
        return next();
      }

      const masterEnv = (process.env.MASTER_ADMIN_KEY || "").trim();
      if (masterEnv && masterEnv === key) {
        req.auth = { role: "master", adminKey: key };
        return next();
      }

      const familyRow = safeGet(
        db,
        `SELECT f.id AS family_uuid, f.admin_key AS family_key, f.name AS family_name, f.status AS family_status
           FROM family_admin fa
           JOIN family f ON f.id = fa.family_id
          WHERE fa.admin_key = ?
          LIMIT 1`,
        key
      ) ||
        safeGet(
          db,
          `SELECT f.id AS family_uuid, f.admin_key AS family_key, f.name AS family_name, f.status AS family_status
             FROM family f
            WHERE f.admin_key = ?
            LIMIT 1`,
          key
        );

      if (familyRow) {
        req.auth = {
          role: "family",
          adminKey: key,
          familyId: String(familyRow.family_uuid),
          familyKey: familyRow.family_key || null,
          familyName: familyRow.family_name || "",
          familyStatus: familyRow.family_status || null
        };
        return next();
      }

      req.auth = { role: null };
      return next();
    } catch (error) {
      console.error("[adminAuth] failed to resolve admin context", error);
      req.auth = { role: null };
      return next();
    }
  };
}

export default createAdminAuth;
