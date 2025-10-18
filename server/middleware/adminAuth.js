import { readAdminKey } from "../auth.js";
import { makeFamilyResolver } from "../lib/familyResolver.js";

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
  const resolveFamily = makeFamilyResolver(db);

  const resolveFamilySafe = (token) => {
    if (!token) return null;
    try {
      return resolveFamily(token);
    } catch (error) {
      if (error?.status === 404) {
        return null;
      }
      throw error;
    }
  };

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

      const familyAdminRow = safeGet(
        db,
        'SELECT family_id FROM "family_admin" WHERE admin_key = ? LIMIT 1',
        key
      );

      let family = null;
      if (familyAdminRow?.family_id) {
        family = resolveFamilySafe(familyAdminRow.family_id);
      }
      if (!family) {
        family = resolveFamilySafe(key);
      }

      if (family) {
        req.auth = {
          role: "family",
          adminKey: key,
          familyId: String(family.id),
          familyKey: family.key || null,
          familyName: family.name || "",
          familyStatus: family.status || null
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
