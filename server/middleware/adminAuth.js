import { db as defaultDb } from "../db.js";

function parseCookies(req) {
  if (req.cookies && typeof req.cookies === "object") {
    return req.cookies;
  }

  const header = typeof req.headers?.cookie === "string" ? req.headers.cookie : "";
  const cookies = {};
  if (header) {
    for (const part of header.split(";")) {
      if (!part) continue;
      const index = part.indexOf("=");
      if (index === -1) continue;
      const key = part.slice(0, index).trim();
      if (!key) continue;
      const rawValue = part.slice(index + 1);
      try {
        cookies[key] = decodeURIComponent(rawValue);
      } catch {
        cookies[key] = rawValue;
      }
    }
  }

  req.cookies = cookies;
  return cookies;
}

function safeGet(db, sql, ...params) {
  try {
    return db.prepare(sql).get(...params);
  } catch (err) {
    if (String(err?.message || "").includes("no such table")) {
      return null;
    }
    throw err;
  }
}

export default function adminAuth(req, res, next) {
  try {
    const cookies = parseCookies(req);
    const headerKey =
      (typeof req.get === "function" && req.get("x-admin-key")) ||
      req.header?.("x-admin-key") ||
      req.headers?.["x-admin-key"] ||
      null;
    const key = (cookies?.ck_admin_key || headerKey || "").toString().trim();

    if (!key) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const db = req.db || defaultDb;

    const masterEnv = (process.env.MASTER_ADMIN_KEY || "").trim();
    const masterRow = safeGet(db, "SELECT id FROM master_admin WHERE admin_key = ? LIMIT 1", key);
    if (masterRow || (masterEnv && masterEnv === key)) {
      req.admin = { role: "master" };
      req.auth = { role: "master", adminKey: key, familyId: null, family_id: null };
      return next();
    }

    if (!req.family?.id) {
      return res.status(403).json({ error: "Family scope required" });
    }

    const familyId = req.family.id;
    let familyRow = safeGet(
      db,
      "SELECT id FROM family_admin WHERE admin_key = ? AND family_id = ? LIMIT 1",
      key,
      familyId
    );

    if (!familyRow) {
      familyRow = safeGet(
        db,
        "SELECT id FROM family WHERE admin_key = ? AND id = ? LIMIT 1",
        key,
        familyId
      );
    }

    if (!familyRow) {
      return res.status(403).json({ error: "Forbidden" });
    }

    req.admin = { role: "family", family_id: familyId };
    req.auth = {
      role: "family",
      adminKey: key,
      familyId,
      family_id: familyId,
      familyKey: req.family?.key ?? null,
      familyName: req.family?.name ?? "",
      familyStatus: req.family?.status ?? null
    };
    return next();
  } catch (error) {
    return next(error);
  }
}
