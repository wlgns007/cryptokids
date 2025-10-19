import { COOKIE_NAME } from "../config.js";
import { db as defaultDb, resolveAdminContext } from "../db.js";

function pickAdminKey(req) {
  const headerKey = req.header("x-admin-key");
  if (typeof headerKey === "string" && headerKey.trim()) {
    return headerKey.trim();
  }

  const familyHeader = req.header("x-family-key");
  if (typeof familyHeader === "string" && familyHeader.trim()) {
    return familyHeader.trim();
  }

  const cookieKey = req.cookies?.[COOKIE_NAME];
  if (typeof cookieKey === "string" && cookieKey.trim()) {
    return cookieKey.trim();
  }

  return "";
}

export function resolveAdmin(req, res, next) {
  try {
    const existingRole = req.admin?.role;
    const existingKey = req.admin?.key;
    if (
      (existingRole === "master" || existingRole === "family") &&
      typeof existingKey === "string" &&
      existingKey.trim()
    ) {
      const currentFamilyIdRaw = req.admin?.familyId ?? req.admin?.family_id ?? null;
      const normalizedFamilyId =
        typeof currentFamilyIdRaw === "string" && currentFamilyIdRaw.trim()
          ? currentFamilyIdRaw.trim()
          : null;
      if (existingRole === "family" && normalizedFamilyId) {
        req.admin.familyId = normalizedFamilyId;
        req.admin.family_id = normalizedFamilyId;
      } else if (existingRole === "master" && normalizedFamilyId) {
        req.admin.familyId = normalizedFamilyId;
        req.admin.family_id = normalizedFamilyId;
      } else if (req.admin && typeof req.admin === "object") {
        req.admin.familyId = null;
        req.admin.family_id = null;
      }
      req.admin.role = existingRole;
      return next();
    }

    const key = pickAdminKey(req);
    if (!key) {
      res.status(401).json({ error: "invalid_admin_key" });
      return;
    }

    const database = req.db || defaultDb;
    const context = resolveAdminContext(database, key);
    if (!context || (context.role !== "master" && context.role !== "family")) {
      res.status(401).json({ error: "invalid_admin_key" });
      return;
    }

    const admin = {
      role: context.role,
      key,
    };

    if (context.role === "family" && context.familyId) {
      const normalizedFamilyId = String(context.familyId).trim();
      if (normalizedFamilyId) {
        admin.familyId = normalizedFamilyId;
        admin.family_id = normalizedFamilyId;
      }
    } else {
      admin.familyId = null;
      admin.family_id = null;
    }

    req.admin = admin;
    next();
  } catch (error) {
    console.error("[admin.resolveAdmin] failed to resolve admin", error?.message || error);
    res.status(500).json({ error: "server_error", detail: "admin_resolution_failed" });
  }
}

export function requireCanAccessFamily(req, res, next) {
  const paramFamily = typeof req.params?.familyId === "string" ? req.params.familyId : "";
  const headerFamily = typeof req.header("x-family") === "string" ? req.header("x-family") : "";
  const rawTarget = paramFamily || headerFamily;
  const targetFamilyId = rawTarget.trim();
  const normalizedTarget = targetFamilyId.toLowerCase();

  const admin = req.admin || {};
  const role = typeof admin.role === "string" ? admin.role : "none";
  const adminFamilyIdRaw = typeof admin.familyId === "string" ? admin.familyId : "";
  const adminFamilyId = adminFamilyIdRaw.trim();
  const normalizedAdminFamilyId = adminFamilyId.toLowerCase();

  const allowed =
    role === "master" ||
    (role === "family" && normalizedTarget && normalizedAdminFamilyId && normalizedTarget === normalizedAdminFamilyId);

  if (!targetFamilyId) {
    res.status(400).json({ error: "family_scope_required" });
    return;
  }

  try {
    console.info("[admin.guard]", {
      method: req.method,
      path: req.originalUrl || req.url || "",
      role,
      adminFamilyId: adminFamilyId || "none",
      targetFamilyId: targetFamilyId || "none",
      decision: allowed ? "allow" : "deny",
    });
  } catch {
    // ignore logging failures
  }

  if (!allowed) {
    res.status(403).json({ error: "forbidden_family_scope" });
    return;
  }

  const nextAdmin = req.admin && typeof req.admin === "object" ? req.admin : {};
  nextAdmin.familyId = targetFamilyId;
  nextAdmin.family_id = targetFamilyId;
  if (!nextAdmin.role) {
    nextAdmin.role = role;
  }
  req.admin = nextAdmin;

  next();
}

export default resolveAdmin;
