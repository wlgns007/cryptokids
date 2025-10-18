import { db } from "../db.js";
import { tableExists, hasColumn } from "../lib/dbUtil.js";

export default function adminAuth(req, res, next) {
  try {
    const key = req.cookies?.ck_admin_key || req.header("x-admin-key") || null;
    if (!key) return res.status(401).json({ error: "Unauthorized" });

    // Master short-circuit
    if (key === process.env.MASTER_ADMIN_KEY) {
      req.admin = { role: "master" };
      return next();
    }

    // Family admin needs a scope
    if (!req.family?.id) return res.status(403).json({ error: "Family scope required" });

    const famId = req.family.id;
    let ok = false;

    // 1) family_admins(admin_key, family_id)
    if (!ok && tableExists("family_admins")) {
      const row = db
        .prepare(
          "SELECT 1 FROM family_admins WHERE admin_key = ? AND family_id = ? LIMIT 1"
        )
        .get(key, famId);
      ok = !!row;
    }

    // 2) families(admin_key)
    if (!ok && tableExists("families") && hasColumn("families", "admin_key")) {
      const row = db
        .prepare("SELECT 1 FROM families WHERE id = ? AND admin_key = ? LIMIT 1")
        .get(famId, key);
      ok = !!row;
    }

    // 3) admins(key, family_id) (legacy)
    if (!ok && tableExists("admins") && hasColumn("admins", "key")) {
      const row = db
        .prepare("SELECT 1 FROM admins WHERE key = ? AND family_id = ? LIMIT 1")
        .get(key, famId);
      ok = !!row;
    }

    if (!ok) return res.status(403).json({ error: "Forbidden" });

    req.admin = { role: "family", family_id: famId };
    next();
  } catch (e) {
    next(e);
  }
}
