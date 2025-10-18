import { db } from "../db.js";
import { tableExists, hasColumn } from "../lib/dbUtil.js";

export default function softAdminAuth(req, _res, next) {
  try {
    const key = req.cookies?.ck_admin_key || req.header("x-admin-key") || null;
    if (!key) {
      req.admin = { role: "none" };
      return next();
    }

    if (key === process.env.MASTER_ADMIN_KEY) {
      req.admin = { role: "master" };
      return next();
    }

    let ok = false;
    if (tableExists("family_admins")) {
      ok = !!db.prepare("SELECT 1 FROM family_admins WHERE admin_key = ? LIMIT 1").get(key);
    }
    if (!ok && tableExists("families") && hasColumn("families", "admin_key")) {
      ok = !!db.prepare("SELECT 1 FROM families WHERE admin_key = ? LIMIT 1").get(key);
    }
    if (!ok && tableExists("admins") && hasColumn("admins", "key")) {
      ok = !!db.prepare("SELECT 1 FROM admins WHERE key = ? LIMIT 1").get(key);
    }

    req.admin = ok ? { role: "family" } : { role: "none" };
    next();
  } catch (error) {
    next(error);
  }
}
