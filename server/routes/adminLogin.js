import { db as defaultDb, resolveAdminContext } from "../db.js";

export function adminLogin(req, res) {
  const { key } = req.body || {};
  const raw = typeof key === "string" ? key.trim() : "";
  if (!raw) {
    return res.status(400).json({ error: "Missing key" });
  }

  const db = req.db || defaultDb;
  const context = resolveAdminContext(db, raw);
  if (!context || context.role === "none") {
    return res.status(403).json({ error: "Invalid key" });
  }

  const secure = process.env.NODE_ENV !== "development";
  res.cookie("ck_admin_key", raw, {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: "/",
    maxAge: 7 * 24 * 60 * 60 * 1000
  });

  const payload = {
    ok: true,
    role: context.role === "master" ? "master" : "family",
    family_id: context.familyId ?? null,
    family_uuid: context.familyId ?? null
  };

  return res.json(payload);
}

export default adminLogin;
