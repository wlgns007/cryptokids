import express from "express";
import crypto from "node:crypto";
import db, { resolveAdminContext } from "./db.js";
import { readAdminKey } from "./auth.js";
import { sendMail } from "./email.js";

export const router = express.Router();

// whoami
router.get("/api/whoami", (req, res) => {
  const ctx = resolveAdminContext(db, readAdminKey(req));
  if (ctx.role === "none") return res.status(403).json({ error: "invalid key" });
  res.json(ctx);
});

// MASTER: list families or fetch by id
router.get("/api/families", (req, res) => {
  const ctx = resolveAdminContext(db, readAdminKey(req));
  if (ctx.role !== "master") return res.status(403).json({ error: "forbidden" });

  const id = (req.query.id || "").trim();
  if (id) {
    if (id.toLowerCase() === "default") {
      return res.status(400).json({ error: "default family is reserved" });
    }
    const row = db
      .prepare(`SELECT id, name, email, status, created_at, updated_at FROM "family" WHERE id = ?`)
      .get(id);
    if (!row) return res.status(404).json({ error: "not found" });
    return res.json(row);
  }
  const rows = db
    .prepare(
      `SELECT id, name, email, status, created_at, updated_at FROM "family" WHERE id <> 'default' ORDER BY created_at DESC`
    )
    .all();
  res.json(rows);
});

router.patch("/api/families/:id", express.json(), (req, res) => {
  const ctx = resolveAdminContext(db, readAdminKey(req));
  if (ctx.role !== "master") return res.status(403).json({ error: "forbidden" });

  const { id } = req.params;
  const body = req.body || {};
  const fields = [];
  const args = [];

  if (typeof body.name === "string" && body.name.trim()) {
    fields.push("name = ?");
    args.push(body.name.trim());
  }
  if (typeof body.email === "string") {
    const email = body.email.trim();
    if (email && !/\S+@\S+\.\S+/.test(email)) return res.status(400).json({ error: "invalid email" });
    fields.push("email = ?");
    args.push(email || null);
  }
  if (typeof body.status === "string" && /^(active|inactive)$/i.test(body.status)) {
    fields.push("status = ?");
    args.push(body.status.toLowerCase());
  }

  if (!fields.length) return res.status(400).json({ error: "no fields to update" });

  fields.push(`updated_at = datetime('now')`);
  const sql = `UPDATE "family" SET ${fields.join(", ")} WHERE id = ?`;
  args.push(id);

  try {
    const info = db.prepare(sql).run(...args);
    if (info.changes === 0) return res.status(404).json({ error: "not found" });
    const row = db
      .prepare(`SELECT id, name, email, status, created_at, updated_at FROM "family" WHERE id = ?`)
      .get(id);
    res.json(row);
  } catch (e) {
    const msg = String(e.message || "");
    if (msg.includes("UNIQUE") && msg.includes("email")) return res.status(409).json({ error: "email already registered" });
    res.status(500).json({ error: "update failed" });
  }
});

// PUBLIC: self-register a new family
router.post("/api/families/self-register", async (req, res) => {
  const { familyName, adminName, email, adminKey } = req.body || {};
  if (!familyName?.trim() || !email?.trim() || !adminKey?.trim()) {
    return res.status(400).json({ error: "familyName, email and adminKey are required" });
  }
  // minimal email sanity
  if (!/\S+@\S+\.\S+/.test(email)) return res.status(400).json({ error: "invalid email" });

  const id = crypto.randomUUID();
  const timestamp = new Date().toISOString();
  try {
    db.prepare(`
      INSERT INTO "family"(id, name, email, admin_key, status, created_at, updated_at)
      VALUES (?, ?, ?, ?, 'active', ?, ?)
    `).run(id, familyName.trim(), email.trim().toLowerCase(), adminKey.trim(), timestamp, timestamp);
  } catch (e) {
    const msg = String(e.message || '');
    if (msg.includes('UNIQUE') && msg.includes('email')) return res.status(409).json({ error: "email already registered" });
    if (msg.includes('UNIQUE') && msg.includes('admin_key')) return res.status(409).json({ error: "admin key already in use" });
    throw e;
  }

  // Send confirmation (asynchronously; don’t block user)
  try {
    await sendMail(
      email.trim(),
      "CryptoKids — Family registration",
      `<p>Hello ${adminName ? adminName : ''},</p>
       <p>Your family <b>${familyName}</b> has been registered.</p>
       <p>Your admin key: <code>${adminKey}</code></p>
       <p>Use this key in the Admin page to manage your family.</p>`
    );
  } catch (e) {
    console.warn("[email] self-register mail failed", e);
  }

  res.status(201).json({ id, name: familyName, email });
});

// PUBLIC: forgot admin key (lookup by email)
router.post("/api/families/forgot-admin-key", async (req, res) => {
  const { email } = req.body || {};
  if (!email?.trim()) return res.status(400).json({ error: "email required" });
  const row = db.prepare(`SELECT name, admin_key FROM "family" WHERE lower(email) = lower(?)`).get(email.trim());
  // respond 200 regardless, to avoid probing emails
  if (!row?.admin_key) return res.json({ ok: true });

  try {
    await sendMail(
      email.trim(),
      "CryptoKids — Your admin key",
      `<p>Your family "${row.name}" admin key:</p><p><code>${row.admin_key}</code></p>`
    );
  } catch (e) {
    console.warn("[email] forgot-admin-key mail failed", e);
  }
  res.json({ ok: true });
});

// List active master tasks not yet adopted by a family
router.get("/api/families/:familyId/master-tasks/available", (req, res) => {
  const { familyId } = req.params;
  const ctx = resolveAdminContext(db, readAdminKey(req));
  if (ctx.role === "none") return res.status(403).json({ error: "forbidden" });
  if (ctx.role === "family" && ctx.familyId !== familyId) {
    return res.status(403).json({ error: "forbidden" });
  }

  const rows = db
    .prepare(
      `SELECT mt.id, mt.title, mt.description, mt.base_points AS points, mt.icon, mt.youtube_url
         FROM master_task mt
        WHERE mt.status = 'active'
          AND NOT EXISTS (
            SELECT 1 FROM task t WHERE t.family_id = ? AND t.master_task_id = mt.id
          )
        ORDER BY mt.created_at DESC`
    )
    .all(familyId);

  res.json(rows);
});

// Create a family task from a master template
router.post("/api/families/:familyId/tasks/from-master", express.json(), (req, res) => {
  const { familyId } = req.params;
  const { master_task_id } = req.body || {};
  if (!master_task_id) return res.status(400).json({ error: "master_task_id required" });

  const ctx = resolveAdminContext(db, readAdminKey(req));
  if (ctx.role === "none") return res.status(403).json({ error: "forbidden" });
  if (ctx.role === "family" && ctx.familyId !== familyId) {
    return res.status(403).json({ error: "forbidden" });
  }

  const mt = db.prepare(`SELECT * FROM master_task WHERE id = ? AND status = 'active'`).get(master_task_id);
  if (!mt) return res.status(404).json({ error: "template not found or inactive" });

  const now = Math.floor(Date.now() / 1000);
  const id = crypto.randomUUID();
  db.prepare(
    `INSERT INTO task (id, family_id, title, description, icon, points, status, master_task_id, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)`
  ).run(id, familyId, mt.title, mt.description, mt.icon, mt.base_points, mt.id, now, now);

  res.status(201).json({ id });
});

// HARD DELETE reward and its dependents (master-only)
router.delete("/api/admin/rewards/:id", (req, res, next) => {
  const ctx = resolveAdminContext(db, readAdminKey(req));
  if (ctx.role !== "master") return next();

  const { id } = req.params;
  if (!id) return res.status(400).json({ error: "invalid id" });

  try {
    db.exec("BEGIN");
    if (hasTable(db, "holds") && hasColumn(db, "holds", "reward_id")) {
      db.prepare(`DELETE FROM holds WHERE reward_id = ?`).run(id);
    }
    if (hasTable(db, "history") && hasColumn(db, "history", "reward_id")) {
      db.prepare(`DELETE FROM history WHERE reward_id = ?`).run(id);
    }

    const info = db.prepare(`DELETE FROM reward WHERE id = ?`).run(id);
    db.exec("COMMIT");
    if (!info.changes) return res.status(404).json({ error: "not found" });
    return res.json({ ok: true });
  } catch (err) {
    db.exec("ROLLBACK");
  }

  try {
    db.exec("PRAGMA foreign_keys=OFF; BEGIN");
    if (hasTable(db, "holds") && hasColumn(db, "holds", "reward_id")) {
      db.prepare(`DELETE FROM holds WHERE reward_id = ?`).run(id);
    }
    if (hasTable(db, "history") && hasColumn(db, "history", "reward_id")) {
      db.prepare(`DELETE FROM history WHERE reward_id = ?`).run(id);
    }
    db.prepare(`DELETE FROM reward WHERE id = ?`).run(id);
    db.exec("COMMIT; PRAGMA foreign_keys=ON;");
    return res.json({ ok: true });
  } catch (err) {
    db.exec("ROLLBACK; PRAGMA foreign_keys=ON;");
    return res.status(500).json({ error: "delete failed" });
  }
});

function hasTable(db, name) {
  return !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?`).get(name);
}

function hasColumn(db, table, col) {
  try {
    const cols = db.prepare(`PRAGMA table_info("${table.replaceAll("\"", "\"\"")}")`).all();
    return cols.some((c) => c.name === col);
  } catch {
    return false;
  }
}
