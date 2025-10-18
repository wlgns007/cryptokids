import express from "express";
import { randomUUID } from "node:crypto";
import db from "./db.js";
import { readAdminKey } from "./auth.js";
import { sendMail } from "./email.js";

export const router = express.Router();

function adminAuth(req, _res, next) {
  const key = readAdminKey(req);
  if (!key) {
    req.auth = { role: null };
    return next();
  }
  if (key === process.env.MASTER_ADMIN_KEY) {
    req.auth = { role: "master" };
    return next();
  }
  const row = db.prepare("SELECT id, name FROM family WHERE admin_key = ?").get(key);
  req.auth = row
    ? { role: "family", familyId: String(row.id), familyName: row.name ? String(row.name) : "" }
    : { role: null };
  next();
}

router.use(adminAuth);

function allowFamilyOrMaster(req, familyId) {
  return (
    req.auth?.role === "master" ||
    (req.auth?.role === "family" && req.auth.familyId === String(familyId))
  );
}

// whoami
router.get("/api/admin/whoami", (req, res) => {
  if (!req.auth?.role) return res.status(401).json({ error: "invalid" });
  const out = { role: req.auth.role };
  if (req.auth.role === "family") {
    out.familyId = req.auth.familyId;
    if (req.auth.familyName) out.familyName = req.auth.familyName;
  }
  res.json(out);
});

router.get("/api/admin/families", (req, res) => {
  if (req.auth?.role !== "master") return res.sendStatus(403);

  const requestedId = (req.query.id || "").toString().trim();
  if (requestedId) {
    if (requestedId.toLowerCase() === "default") {
      return res.status(400).json({ error: "default family is reserved" });
    }
    const row = db
      .prepare(
        `SELECT id, admin_key AS family_key, name, email, status, created_at, updated_at FROM "family" WHERE id = ?`
      )
      .get(requestedId);
    if (!row) return res.status(404).json({ error: "not found" });
    return res.json(row);
  }

  const status = (req.query.status || "").toString().toLowerCase();
  const rows = db
    .prepare(`
      SELECT id, admin_key AS family_key, name, email, status
      FROM family
      WHERE id <> 'default' AND (? = '' OR status = ?)
      ORDER BY created_at DESC
    `)
    .all(status, status);
  res.json(rows);
});

router.get("/api/admin/families/:familyId/members", (req, res) => {
  const { familyId } = req.params;
  if (!allowFamilyOrMaster(req, familyId)) return res.sendStatus(403);
  const rows = db
    .prepare(
      "SELECT id, name, nickname, balance, user_id, created_at, updated_at FROM member WHERE family_id = ? ORDER BY created_at ASC"
    )
    .all(familyId);
  res.json(rows);
});

router.get("/api/admin/families/:familyId/tasks", (req, res) => {
  const { familyId } = req.params;
  if (!allowFamilyOrMaster(req, familyId)) return res.sendStatus(403);
  const rows = db
    .prepare("SELECT * FROM task WHERE family_id = ? ORDER BY updated_at DESC")
    .all(familyId);
  res.json(rows);
});

router.get("/api/admin/families/:familyId/rewards", (req, res) => {
  const { familyId } = req.params;
  if (!allowFamilyOrMaster(req, familyId)) return res.sendStatus(403);
  const rows = db
    .prepare("SELECT * FROM reward WHERE family_id = ? ORDER BY updated_at DESC")
    .all(familyId);
  res.json(rows);
});

router.get("/api/admin/families/:familyId/holds", (req, res) => {
  const { familyId } = req.params;
  if (!allowFamilyOrMaster(req, familyId)) return res.sendStatus(403);
  const rows = db
    .prepare("SELECT * FROM reward_hold WHERE family_id = ? ORDER BY created_at DESC")
    .all(familyId);
  res.json(rows);
});

router.get("/api/admin/families/:familyId/activity", (req, res) => {
  const { familyId } = req.params;
  if (!allowFamilyOrMaster(req, familyId)) return res.sendStatus(403);
  const limit = Math.min(parseInt(req.query.limit || "50", 10), 200);
  const rows = db
    .prepare("SELECT * FROM activity WHERE family_id = ? ORDER BY created_at DESC LIMIT ?")
    .all(familyId, limit);
  res.json(rows);
});

router.patch("/api/admin/families/:id", express.json(), (req, res) => {
  if (req.auth?.role !== "master") return res.sendStatus(403);

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
router.post("/api/admin/families/self-register", async (req, res) => {
  const { familyName, adminName, email, adminKey } = req.body || {};
  if (!familyName?.trim() || !email?.trim() || !adminKey?.trim()) {
    return res.status(400).json({ error: "familyName, email and adminKey are required" });
  }
  // minimal email sanity
  if (!/\S+@\S+\.\S+/.test(email)) return res.status(400).json({ error: "invalid email" });

  const id = randomUUID();
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
      "CleverKids — Family registration",
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
router.post(
  "/api/admin/families/forgot-admin-key",
  async (req, res) => {
    const { email } = req.body || {};
    if (!email?.trim()) return res.status(400).json({ error: "email required" });
    const row = db.prepare(`SELECT name, admin_key FROM "family" WHERE lower(email) = lower(?)`).get(email.trim());
    // respond 200 regardless, to avoid probing emails
    if (!row?.admin_key) return res.json({ ok: true });

    try {
      await sendMail(
        email.trim(),
        "CleverKids — Your admin key",
        `<p>Your family "${row.name}" admin key:</p><p><code>${row.admin_key}</code></p>`
      );
    } catch (e) {
      console.warn("[email] forgot-admin-key mail failed", e);
    }
    res.json({ ok: true });
  }
);

// List active master tasks not yet adopted by a family
router.get("/api/admin/families/:familyId/master-tasks/available", (req, res) => {
  if (!req.auth?.role) return res.sendStatus(401);

  const { familyId } = req.params;
  if (
    !(
      req.auth.role === "master" ||
      (req.auth.role === "family" && req.auth.familyId === String(familyId))
    )
  ) {
    return res.sendStatus(403);
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
router.post("/api/admin/families/:familyId/tasks/from-master", express.json(), (req, res) => {
  if (!req.auth?.role) return res.sendStatus(401);

  const { familyId } = req.params;
  const normalizedFamilyId = String(familyId);
  if (
    !(
      req.auth.role === "master" ||
      (req.auth.role === "family" && req.auth.familyId === normalizedFamilyId)
    )
  ) {
    return res.sendStatus(403);
  }

  const { master_task_id } = req.body || {};
  if (!master_task_id) return res.status(400).json({ error: "master_task_id required" });

  const mt = db.prepare(`SELECT * FROM master_task WHERE id = ? AND status = 'active'`).get(master_task_id);
  if (!mt) return res.status(404).json({ error: "template not found or inactive" });

  const id = randomUUID();
  const now = Date.now();
  const columns = ["id", "family_id"];
  const values = [id, normalizedFamilyId];

  if (hasColumn(db, "task", "title")) {
    columns.push("title");
    values.push(mt.title);
  }
  if (hasColumn(db, "task", "name")) {
    columns.push("name");
    values.push(mt.title);
  }
  if (hasColumn(db, "task", "description")) {
    columns.push("description");
    values.push(mt.description);
  }
  if (hasColumn(db, "task", "icon")) {
    columns.push("icon");
    values.push(mt.icon);
  }
  if (hasColumn(db, "task", "points")) {
    columns.push("points");
    values.push(mt.base_points);
  }
  if (hasColumn(db, "task", "base_points")) {
    columns.push("base_points");
    values.push(mt.base_points);
  }
  if (hasColumn(db, "task", "status")) {
    columns.push("status");
    values.push("active");
  }
  if (hasColumn(db, "task", "master_task_id")) {
    columns.push("master_task_id");
    values.push(mt.id);
  }
  if (hasColumn(db, "task", "sort_order")) {
    columns.push("sort_order");
    values.push(0);
  }
  if (hasColumn(db, "task", "created_at")) {
    columns.push("created_at");
    values.push(now);
  }
  if (hasColumn(db, "task", "updated_at")) {
    columns.push("updated_at");
    values.push(now);
  }

  const placeholders = columns.map(() => "?");
  const sql = `INSERT INTO task (${columns.map((col) => `"${col}"`).join(", ")}) VALUES (${placeholders.join(", ")})`;
  db.prepare(sql).run(...values);

  res.status(201).json({ id });
});

router.post("/api/admin/families/:familyId/tasks", (req, res) => {
  try {
    const { familyId } = req.params;
    if (!allowFamilyOrMaster(req, familyId)) return res.sendStatus(403);

    const fam = db.prepare("SELECT id FROM family WHERE id = ?").get(String(familyId));
    if (!fam) return res.sendStatus(404);

    const {
      title,
      points = 0,
      description = "",
      icon = null,
      youtube_url = null,
      status = "active"
    } = req.body || {};

    if (!title || !String(title).trim()) {
      return res.status(400).json({ error: "title required" });
    }

    const id = randomUUID();
    const titleText = String(title).trim();
    const pointsValue = Number(points);
    const pointsAmount = Number.isFinite(pointsValue) ? Math.max(pointsValue, 0) : 0;
    const descText = description == null ? "" : String(description);
    const iconValue = icon == null ? null : String(icon).trim() || null;
    const youtubeValue = youtube_url == null ? null : String(youtube_url).trim() || null;
    const normalizedStatus = String(status).trim().toLowerCase() === "inactive" ? "inactive" : "active";

    const cols = ["id", "family_id", "title", "points", "description", "status", "created_at", "updated_at"];
    const vals = ["@id", "@fid", "@title", "@points", "@desc", "@status", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"];

    const taskCols = db.prepare(`PRAGMA table_info(task)`).all().map((r) => r.name);
    if (taskCols.includes("icon")) {
      const statusIndex = cols.indexOf("status");
      cols.splice(statusIndex, 0, "icon");
      vals.splice(statusIndex, 0, "@icon");
    }
    if (taskCols.includes("youtube_url")) {
      const statusIndex = cols.indexOf("status");
      cols.splice(statusIndex, 0, "youtube_url");
      vals.splice(statusIndex, 0, "@yt");
    }
    if (taskCols.includes("is_customized")) {
      cols.push("is_customized");
      vals.push("1");
    }

    const sql = `INSERT INTO task (${cols.join(",")}) VALUES (${vals.join(",")})`;
    db.prepare(sql).run({
      id,
      fid: String(familyId),
      title: titleText,
      points: pointsAmount,
      desc: descText,
      icon: iconValue,
      yt: youtubeValue,
      status: normalizedStatus
    });

    return res.json({ id });
  } catch (e) {
    console.error("create custom task failed:", e);
    return res.sendStatus(500);
  }
});

router.post("/api/admin/families/:familyId/rewards", (req, res) => {
  try {
    const { familyId } = req.params;
    if (!allowFamilyOrMaster(req, familyId)) return res.sendStatus(403);

    const fam = db.prepare("SELECT id FROM family WHERE id = ?").get(String(familyId));
    if (!fam) return res.sendStatus(404);

    const {
      title,
      cost = 0,
      description = "",
      icon = null,
      youtube_url = null,
      status = "active"
    } = req.body || {};

    if (!title || !String(title).trim()) {
      return res.status(400).json({ error: "title required" });
    }

    const id = randomUUID();
    const titleText = String(title).trim();
    const costValue = Number(cost);
    const costAmount = Number.isFinite(costValue) ? Math.max(costValue, 0) : 0;
    const descText = description == null ? "" : String(description);
    const iconValue = icon == null ? null : String(icon).trim() || null;
    const youtubeValue = youtube_url == null ? null : String(youtube_url).trim() || null;
    const normalizedStatus = String(status).trim().toLowerCase() === "inactive" ? "inactive" : "active";

    const cols = ["id", "family_id", "title", "cost", "description", "status", "created_at", "updated_at"];
    const vals = ["@id", "@fid", "@title", "@cost", "@desc", "@status", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"];

    const rewardCols = db.prepare(`PRAGMA table_info(reward)`).all().map((r) => r.name);
    if (rewardCols.includes("icon")) {
      const statusIndex = cols.indexOf("status");
      cols.splice(statusIndex, 0, "icon");
      vals.splice(statusIndex, 0, "@icon");
    }
    if (rewardCols.includes("youtube_url")) {
      const statusIndex = cols.indexOf("status");
      cols.splice(statusIndex, 0, "youtube_url");
      vals.splice(statusIndex, 0, "@yt");
    }
    if (rewardCols.includes("is_customized")) {
      cols.push("is_customized");
      vals.push("1");
    }

    const sql = `INSERT INTO reward (${cols.join(",")}) VALUES (${vals.join(",")})`;
    db.prepare(sql).run({
      id,
      fid: String(familyId),
      title: titleText,
      cost: costAmount,
      desc: descText,
      icon: iconValue,
      yt: youtubeValue,
      status: normalizedStatus
    });

    return res.json({ id });
  } catch (e) {
    console.error("create custom reward failed:", e);
    return res.sendStatus(500);
  }
});

router.delete("/api/admin/families/:id", (req, res, next) => {
  if (req.auth?.role !== "master") return res.sendStatus(403);

  const hardMode = (req.query?.hard || "").toString().trim().toLowerCase();
  if (hardMode === "true") {
    return next();
  }

  const id = (req.params?.id || "").toString().trim();
  if (!id) return res.status(400).json({ error: "invalid id" });
  if (id.toLowerCase() === "default") {
    return res.status(400).json({ error: "cannot delete default family" });
  }

  db.exec("PRAGMA foreign_keys=ON; BEGIN");
  try {
    db.prepare(`DELETE FROM holds   WHERE family_id = ?`).run(id);
    db.prepare(`DELETE FROM history WHERE family_id = ?`).run(id);
    db.prepare(`DELETE FROM reward  WHERE family_id = ?`).run(id);
    db.prepare(`DELETE FROM task    WHERE family_id = ?`).run(id);
    db.prepare(`DELETE FROM member  WHERE family_id = ?`).run(id);
    const info = db.prepare(`DELETE FROM family WHERE id = ?`).run(id);
    db.exec("COMMIT");
    if (!info.changes) {
      return res.status(404).json({ error: "not found" });
    }
    return res.json({ ok: true });
  } catch (err) {
    db.exec("ROLLBACK");
  }

  try {
    db.exec("PRAGMA foreign_keys=OFF; BEGIN");
    db.prepare(`DELETE FROM holds   WHERE family_id = ?`).run(id);
    db.prepare(`DELETE FROM history WHERE family_id = ?`).run(id);
    db.prepare(`DELETE FROM reward  WHERE family_id = ?`).run(id);
    db.prepare(`DELETE FROM task    WHERE family_id = ?`).run(id);
    db.prepare(`DELETE FROM member  WHERE family_id = ?`).run(id);
    const info = db.prepare(`DELETE FROM family WHERE id = ?`).run(id);
    db.exec("COMMIT; PRAGMA foreign_keys=ON;");
    if (!info.changes) {
      return res.status(404).json({ error: "not found" });
    }
    return res.json({ ok: true });
  } catch (err) {
    db.exec("ROLLBACK; PRAGMA foreign_keys=ON;");
    return res.status(500).json({ error: "delete failed" });
  }
});

// HARD DELETE reward and its dependents (master-only)
router.delete("/api/admin/rewards/:id", (req, res) => {
  if (req.auth?.role !== "master") return res.sendStatus(403);

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
