import express from "express";
import { randomUUID } from "node:crypto";
import db from "./db.js";
import { sendMail } from "./email.js";
import { makeFamilyResolver } from "./lib/familyResolver.js";
import { getFamilyById, listKidsByFamilyId } from "./lib/adminDb.js";
import { requireFamilyScope } from "./middleware/requireFamilyScope.js";
import { resolveAdmin, requireCanAccessFamily } from "./middleware/resolveAdmin.js";
import { listFamilies } from "./routes/families.js";
import { listActivity } from "./routes/activity.js";
import { listMembers } from "./routes/members.js";
import { listHolds } from "./routes/holds.js";
import { familyForCurrentAdmin } from "./routes/familiesSelf.js";

const router = express.Router();
const resolveFamily = makeFamilyResolver();

export const scopeMiddleware = (req, res, next) => {
  const rawCandidate =
    req.params?.family ??
    req.params?.familyId ??
    req.query?.family ??
    req.header("x-family") ??
    "";
  const raw = typeof rawCandidate === "string" ? rawCandidate.trim() : "";

  if (!raw) {
    req.family = null;
    req.familyScopeError = null;
    return next();
  }

  try {
    const fam = resolveFamily(raw);
    req.family = fam;
    req.familyScopeError = null;
    return next();
  } catch (error) {
    req.family = null;
    req.familyScopeError = { code: "family_not_found", input: raw };
    return next();
  }
};

function getAdminRole(req) {
  return req.admin?.role || req.auth?.role || null;
}

function resolveRequestedFamilyId(req) {
  const candidates = [
    req.params?.familyId,
    req.params?.family,
    req.query?.familyId,
    req.query?.family_id,
    req.query?.family,
    req.header("x-family"),
    req.admin?.familyId,
    req.admin?.family_id,
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "string") {
      const trimmed = candidate.trim();
      if (trimmed) {
        return trimmed;
      }
    }
  }

  return "";
}

function requireFamilyRecord(req, res, next) {
  const familyId = resolveRequestedFamilyId(req);
  if (!familyId) {
    req.family = null;
    req.familyScopeError = { code: "family_scope_required" };
    return res.status(400).json({ error: "family_scope_required" });
  }

  const database = req.db || db;
  const record = getFamilyById(database, familyId);
  if (!record) {
    req.family = null;
    req.familyScopeError = { code: "family_not_found", input: familyId };
    return res.status(404).json({ error: "family_not_found" });
  }

  const normalized = {
    id: record.id,
    name: record.name ?? null,
    status: record.status ?? null,
    key: record.adminKey ?? null,
    adminKey: record.adminKey ?? null,
    admin_key: record.adminKey ?? null,
    email: record.email ?? null,
  };

  req.family = normalized;
  req.familyScopeError = null;
  res.locals.family = { id: normalized.id, record: normalized };

  next();
}

const familyAccessChain = [resolveAdmin, requireCanAccessFamily, requireFamilyRecord];

function attemptFamilyDelete(id, enforceForeignKeys) {
  const normalized = String(id ?? "").trim();
  if (!normalized) {
    return { status: 400, body: { error: "invalid id" } };
  }
  if (normalized.toLowerCase() === "default") {
    return { status: 400, body: { error: "cannot delete default family" } };
  }

  const begin = enforceForeignKeys ? "PRAGMA foreign_keys=ON; BEGIN" : "PRAGMA foreign_keys=OFF; BEGIN";
  const commit = enforceForeignKeys ? "COMMIT" : "COMMIT; PRAGMA foreign_keys=ON;";
  const rollback = enforceForeignKeys ? "ROLLBACK" : "ROLLBACK; PRAGMA foreign_keys=ON;";

  db.exec(begin);
  try {
    const removed = {
      family: 0,
      members: 0,
      tasks: 0,
      ledger: 0,
      member_task: 0
    };

    db.prepare(`DELETE FROM holds   WHERE family_id = ?`).run(normalized);
    db.prepare(`DELETE FROM history WHERE family_id = ?`).run(normalized);
    db.prepare(`DELETE FROM reward  WHERE family_id = ?`).run(normalized);

    try {
      const ledgerColumns = db.prepare("PRAGMA table_info('ledger')").all().map((col) => col.name);
      const ledgerMemberColumn = ledgerColumns.includes("member_id")
        ? "member_id"
        : ledgerColumns.includes("user_id")
          ? "user_id"
          : null;
      if (ledgerMemberColumn) {
        const ledgerInfo = db
          .prepare(
            `DELETE FROM ledger WHERE family_id = ? OR ${ledgerMemberColumn} IN (SELECT id FROM member WHERE family_id = ?)`
          )
          .run(normalized, normalized);
        removed.ledger = ledgerInfo?.changes ? Number(ledgerInfo.changes) : 0;
      }
    } catch (err) {
      console.warn('[admin] unable to clean ledger entries for family', normalized, err?.message || err);
    }

    try {
      const memberTaskColumns = db.prepare("PRAGMA table_info('member_task')").all().map((col) => col.name);
      const memberTaskMemberColumn = memberTaskColumns.includes("member_id")
        ? "member_id"
        : memberTaskColumns.includes("memberId")
          ? "memberId"
          : null;
      const memberTaskTaskColumn = memberTaskColumns.includes("task_id")
        ? "task_id"
        : memberTaskColumns.includes("taskId")
          ? "taskId"
          : null;
      if (memberTaskMemberColumn && memberTaskTaskColumn) {
        const memberTaskInfo = db
          .prepare(
            `DELETE FROM member_task
              WHERE ${memberTaskMemberColumn} IN (SELECT id FROM member WHERE family_id = ?)
                 OR ${memberTaskTaskColumn} IN (SELECT id FROM task WHERE family_id = ?)`
          )
          .run(normalized, normalized);
        removed.member_task = memberTaskInfo?.changes ? Number(memberTaskInfo.changes) : 0;
      }
    } catch (err) {
      console.warn('[admin] unable to clean member_task entries for family', normalized, err?.message || err);
    }

    const taskInfo = db.prepare(`DELETE FROM task WHERE family_id = ?`).run(normalized);
    removed.tasks = taskInfo?.changes ? Number(taskInfo.changes) : 0;
    const memberInfo = db.prepare(`DELETE FROM member WHERE family_id = ?`).run(normalized);
    removed.members = memberInfo?.changes ? Number(memberInfo.changes) : 0;
    const info = db.prepare(`DELETE FROM family WHERE id = ?`).run(normalized);
    removed.family = info?.changes ? Number(info.changes) : 0;
    db.exec(commit);
    if (!info.changes) {
      return { status: 404, body: { error: "not found" } };
    }
    return { status: 200, body: { removed } };
  } catch (err) {
    db.exec(rollback);
    return null;
  }
}

function hardDeleteFamily(id) {
  const normalized = String(id ?? "").trim();
  if (!normalized) {
    return { status: 400, body: { error: "invalid id" } };
  }
  if (normalized.toLowerCase() === "default") {
    return { status: 400, body: { error: "cannot delete default family" } };
  }

  const first = attemptFamilyDelete(normalized, true);
  if (first) return first;

  const fallback = attemptFamilyDelete(normalized, false);
  if (fallback) return fallback;

  return { status: 500, body: { error: "delete failed" } };
}

router.get("/admin/families/self", familyForCurrentAdmin);

router.get("/admin/families/:family", requireFamilyScope, (req, res) => {
  const { id, key, name, status } = req.family;
  return res.json({ id, key, name, status });
});

router.get("/admin/families", (req, res, next) => {
  if (getAdminRole(req) !== "master") {
    return res.status(403).json({ error: "Forbidden" });
  }

  const requestedId = (req.query.id || "").toString().trim();
  if (requestedId) {
    if (requestedId.toLowerCase() === "default") {
      return res.status(400).json({ error: "default family is reserved" });
    }
    const row = db
      .prepare(
        `SELECT id, admin_key, name, email, status, created_at, updated_at FROM "family" WHERE id = ?`
      )
      .get(requestedId);
    if (!row) return res.status(404).json({ error: "not found" });
    const { admin_key, ...rest } = row;
    const key = admin_key != null ? String(admin_key) : null;
    return res.json({
      ...rest,
      admin_key,
      key,
      family_key: key || ""
    });
  }

  return listFamilies(req, res, next);
});

router.get("/api/admin/members", ...listMembers);
router.get("/admin/members", ...listMembers);

router.get("/api/admin/holds", ...familyAccessChain, listHolds);
router.get("/admin/holds", ...familyAccessChain, listHolds);

router.get("/admin/activity", ...familyAccessChain, (req, res) => {
  return listActivity(req, res);
});

router.get(
  "/admin/families/:familyId/members",
  ...familyAccessChain,
  (req, res) => {
    const database = req.db || db;
    const familyId = req.family?.id || resolveRequestedFamilyId(req);
    try {
      const members = listKidsByFamilyId(database, familyId);
      return res.json({ members });
    } catch (error) {
      console.error("[admin.members] failed to list members", {
        familyId,
        error: error?.message || error,
      });
      return res
        .status(500)
        .json({ error: "server_error", detail: "members_query_failed" });
    }
  }
);

router.get("/admin/families/:familyId/tasks", ...familyAccessChain, (req, res) => {
  const database = req.db || db;
  const familyId = req.family?.id || resolveRequestedFamilyId(req);
  try {
    const rows = database
      .prepare("SELECT * FROM task WHERE family_id = ? ORDER BY updated_at DESC")
      .all(familyId);
    return res.json(rows);
  } catch (error) {
    console.error("[admin.tasks] failed to list tasks", {
      familyId,
      error: error?.message || error,
    });
    return res.status(500).json({ error: "server_error", detail: "tasks_query_failed" });
  }
});

router.get("/admin/families/:familyId/rewards", ...familyAccessChain, (req, res) => {
  const database = req.db || db;
  const familyId = req.family?.id || resolveRequestedFamilyId(req);
  try {
    const rows = database
      .prepare("SELECT * FROM reward WHERE family_id = ? ORDER BY updated_at DESC")
      .all(familyId);
    return res.json(rows);
  } catch (error) {
    console.error("[admin.rewards] failed to list rewards", {
      familyId,
      error: error?.message || error,
    });
    return res
      .status(500)
      .json({ error: "server_error", detail: "rewards_query_failed" });
  }
});

router.get("/admin/families/:familyId/holds", ...familyAccessChain, listHolds);

router.get("/admin/families/:familyId/activity", ...familyAccessChain, (req, res) => {
  return listActivity(req, res);
});

router.patch("/admin/families/:id", express.json(), requireFamilyScope, (req, res) => {
  if (getAdminRole(req) !== "master") return res.sendStatus(403);

  const fam = req.family;
  const familyId = String(fam.id);
  const body = req.body || {};
  const normalizedStatus = typeof body.status === "string" ? body.status.trim().toLowerCase() : "";
  const hardMode = body.hard === true || body.hard === "true";

  if (hardMode && normalizedStatus === "deleted") {
    const result = hardDeleteFamily(familyId);
    return res.status(result.status).json(result.body);
  }

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
  if (normalizedStatus && /^(active|inactive|deleted)$/i.test(normalizedStatus)) {
    fields.push("status = ?");
    args.push(normalizedStatus);
  }

  if (!fields.length) return res.status(400).json({ error: "no fields to update" });

  fields.push(`updated_at = datetime('now')`);
  const sql = `UPDATE "family" SET ${fields.join(", ")} WHERE id = ?`;
  args.push(familyId);

  try {
    const info = db.prepare(sql).run(...args);
    if (info.changes === 0) return res.status(404).json({ error: "not found" });
    const row = db
      .prepare(`SELECT id, name, email, status, created_at, updated_at FROM "family" WHERE id = ?`)
      .get(familyId);
    res.json(row);
  } catch (e) {
    const msg = String(e.message || "");
    if (msg.includes("UNIQUE") && msg.includes("email")) return res.status(409).json({ error: "email already registered" });
    res.status(500).json({ error: "update failed" });
  }
});

// PUBLIC: self-register a new family
router.post("/admin/families/self-register", async (req, res) => {
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
  "/admin/families/forgot-admin-key",
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
router.get(
  "/admin/families/:familyId/master-tasks/available",
  ...familyAccessChain,
  (req, res) => {
    const database = req.db || db;
    const familyId = req.family?.id || resolveRequestedFamilyId(req);
    try {
      const rows = database
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
      return res.json(rows);
    } catch (error) {
      console.error("[admin.masterTasks] failed to list templates", {
        familyId,
        error: error?.message || error,
      });
      return res
        .status(500)
        .json({ error: "server_error", detail: "master_tasks_query_failed" });
    }
  }
);

// Create a family task from a master template
router.post(
  "/admin/families/:familyId/tasks/from-master",
  express.json(),
  ...familyAccessChain,
  (req, res) => {
    const database = req.db || db;
    const familyId = req.family?.id || resolveRequestedFamilyId(req);
    const { master_task_id } = req.body || {};
    if (!master_task_id) {
      return res.status(400).json({ error: "master_task_id required" });
    }

    const mt = database
      .prepare(`SELECT * FROM master_task WHERE id = ? AND status = 'active'`)
      .get(master_task_id);
    if (!mt) {
      return res.status(404).json({ error: "template_not_found" });
    }

    const id = randomUUID();
    const now = Date.now();
    const columns = ["id", "family_id"];
    const values = [id, String(familyId)];

    if (hasColumn(database, "task", "title")) {
      columns.push("title");
      values.push(mt.title);
    }
    if (hasColumn(database, "task", "name")) {
      columns.push("name");
      values.push(mt.title);
    }
    if (hasColumn(database, "task", "description")) {
      columns.push("description");
      values.push(mt.description);
    }
    if (hasColumn(database, "task", "icon")) {
      columns.push("icon");
      values.push(mt.icon);
    }
    if (hasColumn(database, "task", "points")) {
      columns.push("points");
      values.push(mt.base_points);
    }
    if (hasColumn(database, "task", "base_points")) {
      columns.push("base_points");
      values.push(mt.base_points);
    }
    if (hasColumn(database, "task", "status")) {
      columns.push("status");
      values.push("active");
    }
    if (hasColumn(database, "task", "master_task_id")) {
      columns.push("master_task_id");
      values.push(mt.id);
    }
    if (hasColumn(database, "task", "sort_order")) {
      columns.push("sort_order");
      values.push(0);
    }
    if (hasColumn(database, "task", "created_at")) {
      columns.push("created_at");
      values.push(now);
    }
    if (hasColumn(database, "task", "updated_at")) {
      columns.push("updated_at");
      values.push(now);
    }

    try {
      const placeholders = columns.map(() => "?");
      const sql = `INSERT INTO task (${columns.map((col) => `"${col}"`).join(", ")}) VALUES (${placeholders.join(", ")})`;
      database.prepare(sql).run(...values);
      return res.status(201).json({ id });
    } catch (error) {
      console.error("[admin.tasks] failed to create from master", {
        familyId,
        master_task_id,
        error: error?.message || error,
      });
      return res
        .status(500)
        .json({ error: "server_error", detail: "task_from_master_failed" });
    }
  }
);

router.post(
  "/admin/families/:familyId/tasks",
  express.json(),
  ...familyAccessChain,
  (req, res) => {
    try {
      const database = req.db || db;
      const familyId = String(req.family?.id || resolveRequestedFamilyId(req));

      const {
        title,
        points = 0,
        description = "",
        icon = null,
        youtube_url = null,
        status = "active",
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

      const taskCols = database.prepare(`PRAGMA table_info(task)`).all().map((r) => r.name);
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
      database.prepare(sql).run({
        id,
        fid: familyId,
        title: titleText,
        points: pointsAmount,
        desc: descText,
        icon: iconValue,
        yt: youtubeValue,
        status: normalizedStatus,
      });

      return res.json({ id });
    } catch (error) {
      console.error("[admin.tasks] failed to create custom task", error?.message || error);
      return res
        .status(500)
        .json({ error: "server_error", detail: "task_create_failed" });
    }
  }
);

router.post(
  "/admin/families/:familyId/rewards",
  express.json(),
  ...familyAccessChain,
  (req, res) => {
    try {
      const database = req.db || db;
      const familyId = String(req.family?.id || resolveRequestedFamilyId(req));

      const {
        title,
        cost = 0,
        description = "",
        icon = null,
        youtube_url = null,
        status = "active",
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

      const rewardCols = database.prepare(`PRAGMA table_info(reward)`).all().map((r) => r.name);
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
      database.prepare(sql).run({
        id,
        fid: familyId,
        title: titleText,
        cost: costAmount,
        desc: descText,
        icon: iconValue,
        yt: youtubeValue,
        status: normalizedStatus,
      });

      return res.json({ id });
    } catch (error) {
      console.error("[admin.rewards] failed to create custom reward", error?.message || error);
      return res
        .status(500)
        .json({ error: "server_error", detail: "reward_create_failed" });
    }
  }
);

router.delete("/admin/families/:id", requireFamilyScope, (req, res) => {
  if (getAdminRole(req) !== "master") return res.sendStatus(403);

  const hardMode = String(req.query?.hard ?? "").trim().toLowerCase() === "true";
  const fam = req.family;
  const result = hardDeleteFamily(fam.id);
  if (hardMode) {
    return res.status(result.status).json(result.body);
  }
  if (result.status === 200) {
    return res.json({ ok: true });
  }
  return res.status(result.status).json(result.body);
});

// HARD DELETE reward and its dependents (master-only)
router.delete("/admin/rewards/:id", (req, res) => {
  if (getAdminRole(req) !== "master") return res.sendStatus(403);

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

export { router };
export default router;
