// CleverKids Parents Shop API (refactored)
import express from "express";
import { fileURLToPath } from "node:url";
import { dirname, join, sep } from "node:path";
import fs from "node:fs";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import QRCode from "qrcode";
import db, { DATA_DIR, ensureMasterCascadeTriggers, resolveAdminContext } from "./db.js";
import { MULTITENANT_ENFORCE } from "./config.js";
import ledgerRoutes from "./routes/ledger.js";
import apiRouter, { scopeMiddleware } from "./routes.js";
import { balanceOf, recordLedgerEntry } from "./ledger/core.js";
import { generateIcon, knownIcon } from "./iconFactory.js";
import { readAdminKey } from "./auth.js";
import adminAuth from "./middleware/adminAuth.js";
import softAdminAuth from "./middleware/softAdminAuth.js";
import { whoAmI } from "./routes/adminWhoAmI.js";
import { adminLogin } from "./routes/adminLogin.js";
import { familyForCurrentAdmin } from "./routes/familiesSelf.js";
import { getActiveFamilies } from "./models/families.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootPackage = JSON.parse(
  fs.readFileSync(new URL("../package.json", import.meta.url), "utf8")
);

const UPLOAD_DIR = process.env.UPLOAD_DIR || join(DATA_DIR, "uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });
const PARENT_SECRET = (process.env.PARENT_SECRET || "dev-secret-change-me").trim();

if (process.argv.includes("--help")) {
  console.log("Parents Shop API");
  console.log("Usage: node server/index.js [--help]");
  process.exit(0);
}

function applyAdminContext(req, ctx) {
  req.auth = {
    role: ctx.role,
    familyId: ctx.familyId,
    family_id: ctx.familyId ?? null,
    familyKey: ctx.familyKey || null,
    familyName: ctx.familyName || ""
  };
}

function authenticateAdmin(req, res, next) {
  const key = readAdminKey(req);
  if (!key) {
    res.status(401).json({ error: "missing admin key" });
    return;
  }
  const ctx = resolveAdminContext(db, key);
  if (!ctx || ctx.role === "none") {
    res.status(403).json({ error: "invalid key" });
    return;
  }
  applyAdminContext(req, ctx);
  next();
}

function createFamilyScopeResolver(options = {}) {
  const { requireFamilyId = true } = options;

  return function resolveFamilyScope(req, res, next) {
    if (!MULTITENANT_ENFORCE) {
      next();
      return;
    }

    if (!req.auth?.role) {
      res.status(403).json({ error: "unauthorized" });
      return;
    }

    if (req.auth.role === "family") {
      const familyId = req.auth.familyId ?? req.auth.family_id;
      if (!familyId) {
        res.status(403).json({ error: "family scope missing" });
        return;
      }
      req.scope = { family_id: familyId };
      next();
      return;
    }

    if (req.auth.role === "master") {
      const headerScope = (req.header("X-Act-As-Family") || "").toString().trim();
      const queryScope = (req.query?.family_id || "").toString().trim();
      const familyId = headerScope || queryScope;
      if (!familyId) {
        if (requireFamilyId) {
          res.status(400).json({ error: "family_id required" });
          return;
        }
        req.scope = { family_id: null };
        next();
        return;
      }
      const normalizedFamilyId = familyId.toString();
      if (normalizedFamilyId.toLowerCase() === "default") {
        res.status(400).json({ error: "default family is reserved" });
        return;
      }
      req.scope = { family_id: normalizedFamilyId };
      next();
      return;
    }

    res.status(403).json({ error: "unsupported role" });
  };
}

const resolveFamilyScope = createFamilyScopeResolver({ requireFamilyId: true });
const resolveFamilyScopeOptional = createFamilyScopeResolver({ requireFamilyId: false });

function requireMaster(req, res, next) {
  if (req.auth?.role !== "master") {
    res.status(403).json({ error: "master only" });
    return;
  }
  next();
}

function tableHasColumn(table, column) {
  try {
    const rows = db.prepare(`PRAGMA table_info(${table})`).all();
    return rows.some((row) => row.name === column);
  } catch {
    return false;
  }
}

function normalizeMasterStatus(value) {
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "inactive" || normalized === "disabled") {
      return "inactive";
    }
    if (normalized === "active") {
      return "active";
    }
  }
  return "active";
}

function normalizeNullableString(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  return text ? text : null;
}

function coerceInteger(value, fallback = 0) {
  const num = Number(value);
  if (Number.isFinite(num)) {
    return Math.trunc(num);
  }
  return fallback;
}

function quoteIdent(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function getTableColumns(table) {
  try {
    const rows = db.prepare(`PRAGMA table_info(${table})`).all();
    return rows.map(row => row.name);
  } catch {
    return [];
  }
}

function insertRecord(table, record) {
  const columns = Object.keys(record);
  const placeholders = columns.map(col => `@${col}`);
  const sql = `INSERT INTO ${quoteIdent(table)} (${columns.map(quoteIdent).join(", ")}) VALUES (${placeholders.join(", ")})`;
  db.prepare(sql).run(record);
}

function getAdoptedMasterIds(table, masterColumn, familyId) {
  if (!tableExists(table)) {
    return new Set();
  }
  const columns = getTableColumns(table);
  const hasSource = columns.includes('source_template_id');
  if (!columns.includes(masterColumn) && !hasSource) {
    return new Set();
  }
  const selector = hasSource
    ? `COALESCE(${quoteIdent('source_template_id')}, ${columns.includes(masterColumn) ? quoteIdent(masterColumn) : 'NULL'})`
    : quoteIdent(masterColumn);
  let sql = `SELECT ${selector} AS master_id FROM ${quoteIdent(table)} WHERE ${selector} IS NOT NULL`;
  const params = [];
  if (columns.includes("family_id")) {
    sql += " AND family_id = ?";
    params.push(familyId);
  }
  const rows = params.length ? db.prepare(sql).all(...params) : db.prepare(sql).all();
  const result = new Set();
  for (const row of rows) {
    if (row?.master_id) {
      result.add(String(row.master_id));
    }
  }
  return result;
}

function mapMasterTaskRow(row) {
  if (!row) return null;
  const baseValue = Number(row.base_points);
  const version = Number(row.version ?? 1) || 1;
  return {
    id: row.id,
    title: row.title ?? "",
    description: row.description ?? null,
    icon: row.icon ?? null,
    youtube_url: row.youtube_url ?? null,
    base_points: Number.isFinite(baseValue) ? baseValue : 0,
    status: row.status ?? "active",
    version
  };
}

function mapMasterRewardRow(row) {
  if (!row) return null;
  const baseValue = Number(row.base_cost);
  const version = Number(row.version ?? 1) || 1;
  return {
    id: row.id,
    title: row.title ?? "",
    description: row.description ?? null,
    icon: row.icon ?? null,
    youtube_url: row.youtube_url ?? null,
    base_cost: Number.isFinite(baseValue) ? baseValue : 0,
    status: row.status ?? "active",
    version
  };
}

function mapTaskRow(row) {
  if (!row) return null;
  const title = row.title ?? row.name ?? "";
  const pointsValue = Number(row.points ?? row.base_points ?? 0) || 0;
  const basePointsValue = Number(row.base_points ?? row.points ?? 0) || 0;
  const status = (row.status || "active").toString().trim().toLowerCase() || "active";
  const sourceTemplateId = row.source_template_id || row.master_task_id || null;
  const masterTaskId = row.master_task_id || null;
  const scopeRaw = row.scope || null;
  const normalizedScope = scopeRaw
    ? String(scopeRaw).trim().toLowerCase()
    : row.family_id
    ? "family"
    : "global";
  const source = sourceTemplateId ? "master" : row.source || null;
  const sortOrder = Number(row.sort_order ?? 0) || 0;
  const youtubeUrl = row.youtube_url ?? null;
  const masterYoutube = row.master_youtube ?? null;
  const sourceVersion = Number(row.source_version ?? 0) || 0;
  const isCustomized = Number(row.is_customized ?? 0) ? 1 : 0;
  return {
    id: row.id,
    title,
    name: title,
    description: row.description ?? "",
    icon: row.icon ?? null,
    points: pointsValue,
    base_points: basePointsValue,
    status,
    source,
    master_task_id: masterTaskId,
    source_template_id: sourceTemplateId,
    source_version: sourceVersion,
    is_customized: isCustomized,
    scope: normalizedScope || "family",
    family_id: row.family_id || null,
    created_at: Number(row.created_at ?? 0) || null,
    updated_at: Number(row.updated_at ?? 0) || null,
    sort_order: sortOrder,
    youtube_url: youtubeUrl,
    master_youtube: masterYoutube,
    active: status === "active"
  };
}

function ensureEarnTemplatesSchema() {
  const exists = !!db
    .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = 'earn_templates'")
    .get();
  if (!exists) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS earn_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        family_id TEXT NOT NULL,
        title TEXT NOT NULL,
        points INTEGER NOT NULL,
        description TEXT,
        youtube_url TEXT,
        active INTEGER NOT NULL DEFAULT 1,
        sort_order INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);
  }
  if (!tableHasColumn("earn_templates", "family_id")) {
    db.exec("ALTER TABLE earn_templates ADD COLUMN family_id TEXT");
    db.prepare("UPDATE earn_templates SET family_id = @family WHERE family_id IS NULL").run({ family: "default" });
  }
  db.exec("CREATE INDEX IF NOT EXISTS idx_earn_templates_family ON earn_templates(family_id)");
}

ensureEarnTemplatesSchema();

function ensureDismissedTemplateTable() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS dismissed_template (
      family_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      master_id TEXT NOT NULL,
      dismissed_at INTEGER NOT NULL,
      PRIMARY KEY (family_id, kind, master_id)
    );
  `);
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_dismissed_template_family ON dismissed_template(family_id, kind)"
  );
}

ensureDismissedTemplateTable();

const TOKEN_TTL_SEC = Number(process.env.QR_TTL_SEC || 120);
const PORT = process.env.PORT || 4000;

const REFUND_REASON_VALUES = [
  "mis_tap",
  "duplicate",
  "wrong_item",
  "canceled",
  "quality_issue",
  "staff_error"
];
const REFUND_REASONS = new Set(REFUND_REASON_VALUES);

const REFUND_WINDOW_DAYS_RAW = process.env.CK_REFUND_WINDOW_DAYS;
const REFUND_WINDOW_DAYS =
  REFUND_WINDOW_DAYS_RAW === undefined || REFUND_WINDOW_DAYS_RAW === null || REFUND_WINDOW_DAYS_RAW === ""
    ? null
    : Number(REFUND_WINDOW_DAYS_RAW);
const REFUND_WINDOW_MS = Number.isFinite(REFUND_WINDOW_DAYS)
  ? Math.max(0, REFUND_WINDOW_DAYS) * 24 * 60 * 60 * 1000
  : null;

const REFUND_RATE_LIMIT_PER_HOUR = Number(process.env.CK_REFUND_RATE_LIMIT_PER_HOUR || 20);
const REFUND_RATE_WINDOW_MS = 60 * 60 * 1000;
const refundRateLimiter = new Map();

const FEATURE_FLAGS = Object.freeze({
  refunds: (process.env.FEATURE_REFUNDS ?? "true").toString().toLowerCase() !== "false",
  multitenantEnforce: MULTITENANT_ENFORCE
});

let BUILD = (process.env.BUILD_VERSION || process.env.RENDER_GIT_COMMIT || process.env.COMMIT_HASH || "").trim();
if (!BUILD) {
  try {
    BUILD = execSync("git rev-parse --short HEAD").toString().trim();
  } catch {
    BUILD = "";
  }
}
if (!BUILD) {
  BUILD = rootPackage?.version || "dev";
}

const PUBLIC_DIR = join(__dirname, "public");
const versionCache = new Map();
function loadVersioned(file) {
  if (!versionCache.has(file)) {
    const raw = fs.readFileSync(join(PUBLIC_DIR, file), "utf8");
    versionCache.set(file, raw.replace(/__BUILD__/g, BUILD));
  }
  return versionCache.get(file);
}

function sendVersioned(res, file, type = "text/html", cacheControl = "no-store") {
  if (type) res.type(type);
  if (cacheControl) res.set("Cache-Control", cacheControl);
  res.send(loadVersioned(file));
}

function makeKey(len = 24) {
  return Buffer.from(crypto.randomBytes(len)).toString("base64url");
}

function attachCookies(req, _res, next) {
  if (req.cookies && typeof req.cookies === "object") {
    return next();
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
  next();
}

function attachDatabase(req, _res, next) {
  req.db = db;
  next();
}

const app = express();
app.use(express.json({ limit: "4mb" }));
app.use(express.urlencoded({ extended: false }));
app.use(attachCookies);
app.use(attachDatabase);
app.use(scopeMiddleware);

app.post("/api/admin/login", adminLogin);
app.get("/api/admin/whoami", softAdminAuth, whoAmI);
app.get("/api/admin/families/self", familyForCurrentAdmin);

app.use("/api/admin", adminAuth);
app.use("/api", ledgerRoutes);
app.use("/api", apiRouter);

app.get("/api/admin/families", authenticateAdmin, requireMaster, (req, res) => {
  const database = req.db || db;
  const families = getActiveFamilies(database);
  const ids = families.map((family) => family.id);
  console.log("[api.admin.families] returning active families", { ids });
  res.json(families);
});

app.get("/api/admin/families/options", authenticateAdmin, requireMaster, (req, res) => {
  const database = req.db || db;
  const families = getActiveFamilies(database);
  const ids = families.map((family) => family.id);
  console.log("[api.admin.families.options] returning active families", { ids });
  res.json(
    families.map((family) => ({
      id: family.id,
      name: family.name,
      key: family.key,
      email: family.email,
      status: family.status,
    }))
  );
});

app.get("/api/admin/families/:id", authenticateAdmin, (req, res) => {
  const id = (req.params?.id ?? "").toString().trim();
  if (!id) {
    res.status(400).json({ error: "family id required" });
    return;
  }
  if (id.toLowerCase() === "default") {
    res.status(400).json({ error: "default family is reserved" });
    return;
  }
  if (req.auth?.role === "family" && req.auth.familyId !== id) {
    res.status(403).json({ error: "forbidden" });
    return;
  }
  if (req.auth?.role !== "master" && req.auth?.role !== "family") {
    res.status(403).json({ error: "forbidden" });
    return;
  }
  const row = selectFamilyByIdStmt.get(id);
  if (!row) {
    res.status(404).json({ error: "family not found" });
    return;
  }
  const { admin_key, ...rest } = row;
  const key = admin_key != null ? String(admin_key) : null;
  const payload = {
    ...rest,
    admin_key,
    key,
    family_key: key || ""
  };
  res.json(payload);
});

app.post("/api/admin/families", authenticateAdmin, requireMaster, (req, res) => {
  const body = req.body ?? {};
  const name = (body.name ?? "").toString().trim();
  if (!name) {
    res.status(400).json({ error: "name required" });
    return;
  }

  const rawEmail = body.email === undefined || body.email === null ? "" : String(body.email);
  const email = rawEmail.trim().toLowerCase();
  if (email && !/\S+@\S+\.\S+/.test(email)) {
    res.status(400).json({ error: "invalid email" });
    return;
  }

  const providedKey = body.adminKey === undefined || body.adminKey === null ? "" : String(body.adminKey);
  const trimmedKey = providedKey.trim();
  const adminKey = trimmedKey || makeKey();
  const id = crypto.randomUUID();
  const timestamp = new Date().toISOString();

  try {
    insertFamilyStmt.run({
      id,
      name,
      email: email || null,
      status: "active",
      admin_key: adminKey,
      created_at: timestamp,
      updated_at: timestamp
    });
  } catch (err) {
    const message = String(err?.message || "");
    if (message.includes("UNIQUE") && message.includes("admin_key")) {
      res.status(409).json({ error: "adminKey already in use" });
      return;
    }
    if (message.includes("UNIQUE") && message.includes("email")) {
      res.status(409).json({ error: "email already registered" });
      return;
    }
    throw err;
  }

  res.status(201).json({ id, name, email: email || null, adminKey });
});

app.post(
  "/api/admin/families/:id/rotate-key",
  authenticateAdmin,
  requireMaster,
  (req, res) => {
  const id = (req.params?.id ?? "").toString().trim();
  if (!id) {
    res.status(404).json({ error: "not found" });
    return;
  }

  const existing = selectFamilyByIdStmt.get(id);
  if (!existing) {
    res.status(404).json({ error: "not found" });
    return;
  }

  const adminKey = makeKey();
  const updated_at = new Date().toISOString();
  updateFamilyAdminKeyStmt.run({ id, admin_key: adminKey, updated_at });
  res.json({ id, adminKey });
  }
);

app.patch("/api/admin/families/:id", authenticateAdmin, requireMaster, (req, res) => {
  const id = (req.params?.id ?? "").toString().trim();
  if (!id) {
    res.status(400).json({ error: "family id required" });
    return;
  }
  const existing = selectFamilyByIdStmt.get(id);
  if (!existing) {
    res.status(404).json({ error: "family not found" });
    return;
  }
  const body = req.body ?? {};
  const hasName = Object.prototype.hasOwnProperty.call(body, "name");
  const hasStatus = Object.prototype.hasOwnProperty.call(body, "status");
  if (!hasName && !hasStatus) {
    res.status(400).json({ error: "no changes provided" });
    return;
  }
  let name = existing.name;
  if (hasName) {
    name = (body.name ?? "").toString().trim();
    if (!name) {
      res.status(400).json({ error: "name required" });
      return;
    }
  }
  let status = existing.status ?? "active";
  if (hasStatus) {
    const normalizedStatus = (body.status ?? "").toString().trim().toLowerCase();
    if (!normalizedStatus) {
      res.status(400).json({ error: "status required" });
      return;
    }
    status = normalizedStatus;
  }
  const updated_at = new Date().toISOString();
  updateFamilyStmt.run({ id, name, status, updated_at });
  const updated = selectFamilyByIdStmt.get(id);
  if (updated) {
    const { admin_key, ...rest } = updated;
    res.json(rest);
    return;
  }
  res.json({ id, name, status });
});

app.delete("/api/admin/families/:id", authenticateAdmin, requireMaster, (req, res) => {
  const id = (req.params?.id ?? "").toString().trim();
  if (!id) {
    res.status(400).json({ error: "family id required" });
    return;
  }
  if (id === "default") {
    res.status(400).json({ error: "cannot delete default family" });
    return;
  }

  const hardMode = (req.query?.hard ?? "").toString().trim().toLowerCase();
  if (hardMode !== "true") {
    res.status(400).json({ error: "hard=true required" });
    return;
  }

  const existing = selectFamilyByIdStmt.get(id);
  if (!existing) {
    res.status(404).json({ error: "family not found" });
    return;
  }

  const removed = hardDeleteFamilyCascade(id);
  try {
    if (removed.family > 0) {
      db.exec("VACUUM");
    }
  } catch (err) {
    console.warn("[admin] vacuum after hard delete failed", err?.message || err);
  }

  res.json({ removed });
});

app.post("/api/master/tasks", authenticateAdmin, requireMaster, (req, res) => {
  const body = req.body ?? {};
  const title = (body.title ?? "").toString().trim();
  if (!title) {
    res.status(400).json({ error: "title required" });
    return;
  }

  const description = normalizeNullableString(body.description);
  const icon = normalizeNullableString(body.icon);
  const youtube_url = normalizeNullableString(body.youtube_url ?? body.youtubeUrl);
  const base_points = coerceInteger(body.base_points ?? body.basePoints, 0);
  const status = normalizeMasterStatus(body.status);
  const id = crypto.randomUUID();
  const now = Date.now();

  insertMasterTaskStmt.run({
    id,
    title,
    description,
    base_points,
    icon,
    youtube_url,
    status,
    version: 1,
    created_at: now,
    updated_at: now
  });

  res.status(201).json({ item: mapMasterTaskRow({ id, title, description, icon, youtube_url, base_points, status }) });
});

app.get("/api/master/tasks", authenticateAdmin, requireMaster, (_req, res) => {
  const rows = listMasterTasksStmt.all();
  res.json({ items: rows.map(mapMasterTaskRow) });
});

app.patch("/api/master/tasks/:id", authenticateAdmin, requireMaster, (req, res) => {
  const id = (req.params?.id ?? "").toString().trim();
  if (!id) {
    res.status(400).json({ error: "task id required" });
    return;
  }

  const existing = selectMasterTaskStmt.get(id);
  if (!existing) {
    res.status(404).json({ error: "not found" });
    return;
  }

  const body = req.body ?? {};
  let title = existing.title ?? "";
  let description = existing.description ?? null;
  let icon = existing.icon ?? null;
  let youtube_url = existing.youtube_url ?? null;
  let base_points = coerceInteger(existing.base_points, 0);
  let status = existing.status ?? "active";
  let hasChange = false;

  if (Object.prototype.hasOwnProperty.call(body, "title")) {
    const nextTitle = (body.title ?? "").toString().trim();
    if (!nextTitle) {
      res.status(400).json({ error: "title required" });
      return;
    }
    title = nextTitle;
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "description")) {
    description = normalizeNullableString(body.description);
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "icon")) {
    icon = normalizeNullableString(body.icon);
    hasChange = true;
  }

  if (
    Object.prototype.hasOwnProperty.call(body, "youtube_url") ||
    Object.prototype.hasOwnProperty.call(body, "youtubeUrl")
  ) {
    youtube_url = normalizeNullableString(body.youtube_url ?? body.youtubeUrl);
    hasChange = true;
  }

  if (
    Object.prototype.hasOwnProperty.call(body, "base_points") ||
    Object.prototype.hasOwnProperty.call(body, "basePoints")
  ) {
    base_points = coerceInteger(body.base_points ?? body.basePoints, base_points);
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "status")) {
    status = normalizeMasterStatus(body.status);
    hasChange = true;
  }

  if (!hasChange) {
    res.json({ item: mapMasterTaskRow(existing) });
    return;
  }

  const updated_at = Date.now();
  const nextVersion = Number(existing.version ?? 1) + 1;
  updateMasterTaskStmt.run({
    id,
    title,
    description,
    icon,
    youtube_url,
    base_points,
    status,
    version: nextVersion,
    updated_at
  });
  const updated = selectMasterTaskStmt.get(id);

  if (tableExists("task")) {
    const taskColumns = getTableColumns("task");
    if (taskColumns.includes("source_template_id")) {
      const updateParts = [];
      const params = {
        template_id: id,
        version: nextVersion
      };
      const now = Date.now();
      if (taskColumns.includes("title")) {
        updateParts.push("title = @title");
        params.title = title;
      }
      if (taskColumns.includes("name")) {
        updateParts.push("name = @name");
        params.name = title;
      }
      if (taskColumns.includes("description")) {
        updateParts.push("description = @description");
        params.description = description;
      }
      if (taskColumns.includes("icon")) {
        updateParts.push("icon = @icon");
        params.icon = icon;
      }
      if (taskColumns.includes("points")) {
        updateParts.push("points = @points");
        params.points = base_points;
      }
      if (taskColumns.includes("base_points")) {
        updateParts.push("base_points = @base_points");
        params.base_points = base_points;
      }
      if (taskColumns.includes("youtube_url")) {
        updateParts.push("youtube_url = @youtube_url");
        params.youtube_url = youtube_url;
      }
      if (taskColumns.includes("source_version")) {
        updateParts.push("source_version = @version");
      }
      if (taskColumns.includes("updated_at")) {
        updateParts.push("updated_at = @updated_at");
        params.updated_at = now;
      }
      if (updateParts.length) {
        const sql = `
          UPDATE task
             SET ${updateParts.join(", ")}
           WHERE source_template_id = @template_id
             AND (is_customized IS NULL OR is_customized = 0)
        `;
        try {
          db.prepare(sql).run(params);
        } catch (err) {
          console.warn('[master_task] propagate update failed', err?.message || err);
        }
      }
    }
  }

  res.json({ item: mapMasterTaskRow(updated) });
});

app.post("/api/master/rewards", authenticateAdmin, requireMaster, (req, res) => {
  const body = req.body ?? {};
  const title = (body.title ?? "").toString().trim();
  if (!title) {
    res.status(400).json({ error: "title required" });
    return;
  }

  const description = normalizeNullableString(body.description);
  const icon = normalizeNullableString(body.icon);
  const youtube_url = normalizeNullableString(body.youtube_url ?? body.youtubeUrl);
  const base_cost = coerceInteger(body.base_cost ?? body.baseCost, 0);
  const status = normalizeMasterStatus(body.status);
  const id = crypto.randomUUID();
  const now = Date.now();

  insertMasterRewardStmt.run({
    id,
    title,
    description,
    base_cost,
    icon,
    youtube_url,
    status,
    version: 1,
    created_at: now,
    updated_at: now
  });

  res.status(201).json({ item: mapMasterRewardRow({ id, title, description, icon, youtube_url, base_cost, status }) });
});

app.get("/api/master/rewards", authenticateAdmin, requireMaster, (_req, res) => {
  const rows = listMasterRewardsStmt.all();
  res.json({ items: rows.map(mapMasterRewardRow) });
});

app.patch("/api/master/rewards/:id", authenticateAdmin, requireMaster, (req, res) => {
  const id = (req.params?.id ?? "").toString().trim();
  if (!id) {
    res.status(400).json({ error: "reward id required" });
    return;
  }

  const existing = selectMasterRewardStmt.get(id);
  if (!existing) {
    res.status(404).json({ error: "not found" });
    return;
  }

  const body = req.body ?? {};
  let title = existing.title ?? "";
  let description = existing.description ?? null;
  let icon = existing.icon ?? null;
  let youtube_url = existing.youtube_url ?? null;
  let base_cost = coerceInteger(existing.base_cost, 0);
  let status = existing.status ?? "active";
  let hasChange = false;

  if (Object.prototype.hasOwnProperty.call(body, "title")) {
    const nextTitle = (body.title ?? "").toString().trim();
    if (!nextTitle) {
      res.status(400).json({ error: "title required" });
      return;
    }
    title = nextTitle;
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "description")) {
    description = normalizeNullableString(body.description);
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "icon")) {
    icon = normalizeNullableString(body.icon);
    hasChange = true;
  }

  if (
    Object.prototype.hasOwnProperty.call(body, "youtube_url") ||
    Object.prototype.hasOwnProperty.call(body, "youtubeUrl")
  ) {
    youtube_url = normalizeNullableString(body.youtube_url ?? body.youtubeUrl);
    hasChange = true;
  }

  if (
    Object.prototype.hasOwnProperty.call(body, "base_cost") ||
    Object.prototype.hasOwnProperty.call(body, "baseCost")
  ) {
    base_cost = coerceInteger(body.base_cost ?? body.baseCost, base_cost);
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "status")) {
    status = normalizeMasterStatus(body.status);
    hasChange = true;
  }

  if (!hasChange) {
    res.json({ item: mapMasterRewardRow(existing) });
    return;
  }

  const updated_at = Date.now();
  const nextVersion = Number(existing.version ?? 1) + 1;
  updateMasterRewardStmt.run({
    id,
    title,
    description,
    icon,
    youtube_url,
    base_cost,
    status,
    version: nextVersion,
    updated_at
  });
  const updated = selectMasterRewardStmt.get(id);

  if (tableExists("reward")) {
    const rewardColumns = getTableColumns("reward");
    if (rewardColumns.includes("source_template_id")) {
      const updateParts = [];
      const params = {
        template_id: id,
        version: nextVersion
      };
      const now = Date.now();
      if (rewardColumns.includes("name")) {
        updateParts.push("name = @name");
        params.name = title;
      }
      if (rewardColumns.includes("description")) {
        updateParts.push("description = @description");
        params.description = description;
      }
      if (rewardColumns.includes("icon")) {
        updateParts.push("icon = @icon");
        params.icon = icon;
      }
      if (rewardColumns.includes("image_url")) {
        updateParts.push("image_url = @image_url");
        params.image_url = icon;
      }
      if (rewardColumns.includes("cost")) {
        updateParts.push("cost = @cost");
        params.cost = base_cost;
      }
      if (rewardColumns.includes("price")) {
        updateParts.push("price = @price");
        params.price = base_cost;
      }
      if (rewardColumns.includes("youtube_url")) {
        updateParts.push("youtube_url = @youtube_url");
        params.youtube_url = youtube_url;
      }
      if (rewardColumns.includes("source_version")) {
        updateParts.push("source_version = @version");
      }
      if (rewardColumns.includes("updated_at")) {
        updateParts.push("updated_at = @updated_at");
        params.updated_at = now;
      }
      if (updateParts.length) {
        const sql = `
          UPDATE reward
             SET ${updateParts.join(", ")}
           WHERE source_template_id = @template_id
             AND (is_customized IS NULL OR is_customized = 0)
        `;
        try {
          db.prepare(sql).run(params);
        } catch (err) {
          console.warn('[master_reward] propagate update failed', err?.message || err);
        }
      }
    }
  }

  res.json({ item: mapMasterRewardRow(updated) });
});

app.get("/api/admin/templates/tasks", authenticateAdmin, requireMaster, (req, res) => {
  if (!tableExists("task")) {
    res.json({ items: [] });
    return;
  }

  const columns = getTableColumns("task");
  if (!columns.includes("scope")) {
    res.status(500).json({ error: "task_scope_missing" });
    return;
  }

  const statusParam = (req.query?.status || "all").toString().trim().toLowerCase();
  const params = [];
  let sql = "SELECT * FROM task WHERE scope = 'global'";

  if (columns.includes("status")) {
    if (statusParam === "active") {
      sql += " AND status = 'active'";
    } else if (statusParam === "inactive") {
      sql += " AND status = 'inactive'";
    } else if (statusParam && statusParam !== "all") {
      sql += " AND status = ?";
      params.push(statusParam);
    }
  }

  if (columns.includes("updated_at")) {
    sql += " ORDER BY updated_at DESC, id DESC";
  } else {
    sql += " ORDER BY id DESC";
  }

  const rows = params.length ? db.prepare(sql).all(...params) : db.prepare(sql).all();
  res.json({ items: rows.map(mapGlobalTask) });
});

app.post("/api/admin/templates/tasks", authenticateAdmin, requireMaster, (req, res) => {
  if (!tableExists("task")) {
    res.status(500).json({ error: "task_table_missing" });
    return;
  }

  const columns = getTableColumns("task");
  if (!columns.includes("scope")) {
    res.status(500).json({ error: "task_scope_missing" });
    return;
  }
  if (!columns.includes("id")) {
    res.status(500).json({ error: "task_table_missing" });
    return;
  }

  const body = req.body ?? {};
  const title = (body.title ?? "").toString().trim();
  if (!title) {
    res.status(400).json({ error: "title required" });
    return;
  }

  let basePoints = coerceInteger(body.base_points ?? body.basePoints ?? body.points, 0);
  if (!Number.isFinite(basePoints) || basePoints < 0) {
    basePoints = 0;
  }

  const description = normalizeNullableString(body.description);
  const icon = normalizeNullableString(body.icon);
  const youtubeUrl = normalizeNullableString(body.youtube_url ?? body.youtubeUrl);
  const status = normalizeMasterStatus(body.status);

  const now = Date.now();
  const newId = crypto.randomUUID();
  const record = { id: newId };

  if (columns.includes("scope")) record.scope = "global";
  if (columns.includes("family_id")) record.family_id = null;
  if (columns.includes("title")) record.title = title;
  if (columns.includes("name")) record.name = title;
  if (columns.includes("description")) record.description = description;
  if (columns.includes("icon")) record.icon = icon;
  if (columns.includes("points")) record.points = basePoints;
  if (columns.includes("base_points")) record.base_points = basePoints;
  if (columns.includes("status")) record.status = status;
  if (columns.includes("source")) record.source = "master";
  if (columns.includes("master_task_id")) record.master_task_id = null;
  if (columns.includes("source_template_id")) record.source_template_id = null;
  if (columns.includes("source_version")) record.source_version = 1;
  if (columns.includes("is_customized")) record.is_customized = 0;
  if (columns.includes("sort_order")) record.sort_order = 0;
  if (columns.includes("youtube_url")) record.youtube_url = youtubeUrl;
  if (columns.includes("created_at")) record.created_at = now;
  if (columns.includes("updated_at")) record.updated_at = now;

  const keys = Object.keys(record);
  const insertSql = `INSERT INTO task (${keys.map(quoteIdent).join(", ")}) VALUES (${keys
    .map(key => `@${key}`)
    .join(", ")})`;
  db.prepare(insertSql).run(record);

  const inserted = db.prepare("SELECT * FROM task WHERE id = ?").get(newId);
  res.status(201).json({ item: mapGlobalTask(inserted) });
});

app.patch("/api/admin/templates/tasks/:id", authenticateAdmin, requireMaster, (req, res) => {
  if (!tableExists("task")) {
    res.status(404).json({ error: "not_found" });
    return;
  }

  const columns = getTableColumns("task");
  if (!columns.includes("scope")) {
    res.status(500).json({ error: "task_scope_missing" });
    return;
  }

  const id = (req.params?.id ?? "").toString().trim();
  if (!id) {
    res.status(400).json({ error: "task id required" });
    return;
  }

  const existing = db.prepare("SELECT * FROM task WHERE id = ? AND scope = 'global'").get(id);
  if (!existing) {
    res.status(404).json({ error: "not_found" });
    return;
  }

  const body = req.body ?? {};
  let title = existing.title ?? existing.name ?? "";
  let description = existing.description ?? null;
  let icon = existing.icon ?? null;
  let youtubeUrl = existing.youtube_url ?? null;
  let basePoints = coerceInteger(existing.base_points ?? existing.points ?? 0, 0);
  if (!Number.isFinite(basePoints) || basePoints < 0) basePoints = 0;
  let status = existing.status ?? "active";
  let hasChange = false;

  if (Object.prototype.hasOwnProperty.call(body, "title")) {
    const nextTitle = (body.title ?? "").toString().trim();
    if (!nextTitle) {
      res.status(400).json({ error: "title required" });
      return;
    }
    title = nextTitle;
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "description")) {
    description = normalizeNullableString(body.description);
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "icon")) {
    icon = normalizeNullableString(body.icon);
    hasChange = true;
  }

  if (
    Object.prototype.hasOwnProperty.call(body, "youtube_url") ||
    Object.prototype.hasOwnProperty.call(body, "youtubeUrl")
  ) {
    youtubeUrl = normalizeNullableString(body.youtube_url ?? body.youtubeUrl);
    hasChange = true;
  }

  if (
    Object.prototype.hasOwnProperty.call(body, "base_points") ||
    Object.prototype.hasOwnProperty.call(body, "basePoints") ||
    Object.prototype.hasOwnProperty.call(body, "points")
  ) {
    const nextPoints = coerceInteger(body.base_points ?? body.basePoints ?? body.points, basePoints);
    basePoints = nextPoints < 0 ? 0 : nextPoints;
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "status")) {
    status = normalizeMasterStatus(body.status);
    hasChange = true;
  }

  if (!hasChange) {
    res.json({ item: mapGlobalTask(existing) });
    return;
  }

  const now = Date.now();
  const nextVersion = columns.includes("source_version")
    ? Number(existing.source_version ?? 1) + 1
    : null;

  const updateFields = [];
  const params = { id };

  if (columns.includes("title")) {
    updateFields.push("title = @title");
    params.title = title;
  }
  if (columns.includes("name")) {
    updateFields.push("name = @name");
    params.name = title;
  }
  if (columns.includes("description")) {
    updateFields.push("description = @description");
    params.description = description;
  }
  if (columns.includes("icon")) {
    updateFields.push("icon = @icon");
    params.icon = icon;
  }
  if (columns.includes("points")) {
    updateFields.push("points = @points");
    params.points = basePoints;
  }
  if (columns.includes("base_points")) {
    updateFields.push("base_points = @base_points");
    params.base_points = basePoints;
  }
  if (columns.includes("status")) {
    updateFields.push("status = @status");
    params.status = status;
  }
  if (columns.includes("youtube_url")) {
    updateFields.push("youtube_url = @youtube_url");
    params.youtube_url = youtubeUrl;
  }
  if (columns.includes("source")) {
    updateFields.push("source = @source");
    params.source = "master";
  }
  if (columns.includes("source_version") && nextVersion !== null) {
    updateFields.push("source_version = @source_version");
    params.source_version = nextVersion;
  }
  if (columns.includes("updated_at")) {
    updateFields.push("updated_at = @updated_at");
    params.updated_at = now;
  }

  const updateSql = `UPDATE task SET ${updateFields.join(", ")} WHERE id = @id`;
  db.prepare(updateSql).run(params);

  const updated = db.prepare("SELECT * FROM task WHERE id = ?").get(id);

  if (columns.includes("source_template_id")) {
    const propagateParts = [];
    const propagateParams = {
      template_id: id,
      version: nextVersion ?? Number(existing.source_version ?? 0)
    };

    if (columns.includes("title")) {
      propagateParts.push("title = @prop_title");
      propagateParams.prop_title = title;
    }
    if (columns.includes("name")) {
      propagateParts.push("name = @prop_name");
      propagateParams.prop_name = title;
    }
    if (columns.includes("description")) {
      propagateParts.push("description = @prop_description");
      propagateParams.prop_description = description;
    }
    if (columns.includes("icon")) {
      propagateParts.push("icon = @prop_icon");
      propagateParams.prop_icon = icon;
    }
    if (columns.includes("points")) {
      propagateParts.push("points = @prop_points");
      propagateParams.prop_points = basePoints;
    }
    if (columns.includes("base_points")) {
      propagateParts.push("base_points = @prop_base_points");
      propagateParams.prop_base_points = basePoints;
    }
    if (columns.includes("youtube_url")) {
      propagateParts.push("youtube_url = @prop_youtube");
      propagateParams.prop_youtube = youtubeUrl;
    }
    if (columns.includes("source_version") && nextVersion !== null) {
      propagateParts.push("source_version = @version");
    }
    if (columns.includes("updated_at")) {
      propagateParts.push("updated_at = @prop_updated_at");
      propagateParams.prop_updated_at = now;
    }

    if (propagateParts.length) {
      const propagateSql = `
        UPDATE task
           SET ${propagateParts.join(", ")}
         WHERE source_template_id = @template_id
           AND (is_customized IS NULL OR is_customized = 0)
           AND (scope IS NULL OR scope <> 'global')
      `;
      try {
        db.prepare(propagateSql).run(propagateParams);
      } catch (err) {
        console.warn("[admin.templates] propagate task update failed", err?.message || err);
      }
    }
  }

  res.json({ item: mapGlobalTask(updated) });
});

app.post(
  "/api/admin/templates/tasks/:id/propagate",
  authenticateAdmin,
  requireMaster,
  (req, res) => {
    if (!tableExists("task")) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const columns = getTableColumns("task");
    if (!columns.includes("scope") || !columns.includes("family_id")) {
      res.status(500).json({ error: "task_scope_missing" });
      return;
    }

    const id = (req.params?.id ?? "").toString().trim();
    if (!id) {
      res.status(400).json({ error: "task id required" });
      return;
    }

    const template = db
      .prepare("SELECT * FROM task WHERE id = ? AND scope = 'global'")
      .get(id);
    if (!template) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const templateStatus = (template.status || "active").toString().trim().toLowerCase();
    if (templateStatus !== "active") {
      res.status(409).json({ error: "template_inactive" });
      return;
    }

    const payload = req.body ?? {};
    const rawFamilies = Array.isArray(payload.familyIds)
      ? payload.familyIds
      : Array.isArray(payload.family_ids)
      ? payload.family_ids
      : Array.isArray(payload.families)
      ? payload.families
      : [];

    const normalizedFamilies = Array.from(
      new Set(
        rawFamilies
          .map(value => (value === null || value === undefined ? "" : String(value).trim()))
          .filter(Boolean)
      )
    );

    if (!normalizedFamilies.length) {
      res.status(400).json({ error: "family_ids required" });
      return;
    }

    const now = Date.now();
    const inserted = [];
    const skipped = [];

    const tx = db.transaction(targetFamilies => {
      for (const familyId of targetFamilies) {
        if (!familyId) continue;

        let duplicate = null;
        if (columns.includes("source_template_id")) {
          duplicate = db
            .prepare(
              `SELECT id FROM task WHERE family_id = ? AND source_template_id = ? LIMIT 1`
            )
            .get(familyId, id);
        }
        if (!duplicate && columns.includes("master_task_id")) {
          duplicate = db
            .prepare(`SELECT id FROM task WHERE family_id = ? AND master_task_id = ? LIMIT 1`)
            .get(familyId, id);
        }
        if (duplicate?.id) {
          skipped.push({ family_id: familyId, task_id: duplicate.id });
          continue;
        }

        const newId = crypto.randomUUID();
        const record = { id: newId };

        if (columns.includes("scope")) record.scope = "family";
        if (columns.includes("family_id")) record.family_id = familyId;
        if (columns.includes("title")) record.title = template.title ?? template.name ?? "";
        if (columns.includes("name")) record.name = template.name ?? template.title ?? "";
        if (columns.includes("description")) record.description = template.description ?? null;
        if (columns.includes("icon")) record.icon = template.icon ?? null;
        const templatePoints = Number(template.points ?? template.base_points ?? 0) || 0;
        if (columns.includes("points")) record.points = templatePoints;
        if (columns.includes("base_points")) record.base_points = template.base_points ?? templatePoints;
        if (columns.includes("status")) record.status = template.status ?? "active";
        if (columns.includes("source")) record.source = "master";
        if (columns.includes("master_task_id")) record.master_task_id = template.master_task_id ?? null;
        if (columns.includes("source_template_id")) record.source_template_id = template.id;
        if (columns.includes("source_version")) record.source_version = template.source_version ?? 1;
        if (columns.includes("is_customized")) record.is_customized = 0;
        if (columns.includes("sort_order")) record.sort_order = 0;
        if (columns.includes("youtube_url")) record.youtube_url = template.youtube_url ?? null;
        if (columns.includes("created_at")) record.created_at = now;
        if (columns.includes("updated_at")) record.updated_at = now;

        const keys = Object.keys(record);
        const insertSql = `INSERT INTO task (${keys.map(quoteIdent).join(", ")}) VALUES (${keys
          .map(key => `@${key}`)
          .join(", ")})`;
        db.prepare(insertSql).run(record);
        inserted.push({ family_id: familyId, task_id: newId });

        deleteDismissedTemplateStmt.run({ family_id: familyId, kind: "task", master_id: id });
      }
    });

    tx(normalizedFamilies);

    res.json({
      inserted,
      skipped,
      totalInserted: inserted.length,
      totalSkipped: skipped.length
    });
  }
);

app.get("/api/admin/templates/rewards", authenticateAdmin, requireMaster, (req, res) => {
  if (!tableExists("reward")) {
    res.json({ items: [] });
    return;
  }

  const columns = getTableColumns("reward");
  if (!columns.includes("scope")) {
    res.status(500).json({ error: "reward_scope_missing" });
    return;
  }

  const statusParam = (req.query?.status || "all").toString().trim().toLowerCase();
  const params = [];
  let sql = "SELECT * FROM reward WHERE scope = 'global'";

  if (columns.includes("status")) {
    if (statusParam === "active") {
      sql += " AND status = 'active'";
    } else if (statusParam === "inactive") {
      sql += " AND status = 'inactive'";
    } else if (statusParam && statusParam !== "all") {
      sql += " AND status = ?";
      params.push(statusParam);
    }
  }

  if (columns.includes("updated_at")) {
    sql += " ORDER BY updated_at DESC, id DESC";
  } else {
    sql += " ORDER BY id DESC";
  }

  const rows = params.length ? db.prepare(sql).all(...params) : db.prepare(sql).all();
  res.json({ items: rows.map(mapGlobalReward) });
});

app.post("/api/admin/templates/rewards", authenticateAdmin, requireMaster, (req, res) => {
  if (!tableExists("reward")) {
    res.status(500).json({ error: "reward_table_missing" });
    return;
  }

  const columns = getTableColumns("reward");
  if (!columns.includes("scope")) {
    res.status(500).json({ error: "reward_scope_missing" });
    return;
  }
  if (!columns.includes("id")) {
    res.status(500).json({ error: "reward_table_missing" });
    return;
  }

  const body = req.body ?? {};
  const title = (body.title ?? body.name ?? "").toString().trim();
  if (!title) {
    res.status(400).json({ error: "title required" });
    return;
  }

  let baseCost = coerceInteger(body.base_cost ?? body.baseCost ?? body.cost ?? body.price, 0);
  if (!Number.isFinite(baseCost) || baseCost < 0) {
    baseCost = 0;
  }

  const description = normalizeNullableString(body.description);
  const icon = normalizeNullableString(body.icon ?? body.image_url ?? body.imageUrl);
  const youtubeUrl = normalizeNullableString(body.youtube_url ?? body.youtubeUrl);
  const status = normalizeMasterStatus(body.status);

  const now = Date.now();
  const newId = crypto.randomUUID();
  const record = { id: newId };

  if (columns.includes("scope")) record.scope = "global";
  if (columns.includes("family_id")) record.family_id = null;
  if (columns.includes("name")) record.name = title;
  if (columns.includes("title")) record.title = title;
  if (columns.includes("description")) record.description = description ?? "";
  if (columns.includes("icon")) record.icon = icon;
  if (columns.includes("image_url")) record.image_url = icon ?? "";
  if (columns.includes("cost")) record.cost = baseCost;
  if (columns.includes("price")) record.price = baseCost;
  if (columns.includes("status")) record.status = status;
  if (columns.includes("source")) record.source = "master";
  if (columns.includes("master_reward_id")) record.master_reward_id = null;
  if (columns.includes("source_template_id")) record.source_template_id = null;
  if (columns.includes("source_version")) record.source_version = 1;
  if (columns.includes("is_customized")) record.is_customized = 0;
  if (columns.includes("youtube_url")) record.youtube_url = youtubeUrl ?? "";
  if (columns.includes("created_at")) record.created_at = now;
  if (columns.includes("updated_at")) record.updated_at = now;

  const keys = Object.keys(record);
  const insertSql = `INSERT INTO reward (${keys.map(quoteIdent).join(", ")}) VALUES (${keys
    .map(key => `@${key}`)
    .join(", ")})`;
  db.prepare(insertSql).run(record);

  const inserted = db.prepare("SELECT * FROM reward WHERE id = ?").get(newId);
  res.status(201).json({ item: mapGlobalReward(inserted) });
});

app.patch("/api/admin/templates/rewards/:id", authenticateAdmin, requireMaster, (req, res) => {
  if (!tableExists("reward")) {
    res.status(404).json({ error: "not_found" });
    return;
  }

  const columns = getTableColumns("reward");
  if (!columns.includes("scope")) {
    res.status(500).json({ error: "reward_scope_missing" });
    return;
  }

  const id = (req.params?.id ?? "").toString().trim();
  if (!id) {
    res.status(400).json({ error: "reward id required" });
    return;
  }

  const existing = db.prepare("SELECT * FROM reward WHERE id = ? AND scope = 'global'").get(id);
  if (!existing) {
    res.status(404).json({ error: "not_found" });
    return;
  }

  const body = req.body ?? {};
  let title = existing.title ?? existing.name ?? "";
  let description = existing.description ?? "";
  let icon = existing.icon ?? existing.image_url ?? null;
  let youtubeUrl = existing.youtube_url ?? null;
  let baseCost = coerceInteger(existing.base_cost ?? existing.cost ?? existing.price ?? 0, 0);
  if (!Number.isFinite(baseCost) || baseCost < 0) baseCost = 0;
  let status = existing.status ?? "active";
  let hasChange = false;

  if (Object.prototype.hasOwnProperty.call(body, "title")) {
    const nextTitle = (body.title ?? body.name ?? "").toString().trim();
    if (!nextTitle) {
      res.status(400).json({ error: "title required" });
      return;
    }
    title = nextTitle;
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "description")) {
    description = normalizeNullableString(body.description) ?? "";
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "icon")) {
    icon = normalizeNullableString(body.icon ?? body.image_url ?? body.imageUrl);
    hasChange = true;
  }

  if (
    Object.prototype.hasOwnProperty.call(body, "youtube_url") ||
    Object.prototype.hasOwnProperty.call(body, "youtubeUrl")
  ) {
    youtubeUrl = normalizeNullableString(body.youtube_url ?? body.youtubeUrl);
    hasChange = true;
  }

  if (
    Object.prototype.hasOwnProperty.call(body, "base_cost") ||
    Object.prototype.hasOwnProperty.call(body, "baseCost") ||
    Object.prototype.hasOwnProperty.call(body, "cost") ||
    Object.prototype.hasOwnProperty.call(body, "price")
  ) {
    const nextCost = coerceInteger(
      body.base_cost ?? body.baseCost ?? body.cost ?? body.price,
      baseCost
    );
    baseCost = nextCost < 0 ? 0 : nextCost;
    hasChange = true;
  }

  if (Object.prototype.hasOwnProperty.call(body, "status")) {
    status = normalizeMasterStatus(body.status);
    hasChange = true;
  }

  if (!hasChange) {
    res.json({ item: mapGlobalReward(existing) });
    return;
  }

  const now = Date.now();
  const nextVersion = columns.includes("source_version")
    ? Number(existing.source_version ?? 1) + 1
    : null;

  const updateFields = [];
  const params = { id };

  if (columns.includes("title")) {
    updateFields.push("title = @title");
    params.title = title;
  }
  if (columns.includes("name")) {
    updateFields.push("name = @name");
    params.name = title;
  }
  if (columns.includes("description")) {
    updateFields.push("description = @description");
    params.description = description ?? "";
  }
  if (columns.includes("icon")) {
    updateFields.push("icon = @icon");
    params.icon = icon;
  }
  if (columns.includes("image_url")) {
    updateFields.push("image_url = @image_url");
    params.image_url = icon ?? "";
  }
  if (columns.includes("cost")) {
    updateFields.push("cost = @cost");
    params.cost = baseCost;
  }
  if (columns.includes("price")) {
    updateFields.push("price = @price");
    params.price = baseCost;
  }
  if (columns.includes("status")) {
    updateFields.push("status = @status");
    params.status = status;
  }
  if (columns.includes("youtube_url")) {
    updateFields.push("youtube_url = @youtube_url");
    params.youtube_url = youtubeUrl ?? "";
  }
  if (columns.includes("source")) {
    updateFields.push("source = @source");
    params.source = "master";
  }
  if (columns.includes("source_version") && nextVersion !== null) {
    updateFields.push("source_version = @source_version");
    params.source_version = nextVersion;
  }
  if (columns.includes("updated_at")) {
    updateFields.push("updated_at = @updated_at");
    params.updated_at = now;
  }

  const updateSql = `UPDATE reward SET ${updateFields.join(", ")} WHERE id = @id`;
  db.prepare(updateSql).run(params);

  const updated = db.prepare("SELECT * FROM reward WHERE id = ?").get(id);

  if (columns.includes("source_template_id")) {
    const propagateParts = [];
    const propagateParams = {
      template_id: id,
      version: nextVersion ?? Number(existing.source_version ?? 0)
    };

    if (columns.includes("name")) {
      propagateParts.push("name = @prop_name");
      propagateParams.prop_name = title;
    }
    if (columns.includes("title")) {
      propagateParts.push("title = @prop_title");
      propagateParams.prop_title = title;
    }
    if (columns.includes("description")) {
      propagateParts.push("description = @prop_description");
      propagateParams.prop_description = description ?? "";
    }
    if (columns.includes("icon")) {
      propagateParts.push("icon = @prop_icon");
      propagateParams.prop_icon = icon;
    }
    if (columns.includes("image_url")) {
      propagateParts.push("image_url = @prop_image_url");
      propagateParams.prop_image_url = icon ?? "";
    }
    if (columns.includes("cost")) {
      propagateParts.push("cost = @prop_cost");
      propagateParams.prop_cost = baseCost;
    }
    if (columns.includes("price")) {
      propagateParts.push("price = @prop_price");
      propagateParams.prop_price = baseCost;
    }
    if (columns.includes("youtube_url")) {
      propagateParts.push("youtube_url = @prop_youtube");
      propagateParams.prop_youtube = youtubeUrl ?? "";
    }
    if (columns.includes("source_version") && nextVersion !== null) {
      propagateParts.push("source_version = @version");
    }
    if (columns.includes("updated_at")) {
      propagateParts.push("updated_at = @prop_updated_at");
      propagateParams.prop_updated_at = now;
    }

    if (propagateParts.length) {
      const propagateSql = `
        UPDATE reward
           SET ${propagateParts.join(", ")}
         WHERE source_template_id = @template_id
           AND (is_customized IS NULL OR is_customized = 0)
           AND (scope IS NULL OR scope <> 'global')
      `;
      try {
        db.prepare(propagateSql).run(propagateParams);
      } catch (err) {
        console.warn("[admin.templates] propagate reward update failed", err?.message || err);
      }
    }
  }

  res.json({ item: mapGlobalReward(updated) });
});

app.post(
  "/api/admin/templates/rewards/:id/propagate",
  authenticateAdmin,
  requireMaster,
  (req, res) => {
    if (!tableExists("reward")) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const columns = getTableColumns("reward");
    if (!columns.includes("scope") || !columns.includes("family_id")) {
      res.status(500).json({ error: "reward_scope_missing" });
      return;
    }

    const id = (req.params?.id ?? "").toString().trim();
    if (!id) {
      res.status(400).json({ error: "reward id required" });
      return;
    }

    const template = db
      .prepare("SELECT * FROM reward WHERE id = ? AND scope = 'global'")
      .get(id);
    if (!template) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const templateStatus = (template.status || "active").toString().trim().toLowerCase();
    if (templateStatus !== "active") {
      res.status(409).json({ error: "template_inactive" });
      return;
    }

    const payload = req.body ?? {};
    const rawFamilies = Array.isArray(payload.familyIds)
      ? payload.familyIds
      : Array.isArray(payload.family_ids)
      ? payload.family_ids
      : Array.isArray(payload.families)
      ? payload.families
      : [];

    const normalizedFamilies = Array.from(
      new Set(
        rawFamilies
          .map(value => (value === null || value === undefined ? "" : String(value).trim()))
          .filter(Boolean)
      )
    );

    if (!normalizedFamilies.length) {
      res.status(400).json({ error: "family_ids required" });
      return;
    }

    const now = Date.now();
    const inserted = [];
    const skipped = [];

    const tx = db.transaction(targetFamilies => {
      for (const familyId of targetFamilies) {
        if (!familyId) continue;

        let duplicate = null;
        if (columns.includes("source_template_id")) {
          duplicate = db
            .prepare(
              `SELECT id FROM reward WHERE family_id = ? AND source_template_id = ? LIMIT 1`
            )
            .get(familyId, id);
        }
        if (!duplicate && columns.includes("master_reward_id")) {
          duplicate = db
            .prepare(`SELECT id FROM reward WHERE family_id = ? AND master_reward_id = ? LIMIT 1`)
            .get(familyId, id);
        }
        if (duplicate?.id) {
          skipped.push({ family_id: familyId, reward_id: duplicate.id });
          continue;
        }

        const newId = crypto.randomUUID();
        const record = { id: newId };

        if (columns.includes("scope")) record.scope = "family";
        if (columns.includes("family_id")) record.family_id = familyId;
        if (columns.includes("name")) record.name = template.name ?? template.title ?? "";
        if (columns.includes("title")) record.title = template.title ?? template.name ?? "";
        if (columns.includes("description")) record.description = template.description ?? "";
        if (columns.includes("icon")) record.icon = template.icon ?? null;
        if (columns.includes("image_url")) record.image_url = template.image_url ?? template.icon ?? "";
        const templateCost = Number(template.cost ?? template.price ?? template.base_cost ?? 0) || 0;
        if (columns.includes("cost")) record.cost = templateCost;
        if (columns.includes("price")) record.price = templateCost;
        if (columns.includes("status")) record.status = template.status ?? "active";
        if (columns.includes("source")) record.source = "master";
        if (columns.includes("master_reward_id")) record.master_reward_id = template.master_reward_id ?? null;
        if (columns.includes("source_template_id")) record.source_template_id = template.id;
        if (columns.includes("source_version")) record.source_version = template.source_version ?? 1;
        if (columns.includes("is_customized")) record.is_customized = 0;
        if (columns.includes("youtube_url")) record.youtube_url = template.youtube_url ?? "";
        if (columns.includes("created_at")) record.created_at = now;
        if (columns.includes("updated_at")) record.updated_at = now;

        const keys = Object.keys(record);
        const insertSql = `INSERT INTO reward (${keys.map(quoteIdent).join(", ")}) VALUES (${keys
          .map(key => `@${key}`)
          .join(", ")})`;
        db.prepare(insertSql).run(record);
        inserted.push({ family_id: familyId, reward_id: newId });

        deleteDismissedTemplateStmt.run({ family_id: familyId, kind: "reward", master_id: id });
      }
    });

    tx(normalizedFamilies);

    res.json({
      inserted,
      skipped,
      totalInserted: inserted.length,
      totalSkipped: skipped.length
    });
  }
);

app.get("/api/family/pending/templates", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const familyId = req.scope?.family_id;
  if (!familyId) {
    res.status(400).json({ error: "family_id required" });
    return;
  }

  try {
    const dismissed = listDismissedTemplatesStmt.all(familyId);
    const dismissedSet = new Set(dismissed.map(row => `${row.kind}:${row.master_id}`));
    const items = [];

    if (tableExists("task")) {
      const taskColumns = getTableColumns("task");
      if (taskColumns.includes("scope")) {
        const adoptedTaskIds = taskColumns.includes("family_id")
          ? db
              .prepare(
                `SELECT source_template_id, master_task_id
                   FROM task
                  WHERE family_id = ?`
              )
              .all(familyId)
              .reduce((set, row) => {
                if (row?.source_template_id) set.add(String(row.source_template_id));
                if (row?.master_task_id) set.add(String(row.master_task_id));
                return set;
              }, new Set())
          : new Set();

        const globalTasks = db
          .prepare("SELECT * FROM task WHERE scope = 'global'")
          .all();
        for (const row of globalTasks) {
          if (!row?.id) continue;
          const status = (row.status || "active").toString().trim().toLowerCase();
          if (status !== "active") continue;
          const templateId = String(row.id);
          if (adoptedTaskIds.has(templateId)) continue;
          if (row.master_task_id && adoptedTaskIds.has(String(row.master_task_id))) continue;
          if (dismissedSet.has(`task:${templateId}`)) continue;
          items.push({
            kind: "task",
            master_id: templateId,
            title: row.title ?? row.name ?? "",
            description: row.description ?? null,
            icon: row.icon ?? null,
            base_points: Number(row.points ?? row.base_points ?? 0) || 0
          });
        }
      }
    }

    if (tableExists("reward")) {
      const rewardColumns = getTableColumns("reward");
      if (rewardColumns.includes("scope")) {
        const adoptedRewardIds = rewardColumns.includes("family_id")
          ? db
              .prepare(
                `SELECT source_template_id, master_reward_id
                   FROM reward
                  WHERE family_id = ?`
              )
              .all(familyId)
              .reduce((set, row) => {
                if (row?.source_template_id) set.add(String(row.source_template_id));
                if (row?.master_reward_id) set.add(String(row.master_reward_id));
                return set;
              }, new Set())
          : new Set();

        const globalRewards = db
          .prepare("SELECT * FROM reward WHERE scope = 'global'")
          .all();
        for (const row of globalRewards) {
          if (!row?.id) continue;
          const status = (row.status || "active").toString().trim().toLowerCase();
          if (status !== "active") continue;
          const templateId = String(row.id);
          if (adoptedRewardIds.has(templateId)) continue;
          if (row.master_reward_id && adoptedRewardIds.has(String(row.master_reward_id))) continue;
          if (dismissedSet.has(`reward:${templateId}`)) continue;
          items.push({
            kind: "reward",
            master_id: templateId,
            title: row.title ?? row.name ?? "",
            description: row.description ?? null,
            icon: row.icon ?? row.image_url ?? null,
            base_cost: Number(row.cost ?? row.price ?? row.base_cost ?? 0) || 0
          });
        }
      }
    }

    res.json({ items });
  } catch (err) {
    console.error("[family.pending] failed", err);
    res.status(500).json({ error: "pending_templates_failed" });
  }
});

app.post("/api/family/adopt", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const familyId = req.scope?.family_id;
  if (!familyId) {
    res.status(400).json({ error: "family_id required" });
    return;
  }

  const body = req.body ?? {};
  const kind = (body.kind ?? "").toString().trim().toLowerCase();
  const masterId = (body.master_id ?? "").toString().trim();
  if (!kind || !masterId) {
    res.status(400).json({ error: "kind_and_master_id_required" });
    return;
  }
  if (kind !== "task" && kind !== "reward") {
    res.status(400).json({ error: "unsupported_kind" });
    return;
  }

  const now = Date.now();

  try {
    if (kind === "task") {
      if (!tableExists("task")) {
        res.status(500).json({ error: "task_table_missing" });
        return;
      }
      const columns = getTableColumns("task");
      if (!columns.includes("scope")) {
        res.status(500).json({ error: "task_scope_missing" });
        return;
      }
      const template = db
        .prepare("SELECT * FROM task WHERE id = ? AND scope = 'global'")
        .get(masterId);
      if (!template) {
        res.status(404).json({ error: "template_not_found" });
        return;
      }
      const templateStatus = (template.status || "active").toString().trim().toLowerCase();
      if (templateStatus !== "active") {
        res.status(409).json({ error: "template_inactive" });
        return;
      }

      const existing = columns.includes("family_id")
        ? db
            .prepare(
              `SELECT source_template_id, master_task_id
                 FROM task
                WHERE family_id = ?`
            )
            .all(familyId)
        : [];
      for (const row of existing) {
        if (row?.source_template_id && String(row.source_template_id) === masterId) {
          res.status(409).json({ error: "already_adopted" });
          return;
        }
        if (row?.master_task_id && String(row.master_task_id) === masterId) {
          res.status(409).json({ error: "already_adopted" });
          return;
        }
      }

      const record = {};
      const newId = crypto.randomUUID();
      record.id = newId;
      const title = template.title ?? template.name ?? "";
      if (columns.includes("scope")) record.scope = "family";
      if (columns.includes("title")) record.title = title;
      if (columns.includes("name")) record.name = title;
      if (columns.includes("description")) record.description = template.description ?? null;
      if (columns.includes("icon")) record.icon = template.icon ?? null;
      const basePoints = Number(template.points ?? template.base_points ?? 0) || 0;
      const templateVersion = Number(template.source_version ?? 1) || 1;
      if (columns.includes("points")) record.points = basePoints;
      if (columns.includes("base_points")) record.base_points = template.base_points ?? basePoints;
      if (columns.includes("status")) record.status = template.status ?? "active";
      if (columns.includes("family_id")) record.family_id = familyId;
      if (columns.includes("master_task_id")) record.master_task_id = template.master_task_id ?? null;
      if (columns.includes("source_template_id")) record.source_template_id = template.id;
      if (columns.includes("source_version")) record.source_version = templateVersion;
      if (columns.includes("is_customized")) record.is_customized = 0;
      if (columns.includes("created_at")) record.created_at = now;
      if (columns.includes("updated_at")) record.updated_at = now;
      insertRecord("task", record);
      deleteDismissedTemplateStmt.run({ family_id: familyId, kind: "task", master_id: template.id });
      res.status(201).json({
        item: {
          kind: "task",
          id: newId,
          master_id: template.id,
          title,
          description: template.description ?? null,
          icon: template.icon ?? null,
          base_points: basePoints
        }
      });
      return;
    }

    if (!tableExists("reward")) {
      res.status(500).json({ error: "reward_table_missing" });
      return;
    }
    const rewardColumns = getTableColumns("reward");
    if (!rewardColumns.includes("scope")) {
      res.status(500).json({ error: "reward_scope_missing" });
      return;
    }
    const template = db
      .prepare("SELECT * FROM reward WHERE id = ? AND scope = 'global'")
      .get(masterId);
    if (!template) {
      res.status(404).json({ error: "template_not_found" });
      return;
    }
    const templateStatus = (template.status || "active").toString().trim().toLowerCase();
    if (templateStatus !== "active") {
      res.status(409).json({ error: "template_inactive" });
      return;
    }

    const existingRewards = rewardColumns.includes("family_id")
      ? db
          .prepare(
            `SELECT source_template_id, master_reward_id
               FROM reward
              WHERE family_id = ?`
          )
          .all(familyId)
      : [];
    for (const row of existingRewards) {
      if (row?.source_template_id && String(row.source_template_id) === masterId) {
        res.status(409).json({ error: "already_adopted" });
        return;
      }
      if (row?.master_reward_id && String(row.master_reward_id) === masterId) {
        res.status(409).json({ error: "already_adopted" });
        return;
      }
    }

    const record = {};
    const newId = crypto.randomUUID();
    record.id = newId;
    const title = template.title ?? template.name ?? "";
    if (rewardColumns.includes("scope")) record.scope = "family";
    if (rewardColumns.includes("title")) record.title = title;
    if (rewardColumns.includes("name")) record.name = title;
    if (rewardColumns.includes("description")) record.description = template.description ?? null;
    if (rewardColumns.includes("icon")) record.icon = template.icon ?? null;
    if (rewardColumns.includes("image_url")) record.image_url = template.image_url ?? template.icon ?? null;
    const baseCost = Number(template.cost ?? template.price ?? template.base_cost ?? 0) || 0;
    const templateVersion = Number(template.source_version ?? 1) || 1;
    if (rewardColumns.includes("cost")) record.cost = baseCost;
    if (rewardColumns.includes("base_cost")) record.base_cost = template.base_cost ?? baseCost;
    if (rewardColumns.includes("status")) record.status = template.status ?? "active";
    if (rewardColumns.includes("source")) record.source = "master";
    if (rewardColumns.includes("family_id")) record.family_id = familyId;
    if (rewardColumns.includes("master_reward_id")) record.master_reward_id = template.master_reward_id ?? null;
    if (rewardColumns.includes("source_template_id")) record.source_template_id = template.id;
    if (rewardColumns.includes("source_version")) record.source_version = templateVersion;
    if (rewardColumns.includes("is_customized")) record.is_customized = 0;
    if (rewardColumns.includes("created_at")) record.created_at = now;
    if (rewardColumns.includes("updated_at")) record.updated_at = now;
    insertRecord("reward", record);
    deleteDismissedTemplateStmt.run({ family_id: familyId, kind: "reward", master_id: template.id });
    res.status(201).json({
      item: {
        kind: "reward",
        id: newId,
        master_id: template.id,
        title,
        description: template.description ?? null,
        icon: template.icon ?? template.image_url ?? null,
        base_cost: baseCost
      }
    });
  } catch (err) {
    console.error("[family.adopt] failed", err);
    res.status(500).json({ error: "adopt_failed" });
  }
});

app.post("/api/family/dismiss", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const familyId = req.scope?.family_id;
  if (!familyId) {
    res.status(400).json({ error: "family_id required" });
    return;
  }

  const body = req.body ?? {};
  const kind = (body.kind ?? "").toString().trim().toLowerCase();
  const masterId = (body.master_id ?? "").toString().trim();
  if (!kind || !masterId) {
    res.status(400).json({ error: "kind_and_master_id_required" });
    return;
  }
  if (kind !== "task" && kind !== "reward") {
    res.status(400).json({ error: "unsupported_kind" });
    return;
  }

  try {
    const exists =
      kind === "task" ? selectMasterTaskStmt.get(masterId) : selectMasterRewardStmt.get(masterId);
    if (!exists) {
      res.status(404).json({ error: "master_not_found" });
      return;
    }
    upsertDismissedTemplateStmt.run({
      family_id: familyId,
      kind,
      master_id: masterId,
      dismissed_at: Date.now()
    });
    res.status(204).end();
  } catch (err) {
    console.error("[family.dismiss] failed", err);
    res.status(500).json({ error: "dismiss_failed" });
  }
});

app.get(
  "/api/admin/list-earn-templates",
  authenticateAdmin,
  resolveFamilyScopeOptional,
  (req, res) => {
    if (!tableExists("task")) {
      res.json([]);
      return;
    }

    const columns = getTableColumns("task");
    const hasFamilyColumn = columns.includes("family_id");
    const hasStatus = columns.includes("status");
    const hasSortOrder = columns.includes("sort_order");
    const hasUpdatedAt = columns.includes("updated_at");
    const hasCreatedAt = columns.includes("created_at");
    const hasMasterId = columns.includes("master_task_id");
    const hasYoutube = columns.includes("youtube_url");
    const hasSource = columns.includes("source");
    const hasSourceTemplate = columns.includes("source_template_id");
    const hasSourceVersion = columns.includes("source_version");
    const hasIsCustomized = columns.includes("is_customized");

    if (MULTITENANT_ENFORCE && !hasFamilyColumn) {
      res.status(500).json({ error: "task_missing_family_scope" });
      return;
    }

    const scopeFamily = req.scope?.family_id || null;
    const queryFamily = (req.query?.familyId || req.query?.family_id || "").toString().trim();
    const familyId = scopeFamily || (queryFamily ? queryFamily : null);
    if (!familyId) {
      res.status(400).json({ error: "family_id required" });
      return;
    }

    const statusParam = (req.query?.status || "").toString().trim().toLowerCase();
    const status = statusParam === "inactive" ? "inactive" : "active";

    const joinMaster = tableExists("master_task");
    const selectParts = [
      "t.id",
      columns.includes("title") ? "t.title" : columns.includes("name") ? "t.name AS title" : "t.id AS title",
      columns.includes("name") ? "t.name" : columns.includes("title") ? "t.title AS name" : "t.id AS name",
      columns.includes("description") ? "t.description" : "NULL AS description",
      columns.includes("icon") ? "t.icon" : "NULL AS icon",
      columns.includes("points") ? "t.points" : columns.includes("base_points") ? "t.base_points AS points" : "0 AS points",
      hasStatus ? "t.status" : "'active' AS status",
      hasSource ? "t.source" : "NULL AS source",
      hasMasterId ? "t.master_task_id" : "NULL AS master_task_id",
      hasSourceTemplate ? "t.source_template_id" : "NULL AS source_template_id",
      hasSourceVersion ? "t.source_version" : "0 AS source_version",
      hasIsCustomized ? "t.is_customized" : "0 AS is_customized",
      hasFamilyColumn ? "t.family_id" : "NULL AS family_id",
      hasCreatedAt ? "t.created_at" : "0 AS created_at",
      hasUpdatedAt ? "t.updated_at" : "0 AS updated_at",
      hasSortOrder ? "t.sort_order" : "0 AS sort_order",
      hasYoutube ? "t.youtube_url" : "NULL AS youtube_url"
    ];
    if (joinMaster) {
      selectParts.push("mt.youtube_url AS master_youtube");
    }

    let sql = `SELECT ${selectParts.join(", ")} FROM task t`;
    if (joinMaster) {
      sql += " LEFT JOIN master_task mt ON mt.id = t.master_task_id";
    }
    sql += " WHERE 1=1";

    const params = [];
    if (hasFamilyColumn) {
      sql += " AND t.family_id = ?";
      params.push(familyId);
    }

    if (hasStatus) {
      sql += " AND t.status = ?";
      params.push(status);
    } else if (status === "inactive") {
      res.json([]);
      return;
    }

    if (hasSortOrder) {
      const order = ["t.sort_order ASC"];
      if (hasUpdatedAt) {
        order.push("t.updated_at DESC");
      } else {
        order.push("t.id DESC");
      }
      sql += ` ORDER BY ${order.join(", ")}`;
    } else if (hasUpdatedAt) {
      sql += " ORDER BY t.updated_at DESC";
    } else {
      sql += " ORDER BY t.id DESC";
    }

    const rows = db.prepare(sql).all(...params);
    res.json(rows.map(mapTaskRow));
  }
);

app.get("/api/admin/earn-templates", authenticateAdmin, resolveFamilyScope, (req, res) => {
  if (!tableExists("task")) {
    res.json([]);
    return;
  }

  const columns = getTableColumns("task");
  const hasFamilyColumn = columns.includes("family_id");
  const hasTitle = columns.includes("title");
  const hasName = columns.includes("name");
  const hasDescription = columns.includes("description");
  const hasIcon = columns.includes("icon");
  const hasStatus = columns.includes("status");
  const hasSortOrder = columns.includes("sort_order");
  const hasUpdatedAt = columns.includes("updated_at");
  const hasCreatedAt = columns.includes("created_at");
  const hasMasterId = columns.includes("master_task_id");
  const hasYoutube = columns.includes("youtube_url");
  const hasSourceTemplate = columns.includes("source_template_id");
  const hasSourceVersion = columns.includes("source_version");
  const hasIsCustomized = columns.includes("is_customized");

  const scopedFamilyId = req.scope?.family_id ?? null;
  if (MULTITENANT_ENFORCE) {
    if (!hasFamilyColumn) {
      res.status(500).json({ error: "task_missing_family_scope" });
      return;
    }
    if (!scopedFamilyId) {
      res.status(400).json({ error: "family_id required" });
      return;
    }
  }

  const modeParam = (req.query?.mode || "").toString().trim().toLowerCase();
  const mode = modeParam === "inactive" ? "inactive" : "active";

  const selectParts = [];
  selectParts.push("t.id");
  const titleExpr = hasTitle ? "t.title" : hasName ? "t.name" : "t.id";
  selectParts.push(`${titleExpr} AS title`);
  if (hasName) {
    selectParts.push("t.name");
  }
  selectParts.push(hasDescription ? "t.description" : "NULL AS description");
  selectParts.push(hasIcon ? "t.icon" : "NULL AS icon");
  if (columns.includes("points")) {
    selectParts.push("t.points");
  }
  if (columns.includes("base_points")) {
    selectParts.push("t.base_points");
  }
  selectParts.push(hasStatus ? "t.status" : "'active' AS status");
  selectParts.push(hasSortOrder ? "t.sort_order" : "0 AS sort_order");
  selectParts.push(hasUpdatedAt ? "t.updated_at" : "0 AS updated_at");
  selectParts.push(hasCreatedAt ? "t.created_at" : "0 AS created_at");
  selectParts.push(hasMasterId ? "t.master_task_id" : "NULL AS master_task_id");
  selectParts.push(hasSourceTemplate ? "t.source_template_id" : "NULL AS source_template_id");
  selectParts.push(hasSourceVersion ? "t.source_version" : "0 AS source_version");
  selectParts.push(hasIsCustomized ? "t.is_customized" : "0 AS is_customized");
  selectParts.push(hasYoutube ? "t.youtube_url" : "NULL AS youtube_url");
  if (hasFamilyColumn) {
    selectParts.push("t.family_id");
  }

  const joinMaster = tableExists("master_task");
  if (joinMaster) {
    selectParts.push("mt.youtube_url AS master_youtube");
  }

  let sql = `SELECT ${selectParts.join(", ")} FROM task t`;
  if (joinMaster) {
    sql += " LEFT JOIN master_task mt ON mt.id = t.master_task_id";
  }

  const filters = [];
  const params = [];

  if (hasFamilyColumn && scopedFamilyId) {
    filters.push("t.family_id = ?");
    params.push(scopedFamilyId);
  }

  if (hasStatus) {
    filters.push("t.status = ?");
    params.push(mode);
  } else if (mode === "inactive") {
    res.json([]);
    return;
  }

  if (filters.length) {
    sql += ` WHERE ${filters.join(" AND ")}`;
  }

  if (mode === "active" && hasSortOrder) {
    if (hasUpdatedAt) {
      sql += " ORDER BY t.sort_order ASC, t.updated_at DESC";
    } else {
      sql += " ORDER BY t.sort_order ASC, t.id DESC";
    }
  } else if (hasUpdatedAt) {
    sql += " ORDER BY t.updated_at DESC";
  } else {
    sql += " ORDER BY t.id DESC";
  }

  const rows = db.prepare(sql).all(...params);
  res.json(rows.map(mapTaskRow));
});

app.post(
  "/api/admin/templates/:templateId/adopt",
  authenticateAdmin,
  resolveFamilyScopeOptional,
  (req, res) => {
    const templateId = (req.params?.templateId ?? "").toString().trim();
    if (!templateId) {
      res.status(400).json({ error: "templateId required" });
      return;
    }

    const scopeFamily = req.scope?.family_id || null;
    const queryFamily = (req.query?.familyId || req.query?.family_id || "").toString().trim();
    const familyId = scopeFamily || (queryFamily ? queryFamily : null);
    if (!familyId) {
      res.status(400).json({ error: "family_id required" });
      return;
    }

    if (!tableExists("task")) {
      res.status(500).json({ error: "task_table_missing" });
      return;
    }

    const columns = getTableColumns("task");
    if (!columns.includes("id")) {
      res.status(500).json({ error: "task_table_missing" });
      return;
    }
    if (!columns.includes("scope")) {
      res.status(500).json({ error: "task_scope_missing" });
      return;
    }
    const template = db
      .prepare("SELECT * FROM task WHERE id = ? AND scope = 'global'")
      .get(templateId);
    if (!template) {
      res.status(404).json({ error: "template_not_found" });
      return;
    }

    const templateStatus = (template.status || "active").toString().trim().toLowerCase() || "active";
    if (templateStatus !== "active") {
      res.status(409).json({ error: "template_inactive" });
      return;
    }
    if (MULTITENANT_ENFORCE && !columns.includes("family_id")) {
      res.status(500).json({ error: "task_missing_family_scope" });
      return;
    }

    if ((columns.includes("master_task_id") || columns.includes("source_template_id")) && columns.includes("family_id")) {
      let duplicate = null;
      if (columns.includes("source_template_id")) {
        duplicate = db
          .prepare(`SELECT id FROM task WHERE family_id = ? AND source_template_id = ? LIMIT 1`)
          .get(familyId, templateId);
      }
      if (!duplicate && columns.includes("master_task_id")) {
        duplicate = db
          .prepare(`SELECT id FROM task WHERE family_id = ? AND master_task_id = ? LIMIT 1`)
          .get(familyId, templateId);
      }
      if (duplicate?.id) {
        res.status(409).json({ error: "already_adopted", taskId: duplicate.id });
        return;
      }
    }

    const now = Date.now();
    const newId = crypto.randomUUID();
    const basePoints = Number(template.points ?? template.base_points ?? 0) || 0;
    const templateVersion = Number(template.source_version ?? 1) || 1;
    const record = { id: newId };
    if (columns.includes("scope")) record.scope = "family";
    if (columns.includes("family_id")) record.family_id = familyId;
    if (columns.includes("title")) record.title = template.title ?? template.name ?? "";
    if (columns.includes("name")) record.name = template.name ?? template.title ?? "";
    if (columns.includes("description")) record.description = template.description ?? null;
    if (columns.includes("icon")) record.icon = template.icon ?? null;
    if (columns.includes("points")) record.points = basePoints;
    if (columns.includes("base_points")) record.base_points = template.base_points ?? basePoints;
    if (columns.includes("status")) record.status = template.status ?? "active";
    if (columns.includes("source")) record.source = "master";
    if (columns.includes("master_task_id")) record.master_task_id = template.master_task_id ?? null;
    if (columns.includes("source_template_id")) record.source_template_id = template.id;
    if (columns.includes("source_version")) record.source_version = templateVersion;
    if (columns.includes("is_customized")) record.is_customized = 0;
    if (columns.includes("sort_order")) record.sort_order = 0;
    if (columns.includes("youtube_url")) record.youtube_url = template.youtube_url ?? null;
    if (columns.includes("created_at")) record.created_at = now;
    if (columns.includes("updated_at")) record.updated_at = now;

    insertRecord("task", record);
    deleteDismissedTemplateStmt.run({ family_id: familyId, kind: "task", master_id: template.id });

    res.status(201).json({ taskId: newId });
  }
);

app.get("/api/admin/templates/available", authenticateAdmin, (req, res) => {
  const rawFamilyId = (req.query?.familyId || req.query?.family_id || "").toString().trim();
  if (!rawFamilyId) {
    res.status(400).json({ error: "familyId required" });
    return;
  }
  const kind = (req.query?.kind || "task").toString().trim().toLowerCase();
  if (kind !== "task" && kind !== "reward") {
    res.status(400).json({ error: "unsupported_kind" });
    return;
  }

  if (req.auth?.role === "family" && req.auth.familyId !== rawFamilyId) {
    res.status(403).json({ error: "forbidden" });
    return;
  }
  if (req.auth?.role !== "master" && req.auth?.role !== "family") {
    res.status(403).json({ error: "forbidden" });
    return;
  }

  const familyId = rawFamilyId;

  if (kind === "task") {
    if (!tableExists("task")) {
      res.json([]);
      return;
    }
    const taskColumns = getTableColumns("task");
    if (!taskColumns.includes("scope")) {
      res.status(500).json({ error: "task_scope_missing" });
      return;
    }

    const existingRows = taskColumns.includes("family_id")
      ? db
          .prepare(
            `SELECT source_template_id, master_task_id
               FROM task
              WHERE family_id = ?`
          )
          .all(familyId)
      : [];
    const adopted = new Set();
    for (const row of existingRows) {
      if (row?.source_template_id) adopted.add(String(row.source_template_id));
      if (row?.master_task_id) adopted.add(String(row.master_task_id));
    }

    const globalTasks = db
      .prepare("SELECT * FROM task WHERE scope = 'global'")
      .all();
    const items = [];
    for (const row of globalTasks) {
      if (!row?.id) continue;
      const status = (row.status || "active").toString().trim().toLowerCase();
      if (status !== "active") continue;
      const templateId = String(row.id);
      if (adopted.has(templateId)) continue;
      if (row.master_task_id && adopted.has(String(row.master_task_id))) continue;

      items.push({
        id: templateId,
        title: row.title ?? row.name ?? "",
        description: row.description ?? null,
        icon: row.icon ?? null,
        points: Number(row.points ?? row.base_points ?? 0) || 0,
        youtube_url: row.youtube_url ?? null,
        version: Number(row.source_version ?? 1) || 1
      });
    }

    items.sort((a, b) => b.version - a.version || a.title.localeCompare(b.title));
    res.json(items);
    return;
  }

  if (!tableExists("reward")) {
    res.json([]);
    return;
  }
  const rewardColumns = getTableColumns("reward");
  if (!rewardColumns.includes("scope")) {
    res.status(500).json({ error: "reward_scope_missing" });
    return;
  }

  const existingRewards = rewardColumns.includes("family_id")
    ? db
        .prepare(
          `SELECT source_template_id, master_reward_id
             FROM reward
            WHERE family_id = ?`
        )
        .all(familyId)
    : [];
  const adoptedRewards = new Set();
  for (const row of existingRewards) {
    if (row?.source_template_id) adoptedRewards.add(String(row.source_template_id));
    if (row?.master_reward_id) adoptedRewards.add(String(row.master_reward_id));
  }

  const globalRewards = db
    .prepare("SELECT * FROM reward WHERE scope = 'global'")
    .all();
  const rewardItems = [];
  for (const row of globalRewards) {
    if (!row?.id) continue;
    const status = (row.status || "active").toString().trim().toLowerCase();
    if (status !== "active") continue;
    const templateId = String(row.id);
    if (adoptedRewards.has(templateId)) continue;
    if (row.master_reward_id && adoptedRewards.has(String(row.master_reward_id))) continue;

    rewardItems.push({
      id: templateId,
      title: row.title ?? row.name ?? "",
      description: row.description ?? null,
      icon: row.icon ?? row.image_url ?? null,
      cost: Number(row.cost ?? row.price ?? row.base_cost ?? 0) || 0,
      youtube_url: row.youtube_url ?? null,
      version: Number(row.source_version ?? 1) || 1
    });
  }

  rewardItems.sort((a, b) => b.version - a.version || a.title.localeCompare(b.title));
  res.json(rewardItems);
});

function buildTaskStatusUpdater(targetStatus) {
  return function handleTaskStatusUpdate(req, res) {
    if (!tableExists("task")) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const columns = getTableColumns("task");
    if (!columns.includes("status")) {
      res.status(400).json({ error: "status_not_supported" });
      return;
    }

    const id = (req.params?.id ?? "").toString().trim();
    if (!id) {
      res.status(400).json({ error: "invalid_id" });
      return;
    }

    const scopeFamily = req.scope?.family_id || null;
    const queryFamily = (req.query?.familyId || req.query?.family_id || "").toString().trim();
    const requestedFamily = scopeFamily || (queryFamily ? queryFamily : null);

    const joinMaster = tableExists("master_task");
    let selectSql = "SELECT t.*";
    if (joinMaster) {
      selectSql += ", mt.youtube_url AS master_youtube";
    }
    selectSql += " FROM task t";
    if (joinMaster) {
      selectSql += " LEFT JOIN master_task mt ON mt.id = t.master_task_id";
    }
    selectSql += " WHERE t.id = ?";
    const selectParams = [id];
    if (columns.includes("family_id") && requestedFamily) {
      selectSql += " AND t.family_id = ?";
      selectParams.push(requestedFamily);
    }

    const existing = db.prepare(selectSql).get(...selectParams);
    if (!existing) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const normalizedStatus = targetStatus;
    const now = Date.now();
    const updateFields = ["status = ?"];
    const updateParams = [normalizedStatus];
    if (columns.includes("updated_at")) {
      updateFields.push("updated_at = ?");
      updateParams.push(now);
    }

    let updateSql = `UPDATE task SET ${updateFields.join(", ")} WHERE id = ?`;
    const guardParams = [...updateParams, id];
    const guardFamilyId = columns.includes("family_id")
      ? requestedFamily || existing.family_id || null
      : null;
    if (guardFamilyId) {
      updateSql += " AND family_id = ?";
      guardParams.push(guardFamilyId);
    }

    const info = db.prepare(updateSql).run(...guardParams);
    const refreshed = db.prepare(selectSql).get(...selectParams);
    if (!info.changes && !refreshed) {
      res.status(409).json({ error: "no_change" });
      return;
    }
    res.json({ task: mapTaskRow(refreshed) });
  };
}

app.patch(
  "/api/admin/tasks/:id/deactivate",
  authenticateAdmin,
  resolveFamilyScopeOptional,
  buildTaskStatusUpdater("inactive")
);

app.patch(
  "/api/admin/tasks/:id/reactivate",
  authenticateAdmin,
  resolveFamilyScopeOptional,
  buildTaskStatusUpdater("active")
);

app.patch("/api/tasks/:id", authenticateAdmin, resolveFamilyScope, (req, res) => {
  if (!tableExists("task")) {
    res.status(404).json({ error: "not_found" });
    return;
  }
  const columns = getTableColumns("task");
  if (!columns.includes("id")) {
    res.status(404).json({ error: "not_found" });
    return;
  }

  const id = (req.params?.id ?? "").toString().trim();
  if (!id) {
    res.status(400).json({ error: "invalid_id" });
    return;
  }

  const hasFamilyColumn = columns.includes("family_id");
  const scopedFamilyId = req.scope?.family_id ?? null;
  if (MULTITENANT_ENFORCE && hasFamilyColumn && !scopedFamilyId) {
    res.status(400).json({ error: "family_id required" });
    return;
  }

  let selectSql = `SELECT * FROM ${quoteIdent("task")} WHERE id = ?`;
  const selectParams = [id];
  let guardFamilyId = null;
  if (hasFamilyColumn) {
    if (MULTITENANT_ENFORCE) {
      guardFamilyId = scopedFamilyId;
    } else if (scopedFamilyId) {
      guardFamilyId = scopedFamilyId;
    }
    if (guardFamilyId) {
      selectSql += " AND family_id = ?";
      selectParams.push(guardFamilyId);
    }
  } else if (MULTITENANT_ENFORCE) {
    res.status(500).json({ error: "task_missing_family_scope" });
    return;
  }

  const existing = db.prepare(selectSql).get(...selectParams);
  if (!existing) {
    res.status(404).json({ error: "not_found" });
    return;
  }

  const body = req.body || {};
  const fields = [];
  const params = [];
  const isMasterLinked = !!(existing.master_task_id && String(existing.master_task_id).trim());
  const hasCustomizationFlag = columns.includes("is_customized");
  let markCustomized = false;

  if (!isMasterLinked && body.title !== undefined && columns.includes("title")) {
    const title = String(body.title).trim();
    if (!title) {
      res.status(400).json({ error: "title_required" });
      return;
    }
    fields.push("title = ?");
    params.push(title);
    if (columns.includes("name")) {
      fields.push("name = ?");
      params.push(title);
    }
  } else if (!isMasterLinked && body.name !== undefined && columns.includes("name")) {
    const title = String(body.name).trim();
    if (!title) {
      res.status(400).json({ error: "title_required" });
      return;
    }
    fields.push("name = ?");
    params.push(title);
    if (columns.includes("title")) {
      fields.push("title = ?");
      params.push(title);
    }
  }

  if (!isMasterLinked && body.description !== undefined && columns.includes("description")) {
    const desc = body.description === null ? null : String(body.description);
    fields.push("description = ?");
    params.push(desc);
  }

  if (!isMasterLinked && body.icon !== undefined && columns.includes("icon")) {
    const icon = body.icon === null ? null : String(body.icon).trim() || null;
    fields.push("icon = ?");
    params.push(icon);
  }

  if (body.points !== undefined || body.base_points !== undefined) {
    const value = body.points ?? body.base_points;
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      res.status(400).json({ error: "invalid_points" });
      return;
    }
    const normalized = Math.trunc(numeric);
    if (columns.includes("points")) {
      fields.push("points = ?");
      params.push(normalized);
    }
    if (columns.includes("base_points")) {
      fields.push("base_points = ?");
      params.push(normalized);
    }
    if (isMasterLinked) {
      markCustomized = true;
    }
  }

  if (body.sort_order !== undefined && columns.includes("sort_order")) {
    const order = Number(body.sort_order);
    if (!Number.isFinite(order)) {
      res.status(400).json({ error: "invalid_sort_order" });
      return;
    }
    fields.push("sort_order = ?");
    params.push(Math.trunc(order));
  }

  if (!isMasterLinked && (body.youtubeUrl !== undefined || body.youtube_url !== undefined) && columns.includes("youtube_url")) {
    const youtubeValue = body.youtubeUrl ?? body.youtube_url;
    const normalizedYoutube = youtubeValue == null ? null : String(youtubeValue).trim() || null;
    fields.push("youtube_url = ?");
    params.push(normalizedYoutube);
  }

  if (body.status !== undefined && columns.includes("status")) {
    fields.push("status = ?");
    params.push(String(body.status).trim().toLowerCase());
  }

  if (!fields.length) {
    res.json({ ok: true, task: mapTaskRow(existing) });
    return;
  }
  if (markCustomized && hasCustomizationFlag) {
    fields.push("is_customized = 1");
  }

  if (columns.includes("updated_at")) {
    fields.push("updated_at = ?");
    params.push(Date.now());
  }

  let updateSql = `UPDATE ${quoteIdent("task")} SET ${fields.join(", ")} WHERE id = ?`;
  const updateParams = [...params, id];
  if (guardFamilyId) {
    updateSql += " AND family_id = ?";
    updateParams.push(guardFamilyId);
  }

  try {
    const info = db.prepare(updateSql).run(...updateParams);
    if (!info.changes) {
      res.status(404).json({ error: "not_found" });
      return;
    }
  } catch (err) {
    console.error("[tasks.patch] failed", err);
    res.status(500).json({ error: "update_failed" });
    return;
  }

  const updated = db.prepare(selectSql).get(...selectParams);
  res.json({ ok: true, task: mapTaskRow(updated) });
});

app.delete("/api/tasks/:id", authenticateAdmin, resolveFamilyScope, (req, res) => {
  if (!tableExists("task")) {
    res.status(404).json({ error: "not_found" });
    return;
  }

  const id = (req.params?.id ?? "").toString().trim();
  if (!id) {
    res.status(400).json({ error: "invalid_id" });
    return;
  }

  const columns = getTableColumns("task");
  if (!columns.includes("id")) {
    res.status(404).json({ error: "not_found" });
    return;
  }

  const hasFamilyColumn = columns.includes("family_id");
  const scopedFamilyId = req.scope?.family_id ?? null;
  if (MULTITENANT_ENFORCE && hasFamilyColumn && !scopedFamilyId) {
    res.status(400).json({ error: "family_id required" });
    return;
  }

  let sql = `DELETE FROM ${quoteIdent("task")} WHERE id = ?`;
  const params = [id];
  if (hasFamilyColumn && scopedFamilyId) {
    sql += " AND family_id = ?";
    params.push(scopedFamilyId);
  }

  try {
    const info = db.prepare(sql).run(...params);
    if (!info.changes) {
      res.status(404).json({ error: "not_found" });
      return;
    }
  } catch (err) {
    console.error("[tasks.delete] failed", err);
    res.status(500).json({ error: "delete_failed" });
    return;
  }

  res.json({ ok: true });
});

app.get(["/", "/index.html", "/child", "/child.html"], (_req, res) => {
  sendVersioned(res, "child.html");
});

app.get(["/admin", "/admin.html"], (req, res) => {
  let html = loadVersioned("admin.html");

  try {
    const adminKey = readAdminKey(req);
    if (adminKey) {
      const context = resolveAdminContext(db, adminKey);
      if (context?.role === "family" && context.familyId) {
        const familyId = String(context.familyId);
        const bootstrap = {
          familyId,
          familyKey: context.familyKey || null,
          familyName: context.familyName || "",
          familyStatus: context.familyStatus || context.family_status || null
        };
        const scopeScript = `<script>(function(){try{const scope=window.currentScope&&typeof window.currentScope==='object'?{...window.currentScope}:{ };const fid=${JSON.stringify(familyId)};scope.familyId=fid;scope.family_id=fid;if(!scope.uuid)scope.uuid=fid;window.currentScope=scope;window.currentFamilyId=fid;window.__CK_FAMILY_BOOTSTRAP__=${JSON.stringify(bootstrap)};}catch(err){console.warn('Failed to bootstrap family scope',err);}})();</script>`;
        const marker = '<script type="module" src="/js/admin.js';
        const index = html.indexOf(marker);
        if (index !== -1) {
          html = `${html.slice(0, index)}${scopeScript}\n${html.slice(index)}`;
        } else {
          html = html.replace("</body>", `${scopeScript}</body>`);
        }
      }
    }
  } catch (error) {
    console.warn("[admin.page] unable to resolve admin scope", error?.message || error);
  }

  res.type("text/html");
  res.set("Cache-Control", "no-store");
  res.send(html);
});

app.get("/install", (req, res) => {
  if (req.query?.src && typeof req.query.src === "string") {
    res.set("Cache-Control", "no-store");
  }
  sendVersioned(res, "install.html");
});

app.get("/assets/icons/:iconName", (req, res, next) => {
  const iconName = req.params?.iconName || "";
  if (!knownIcon(iconName)) {
    next();
    return;
  }
  try {
    const png = generateIcon(iconName);
    if (!png) {
      next();
      return;
    }
    res.type("image/png");
    res.set("Cache-Control", "public, max-age=31536000, immutable");
    res.send(png);
  } catch (error) {
    next(error);
  }
});

app.get("/install/qr.png", async (req, res, next) => {
  try {
    const origin = req.get("x-forwarded-proto")
      ? `${req.get("x-forwarded-proto")}://${req.get("x-forwarded-host") || req.get("host")}`
      : `${req.protocol}://${req.get("host")}`;
    const search = req.query?.src ? `?src=${encodeURIComponent(String(req.query.src))}` : "?src=qr";
    const url = `${origin}/install${search}`;
    const png = await QRCode.toBuffer(url, { width: 600, margin: 1, color: { dark: "#2D2A6A", light: "#FAFAFA" } });
    res.type("image/png");
    res.set("Cache-Control", "public, max-age=31536000, immutable");
    res.send(png);
  } catch (error) {
    next(error);
  }
});

app.get(["/scan", "/scan.html"], async (req, res) => {
  res.type("html");
  res.set("Cache-Control", "no-store");
  const token = (req.query?.t ?? req.query?.token ?? "").toString().trim();
  if (!token) {
    res.status(400).send(renderScanPage({ success: false, error: friendlyScanError("missing_token"), rawCode: "missing_token" }));
    return;
  }

  let payload;
  try {
    payload = verifyToken(token);
  } catch (err) {
    const message = err?.message || "scan_failed";
    const status = err?.status || (message === "TOKEN_USED" ? 409 : 400);
    res.status(status).send(renderScanPage({ success: false, error: friendlyScanError(message), rawCode: message }));
    return;
  }

  if (payload?.typ === "spend") {
    const holdId = payload?.data?.holdId ? String(payload.data.holdId) : "";
    if (!holdId) {
      res.status(400).send(renderSpendApprovalPage({ message: friendlyScanError("invalid_payload") }));
      return;
    }

    const holdRow = getHoldRow(holdId);
    const hold = mapHoldRow(holdRow);
    if (!hold) {
      res.status(404).send(renderSpendApprovalPage({ message: "We couldn't find this reward request. Please ask the child to generate a new QR code." }));
      return;
    }

    let balance = null;
    if (hold.userId) {
      const value = await balanceOf(hold.userId);
      const parsed = Number(value);
      if (Number.isFinite(parsed)) balance = parsed;
    }

    const costValue = Number(hold?.finalCost ?? hold?.quotedCost ?? payload?.data?.cost ?? 0);
    const cost = Number.isFinite(costValue) ? costValue : null;
    const afterBalance = balance !== null && cost !== null ? balance - cost : null;
    const expiresAt = payload?.exp ? payload.exp * 1000 : null;
    const tokenUsed = !!checkTokenStmt.get(payload.jti);

    res.send(renderSpendApprovalPage({
      hold,
      balance,
      cost,
      afterBalance,
      expiresAt,
      tokenUsed,
      token
    }));
    return;
  }

  try {
    const result = await redeemToken({
      token,
      req,
      actor: typ => (typ === "earn" ? "link_earn" : "link_give"),
      allowEarnWithoutAdmin: true
    });
    res.send(renderScanPage({ success: true, result }));
  } catch (err) {
    const message = err?.message || "scan_failed";
    const status = err?.status || (message === "TOKEN_USED" ? 409 : 400);
    res.status(status).send(renderScanPage({ success: false, error: friendlyScanError(message), rawCode: message }));
  }
});

app.get("/ck-wallet-manifest.v1.webmanifest", (_req, res) => {
  sendVersioned(
    res,
    "ck-wallet-manifest.v1.webmanifest",
    "application/manifest+json",
    "public, max-age=31536000, immutable"
  );
});

app.get("/sw.js", (_req, res) => {
  sendVersioned(res, "sw.js", "application/javascript");
});

app.use(express.static(PUBLIC_DIR, {
  maxAge: "1h",
  setHeaders(res, filePath) {
    if (filePath.endsWith(".html")) {
      res.setHeader("Cache-Control", "no-store");
    }
    if (filePath.includes(`${sep}assets${sep}icons${sep}`) || filePath.includes("ck-wallet-manifest")) {
      res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
    }
    if (filePath.endsWith(".webmanifest")) {
      res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
    }
  }
}));
app.use("/uploads", express.static(UPLOAD_DIR, { maxAge: "1y" }));

app.set("trust proxy", 1);

function normId(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeTimestamp(value, fallback = Date.now()) {
  if (value === null || value === undefined) return fallback;
  const num = Number(value);
  if (Number.isFinite(num)) {
    if (num >= 1e12) return Math.trunc(num);
    if (num >= 1e9) return Math.trunc(num * 1000);
    if (num > 0) return Math.trunc(num);
  }
  const str = String(value || '').trim();
  if (!str) return fallback;
  const parsed = Date.parse(str);
  if (Number.isNaN(parsed)) return fallback;
  return parsed;
}

function tableExists(name) {
  if (!name) return false;
  return !!db
    .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?")
    .get(String(name));
}

function rebuildMenuTableForScope(tableName, { scopeDefault = "family" } = {}) {
  if (!tableName || !tableExists(tableName)) {
    return;
  }

  let info;
  try {
    info = db.prepare(`PRAGMA table_info(${quoteIdent(tableName)})`).all();
  } catch (error) {
    console.warn(`[migration] unable to inspect ${tableName}`, error?.message || error);
    return;
  }

  if (!Array.isArray(info) || info.length === 0) {
    return;
  }

  const hasScopeColumn = info.some(col => col?.name === "scope");
  const familyColumn = info.find(col => col?.name === "family_id");
  const familyIsNotNull = familyColumn ? Number(familyColumn.notnull) !== 0 : false;

  if (hasScopeColumn && !familyIsNotNull) {
    return;
  }

  const indices = db
    .prepare(
      "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name = ? AND sql IS NOT NULL"
    )
    .all(tableName);

  const scopeDefaultSql = `'${scopeDefault}'`;

  const columns = info.map(col => {
    if (!col?.name) return col;
    if (col.name === "family_id") {
      return { ...col, notnull: 0 };
    }
    if (col.name === "scope") {
      return {
        ...col,
        notnull: 1,
        dflt_value: col.dflt_value ?? scopeDefaultSql
      };
    }
    return { ...col };
  });

  if (!hasScopeColumn) {
    columns.push({
      cid: columns.length,
      name: "scope",
      type: "TEXT",
      notnull: 1,
      dflt_value: scopeDefaultSql,
      pk: 0
    });
  }

  const tempName = `${tableName}_scope_legacy_${Date.now()}`;
  const columnDefs = columns.map(col => {
    const parts = [quoteIdent(col.name)];
    if (col.type) {
      parts.push(col.type);
    }
    if (col.pk) {
      parts.push("PRIMARY KEY");
    }
    if (col.notnull && !col.pk) {
      parts.push("NOT NULL");
    }
    if (col.dflt_value !== null && col.dflt_value !== undefined) {
      parts.push(`DEFAULT ${col.dflt_value}`);
    }
    return parts.join(" ");
  });

  const newColumnNames = columns.map(col => col.name);
  const oldColumnNames = info.map(col => col.name);

  db.exec("BEGIN");
  let renamed = false;
  try {
    db.exec(`ALTER TABLE ${quoteIdent(tableName)} RENAME TO ${quoteIdent(tempName)}`);
    renamed = true;
    db.exec(`CREATE TABLE ${quoteIdent(tableName)} (${columnDefs.join(", ")});`);

    const selectParts = newColumnNames.map(name => {
      if (oldColumnNames.includes(name)) {
        return `${quoteIdent(name)} AS ${quoteIdent(name)}`;
      }
      if (name === "scope") {
        return `${scopeDefaultSql} AS ${quoteIdent(name)}`;
      }
      return `NULL AS ${quoteIdent(name)}`;
    });

    db.exec(
      `INSERT INTO ${quoteIdent(tableName)} (${newColumnNames.map(quoteIdent).join(", ")})
       SELECT ${selectParts.join(", ")}
         FROM ${quoteIdent(tempName)};`
    );
    db.exec(`DROP TABLE ${quoteIdent(tempName)};`);

    for (const index of indices) {
      if (index?.sql) {
        try {
          db.exec(index.sql);
        } catch (indexError) {
          console.warn(
            `[migration] unable to recreate index ${index?.name || "unknown"} on ${tableName}`,
            indexError?.message || indexError
          );
        }
      }
    }

    db.exec("COMMIT");
  } catch (error) {
    db.exec("ROLLBACK");
    console.warn(`[migration] unable to rebuild ${tableName} for scope`, error?.message || error);
    if (renamed) {
      try {
        if (tableExists(tempName) && !tableExists(tableName)) {
          db.exec(`ALTER TABLE ${quoteIdent(tempName)} RENAME TO ${quoteIdent(tableName)}`);
        }
      } catch (restoreError) {
        console.warn(
          `[migration] unable to restore ${tableName} after failed scope migration`,
          restoreError?.message || restoreError
        );
      }
    }
    return;
  }

  try {
    db.exec(
      `UPDATE ${quoteIdent(tableName)} SET scope = ${scopeDefaultSql}
         WHERE scope IS NULL OR TRIM(scope) = ''`
    );
  } catch (error) {
    console.warn(`[migration] unable to backfill scope for ${tableName}`, error?.message || error);
  }
}

function mapGlobalTask(row) {
  const mapped = mapTaskRow(row);
  if (!mapped) return null;
  return {
    id: mapped.id,
    title: mapped.title || mapped.name || "",
    description: mapped.description || null,
    icon: mapped.icon || null,
    youtube_url: mapped.youtube_url || null,
    base_points: Number(mapped.base_points ?? mapped.points ?? 0) || 0,
    status: mapped.status || "active",
    scope: mapped.scope || "family",
    created_at: mapped.created_at,
    updated_at: mapped.updated_at,
    source_version: mapped.source_version ?? 0
  };
}

function mapGlobalReward(row) {
  const mapped = mapRewardRow(row);
  if (!mapped) return null;
  return {
    id: mapped.id,
    title: mapped.title || mapped.name || "",
    description: mapped.description || null,
    icon: mapped.icon || mapped.image_url || null,
    youtube_url: mapped.youtube_url || null,
    base_cost: Number(mapped.base_cost ?? mapped.cost ?? mapped.price ?? 0) || 0,
    status: mapped.status || "active",
    scope: mapped.scope || "family",
    created_at: mapped.created_at,
    updated_at: mapped.updated_at,
    source_version: mapped.source_version ?? 0
  };
}

function normalizeLegacyMemberId(raw) {
  if (raw === null || raw === undefined) return null;
  const trimmed = String(raw).trim();
  if (!trimmed) return null;
  let normalized = trimmed.toLowerCase();
  const prefixes = [
    "child:",
    "child-",
    "child/",
    "kid:",
    "kid-",
    "member:",
    "member-",
    "user:",
    "user-",
    "parent:",
    "parent-",
    "children:",
    "children-"
  ];
  for (const prefix of prefixes) {
    if (normalized.startsWith(prefix)) {
      normalized = normalized.slice(prefix.length);
      break;
    }
  }
  normalized = normalized.replace(/^[^a-z0-9]+/, "");
  normalized = normalized.replace(/[^a-z0-9_-]/g, "");
  return normalized || null;
}

function toDisplayName(id, fallbackName) {
  const direct = fallbackName ? String(fallbackName).trim() : "";
  if (direct) return direct;
  const safeId = String(id || "").trim();
  if (!safeId) return "Member";
  const parts = safeId
    .split(/[^a-z0-9]+/i)
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1));
  if (parts.length) return parts.join(" ");
  return safeId.charAt(0).toUpperCase() + safeId.slice(1);
}

function randomId() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return crypto.randomBytes(16).toString("hex");
}


async function ensureColumn(db, table, column, type = "INTEGER") {
  // 1) column already exists?
  const cols = db.prepare(`PRAGMA table_info(${table})`).all();
  if (cols.some(c => c.name === column)) return;

  // 2) add column WITHOUT DEFAULT (SQLite ALTER only allows literal defaults; skip entirely)
  const sql = `ALTER TABLE ${table} ADD COLUMN ${column} ${type}`;
  try {
    db.exec(sql);
  } catch (e) {
    // If something truly blocks (e.g., duplicate), rethrow
    throw new Error(`[ensureColumn] ${table}.${column}: ${e.message}`);
  }

  // 3) backfill common timestamp columns in one go (epoch seconds)
  if (column === "created_at" || column === "updated_at") {
    const now = Math.floor(Date.now() / 1000);
    db.prepare(
      `UPDATE ${table}
         SET ${column} = COALESCE(${column}, ?)
       WHERE ${column} IS NULL OR ${column} = 0`
    ).run(now);
  }
}

// Safe column add: never uses NOT NULL on ALTER; supports default and backfill.
function addCol(table, col, type, {
  defaultSql = null,     // e.g., "'posted'" or "0" or "CURRENT_TIMESTAMP"
  backfillSql = null,    // e.g., "'posted'" or "unixepoch('now')" or "0"
  ifNotExists = true
} = {}) {
  const hasCol = !!db.prepare(`PRAGMA table_info(${table})`).all()
    .some(r => r.name === col);
  if (ifNotExists && hasCol) return;

  // 1) Add column as NULLABLE, with DEFAULT only if you want new rows to get it.
  const def = defaultSql ? ` DEFAULT ${defaultSql}` : '';
  const defLog = defaultSql ?? "null";
  const backfillLog = backfillSql ?? "null";
  const alterSql = `ALTER TABLE ${table} ADD COLUMN ${col} ${type}${def};`;
  console.log(`[ensureLedgerSchema] Adding column ${col} to ${table} (type=${type}, default=${defLog}, backfill=${backfillLog})`);
  try {
    db.exec(alterSql);
  } catch (err) {
    console.error(`[ensureLedgerSchema] Failed SQL: ${alterSql}`);
    throw err;
  }

  // 2) Backfill existing rows so the app logic has non-null data to work with.
  if (backfillSql) {
    const updateSql = `UPDATE ${table} SET ${col} = ${backfillSql} WHERE ${col} IS NULL;`;
    try {
      db.exec(updateSql);
    } catch (err) {
      console.error(`[ensureLedgerSchema] Failed SQL: ${updateSql}`);
      throw err;
    }
  }
}

function importLegacyRewards() {
  if (!tableExists("reward") || !tableExists("rewards")) {
    return;
  }
  let rows = [];
  try {
    rows = db.prepare("SELECT * FROM rewards").all();
  } catch (err) {
    console.warn("legacy reward import skipped", err?.message || err);
    return;
  }
  if (!rows.length) return;

  const insert = db.prepare(`
    INSERT OR IGNORE INTO reward (
      id, name, cost, description, image_url, youtube_url,
      status, tags, campaign_id, source, created_at, updated_at
    ) VALUES (@id, @name, @cost, @description, @image_url, @youtube_url,
      @status, @tags, @campaign_id, @source, @created_at, @updated_at)
  `);
  const now = Date.now();
  const payload = rows.map(row => {
    const rawId = row.id ?? row.ID ?? randomId();
    const id = String(rawId).trim() || randomId();
    const createdAt = normalizeTimestamp(row.created_at ?? row.createdAt, now);
    const updatedAt = normalizeTimestamp(row.updated_at ?? row.updatedAt, createdAt);
    const costValue = Number(row.cost ?? row.price ?? 0);
    const status = Number(row.active ?? row.status ?? 1) === 1 ? "active" : "disabled";
    return {
      id,
      name: row.name ? String(row.name).trim() || id : id,
      cost: Number.isFinite(costValue) ? Math.trunc(costValue) : 0,
      description: row.description ? String(row.description) : "",
      image_url: row.image_url ? String(row.image_url).trim() : row.imageUrl ? String(row.imageUrl).trim() : "",
      youtube_url: row.youtube_url ? String(row.youtube_url).trim() : row.youtubeUrl ? String(row.youtubeUrl).trim() : null,
      status,
      tags: null,
      campaign_id: null,
      source: "legacy",
      created_at: createdAt,
      updated_at: updatedAt
    };
  });

  const insertMany = db.transaction(items => {
    for (const item of items) {
      insert.run(item);
    }
  });
  insertMany(payload);
}

function importLegacyMembers() {
  if (!tableExists("member")) return;

  const existing = new Set(
    db.prepare("SELECT id FROM member").all().map(row => row.id)
  );

  const insert = db.prepare(`
    INSERT OR IGNORE INTO member (
      id, name, date_of_birth, sex, status, tags, campaign_id, source, created_at, updated_at
    ) VALUES (@id, @name, @date_of_birth, @sex, @status, @tags, @campaign_id, @source, @created_at, @updated_at)
  `);

  const candidates = new Map();
  function enqueue(rawId, displayName, createdAt) {
    const normalized = normalizeLegacyMemberId(rawId);
    if (!normalized || normalized === "system") return;
    if (existing.has(normalized) || candidates.has(normalized)) return;
    const created = normalizeTimestamp(createdAt, Date.now());
    candidates.set(normalized, {
      id: normalized,
      name: toDisplayName(normalized, displayName),
      date_of_birth: null,
      sex: null,
      status: "active",
      tags: null,
      campaign_id: null,
      source: "legacy",
      created_at: created,
      updated_at: created
    });
  }

  const backupPath = join(__dirname, "parentshop.backup.db");
  if (fs.existsSync(backupPath)) {
    const alias = "legacy_backup";
    const escaped = backupPath.replace(/'/g, "''");
    try {
      db.exec(`ATTACH DATABASE '${escaped}' AS ${alias}`);
      try {
        const hasUsers = db
          .prepare(`SELECT name FROM ${alias}.sqlite_master WHERE type='table' AND name='users'`)
          .get();
        if (hasUsers) {
          const rows = db.prepare(`SELECT id, display_name, created_at FROM ${alias}.users`).all();
          for (const row of rows) {
            enqueue(row.id, row.display_name, row.created_at);
          }
        }
        const hasBalances = db
          .prepare(`SELECT name FROM ${alias}.sqlite_master WHERE type='table' AND name='balances'`)
          .get();
        if (hasBalances) {
          const rows = db.prepare(`SELECT DISTINCT user_id FROM ${alias}.balances`).all();
          for (const row of rows) {
            enqueue(row.user_id, null, null);
          }
        }
        const hasLedger = db
          .prepare(`SELECT name FROM ${alias}.sqlite_master WHERE type='table' AND name='ledger'`)
          .get();
        if (hasLedger) {
          const rows = db.prepare(`SELECT DISTINCT user_id FROM ${alias}.ledger`).all();
          for (const row of rows) {
            enqueue(row.user_id, null, null);
          }
        }
      } finally {
        db.exec(`DETACH DATABASE ${alias}`);
      }
    } catch (err) {
      console.warn("legacy member import failed", err?.message || err);
    }
  }

  if (tableExists("users")) {
    try {
      const rows = db.prepare("SELECT id, display_name, created_at FROM users").all();
      for (const row of rows) {
        enqueue(row.id, row.display_name, row.created_at);
      }
    } catch (err) {
      console.warn("legacy users table import skipped", err?.message || err);
    }
  }

  if (tableExists("balances")) {
    try {
      const rows = db.prepare("SELECT DISTINCT user_id FROM balances").all();
      for (const row of rows) {
        enqueue(row.user_id, null, null);
      }
    } catch (err) {
      console.warn("legacy balances import skipped", err?.message || err);
    }
  }

  if (tableExists("ledger")) {
    try {
      const rows = db.prepare("SELECT DISTINCT user_id FROM ledger").all();
      for (const row of rows) {
        enqueue(row.user_id, null, null);
      }
    } catch (err) {
      console.warn("legacy ledger import skipped", err?.message || err);
    }
  }

  if (!candidates.size) return;

  const insertMany = db.transaction(items => {
    for (const item of items) {
      insert.run(item);
      existing.add(item.id);
    }
  });
  insertMany([...candidates.values()]);
}

function rebuildLedgerTableIfLegacy() {
  const hasLedger = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='ledger'").get();
  if (!hasLedger) return false;

  if (!isLegacyLedger()) return false;

  const cols = db.prepare("PRAGMA table_info(ledger)").all().map(c => c.name);
  const colSet = new Set(cols);
  const hasCol = name => colSet.has(name);
  const buildCoalesce = (names, fallbackSql = null) => {
    const terms = names.filter(hasCol).map(name => `L.${name}`);
    if (fallbackSql) terms.push(fallbackSql);
    if (terms.length === 0) return fallbackSql ?? 'NULL';
    if (terms.length === 1) return terms[0];
    return `COALESCE(${terms.join(', ')})`;
  };

  // Make sure a fallback user exists
  const sys = db.prepare("SELECT 1 FROM member WHERE id = 'system'").get();
  if (!sys) {
    const ts = Date.now();
    db.prepare(`
      INSERT INTO member (id, family_id, name, status, created_at, updated_at)
      VALUES ('system', 'default', 'System', 'active', ?, ?)
    `).run(ts, ts);
  }

  db.exec("PRAGMA foreign_keys = OFF;");

  db.exec(`
    CREATE TABLE IF NOT EXISTS ledger_new (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      verb TEXT NOT NULL,
      amount INTEGER NOT NULL DEFAULT 0,
      balance_after INTEGER,
      reward_id TEXT,
      parent_hold_id TEXT,
      parent_ledger_id TEXT,
      template_ids TEXT,
      final_amount INTEGER,
      note TEXT,
      notes TEXT,
      actor_id TEXT,
      ip_address TEXT,
      user_agent TEXT,
      status TEXT NOT NULL DEFAULT 'posted',
      source TEXT,
      tags TEXT,
      campaign_id TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      metadata TEXT,
      refund_reason TEXT,
      refund_notes TEXT,
      idempotency_key TEXT,
      family_id TEXT NOT NULL,
      FOREIGN KEY (user_id) REFERENCES member(id) ON DELETE CASCADE,
      FOREIGN KEY (reward_id) REFERENCES reward(id),
      FOREIGN KEY (parent_hold_id) REFERENCES hold(id),
      FOREIGN KEY (parent_ledger_id) REFERENCES ledger(id)
    );
  `);

  // Copy & coerce from legacy, guarding all FKs with EXISTS
  const nowExpr = "strftime('%s','now')*1000";
  const userIdSources = ['user_id', 'userId'].filter(hasCol);
  const userIdExpr = userIdSources.length === 0
    ? null
    : (userIdSources.length === 1 ? `L.${userIdSources[0]}` : `COALESCE(${userIdSources.map(name => `L.${name}`).join(', ')})`);
  const userIdSelect = userIdExpr
    ? `CASE WHEN ${userIdExpr} IS NOT NULL AND EXISTS(SELECT 1 FROM member M WHERE M.id = ${userIdExpr}) THEN ${userIdExpr} ELSE 'system' END`
    : `'system'`;

  const rewardIdExpr = buildCoalesce(['reward_id', 'itemId']);
  const rewardIdSelect = rewardIdExpr === 'NULL'
    ? 'NULL'
    : `CASE WHEN ${rewardIdExpr} IS NOT NULL AND EXISTS(SELECT 1 FROM reward R WHERE R.id = ${rewardIdExpr}) THEN ${rewardIdExpr} ELSE NULL END`;

  const parentHoldExpr = buildCoalesce(['parent_hold_id', 'holdId']);
  const parentHoldSelect = parentHoldExpr === 'NULL'
    ? 'NULL'
    : `CASE WHEN ${parentHoldExpr} IS NOT NULL AND EXISTS(SELECT 1 FROM hold H WHERE H.id = ${parentHoldExpr}) THEN ${parentHoldExpr} ELSE NULL END`;

  const parentLedgerExpr = buildCoalesce(['parent_ledger_id', 'parent_tx_id']);
  const parentLedgerSelect = parentLedgerExpr === 'NULL'
    ? 'NULL'
    : `CASE WHEN ${parentLedgerExpr} IS NOT NULL AND EXISTS(SELECT 1 FROM ledger LL WHERE LL.id = ${parentLedgerExpr}) THEN ${parentLedgerExpr} ELSE NULL END`;

  const tagsSelect = hasCol('tags')
    ? `CASE WHEN L.tags IS NULL OR L.tags = '' THEN '[]' ELSE L.tags END`
    : `'[]'`;

  const updatedAtSources = [];
  if (hasCol('updated_at')) updatedAtSources.push('L.updated_at');
  if (hasCol('created_at')) updatedAtSources.push('L.created_at');
  if (hasCol('at')) updatedAtSources.push('L.at');
  updatedAtSources.push(nowExpr);
  const updatedAtExpr = updatedAtSources.length === 1
    ? updatedAtSources[0]
    : `COALESCE(${updatedAtSources.join(', ')})`;

  db.exec(`
    INSERT INTO ledger_new (
      id, user_id, verb, amount, balance_after,
      reward_id, parent_hold_id, parent_ledger_id,
      template_ids, final_amount, note, notes,
      actor_id, ip_address, user_agent, status, source, tags,
      campaign_id, created_at, updated_at, metadata,
      refund_reason, refund_notes, idempotency_key
    )
    SELECT
      L.id,
      ${userIdSelect} AS user_id,
      ${buildCoalesce(['verb', 'action'], "'adjust'")} AS verb,
      ${buildCoalesce(['amount', 'delta'], '0')} AS amount,
      ${buildCoalesce(['balance_after'])} AS balance_after,

      ${rewardIdSelect} AS reward_id,

      ${parentHoldSelect} AS parent_hold_id,

      ${parentLedgerSelect} AS parent_ledger_id,

      ${buildCoalesce(['template_ids', 'templates'])} AS template_ids,
      ${buildCoalesce(['final_amount', 'finalCost'])} AS final_amount,
      ${buildCoalesce(['note'])} AS note,
      ${buildCoalesce(['notes'])} AS notes,
      ${buildCoalesce(['actor_id', 'actor'])} AS actor_id,
      ${buildCoalesce(['ip_address', 'ip'])} AS ip_address,
      ${buildCoalesce(['user_agent', 'ua'])} AS user_agent,
      ${buildCoalesce(['status'], "'posted'")} AS status,
      ${buildCoalesce(['source'])} AS source,
      ${tagsSelect} AS tags,
      ${buildCoalesce(['campaign_id'])} AS campaign_id,
      ${buildCoalesce(['created_at', 'at'], nowExpr)} AS created_at,
      ${updatedAtExpr} AS updated_at,
      ${buildCoalesce(['metadata'])} AS metadata,
      ${buildCoalesce(['refund_reason'])} AS refund_reason,
      ${buildCoalesce(['refund_notes'])} AS refund_notes,
      ${buildCoalesce(['idempotency_key'])} AS idempotency_key
    FROM ledger L;
  `);

  db.exec("DROP TABLE ledger;");
  db.exec("ALTER TABLE ledger_new RENAME TO ledger;");

  db.exec("CREATE INDEX IF NOT EXISTS idx_ledger_user_id_created_at ON ledger(user_id, created_at DESC);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_ledger_verb_created_at ON ledger(verb, created_at DESC);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_ledger_status_created_at ON ledger(status, created_at DESC);");

  db.exec("PRAGMA foreign_keys = ON;");
  return true;
}

function isLegacyLedger() {
  const info = db.prepare("PRAGMA table_info(ledger)").all();
  if (info.length === 0) return false;
  const cols = info.map(c => c.name);
  const idCol = info.find(c => c.name === 'id');
  const idIsText = idCol ? String(idCol.type || '').toUpperCase().includes('TEXT') : false;
  return (
    !idIsText ||
    cols.includes('userId') ||
    cols.includes('action') ||
    cols.includes('at') ||
    cols.includes('delta') ||
    cols.includes('kind') ||
    cols.includes('ts')
  );
}

function ensureLedgerSchema() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS ledger (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      verb TEXT NOT NULL,
      amount INTEGER NOT NULL,
      balance_after INTEGER,
      description TEXT,
      reward_id TEXT,
      parent_hold_id TEXT,
      parent_ledger_id TEXT,
      template_ids TEXT,
      final_amount INTEGER,
      note TEXT,
      notes TEXT,
      actor_id TEXT,
      ip_address TEXT,
      user_agent TEXT,
      status TEXT NOT NULL DEFAULT 'posted',
      source TEXT,
      tags TEXT,
      campaign_id TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      metadata TEXT,
      refund_reason TEXT,
      refund_notes TEXT,
      idempotency_key TEXT,
      family_id TEXT NOT NULL,
      FOREIGN KEY (user_id) REFERENCES member(id),
      FOREIGN KEY (reward_id) REFERENCES reward(id),
      FOREIGN KEY (parent_hold_id) REFERENCES hold(id),
      FOREIGN KEY (parent_ledger_id) REFERENCES ledger(id)
    );
  `);

  if (isLegacyLedger()) {
    return;
  }

  addCol('ledger', 'user_id', 'TEXT',       { defaultSql: null, backfillSql: null });
  addCol('ledger', 'verb',    'TEXT',       { defaultSql: null, backfillSql: null });
  addCol('ledger', 'status',  'TEXT',       { defaultSql: "'posted'", backfillSql: "'posted'" });
  addCol('ledger', 'description', 'TEXT',   { defaultSql: "NULL",     backfillSql: "NULL" });
  addCol('ledger', 'note',   'TEXT',        { defaultSql: "NULL",     backfillSql: "NULL" });
  addCol('ledger', 'notes',  'TEXT',        { defaultSql: "NULL",     backfillSql: "NULL" });
  addCol('ledger', 'source', 'TEXT',        { defaultSql: "NULL",     backfillSql: "NULL" });
  addCol('ledger', 'tags',   'TEXT',        { defaultSql: "'[]'",     backfillSql: "'[]'" });
  addCol('ledger', 'metadata','TEXT',       { defaultSql: "NULL",     backfillSql: "NULL" });

  addCol('ledger', 'template_ids', 'TEXT', { defaultSql: "NULL", backfillSql: "NULL" });
  addCol('ledger', 'amount',        'INTEGER', { defaultSql: "0",    backfillSql: "0" });
  addCol('ledger', 'balance_after', 'INTEGER', { defaultSql: "0",    backfillSql: "0" });
  addCol('ledger', 'final_amount',  'INTEGER', { defaultSql: "NULL", backfillSql: "NULL" });

  addCol('ledger', 'reward_id',       'TEXT', { defaultSql: "NULL", backfillSql: "NULL" });
  addCol('ledger', 'parent_hold_id',  'TEXT', { defaultSql: "NULL", backfillSql: "NULL" });
  addCol('ledger', 'parent_ledger_id','TEXT', { defaultSql: "NULL", backfillSql: "NULL" });
  addCol('ledger', 'campaign_id',     'TEXT', { defaultSql: "NULL", backfillSql: "NULL" });
  addCol('ledger', 'actor_id',        'TEXT', { defaultSql: "NULL", backfillSql: "NULL" });
  addCol('ledger', 'ip_address',      'TEXT', { defaultSql: "NULL", backfillSql: "NULL" });
  addCol('ledger', 'user_agent',      'TEXT', { defaultSql: "NULL", backfillSql: "NULL" });
  addCol('ledger', 'refund_reason',   'TEXT', { defaultSql: "NULL", backfillSql: "NULL" });
  addCol('ledger', 'refund_notes',    'TEXT', { defaultSql: "NULL", backfillSql: "NULL" });
  addCol('ledger', 'idempotency_key', 'TEXT', { defaultSql: "NULL", backfillSql: "NULL" });

  addCol('ledger', 'created_at', 'INTEGER', { defaultSql: null, backfillSql: "strftime('%s','now')*1000" });
  addCol('ledger', 'updated_at', 'INTEGER', { defaultSql: null, backfillSql: "strftime('%s','now')*1000" });

  db.exec(`CREATE INDEX IF NOT EXISTS idx_ledger_user_id_created_at ON ledger(user_id, created_at DESC);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_ledger_verb_created_at ON ledger(verb, created_at DESC);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_ledger_status_created_at ON ledger(status, created_at DESC);`);
}

function ensureMemberTable() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS member (
      id TEXT PRIMARY KEY,
      family_id TEXT NOT NULL,
      name TEXT NOT NULL,
      date_of_birth TEXT,
      sex TEXT,
      status TEXT NOT NULL DEFAULT 'active',
      tags TEXT,
      campaign_id TEXT,
      source TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);
  ensureColumn(db, "member", "status", "TEXT");
  ensureColumn(db, "member", "date_of_birth", "TEXT");
  ensureColumn(db, "member", "sex", "TEXT");
  ensureColumn(db, "member", "tags", "TEXT");
  ensureColumn(db, "member", "campaign_id", "TEXT");
  ensureColumn(db, "member", "source", "TEXT");
  ensureColumn(db, "member", "created_at", "INTEGER");
  ensureColumn(db, "member", "updated_at", "INTEGER");
  db.exec("CREATE INDEX IF NOT EXISTS idx_member_status ON member(status)");
}
// BEGIN ensureTables
const ensureTables = db.transaction(() => {
  const tableExists = name =>
    !!db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?").get(name);
  const backupTable = name => {
    const legacyName = `${name}_legacy_${Date.now()}`;
    if (!isSafeIdent(name)) throw new Error(`Unsafe table name: ${name}`);
    if (!isSafeIdent(legacyName)) throw new Error(`Unsafe table name: ${legacyName}`);
    db.exec("ALTER TABLE " + quoteIdent(name) + " RENAME TO " + quoteIdent(legacyName));
    return legacyName;
  };
  // --- Identifier helpers for SQLite DDL ---
  function isSafeIdent(s) {
    return typeof s === "string" && /^[A-Za-z_][A-Za-z0-9_]*$/.test(s);
  }
  function quoteIdent(s) {
    return '"' + String(s).replace(/"/g, '""') + '"';
  }

  const dropTable = name => {
    if (!name) return false;
    if (!isSafeIdent(name)) throw new Error(`Unsafe table name: ${name}`);
    db.exec("DROP TABLE IF EXISTS " + quoteIdent(name));
    return true;
  };

  ensureMemberTable();

  db.exec(`
    CREATE TABLE IF NOT EXISTS task (
      id TEXT PRIMARY KEY,
      scope TEXT NOT NULL DEFAULT 'family',
      family_id TEXT,
      title TEXT NOT NULL,
      description TEXT,
      icon TEXT,
      points INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'active',
      master_task_id TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);
  ensureColumn(db, "task", "family_id", "TEXT");
  ensureColumn(db, "task", "title", "TEXT");
  ensureColumn(db, "task", "description", "TEXT");
  ensureColumn(db, "task", "icon", "TEXT");
  ensureColumn(db, "task", "points", "INTEGER");
  ensureColumn(db, "task", "status", "TEXT");
  ensureColumn(db, "task", "master_task_id", "TEXT");
  ensureColumn(db, "task", "created_at", "INTEGER");
  ensureColumn(db, "task", "updated_at", "INTEGER");
  db.exec("CREATE INDEX IF NOT EXISTS idx_task_status ON task(status)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_task_family_status ON task(family_id, status)");

  db.exec(`
    CREATE TABLE IF NOT EXISTS reward (
      id TEXT PRIMARY KEY,
      scope TEXT NOT NULL DEFAULT 'family',
      family_id TEXT,
      name TEXT NOT NULL,
      cost INTEGER NOT NULL,
      description TEXT DEFAULT '',
      image_url TEXT DEFAULT '',
      youtube_url TEXT,
      status TEXT NOT NULL DEFAULT 'active',
      tags TEXT,
      campaign_id TEXT,
      source TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);
  ensureColumn(db, "reward", "name", "TEXT");
  ensureColumn(db, "reward", "cost", "INTEGER");
  ensureColumn(db, "reward", "description", "TEXT");
  ensureColumn(db, "reward", "image_url", "TEXT");
  ensureColumn(db, "reward", "youtube_url", "TEXT");
  ensureColumn(db, "reward", "status", "TEXT");
  ensureColumn(db, "reward", "tags", "TEXT");
  ensureColumn(db, "reward", "campaign_id", "TEXT");
  ensureColumn(db, "reward", "source", "TEXT");
  ensureColumn(db, "reward", "master_reward_id", "TEXT");
  ensureColumn(db, "reward", "created_at", "INTEGER");
  ensureColumn(db, "reward", "updated_at", "INTEGER");
  db.exec("CREATE INDEX IF NOT EXISTS idx_reward_status ON reward(status)");
 
  // --- HOLD table (pending token holds/escrows) ---
  if (!tableExists("hold")) {
    // Optional legacy migrations (uncomment if you actually had these):
    // if (tableExists("holds")) backupTable("holds");
    // if (tableExists("token_hold")) backupTable("token_hold");

    db.exec(`
      CREATE TABLE IF NOT EXISTS hold (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        amount INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending', -- pending|released|expired|canceled
        reason TEXT,
        metadata TEXT,          -- JSON string (optional)
        expires_at INTEGER,     -- epoch ms (optional)
        released_at INTEGER,    -- epoch ms (optional)
        release_txn_id TEXT,    -- link to a ledger/txn table if applicable
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        FOREIGN KEY (user_id) REFERENCES member(id)
      );
    `);
  }
  // Minimal indexes AFTER table exists:
  db.exec(`CREATE INDEX IF NOT EXISTS idx_hold_user ON hold(user_id)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_hold_status ON hold(status)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_hold_expires ON hold(expires_at)`);
  db.exec("CREATE INDEX IF NOT EXISTS idx_hold_user_status ON hold(user_id, status)");

  ensureColumn(db, "hold", "user_id", "TEXT");
  ensureColumn(db, "hold", "actor_id", "TEXT");
  ensureColumn(db, "hold", "reward_id", "TEXT");
  ensureColumn(db, "hold", "reward_name", "TEXT");
  ensureColumn(db, "hold", "reward_image_url", "TEXT");
  ensureColumn(db, "hold", "status", "TEXT");
  ensureColumn(db, "hold", "quoted_amount", "INTEGER");
  ensureColumn(db, "hold", "final_amount", "INTEGER");
  ensureColumn(db, "hold", "note", "TEXT");
  ensureColumn(db, "hold", "metadata", "TEXT");
  ensureColumn(db, "hold", "source", "TEXT");
  ensureColumn(db, "hold", "tags", "TEXT");
  ensureColumn(db, "hold", "campaign_id", "TEXT");
  ensureColumn(db, "hold", "created_at", "INTEGER");
  ensureColumn(db, "hold", "updated_at", "INTEGER");
  ensureColumn(db, "hold", "released_at", "INTEGER");
  ensureColumn(db, "hold", "redeemed_at", "INTEGER");
  ensureColumn(db, "hold", "expires_at", "INTEGER");

  db.exec(`
    CREATE TABLE IF NOT EXISTS spend_request (
      id TEXT PRIMARY KEY,
      token TEXT UNIQUE,
      user_id TEXT NOT NULL,
      reward_id TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      amount INTEGER,
      title TEXT,
      image_url TEXT,
      actor_id TEXT,
      source TEXT,
      tags TEXT,
      campaign_id TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      FOREIGN KEY (user_id) REFERENCES member(id),
      FOREIGN KEY (reward_id) REFERENCES reward(id)
    );
  `);
  ensureColumn(db, "spend_request", "token", "TEXT");
  ensureColumn(db, "spend_request", "user_id", "TEXT");
  ensureColumn(db, "spend_request", "reward_id", "TEXT");
  ensureColumn(db, "spend_request", "status", "TEXT");
  ensureColumn(db, "spend_request", "amount", "INTEGER");
  ensureColumn(db, "spend_request", "title", "TEXT");
  ensureColumn(db, "spend_request", "image_url", "TEXT");
  ensureColumn(db, "spend_request", "actor_id", "TEXT");
  ensureColumn(db, "spend_request", "source", "TEXT");
  ensureColumn(db, "spend_request", "tags", "TEXT");
  ensureColumn(db, "spend_request", "campaign_id", "TEXT");
  ensureColumn(db, "spend_request", "created_at", "INTEGER");
  ensureColumn(db, "spend_request", "updated_at", "INTEGER");
  db.exec("CREATE INDEX IF NOT EXISTS idx_spend_request_token ON spend_request(token)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_spend_request_user_status ON spend_request(user_id, status)");

  db.exec(`
    CREATE TABLE IF NOT EXISTS ledger (
      id TEXT PRIMARY KEY,
      token TEXT UNIQUE,
      user_id TEXT NOT NULL,
      reward_id TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      amount INTEGER,
      title TEXT,
      image_url TEXT,
      actor_id TEXT,
      source TEXT,
      tags TEXT,
      idempotency_key TEXT,
      campaign_id TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      family_id TEXT NOT NULL,
      FOREIGN KEY (user_id) REFERENCES member(id),
      FOREIGN KEY (reward_id) REFERENCES reward(id)
    );
  `);
  ensureColumn(db, "spend_request", "token", "TEXT");
  ensureColumn(db, "spend_request", "user_id", "TEXT");
  ensureColumn(db, "spend_request", "reward_id", "TEXT");
  ensureColumn(db, "spend_request", "status", "TEXT");
  ensureColumn(db, "spend_request", "amount", "INTEGER");
  ensureColumn(db, "spend_request", "title", "TEXT");
  ensureColumn(db, "spend_request", "image_url", "TEXT");
  ensureColumn(db, "spend_request", "actor_id", "TEXT");
  ensureColumn(db, "spend_request", "source", "TEXT");
  ensureColumn(db, "spend_request", "tags", "TEXT");
  ensureColumn(db, "spend_request", "campaign_id", "TEXT");
  ensureColumn(db, "spend_request", "created_at", "INTEGER");
  ensureColumn(db, "spend_request", "updated_at", "INTEGER");
  db.exec("CREATE INDEX IF NOT EXISTS idx_spend_request_token ON spend_request(token)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_spend_request_user_status ON spend_request(user_id, status)");

  db.exec(`
    CREATE TABLE IF NOT EXISTS ledger (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      actor_id TEXT,
      source TEXT,
      tags TEXT,
      campaign_id TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      FOREIGN KEY (user_id) REFERENCES member(id),
      FOREIGN KEY (reward_id) REFERENCES reward(id)
    );
  `);
});

function ensureConsumedTokens() {
  let consumedCols;
  const has = db
    .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='consumed_tokens'")
    .get();

  if (!has) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS consumed_tokens (
        id TEXT PRIMARY KEY,
        token TEXT,
        typ TEXT,
        request_id TEXT,
        user_id TEXT,
        reward_id TEXT,
        source TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        FOREIGN KEY (request_id) REFERENCES spend_request(id),
        FOREIGN KEY (user_id) REFERENCES member(id),
        FOREIGN KEY (reward_id) REFERENCES reward(id)
      );
    `);
  } else {
    consumedCols = db.prepare("PRAGMA table_info('consumed_tokens')").all().map(c => c.name);
    if (!consumedCols.includes("id") && consumedCols.includes("jti")) {
      db.exec("ALTER TABLE consumed_tokens RENAME COLUMN jti TO id");
      consumedCols = db.prepare("PRAGMA table_info('consumed_tokens')").all().map(c => c.name);
    }
    if (!consumedCols.includes("created_at") && consumedCols.includes("consumed_at")) {
      db.exec("ALTER TABLE consumed_tokens RENAME COLUMN consumed_at TO created_at");
      consumedCols = db.prepare("PRAGMA table_info('consumed_tokens')").all().map(c => c.name);
    }
    // Safe, nullable adds only
    ensureColumn(db, "consumed_tokens", "token", "TEXT");
    ensureColumn(db, "consumed_tokens", "typ", "TEXT");
    ensureColumn(db, "consumed_tokens", "request_id", "TEXT");
    ensureColumn(db, "consumed_tokens", "user_id", "TEXT");
    ensureColumn(db, "consumed_tokens", "reward_id", "TEXT");
    ensureColumn(db, "consumed_tokens", "source", "TEXT");
    ensureColumn(db, "consumed_tokens", "created_at", "INTEGER");
    ensureColumn(db, "consumed_tokens", "updated_at", "INTEGER");
  }

  db.exec("CREATE INDEX IF NOT EXISTS idx_consumed_tokens_user ON consumed_tokens(user_id)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_consumed_tokens_reward ON consumed_tokens(reward_id)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_consumed_tokens_request ON consumed_tokens(request_id)");
}

const ensureBaseSchema = db.transaction(() => {
  // 1) Create ALL base tables first (member, reward, hold, spend_request, etc.)
  ensureTables();

  // 2) Tables needed by other features
  ensureConsumedTokens();
});

const ensureLedgerSchemaTx = db.transaction(() => {
  ensureLedgerSchema();
});

function shouldEnableScopeMigration() {
  const raw = process.env.CK_MIGRATE_SCOPE;
  if (raw === undefined || raw === null || raw === "") {
    return true;
  }
  const normalized = String(raw).trim().toLowerCase();
  return !(normalized === "0" || normalized === "false");
}

function ensureScopeMigration() {
  const migrationEnabled = shouldEnableScopeMigration();
  const taskHasScope = tableHasColumn("task", "scope");
  const rewardHasScope = tableHasColumn("reward", "scope");

  if (!migrationEnabled) {
    if (!rewardHasScope) {
      console.info("[migration] CK_MIGRATE_SCOPE disabled; skipping scope migration");
    }
    return { taskScopeAdded: false, rewardScopeAdded: false, rewardHasScope };
  }

  const shouldMigrateTask = tableExists("task") && !taskHasScope;
  const shouldMigrateReward = tableExists("reward") && !rewardHasScope;

  if (!shouldMigrateTask && !shouldMigrateReward) {
    console.info("[migration] reward.scope already present; skipping scope migration");
    return { taskScopeAdded: false, rewardScopeAdded: false, rewardHasScope: true };
  }

  try {
    db.exec("BEGIN");
    const taskResult = shouldMigrateTask
      ? rebuildMenuTableForScope("task", {
          scopeDefault: "family",
          onBackfill: () => {
            db.exec("UPDATE task SET scope = 'global' WHERE master_task_id IS NOT NULL");
            db.exec(
              "UPDATE task SET scope = 'family' WHERE scope IS NULL OR TRIM(scope) = ''"
            );
          }
        })
      : { addedScopeColumn: false };

    const rewardResult = shouldMigrateReward
      ? rebuildMenuTableForScope("reward", {
          scopeDefault: "family",
          scopeCheck: "scope IN ('global','family')",
          onBackfill: () => {
            db.exec(
              "UPDATE reward SET scope = 'global' WHERE master_reward_id IS NOT NULL"
            );
            db.exec(
              "UPDATE reward SET scope = 'family' WHERE scope IS NULL OR TRIM(scope) = ''"
            );
          }
        })
      : { addedScopeColumn: false };

    db.exec("COMMIT");

    if (rewardResult.addedScopeColumn) {
      console.info("[migration] reward.scope column added and backfilled");
    } else {
      console.info("[migration] reward.scope already present; backfill skipped");
    }

    return {
      taskScopeAdded: !!taskResult.addedScopeColumn,
      rewardScopeAdded: !!rewardResult.addedScopeColumn,
      rewardHasScope: rewardHasScope || !!rewardResult.addedScopeColumn
    };
  } catch (error) {
    db.exec("ROLLBACK");
    console.warn("[migration] scope migration failed", error?.message || error);
    throw error;
  }
}

function ensureScopeIndexes() {
  const taskHasScope = tableHasColumn("task", "scope");
  const rewardHasScope = tableHasColumn("reward", "scope");

  if (taskHasScope && tableHasColumn("task", "family_id") && !indexExists("idx_task_scope_family")) {
    try {
      db.exec("CREATE INDEX idx_task_scope_family ON task(scope, family_id)");
    } catch (err) {
      console.warn("[schema] unable to ensure idx_task_scope_family", err?.message || err);
    }
  }

  if (
    rewardHasScope &&
    tableHasColumn("reward", "family_id") &&
    !indexExists("idx_reward_scope_family")
  ) {
    try {
      db.exec("CREATE INDEX idx_reward_scope_family ON reward(scope, family_id)");
    } catch (err) {
      console.warn("[schema] unable to ensure idx_reward_scope_family", err?.message || err);
    }
  }
}

function ensureSchema() {
  // Base tables + seeds run inside a transaction for atomicity.
  ensureBaseSchema();

  rebuildMenuTableForScope("task", { scopeDefault: "family" });
  rebuildMenuTableForScope("reward", { scopeDefault: "family" });

  try {
    db.exec("CREATE INDEX IF NOT EXISTS idx_task_scope_family ON task(scope, family_id)");
  } catch (err) {
    console.warn("[schema] unable to ensure idx_task_scope_family", err?.message || err);
  }
  try {
    db.exec("CREATE INDEX IF NOT EXISTS idx_reward_scope_family ON reward(scope, family_id)");
  } catch (err) {
    console.warn("[schema] unable to ensure idx_reward_scope_family", err?.message || err);
  }

  // Rebuild legacy -> modern ledger outside the transaction so we can
  // temporarily disable foreign key checks while swapping tables.
  rebuildLedgerTableIfLegacy();

  // Apply any incremental, safe column adds (no NOT NULL on ALTER).
  ensureLedgerSchemaTx();
}

ensureSchema();
importLegacyRewards();
importLegacyMembers();
ensureSystemMember();
ensureDefaultMembers();
ensureMasterCascadeTriggers();

const EARN_TEMPLATES_HAS_FAMILY_COLUMN = tableHasColumn("earn_templates", "family_id");
let REWARD_HAS_FAMILY_COLUMN = tableHasColumn("reward", "family_id");
if (!REWARD_HAS_FAMILY_COLUMN) {
  try {
    db.exec("ALTER TABLE reward ADD COLUMN family_id TEXT");
    db.prepare("UPDATE reward SET family_id = @family WHERE family_id IS NULL").run({ family: "default" });
    REWARD_HAS_FAMILY_COLUMN = tableHasColumn("reward", "family_id");
    if (REWARD_HAS_FAMILY_COLUMN) {
      db.exec("CREATE INDEX IF NOT EXISTS idx_reward_family ON reward(family_id)");
    }
  } catch (err) {
    if (!(err?.code === "SQLITE_ERROR" && /no such table/i.test(err?.message || ""))) {
      console.warn("[multitenant] unable to ensure reward.family_id column", err);
    }
  }
}
if (REWARD_HAS_FAMILY_COLUMN) {
  db.exec("CREATE INDEX IF NOT EXISTS idx_reward_family ON reward(family_id)");
}

const REWARD_HAS_IMAGE_URL_COLUMN = tableHasColumn("reward", "image_url");
const REWARD_HAS_YOUTUBE_URL_COLUMN = tableHasColumn("reward", "youtube_url");
const REWARD_HAS_STATUS_COLUMN = tableHasColumn("reward", "status");

const listPublicTasksStmt = EARN_TEMPLATES_HAS_FAMILY_COLUMN
  ? db.prepare(
      `SELECT id, title, points, description, youtube_url, sort_order
       FROM earn_templates
       WHERE family_id = @family_id AND active = 1
       ORDER BY sort_order ASC, id ASC`
    )
  : null;

const rewardPublicColumns = [
  "id",
  "name",
  "description",
  "cost",
  REWARD_HAS_IMAGE_URL_COLUMN ? "image_url" : "NULL AS image_url",
  REWARD_HAS_YOUTUBE_URL_COLUMN ? "youtube_url" : "NULL AS youtube_url",
  REWARD_HAS_STATUS_COLUMN ? "status" : "'active' AS status"
];

const listPublicRewardsStmt = REWARD_HAS_FAMILY_COLUMN
  ? db.prepare(
      `SELECT ${rewardPublicColumns.join(", ")}
       FROM reward
       WHERE family_id = @family_id AND (${REWARD_HAS_STATUS_COLUMN ? "status" : "'active'"} = 'active')
       ORDER BY created_at DESC, id DESC`
    )
  : null;

const childRewardColumns = [
  "r.id",
  "r.name",
  "r.cost",
  "r.description",
  "r.image_url",
  "r.youtube_url",
  "r.status",
  "r.tags",
  "r.created_at",
  "r.updated_at",
  "r.master_reward_id"
];
if (REWARD_HAS_FAMILY_COLUMN) {
  childRewardColumns.push("r.family_id");
}
childRewardColumns.push("mr.youtube_url AS master_youtube");

const childTaskColumns = [
  "t.id",
  "t.title",
  "t.description",
  "t.icon",
  "t.points",
  "t.status",
  "t.master_task_id"
];
if (tableHasColumn("task", "family_id")) {
  childTaskColumns.push("t.family_id");
}
childTaskColumns.push("mt.youtube_url AS master_youtube");

const CHILD_REWARDS_SQL = `
  SELECT ${childRewardColumns.join(",\n         ")}
    FROM reward r
    LEFT JOIN master_reward mr ON mr.id = r.master_reward_id
   WHERE r.family_id = @family_id
     AND r.status = 'active'
     AND (mr.id IS NULL OR mr.status = 'active')
   ORDER BY r.created_at DESC, r.id DESC
`;

const CHILD_TASKS_SQL = `
  SELECT ${childTaskColumns.join(",\n         ")}
    FROM task t
    LEFT JOIN master_task mt ON mt.id = t.master_task_id
   WHERE t.family_id = @family_id
     AND t.status = 'active'
     AND (mt.id IS NULL OR mt.status = 'active')
   ORDER BY t.created_at DESC, t.id DESC
`;

let listChildRewardsStmt = null;
let listChildTasksStmt = null;

function ensureChildRewardsStmt() {
  if (!listChildRewardsStmt) {
    if (!tableExists("reward")) return null;
    listChildRewardsStmt = db.prepare(CHILD_REWARDS_SQL);
  }
  return listChildRewardsStmt;
}

function ensureChildTasksStmt() {
  if (!listChildTasksStmt) {
    if (!tableExists("task")) return null;
    listChildTasksStmt = db.prepare(CHILD_TASKS_SQL);
  }
  return listChildTasksStmt;
}

let selectFamilyIdByMemberStmt = null;

function familyIdByMember(memberId) {
  const normalized = normId(memberId);
  if (!normalized) {
    return null;
  }
  try {
    if (!selectFamilyIdByMemberStmt) {
      if (!tableExists("member")) return null;
      selectFamilyIdByMemberStmt = db.prepare(
        "SELECT family_id FROM member WHERE LOWER(id) = LOWER(?) LIMIT 1"
      );
    }
    const row = selectFamilyIdByMemberStmt.get(normalized);
    return row?.family_id || null;
  } catch (err) {
    if (err?.code === "SQLITE_ERROR" && /no such table/i.test(err?.message || "")) {
      selectFamilyIdByMemberStmt = null;
      return null;
    }
    throw err;
  }
}

const insertFamilyStmt = db.prepare(
  `INSERT INTO family (id, name, email, status, admin_key, created_at, updated_at)
   VALUES (@id, @name, @email, @status, @admin_key, @created_at, @updated_at)`
);

const selectMasterTaskStmt = db.prepare(
  `SELECT id, title, description, icon, youtube_url, base_points, status, version, created_at, updated_at
   FROM master_task
   WHERE id = ?`
);
const listMasterTasksStmt = db.prepare(
  `SELECT id, title, description, icon, youtube_url, base_points, status, version, created_at, updated_at
   FROM master_task
   ORDER BY created_at DESC, id DESC`
);
const insertMasterTaskStmt = db.prepare(
  `INSERT INTO master_task (id, title, description, base_points, icon, youtube_url, status, version, created_at, updated_at)
   VALUES (@id, @title, @description, @base_points, @icon, @youtube_url, @status, @version, @created_at, @updated_at)`
);
const updateMasterTaskStmt = db.prepare(
  `UPDATE master_task
      SET title = @title,
          description = @description,
          icon = @icon,
          youtube_url = @youtube_url,
          base_points = @base_points,
          status = @status,
          version = @version,
          updated_at = @updated_at
    WHERE id = @id`
);

const selectMasterRewardStmt = db.prepare(
  `SELECT id, title, description, icon, youtube_url, base_cost, status, version, created_at, updated_at
   FROM master_reward
   WHERE id = ?`
);
const listMasterRewardsStmt = db.prepare(
  `SELECT id, title, description, icon, youtube_url, base_cost, status, version, created_at, updated_at
   FROM master_reward
   ORDER BY created_at DESC, id DESC`
);
const insertMasterRewardStmt = db.prepare(
  `INSERT INTO master_reward (id, title, description, base_cost, icon, youtube_url, status, version, created_at, updated_at)
   VALUES (@id, @title, @description, @base_cost, @icon, @youtube_url, @status, @version, @created_at, @updated_at)`
);
const updateMasterRewardStmt = db.prepare(
  `UPDATE master_reward
      SET title = @title,
          description = @description,
          icon = @icon,
          youtube_url = @youtube_url,
          base_cost = @base_cost,
          status = @status,
          version = @version,
          updated_at = @updated_at
    WHERE id = @id`
);

const listDismissedTemplatesStmt = db.prepare(
  `SELECT kind, master_id FROM dismissed_template WHERE family_id = ?`
);
const upsertDismissedTemplateStmt = db.prepare(
  `INSERT INTO dismissed_template (family_id, kind, master_id, dismissed_at)
   VALUES (@family_id, @kind, @master_id, @dismissed_at)
   ON CONFLICT(family_id, kind, master_id) DO UPDATE SET dismissed_at = excluded.dismissed_at`
);
const deleteDismissedTemplateStmt = db.prepare(
  `DELETE FROM dismissed_template WHERE family_id = @family_id AND kind = @kind AND master_id = @master_id`
);
const selectFamilyByIdStmt = db.prepare(
  "SELECT id, name, email, status, admin_key, created_at, updated_at FROM family WHERE id = ? LIMIT 1"
);
const updateFamilyStmt = db.prepare(
  `UPDATE family SET name = @name, status = @status, updated_at = @updated_at WHERE id = @id`
);
const updateFamilyAdminKeyStmt = db.prepare(
  `UPDATE family SET admin_key = @admin_key, updated_at = @updated_at WHERE id = @id`
);
const ledgerTableColumns = db.prepare("PRAGMA table_info('ledger')").all().map((col) => col.name);
const LEDGER_MEMBER_COLUMN = ledgerTableColumns.includes("member_id") ? "member_id" : "user_id";
const deleteFamilyByIdStmt = db.prepare(
  `DELETE FROM family WHERE id = @familyId`
);
const deleteMembersByFamilyStmt = db.prepare(
  `DELETE FROM member WHERE family_id = @familyId`
);
const deleteTasksByFamilyStmt = db.prepare(
  `DELETE FROM task WHERE family_id = @familyId`
);
const deleteLedgerByFamilyStmt = db.prepare(
  `DELETE FROM ledger WHERE family_id = @familyId OR ${LEDGER_MEMBER_COLUMN} IN (SELECT id FROM member WHERE family_id = @familyId)`
);
function deleteMemberTasksForFamily(payload) {
  if (!tableExists("member_task")) {
    return 0;
  }
  const columns = db.prepare("PRAGMA table_info('member_task')").all().map((col) => col.name);
  let memberColumn = null;
  if (columns.includes("member_id")) {
    memberColumn = "member_id";
  } else if (columns.includes("memberId")) {
    memberColumn = "memberId";
  }
  if (!memberColumn) {
    return 0;
  }
  const countStmt = db.prepare(
    `SELECT COUNT(*) AS count FROM member_task WHERE ${memberColumn} IN (SELECT id FROM member WHERE family_id = @familyId)`
  );
  const prior = countStmt.get(payload)?.count ?? 0;
  if (!prior) {
    return 0;
  }
  const stmt = db.prepare(
    `DELETE FROM member_task WHERE ${memberColumn} IN (SELECT id FROM member WHERE family_id = @familyId)`
  );
  stmt.run(payload);
  return Number(prior) || 0;
}

const runHardDeleteFamilyTxn = db.transaction((payload) => {
  const removed = {
    family: 0,
    members: 0,
    tasks: 0,
    ledger: 0,
    member_task: 0
  };
  removed.member_task = deleteMemberTasksForFamily(payload);
  removed.ledger = deleteLedgerByFamilyStmt.run(payload).changes;
  removed.tasks = deleteTasksByFamilyStmt.run(payload).changes;
  removed.members = deleteMembersByFamilyStmt.run(payload).changes;
  removed.family = deleteFamilyByIdStmt.run(payload).changes;
  return removed;
});

function hardDeleteFamilyCascade(familyId) {
  return runHardDeleteFamilyTxn({ familyId });
}

function ensureMemberFamilyColumn() {
  try {
    const columns = db.prepare("PRAGMA table_info(member)").all();
    const hasFamily = columns.some((col) => col.name === "family_id");
    if (!hasFamily) {
      db.exec("ALTER TABLE member ADD COLUMN family_id TEXT");
      db.prepare("UPDATE member SET family_id = @family WHERE family_id IS NULL").run({ family: "default" });
    }
  } catch (err) {
    console.warn("ensureMemberFamilyColumn failed", err);
  }
}

ensureMemberFamilyColumn();
function getHoldRow(holdId) {
  return db
    .prepare(
      `SELECT h.*, m.family_id AS member_family_id FROM hold h LEFT JOIN member m ON m.id = h.user_id WHERE h.id = ?`
    )
    .get(holdId);
}
function nowSec() {
  return Math.floor(Date.now() / 1000);
}

const selectMemberStmt = db.prepare(`
  SELECT id, name, date_of_birth, sex, status, created_at, updated_at
  FROM member
  WHERE id = ?
`);

const selectMemberWithFamilyStmt = db.prepare(`
  SELECT id, family_id, name, date_of_birth, sex, status, created_at, updated_at
  FROM member
  WHERE id = ?
`);

const listMembersStmt = db.prepare(`
  SELECT id, name, date_of_birth, sex, status, created_at, updated_at
  FROM member
  ORDER BY id ASC
  LIMIT 200
`);

const searchMembersStmt = db.prepare(`
  SELECT id, name, date_of_birth, sex, status, created_at, updated_at
  FROM member
  WHERE id LIKE @like OR LOWER(name) LIKE @like
  ORDER BY id ASC
  LIMIT 200
`);

const listMembersByFamilyStmt = db.prepare(`
  SELECT id, name, date_of_birth, sex, status, created_at, updated_at
  FROM member
  WHERE family_id = @family_id
  ORDER BY id ASC
  LIMIT 200
`);

const searchMembersByFamilyStmt = db.prepare(`
  SELECT id, name, date_of_birth, sex, status, created_at, updated_at
  FROM member
  WHERE family_id = @family_id
    AND (id LIKE @like OR LOWER(name) LIKE @like)
  ORDER BY id ASC
  LIMIT 200
`);

const insertMemberStmt = db.prepare(`
  INSERT INTO member (id, family_id, name, date_of_birth, sex, status, created_at, updated_at)
  VALUES (@id, @family_id, @name, @date_of_birth, @sex, @status, @created_at, @updated_at)
`);

const insertMemberForFamilyStmt = insertMemberStmt;

const MEMBER_HAS_FAMILY_COLUMN = tableHasColumn("member", "family_id");
const MEMBER_ID_PATTERN = /^[a-z0-9][a-z0-9._-]{1,62}$/i;

function ensureDefaultMembers() {
  ensureMemberTable();

  const memberCount = db.prepare("SELECT COUNT(*) AS c FROM member WHERE id != 'system'").get();
  if ((memberCount?.c ?? 0) > 0) {
    return;
  }

  const defaults = [
    { id: "leo", name: "Leo", date_of_birth: null, sex: null, status: "active" }
  ];

  const checkStmt = db.prepare("SELECT 1 FROM member WHERE id = ?");
  const insertStmt = db.prepare(`
    INSERT INTO member (id, family_id, name, date_of_birth, sex, status, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertMissing = db.transaction(members => {
    for (const member of members) {
      if (!checkStmt.get(member.id)) {
        const ts = Date.now();
        insertStmt.run(
          member.id,
          "default",
          member.name,
          member.date_of_birth,
          member.sex,
          member.status || "active",
          ts,
          ts
        );
      }
    }
  });

  insertMissing(defaults);
}

function ensureSystemMember() {
  const exists = db.prepare("SELECT 1 FROM member WHERE id = 'system'").get();
  if (!exists) {
    const ts = Date.now();
    db.prepare(`
      INSERT INTO member (id, family_id, name, status, created_at, updated_at)
      VALUES ('system', 'default', 'System', 'active', ?, ?)
    `).run(ts, ts);
  }
}

ensureDefaultMembers();
ensureSystemMember();

function getBalance(userId, familyId = null) {
  return Number(balanceOf(userId, familyId) || 0);
}

function mapMember(row) {
  if (!row) return null;
  return {
    userId: row.id,
    name: row.name,
    dob: row.date_of_birth || null,
    sex: row.sex || null,
    status: row.status || 'active',
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function getMember(userId) {
  const normalized = normId(userId);
  if (!normalized) return null;
  const row = selectMemberStmt.get(normalized);
  return mapMember(row);
}

function listMembers(search) {
  if (search) {
    const like = `%${search}%`;
    return searchMembersStmt.all({ like }).map(mapMember);
  }
  return listMembersStmt.all().map(mapMember);
}

let LEDGER_HAS_FAMILY_COLUMN = tableHasColumn("ledger", "family_id");
if (!LEDGER_HAS_FAMILY_COLUMN) {
  try {
    db.exec("ALTER TABLE ledger ADD COLUMN family_id TEXT");
    db.prepare("UPDATE ledger SET family_id = @family WHERE family_id IS NULL").run({ family: "default" });
    LEDGER_HAS_FAMILY_COLUMN = tableHasColumn("ledger", "family_id");
  } catch (err) {
    if (!(err?.code === "SQLITE_ERROR" && /no such table/i.test(err?.message || ""))) {
      console.warn("[multitenant] unable to ensure ledger.family_id column", err);
    }
  }
}

const selectLedgerByIdStmt = db.prepare("SELECT * FROM ledger WHERE id = ?");
const selectLedgerByKeyStmt = db.prepare("SELECT * FROM ledger WHERE idempotency_key = ?");
const sumRefundsByParentStmt = db.prepare(
  "SELECT COALESCE(SUM(amount), 0) AS total FROM ledger WHERE parent_ledger_id = ? AND verb = 'refund'"
);
const listLedgerByUserStmt = db.prepare(
  LEDGER_HAS_FAMILY_COLUMN
    ? `
  SELECT *
  FROM ledger
  WHERE user_id = @user_id
    AND (@family_id IS NULL OR family_id = @family_id)
  ORDER BY created_at DESC, id DESC
`
    : `
  SELECT *
  FROM ledger
  WHERE user_id = @user_id
  ORDER BY created_at DESC, id DESC
`
);
const checkTokenStmt = db.prepare("SELECT 1 FROM consumed_tokens WHERE id = ?");
const consumeTokenStmt = db.prepare(
  "INSERT INTO consumed_tokens (id, token, typ, source, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
);
const listRecentRedeemsStmt = db.prepare(
  LEDGER_HAS_FAMILY_COLUMN
    ? `
  SELECT id, created_at, amount
  FROM ledger
  WHERE user_id = @user_id
    AND (@family_id IS NULL OR family_id = @family_id)
    AND verb = 'redeem'
  ORDER BY created_at DESC, id DESC
  LIMIT 50
`
    : `
  SELECT id, created_at, amount
  FROM ledger
  WHERE user_id = @user_id
    AND verb = 'redeem'
  ORDER BY created_at DESC, id DESC
  LIMIT 50
`
);

const historyPreviewByUserStmt = db.prepare(
  LEDGER_HAS_FAMILY_COLUMN
    ? `
  SELECT created_at AS at, description AS action, amount AS delta, balance_after, note
  FROM ledger
  WHERE user_id = @user_id
    AND (@family_id IS NULL OR family_id = @family_id)
  ORDER BY created_at DESC, id DESC
  LIMIT @limit
`
    : `
  SELECT created_at AS at, description AS action, amount AS delta, balance_after, note
  FROM ledger
  WHERE user_id = @user_id
  ORDER BY created_at DESC, id DESC
  LIMIT @limit
`
);

function listLedgerRowsForUser(normalizedUserId, familyId) {
  if (LEDGER_HAS_FAMILY_COLUMN) {
    return listLedgerByUserStmt.all({ user_id: normalizedUserId, family_id: familyId });
  }
  return listLedgerByUserStmt.all({ user_id: normalizedUserId });
}

function listRecentRedeemsForUser(normalizedUserId, familyId) {
  if (LEDGER_HAS_FAMILY_COLUMN) {
    return listRecentRedeemsStmt.all({ user_id: normalizedUserId, family_id: familyId });
  }
  return listRecentRedeemsStmt.all({ user_id: normalizedUserId });
}
const findRecentHoldsStmt = db.prepare(`
  SELECT id, status, quoted_amount, final_amount, created_at
  FROM hold
  WHERE user_id = ?
  ORDER BY created_at DESC
  LIMIT 10
`);
const countPendingHoldsStmt = db.prepare(`
  SELECT COUNT(*) AS pending
  FROM hold
  WHERE user_id = ?
    AND status = 'pending'
`);

let selectMemberFamilyStmt = null;

function getMemberFamilyRow(normalizedUserId) {
  try {
    if (!selectMemberFamilyStmt) {
      selectMemberFamilyStmt = db.prepare(
        "SELECT family_id FROM member WHERE LOWER(id) = LOWER(?) LIMIT 1"
      );
    }
    return selectMemberFamilyStmt.get(normalizedUserId);
  } catch (err) {
    if (err?.code === "SQLITE_ERROR" && /no such table/i.test(err?.message || "")) {
      selectMemberFamilyStmt = null;
      return null;
    }
    throw err;
  }
}

function resolveMemberFamilyId(normalizedUserId, explicitFamilyId = null) {
  const normalizedExplicit = explicitFamilyId ? String(explicitFamilyId) : null;
  if (!normalizedUserId) {
    return normalizedExplicit ?? (MULTITENANT_ENFORCE ? null : "default");
  }
  const row = getMemberFamilyRow(normalizedUserId);
  const memberFamily = row?.family_id || null;
  if (normalizedExplicit) {
    if (memberFamily && memberFamily !== normalizedExplicit) {
      return MULTITENANT_ENFORCE ? null : memberFamily;
    }
    if (!memberFamily && MULTITENANT_ENFORCE) {
      return null;
    }
    return normalizedExplicit;
  }
  if (memberFamily) {
    return memberFamily;
  }
  return MULTITENANT_ENFORCE ? null : "default";
}

function mapLedgerRow(row) {
  if (!row) return null;
  let parsedTemplates = null;
  if (row.template_ids) {
    try {
      parsedTemplates = JSON.parse(row.template_ids);
    } catch {
      parsedTemplates = null;
    }
  }
  let parsedTags = null;
  if (row.tags) {
    try {
      parsedTags = JSON.parse(row.tags);
    } catch {
      parsedTags = null;
    }
  }
  const resolvedVerb = row.verb || (row.amount > 0 ? 'earn' : row.amount < 0 ? 'redeem' : 'adjust');
  return {
    id: row.id,
    at: row.created_at,
    userId: row.user_id,
    action: row.description,
    verb: resolvedVerb,
    delta: Number(row.amount),
    balance_after: Number(row.balance_after),
    itemId: row.reward_id || null,
    holdId: row.parent_hold_id || null,
    templates: parsedTemplates,
    finalCost: row.final_amount ?? null,
    note: row.note || null,
    notes: row.notes || null,
    actor: row.actor_id || null,
    ip: row.ip_address || null,
    ua: row.user_agent || null,
    parent_tx_id: row.parent_ledger_id || null,
    refund_reason: row.refund_reason || null,
    refund_notes: row.refund_notes || null,
    idempotency_key: row.idempotency_key || null,
    status: row.status || 'posted',
    source: row.source || null,
    tags: parsedTags,
    campaign_id: row.campaign_id || null,
    updated_at: row.updated_at,
    metadata: row.metadata || null,
    family_id: row.family_id || null
  };
}

function mapHoldRow(row) {
  if (!row) return null;
  let parsedMetadata = null;
  if (row.metadata) {
    try {
      parsedMetadata = JSON.parse(row.metadata);
    } catch {
      parsedMetadata = null;
    }
  }
  return {
    id: row.id,
    userId: row.user_id || null,
    familyId: row.family_id ?? row.member_family_id ?? null,
    status: row.status || 'pending',
    quotedCost: Number(row.quoted_amount ?? row.quotedCost ?? 0) || 0,
    finalCost:
      row.final_amount !== undefined && row.final_amount !== null
        ? Number(row.final_amount)
        : row.finalCost !== undefined && row.finalCost !== null
        ? Number(row.finalCost)
        : null,
    note: row.note || null,
    rewardId: row.reward_id || null,
    rewardName: row.reward_name || null,
    rewardImage: row.reward_image_url || null,
    itemId: row.reward_id || null,
    itemName: row.reward_name || null,
    itemImage: row.reward_image_url || null,
    actorId: row.actor_id || null,
    createdAt: Number(row.created_at ?? row.createdAt) || null,
    updatedAt: Number(row.updated_at ?? row.updatedAt) || null,
    approvedAt: Number(row.redeemed_at ?? row.approvedAt ?? 0) || null,
    redeemedAt: Number(row.redeemed_at ?? 0) || null,
    releasedAt: Number(row.released_at ?? 0) || null,
    expiresAt: Number(row.expires_at ?? 0) || null,
    metadata: parsedMetadata
  };
}

function mapRewardRow(row) {
  if (!row) return null;
  let parsedTags = null;
  if (row.tags) {
    try {
      parsedTags = JSON.parse(row.tags);
    } catch {
      parsedTags = null;
    }
  }
  const status = (row.status || "active").toString().trim().toLowerCase() || "active";
  const cost = Number(row.cost ?? row.price ?? 0) || 0;
  const baseCost = Number(row.base_cost ?? row.cost ?? row.price ?? 0) || 0;
  const icon = row.icon || row.image_url || "";
  const sourceTemplateId = row.source_template_id || row.master_reward_id || null;
  const masterRewardId = row.master_reward_id || null;
  const scopeRaw = row.scope || null;
  const normalizedScope = scopeRaw
    ? String(scopeRaw).trim().toLowerCase()
    : row.family_id
    ? "family"
    : "global";
  const source = sourceTemplateId ? "master" : row.source || null;
  const sourceVersion = Number(row.source_version ?? 0) || 0;
  const isCustomized = Number(row.is_customized ?? 0) ? 1 : 0;
  return {
    id: row.id,
    name: row.name || "",
    title: row.name || "",
    cost,
    price: cost,
    base_cost: baseCost,
    description: row.description || "",
    icon,
    image_url: row.image_url || icon || "",
    imageUrl: row.image_url || icon || "",
    youtube_url: row.youtube_url || "",
    youtubeUrl: row.youtube_url || "",
    status,
    active: status === "active",
    tags: parsedTags,
    campaign_id: row.campaign_id || null,
    source,
    master_reward_id: masterRewardId,
    source_template_id: sourceTemplateId,
    source_version: sourceVersion,
    is_customized: isCustomized,
    scope: normalizedScope || "family",
    created_at: Number(row.created_at ?? 0) || null,
    updated_at: Number(row.updated_at ?? 0) || null,
    family_id: row.family_id || null
  };
}

function mapPublicReward(row) {
  if (!row) return null;
  return {
    id: row.id,
    name: row.name || "",
    cost: Number(row.cost ?? 0) || 0,
    description: row.description || "",
    image_url: row.image_url || "",
    youtube_url: row.youtube_url || "",
    status: (row.status || "active").toString().trim().toLowerCase() || "active"
  };
}


const telemetry = {
  startedAt: Date.now(),
  verbs: new Map()
};

function recordTelemetry(verb, { ok = true, error = null, durationMs = 0 } = {}) {
  const key = String(verb || "unknown");
  if (!telemetry.verbs.has(key)) {
    telemetry.verbs.set(key, {
      count: 0,
      failures: 0,
      totalDurationMs: 0,
      errors: new Map()
    });
  }
  const entry = telemetry.verbs.get(key);
  entry.count += 1;
  entry.totalDurationMs += Number.isFinite(durationMs) ? Number(durationMs) : 0;
  if (!ok) {
    entry.failures += 1;
    if (error) {
      const label = String(error);
      entry.errors.set(label, (entry.errors.get(label) || 0) + 1);
    }
  }
}

function summarizeTelemetry() {
  const verbs = [];
  let totalCount = 0;
  let totalFailures = 0;
  const aggregateFailures = new Map();
  for (const [verb, data] of telemetry.verbs.entries()) {
    totalCount += data.count;
    totalFailures += data.failures;
    const avgDuration = data.count ? data.totalDurationMs / data.count : 0;
    const topFailures = Array.from(data.errors.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([message, count]) => ({ message, count }));
    verbs.push({
      verb,
      count: data.count,
      failures: data.failures,
      errorRate: data.count ? data.failures / data.count : 0,
      avgDurationMs: Math.round(avgDuration * 100) / 100,
      topFailures
    });
    for (const [message, count] of data.errors.entries()) {
      aggregateFailures.set(message, (aggregateFailures.get(message) || 0) + count);
    }
  }
  const topFailureReasons = Array.from(aggregateFailures.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([message, count]) => ({ message, count }));
  return {
    since: telemetry.startedAt,
    totalCount,
    totalFailures,
    errorRate: totalCount ? totalFailures / totalCount : 0,
    verbs,
    topFailureReasons
  };
}

function buildDefaultHints() {
  return {
    balance: 0,
    can_redeem: false,
    max_redeem: 0,
    max_redeem_for_reward: 0,
    can_refund: false,
    max_refund: 0,
    refundable_redeems: [],
    refund_window_ms: REFUND_WINDOW_MS,
    hold_status: "unknown",
    active_hold_id: null,
    pending_hold_count: 0,
    features: FEATURE_FLAGS
  };
}

function getStateHints(userId, familyId = null) {
  const normalized = normId(userId);
  if (!normalized) {
    return buildDefaultHints();
  }

  const resolvedFamilyId = resolveMemberFamilyId(normalized, familyId);
  if (MULTITENANT_ENFORCE && !resolvedFamilyId) {
    return buildDefaultHints();
  }

  const balance = getBalance(normalized, resolvedFamilyId);
  const holds = findRecentHoldsStmt.all(normalized).map(row => ({
    id: row.id,
    status: row.status || 'pending',
    quotedCost: Number(row.quoted_amount ?? row.quotedCost ?? 0) || 0,
    finalCost:
      row.final_amount !== undefined && row.final_amount !== null
        ? Number(row.final_amount)
        : row.finalCost !== undefined && row.finalCost !== null
        ? Number(row.finalCost)
        : null,
    createdAt: Number(row.created_at ?? row.createdAt) || null
  }));
  const pendingHold = holds.find(h => h.status === "pending") || null;
  const pendingHoldCount = Number(countPendingHoldsStmt.get(normalized)?.pending || 0);

  const now = Date.now();
  let maxRefund = 0;
  const refundableRedeems = [];
  if (FEATURE_FLAGS.refunds) {
    const redeemRows = listRecentRedeemsForUser(normalized, resolvedFamilyId);
    for (const row of redeemRows) {
      const redeemAmount = Math.abs(Number(row.amount) || 0);
      if (!redeemAmount) continue;
      if (REFUND_WINDOW_MS !== null) {
        const age = now - Number(row.created_at || 0);
        if (Number.isFinite(age) && age > REFUND_WINDOW_MS) {
          continue;
        }
      }
      const totals = sumRefundsByParentStmt.get(String(row.id));
      const refunded = Math.abs(Number(totals?.total || 0));
      const remaining = Math.max(0, redeemAmount - refunded);
      if (remaining > 0) {
        if (remaining > maxRefund) maxRefund = remaining;
        refundableRedeems.push({
          redeemTxId: String(row.id),
          remaining,
          redeemed: redeemAmount,
          at: Number(row.created_at || 0) || null
        });
      }
    }
  }

  const canRefund = FEATURE_FLAGS.refunds && maxRefund > 0;
  const hints = {
    balance,
    can_redeem: balance > 0,
    max_redeem: balance,
    max_redeem_for_reward: Math.max(0, balance - (pendingHold?.quotedCost || 0)),
    can_refund: canRefund,
    max_refund: canRefund ? maxRefund : 0,
    refundable_redeems: canRefund ? refundableRedeems : [],
    refund_window_ms: REFUND_WINDOW_MS,
    hold_status: pendingHold ? "pending" : (holds[0]?.status || "none"),
    active_hold_id: pendingHold?.id ?? null,
    pending_hold_count: pendingHoldCount,
    features: { ...FEATURE_FLAGS }
  };
  return hints;
}

function buildActionResponse({ userId, familyId = null, txRow = null, extras = {} }) {
  const hints = getStateHints(userId, familyId ?? txRow?.family_id ?? null);
  return {
    ok: extras.ok !== undefined ? extras.ok : true,
    txId: txRow?.id ? String(txRow.id) : null,
    tx: txRow || null,
    balance: hints.balance,
    hints,
    ...extras
  };
}

function buildErrorResponse({ err, userId, fallback = "ACTION_FAILED" }) {
  const code = err?.message || fallback;
  const hints = userId ? getStateHints(userId) : null;
  const payload = { error: code };
  if (err?.remaining !== undefined) payload.remaining_refundable = err.remaining;
  if (err?.retryAfterMs !== undefined) payload.retry_after_ms = err.retryAfterMs;
  if (err?.existing) payload.existing = err.existing;
  if (hints) {
    payload.balance = hints.balance;
    payload.hints = hints;
  }
  return payload;
}

function resolveIdempotencyKey(req, bodyKey) {
  if (req.headers?.["idempotency-key"]) {
    return String(req.headers["idempotency-key"]).trim() || null;
  }
  if (bodyKey) {
    return String(bodyKey).trim() || null;
  }
  return null;
}

function requireRole(required) {
  return (req, res, next) => {
    const provided = (req.headers?.["x-actor-role"] || "").toString().trim().toLowerCase();
    if (required === "admin") {
      if (provided !== "admin") {
        return res.status(403).json({ error: "ROLE_REQUIRED", requiredRole: "admin" });
      }
    } else if (required === "parent") {
      if (provided !== "parent" && provided !== "admin") {
        return res.status(403).json({ error: "ROLE_REQUIRED", requiredRole: required });
      }
    }
    next();
  };
}

function applyLedger({
  userId,
  delta,
  action,
  note = null,
  itemId = null,
  holdId = null,
  templates = null,
  finalCost = null,
  actor = null,
  req = null,
  tokenInfo = null,
  verb = null,
  parentTxId = null,
  refundReason = null,
  refundNotes = null,
  notes = null,
  idempotencyKey = null,
  returnRow = false,
  familyId = null
}) {
  const ip = req?.ip || null;
  const ua = req?.headers?.['user-agent'] || null;
  const createdAt = Date.now();
  const normalizedDelta = Number(delta) | 0;

  const resolvedVerb =
    verb || (normalizedDelta > 0 ? 'earn' : normalizedDelta < 0 ? 'redeem' : 'adjust');
  const explicitLedgerKey = idempotencyKey ? `api:${String(idempotencyKey)}` : null;

  if (explicitLedgerKey) {
    const existing = selectLedgerByKeyStmt.get(explicitLedgerKey);
    if (existing) {
      const mapped = mapLedgerRow(existing);
      return returnRow ? { balance: mapped.balance_after, row: mapped } : mapped.balance_after;
    }
  }

  let tokenLedgerKey = null;
  return db.transaction(() => {
    if (tokenInfo?.jti) {
      tokenLedgerKey = `token:${tokenInfo.jti}:${action}:${userId}:${normalizedDelta}`;
      if (checkTokenStmt.get(tokenInfo.jti)) {
        throw new Error('TOKEN_USED');
      }
    }
    const ledgerKey = explicitLedgerKey || tokenLedgerKey || undefined;
    const metadata = tokenInfo?.jti
      ? { token: tokenInfo.jti, type: tokenInfo.typ || null, raw: tokenInfo.token || null }
      : null;

    const effectiveFamilyId = familyId ?? req?.scope?.family_id ?? null;

    const ledgerResult = recordLedgerEntry({
      userId,
      familyId: effectiveFamilyId,
      amount: normalizedDelta,
      verb: resolvedVerb,
      description: action,
      rewardId: itemId,
      parentHoldId: holdId,
      note,
      notes: notes ?? null,
      templateIds: templates,
      finalAmount: finalCost ?? null,
      actorId: actor,
      refundReason,
      refundNotes,
      idempotencyKey: ledgerKey,
      source: tokenInfo?.typ || null,
      metadata,
      ipAddress: ip,
      userAgent: ua,
      campaignId: null,
      tags: null,
      parentLedgerId: parentTxId ? String(parentTxId) : null,
      createdAt,
      updatedAt: createdAt
    });

    if (tokenInfo?.jti) {
      const tokenSource = tokenInfo.source || action || null;
      consumeTokenStmt.run(
        tokenInfo.jti,
        tokenInfo.token || null,
        tokenInfo.typ || null,
        tokenSource ? String(tokenSource) : null,
        createdAt,
        createdAt
      );
    }

    const insertedRow = ledgerResult.row
      ? mapLedgerRow(ledgerResult.row)
      : mapLedgerRow(selectLedgerByIdStmt.get(ledgerResult.id));

    return returnRow
      ? { balance: ledgerResult.balance_after, row: insertedRow }
      : ledgerResult.balance_after;
  })();
}

function requireAdminKey(req, res, next) {
  const key = readAdminKey(req);
  if (!key) {
    return res.status(401).json({ error: "missing admin key" });
  }
  const ctx = resolveAdminContext(db, key);
  if (!ctx || ctx.role === "none") {
    return res.status(403).json({ error: "invalid key" });
  }
  applyAdminContext(req, ctx);
  next();
}

function encodeBase64Url(str) {
  return Buffer.from(str, "utf8").toString("base64url");
}

function createHttpError(status, message) {
  const err = new Error(message);
  err.status = status;
  return err;
}

function assertRefundRateLimit(actorId) {
  if (!REFUND_RATE_LIMIT_PER_HOUR || REFUND_RATE_LIMIT_PER_HOUR <= 0) return;
  const key = (actorId || "admin").toLowerCase();
  const now = Date.now();
  const cutoff = now - REFUND_RATE_WINDOW_MS;
  const existing = refundRateLimiter.get(key) || [];
  const recent = existing.filter(ts => ts > cutoff);
  if (recent.length >= REFUND_RATE_LIMIT_PER_HOUR) {
    const err = createHttpError(429, "REFUND_RATE_LIMIT");
    err.retryAfterMs = Math.max(0, recent[0] + REFUND_RATE_WINDOW_MS - now);
    throw err;
  }
  recent.push(now);
  refundRateLimiter.set(key, recent);
}

function __resetRefundRateLimiter() {
  refundRateLimiter.clear();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function redeemToken({ token, req, actor, isAdmin = false, allowEarnWithoutAdmin = false, familyId = null }) {
  if (!token) {
    throw createHttpError(400, "missing_token");
  }
  const payload = verifyToken(token);
  if (!payload || !["earn", "give"].includes(payload.typ)) {
    throw createHttpError(400, "unsupported_token");
  }
  if (checkTokenStmt.get(payload.jti)) {
    throw createHttpError(409, "TOKEN_USED");
  }
  const resolvedActor = typeof actor === "function" ? actor(payload.typ) : actor;
  const tokenSourceLabel = payload.typ === "earn" ? "earn.qr" : payload.typ === "give" ? "give.qr" : payload.typ;
  if (payload.typ === "earn") {
    if (!isAdmin && !allowEarnWithoutAdmin) {
      throw createHttpError(403, "ADMIN_REQUIRED");
    }
    const data = payload.data || {};
    const userId = normId(data.userId);
    if (!userId) {
      throw createHttpError(400, "invalid_user");
    }
    const templateEntries = Array.isArray(data.templates) ? data.templates : [];
    if (!templateEntries.length) {
      throw createHttpError(400, "invalid_templates");
    }
    const ids = templateEntries.map(t => Number(t.id));
    const placeholders = ids.map(() => "?").join(",");
    const rows = db.prepare(`SELECT * FROM earn_templates WHERE id IN (${placeholders})`).all(...ids);
    const byId = new Map(rows.map(r => [Number(r.id), r]));
    let total = 0;
    const normalized = templateEntries.map(entry => {
      const tpl = byId.get(Number(entry.id));
      if (!tpl) throw createHttpError(400, "TEMPLATE_MISSING");
      const count = Math.max(1, Number(entry.count || 1));
      total += tpl.points * count;
      return { id: tpl.id, title: tpl.title, points: tpl.points, count };
    });
    const result = applyLedger({
      userId,
      delta: total,
      action: "earn_qr",
      note: data.note || null,
      templates: normalized,
      actor: resolvedActor || null,
      req,
      tokenInfo: { jti: payload.jti, typ: payload.typ, token, source: tokenSourceLabel },
      returnRow: true,
      familyId
    });
    return {
      ok: true,
      userId,
      amount: total,
      balance: result.balance,
      action: "earn_qr",
      note: data.note || null,
      templates: normalized,
      tokenType: payload.typ,
      tx: result.row
    };
  }

  if (payload.typ === "give") {
    const data = payload.data || {};
    const userId = normId(data.userId);
    const amount = Math.floor(Number(data.amount || 0));
    if (!userId || amount <= 0) {
      throw createHttpError(400, "invalid_payload");
    }
    const result = applyLedger({
      userId,
      delta: amount,
      action: "earn_admin_give",
      note: data.note || null,
      actor: resolvedActor || null,
      req,
      tokenInfo: { jti: payload.jti, typ: payload.typ, token, source: tokenSourceLabel },
      returnRow: true,
      familyId
    });
    return {
      ok: true,
      userId,
      amount,
      balance: result.balance,
      action: "earn_admin_give",
      note: data.note || null,
      templates: null,
      tokenType: payload.typ,
      tx: result.row
    };
  }

  throw createHttpError(400, "unsupported_token");
}

function createRefundTransaction({
  userId,
  redeemTxId,
  amount,
  reason,
  notes,
  actorId,
  idempotencyKey,
  req,
  familyId = null
}) {
  const normalizedUser = normId(userId);
  if (!normalizedUser) {
    throw createHttpError(400, "INVALID_USER");
  }
  const redeemId = String(redeemTxId ?? "").trim();
  if (!redeemId) {
    throw createHttpError(400, "INVALID_PARENT_TX");
  }
  const amountValue = Math.floor(Number(amount));
  if (!Number.isFinite(amountValue) || amountValue <= 0) {
    throw createHttpError(400, "INVALID_AMOUNT");
  }
  const reasonKey = String(reason ?? "").trim();
  if (!REFUND_REASONS.has(reasonKey)) {
    const err = createHttpError(400, "INVALID_REASON");
    err.allowed = Array.from(REFUND_REASONS);
    throw err;
  }
  const trimmedNotes = notes === undefined || notes === null ? null : String(notes).trim();
  const actorLabel = actorId ? `admin_refund:${actorId}` : "admin_refund";
  const ledgerKeyRaw = idempotencyKey ? String(idempotencyKey).trim() : null;
  const ledgerKey = ledgerKeyRaw ? `refund:${ledgerKeyRaw}` : null;
  const ledgerLookupKey = ledgerKey ? `api:${ledgerKey}` : null;

  const run = db.transaction(() => {
    const parentRaw = selectLedgerByIdStmt.get(redeemId);
    if (!parentRaw) {
      throw createHttpError(400, "REDEEM_NOT_FOUND");
    }
    const parent = mapLedgerRow(parentRaw);
    if (
      MULTITENANT_ENFORCE &&
      familyId &&
      parent?.family_id &&
      parent.family_id !== familyId
    ) {
      throw createHttpError(403, "FAMILY_MISMATCH");
    }
    if (normId(parent.userId) !== normalizedUser) {
      throw createHttpError(400, "USER_MISMATCH");
    }
    if (Number(parent.delta) >= 0) {
      const err = createHttpError(400, "NOT_REDEEM_TX");
      err.details = { action: parent.action, verb: parent.verb };
      throw err;
    }
    if (parent.verb && parent.verb !== "redeem") {
      const err = createHttpError(400, "NOT_REDEEM_TX");
      err.details = { verb: parent.verb };
      throw err;
    }
    if (REFUND_WINDOW_MS !== null) {
      const age = Date.now() - Number(parent.at || 0);
      if (age > REFUND_WINDOW_MS) {
        const err = createHttpError(400, "REFUND_WINDOW_EXPIRED");
        err.windowMs = REFUND_WINDOW_MS;
        throw err;
      }
    }

    if (ledgerLookupKey) {
      const existing = selectLedgerByKeyStmt.get(ledgerLookupKey);
      if (existing) {
        const mappedExisting = mapLedgerRow(existing);
        if ((mappedExisting.parent_tx_id || null) !== String(parent.id)) {
          const conflict = createHttpError(409, "IDEMPOTENCY_CONFLICT");
          conflict.existing = mappedExisting;
          throw conflict;
        }
        const totals = sumRefundsByParentStmt.get(String(parent.id));
        const totalRefunded = Number(totals?.total || 0);
        const remaining = Math.max(0, Math.abs(Number(parent.delta)) - totalRefunded);
        const conflict = createHttpError(409, "REFUND_EXISTS");
        conflict.existing = mappedExisting;
        conflict.balance = getBalance(normalizedUser, familyId);
        conflict.remaining = remaining;
        throw conflict;
      }
    }

    const parentAmount = Math.abs(Number(parent.delta) || 0);
    const totals = sumRefundsByParentStmt.get(String(parent.id));
    const alreadyRefunded = Number(totals?.total || 0);
    if (alreadyRefunded >= parentAmount) {
      const err = createHttpError(400, "REFUND_NOT_ALLOWED");
      err.remaining = 0;
      throw err;
    }
    if (amountValue + alreadyRefunded > parentAmount) {
      const err = createHttpError(400, "OVER_REFUND");
      err.remaining = parentAmount - alreadyRefunded;
      throw err;
    }

    const ledgerResult = applyLedger({
      userId: normalizedUser,
      delta: amountValue,
      action: "refund",
      note: parent.note,
      notes: trimmedNotes,
      actor: actorLabel,
      verb: "refund",
      parentTxId: String(parent.id),
      refundReason: reasonKey,
      refundNotes: trimmedNotes,
      idempotencyKey: ledgerKey,
      req,
      returnRow: true,
      familyId
    });

    const afterTotals = sumRefundsByParentStmt.get(String(parent.id));
    const totalAfter = Number(afterTotals?.total || alreadyRefunded + amountValue);
    const remaining = Math.max(0, parentAmount - totalAfter);

    return {
      balance: ledgerResult.balance,
      refund: ledgerResult.row,
      remaining
    };
  });

  return run();
}

function getLedgerViewForUser(userId, familyId = null) {
  const normalized = normId(userId);
  if (!normalized) return [];
  const resolvedFamilyId = resolveMemberFamilyId(normalized, familyId);
  if (MULTITENANT_ENFORCE && !resolvedFamilyId) {
    return [];
  }
  const rows = listLedgerRowsForUser(normalized, resolvedFamilyId).map(mapLedgerRow);
  const refundsByParent = new Map();
  for (const row of rows) {
    if (row.verb === "refund" && row.parent_tx_id) {
      const key = String(row.parent_tx_id);
      if (!refundsByParent.has(key)) refundsByParent.set(key, []);
      refundsByParent.get(key).push(row);
    }
  }

  const redeems = [];
  for (const row of rows) {
    const isRedeem = row.verb === "redeem" || Number(row.delta) < 0;
    if (!isRedeem) continue;
    const key = String(row.id);
    const refunds = (refundsByParent.get(key) || []).slice().sort((a, b) => (a.at || 0) - (b.at || 0));
    const totalRefunded = refunds.reduce((sum, r) => sum + Number(r.delta || 0), 0);
    const redeemAmount = Math.abs(Number(row.delta) || 0);
    const remaining = Math.max(0, redeemAmount - totalRefunded);
    let status = "open";
    if (refunds.length) {
      status = remaining === 0 ? "refunded" : "partial";
    }
    redeems.push({
      ...row,
      redeem_amount: redeemAmount,
      refunded_amount: totalRefunded,
      remaining_refundable: remaining,
      refund_status: status,
      refunds
    });
  }

  const now = Date.now();
  const sevenAgo = now - 7 * 24 * 60 * 60 * 1000;
  const thirtyAgo = now - 30 * 24 * 60 * 60 * 1000;
  const summary = {
    earn: { d7: 0, d30: 0 },
    redeem: { d7: 0, d30: 0 },
    refund: { d7: 0, d30: 0 }
  };
  for (const row of rows) {
    const bucket = summary[row.verb];
    if (!bucket) continue;
    const amountAbs = Math.abs(Number(row.delta) || 0);
    const ts = Number(row.at) || 0;
    if (ts >= sevenAgo) bucket.d7 += amountAbs;
    if (ts >= thirtyAgo) bucket.d30 += amountAbs;
  }

  return {
    userId,
    balance: getBalance(userId, resolvedFamilyId),
    rows,
    redeems,
    summary
  };
}

function friendlyScanError(code) {
  switch (code) {
    case "missing_token":
      return "No QR token was included in this link.";
    case "TOKEN_USED":
      return "This QR code has already been used.";
    case "EXPIRED":
      return "This QR code expired. Please generate a new one.";
    case "BAD_TOKEN":
    case "BAD_SIGNATURE":
      return "The QR code is invalid.";
    case "invalid_user":
      return "The QR code is missing a user.";
    case "invalid_templates":
    case "TEMPLATE_MISSING":
      return "The QR code referenced tasks that are no longer available.";
    case "invalid_payload":
      return "The QR code is missing required information.";
    default:
      return "Unable to redeem this QR code.";
  }
}

function formatDateTime(ms) {
  if (!ms) return "";
  try {
    const date = new Date(ms);
    if (Number.isNaN(date.getTime())) {
      return String(ms);
    }
    return date.toLocaleString();
  } catch {
    return String(ms);
  }
}

function renderSpendApprovalPage({ hold = null, balance = null, cost = null, afterBalance = null, expiresAt = null, tokenUsed = false, message = null, token = "" }) {
  const hasHold = !!hold;
  const normalizedStatus = hasHold && hold.status ? String(hold.status).toLowerCase() : "pending";
  const title = hasHold
    ? `Reward approval – ${hold.itemName || "Reward"}`
    : "Reward approval";

  let statusLabel = "Pending approval";
  let statusDescription = message || "Review the request details below, then approve it to redeem the reward.";
  let statusColor = "#2563eb";

  if (!hasHold) {
    statusLabel = "Not available";
    statusDescription = message || "We couldn't find this reward request. Please ask the child to generate a new QR code.";
    statusColor = "#b91c1c";
  } else if (tokenUsed) {
    statusLabel = "Already used";
    statusDescription = message || "This QR code has already been used.";
    statusColor = "#b91c1c";
  } else if (normalizedStatus === "redeemed") {
    statusLabel = "Already redeemed";
    statusDescription = message || "This reward has already been redeemed.";
    statusColor = "#15803d";
  } else if (normalizedStatus === "released") {
    statusLabel = "Canceled";
    statusDescription = message || "This reward request was released. Ask the child to generate a new QR code if needed.";
    statusColor = "#b91c1c";
  }

  const summaryRows = [];
  if (hasHold) {
    summaryRows.push({ label: "Child", value: hold.userId || "Unknown", key: "child" });
    summaryRows.push({ label: "Reward", value: hold.itemName || "Reward", key: "reward" });
    if (cost !== null) summaryRows.push({ label: "Points required", value: `${cost} points`, key: "cost" });
    if (balance !== null) summaryRows.push({ label: "Current balance", value: `${balance} points`, key: "balance" });
    if (normalizedStatus === "pending" && afterBalance !== null) summaryRows.push({ label: "Balance after approval", value: `${afterBalance} points`, key: "after" });
    const statusTitle = tokenUsed && normalizedStatus === "pending"
      ? "Pending (token already used)"
      : normalizedStatus.charAt(0).toUpperCase() + Status.slice(1);
    summaryRows.push({ label: "Status", value: statusTitle, key: "status" });
    const requestedAt = formatDateTime(hold.createdAt);
    if (requestedAt) summaryRows.push({ label: "Requested", value: requestedAt });
    const approvedAt = formatDateTime(hold.approvedAt);
    if (approvedAt) summaryRows.push({ label: "Approved", value: approvedAt });
    const expiresText = formatDateTime(expiresAt);
    if (expiresText) summaryRows.push({ label: "QR expires", value: expiresText });
    summaryRows.push({ label: "Request ID", value: hold.id });
  }

  const summaryHtml = summaryRows.length
    ? `<div class="summary">${summaryRows.map(row => {
        const keyAttr = row.key ? ` data-key="${escapeHtml(row.key)}"` : "";
        return `<div class="summary-row"${keyAttr}><span>${escapeHtml(row.label)}</span><strong>${escapeHtml(row.value)}</strong></div>`;
      }).join("")}</div>`
    : "";

  const noteHtml = hasHold && hold.note
    ? `<p class="note"><strong>Note:</strong> ${escapeHtml(hold.note)}</p>`
    : "";

  const imageHtml = hasHold && hold.itemImage
    ? `<img class="reward-image" src="${escapeHtml(hold.itemImage)}" alt="${escapeHtml(hold.itemName || "Reward image")}" onerror="this.style.display='none'" />`
    : "";

  const canApprove = hasHold && normalizedStatus === "pending" && !tokenUsed && token;

  const adminHint = hasHold
    ? canApprove
      ? ""
      : `<p class="muted">Approve or cancel this reward in the <a href="/admin" target="_blank" rel="noopener">CleverKids admin console</a>.</p>`
    : `<p class="muted">Ask the child to open the shop again and generate a fresh QR code.</p>`;

  const actionCost = cost !== null ? `${cost} points` : "the required points";
  const actionsHtml = canApprove
    ? `<div class="actions" data-role="actions">
        <h2>Approve &amp; redeem</h2>
        <p>Confirm the child has received the reward, then approve to deduct the points.</p>
        <div class="actions-row">
          <input id="adminKeyInput" type="password" placeholder="Admin key" autocomplete="one-time-code" />
          <button id="approveBtn" type="button">Approve reward</button>
        </div>
        <p class="actions-hint">Approving will deduct ${escapeHtml(actionCost)} from <strong>${escapeHtml(hold.userId || "the child")}</strong>.</p>
        <p class="actions-hint">Need to cancel instead? Open the <a href="/admin" target="_blank" rel="noopener">admin console</a>.</p>
        <div class="actions-status" id="approveStatus" role="status"></div>
      </div>`
    : "";

  const badgeStyle = `color:${statusColor}; background-color:${statusColor}20; border-color:${statusColor}40;`;
  const heading = hasHold ? "Reward approval" : "QR issue";
  const intro = hasHold
    ? "Scan complete! Review the request details below."
    : "We couldn’t display this reward request.";

  return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)}</title>
    <style>
      body { font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; padding: 24px; background: #f6f7f9; color: #0f172a; }
      main { max-width: 640px; margin: 40px auto; }
      h1 { font-size: 28px; margin: 0 0 8px; }
      .intro { color: #475569; margin: 0 0 24px; font-size: 16px; }
      .card { background: #fff; border-radius: 18px; padding: 28px; box-shadow: 0 20px 45px rgba(15, 23, 42, 0.12); }
      .status-badge { display: inline-flex; align-items: center; font-weight: 600; font-size: 14px; padding: 6px 14px; border-radius: 999px; border: 1px solid; margin-bottom: 12px; }
      .status-text { margin: 0 0 16px; font-size: 16px; line-height: 1.5; }
      .reward-image { width: 100%; max-height: 220px; object-fit: cover; border-radius: 14px; border: 1px solid #e2e8f0; margin: 12px 0 20px; }
      .summary { border-top: 1px solid #e2e8f0; margin-top: 16px; padding-top: 16px; display: grid; gap: 12px; }
      .summary-row { display: flex; justify-content: space-between; gap: 12px; font-size: 15px; flex-wrap: wrap; }
      .summary-row span { color: #64748b; }
      .summary-row strong { font-weight: 600; }
      .note { margin-top: 20px; padding: 14px; background: #f1f5f9; border-radius: 12px; font-size: 15px; color: #1e293b; }
      .note strong { font-weight: 600; }
      .muted { color: #64748b; font-size: 14px; margin-top: 24px; }
      .muted a { color: inherit; text-decoration: underline; }
      button { font-family: inherit; font-size: 15px; padding: 10px 18px; border-radius: 10px; border: none; background: #2563eb; color: #fff; font-weight: 600; cursor: pointer; }
      button[disabled] { opacity: 0.6; cursor: not-allowed; }
      input[type="password"] { font-family: inherit; }
      .actions { margin-top: 24px; padding: 20px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 14px; display: grid; gap: 12px; }
      .actions h2 { margin: 0; font-size: 18px; }
      .actions p { margin: 0; font-size: 15px; color: #334155; }
      .actions-row { display: flex; gap: 12px; flex-wrap: wrap; }
      .actions-row input { flex: 1 1 220px; padding: 10px 12px; border-radius: 10px; border: 1px solid #cbd5f5; font-size: 15px; }
      .actions-row button { flex: 0 0 auto; }
      .actions-hint { font-size: 13px; color: #64748b; }
      .actions-hint strong { font-weight: 600; color: #0f172a; }
      .actions-hint a { color: inherit; text-decoration: underline; }
      .actions-status { min-height: 20px; font-size: 14px; color: #334155; }
      .actions-status.error { color: #b91c1c; }
      .actions-status.success { color: #15803d; }
    </style>
  </head>
  <body>
    <main>
      <h1>${escapeHtml(heading)}</h1>
      <p class="intro">${escapeHtml(intro)}</p>
      <section class="card">
        <span class="status-badge" id="statusBadge" style="${escapeHtml(badgeStyle)}">${escapeHtml(statusLabel)}</span>
        <p class="status-text" id="statusText">${escapeHtml(statusDescription)}</p>
        ${imageHtml}
        ${summaryHtml}
        ${noteHtml}
        ${actionsHtml}
        ${adminHint}
      </section>
    </main>
    <script>
    (() => {
      const config = ${JSON.stringify({
        holdId: hold?.id ?? null,
        token,
        cost,
        balance,
        afterBalance,
        userId: hold?.userId ?? null
      })};
      const actions = document.querySelector('[data-role="actions"]');
      if (!actions || !config.holdId || !config.token) return;
      const adminInput = document.getElementById('adminKeyInput');
      const approveBtn = document.getElementById('approveBtn');
      const statusEl = document.getElementById('approveStatus');
      const badge = document.getElementById('statusBadge');
      const statusText = document.getElementById('statusText');
      const summaryStatus = document.querySelector('.summary-row[data-key="status"] strong');
      const summaryAfter = document.querySelector('.summary-row[data-key="after"] strong');
      const summaryBalance = document.querySelector('.summary-row[data-key="balance"] strong');
      const summaryCost = document.querySelector('.summary-row[data-key="cost"] strong');

      function setStatus(message, tone) {
        if (!statusEl) return;
        statusEl.textContent = message || '';
        statusEl.classList.remove('error', 'success');
        if (tone === 'error') statusEl.classList.add('error');
        else if (tone === 'success') statusEl.classList.add('success');
      }

      function friendlyError(code) {
        switch (code) {
          case 'UNAUTHORIZED':
            return 'Admin key is incorrect.';
          case 'TOKEN_USED':
            return 'This QR code has already been used.';
          case 'hold_not_pending':
            return 'This reward request is no longer pending.';
          case 'missing_token':
            return 'The QR code token is missing or invalid.';
          case 'approve_failed':
            return 'Unable to approve reward.';
          case 'Failed to fetch':
          case 'TypeError: Failed to fetch':
            return 'Network error. Check your connection and try again.';
          default:
            return typeof code === 'string' ? code.replace(/_/g, ' ') : 'Unable to approve reward.';
        }
      }

      async function approve() {
        if (!approveBtn || !adminInput) return;
        const key = (adminInput.value || '').trim();
        if (!key) {
          setStatus('Enter the admin key to approve.', 'error');
          adminInput.focus();
          return;
        }
        approveBtn.disabled = true;
        adminInput.disabled = true;
        setStatus('Approving reward...');
        try {
          const res = await fetch('/api/holds/' + encodeURIComponent(config.holdId) + '/approve', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'x-admin-key': key
            },
            body: JSON.stringify({ token: config.token })
          });
          let data = {};
          try { data = await res.json(); } catch { data = {}; }
          if (!res.ok) {
            const err = new Error(data?.error || 'approve_failed');
            throw err;
          }
          setStatus('Reward redeemed successfully!', 'success');
          if (badge) {
            badge.textContent = 'Redeemed';
            badge.style.color = '#15803d';
            badge.style.backgroundColor = '#15803d20';
            badge.style.borderColor = '#15803d40';
          }
          if (statusText) {
            statusText.textContent = 'Points have been deducted and the reward is marked as redeemed.';
          }
          if (summaryStatus) summaryStatus.textContent = 'Redeemed';
          const balanceValue = typeof data.balance === 'number' ? data.balance : null;
          const finalCost = typeof data.finalCost === 'number' ? data.finalCost : null;
          if (summaryCost && finalCost !== null) summaryCost.textContent = finalCost + ' points';
          if (summaryBalance && balanceValue !== null) summaryBalance.textContent = balanceValue + ' points';
          if (summaryAfter && balanceValue !== null) {
            summaryAfter.textContent = balanceValue + ' points';
          } else if (balanceValue !== null) {
            const summary = document.querySelector('.summary');
            if (summary) {
              const row = document.createElement('div');
              row.className = 'summary-row';
              row.innerHTML = '<span>Balance after redemption</span><strong>' + balanceValue + ' points</strong>';
              summary.appendChild(row);
            }
          }
          approveBtn.textContent = 'Redeemed';
        } catch (err) {
          const code = err && err.message ? err.message : 'approve_failed';
          const message = friendlyError(code);
          setStatus(message, 'error');
          if (code === 'UNAUTHORIZED') {
            approveBtn.disabled = false;
            adminInput.disabled = false;
            adminInput.focus();
            if (typeof adminInput.select === 'function') adminInput.select();
          } else if (code !== 'TOKEN_USED' && code !== 'hold_not_pending') {
            approveBtn.disabled = false;
            adminInput.disabled = false;
          }
        }
      }

      approveBtn?.addEventListener('click', approve);
      adminInput?.addEventListener('keydown', (ev) => {
        if (ev.key === 'Enter') {
          ev.preventDefault();
          approve();
        }
      });
    })();
    </script>
  </body>
</html>`;
}

function renderScanPage({ success, result = null, error = null, rawCode = null }) {
  const title = success ? "Points Added" : "Scan Failed";
  const heading = success ? "Success!" : "Uh oh.";
  const accent = success ? "#0a7" : "#c00";
  const amountLine = success && result
    ? `<p class="message">Added <strong>${escapeHtml(result.amount)}</strong> RT to <strong>${escapeHtml(result.userId)}</strong>.</p>`
    : "";
  const balanceLine = success && result
    ? `<p class="muted">New balance: ${escapeHtml(result.balance)}</p>`
    : "";
  const noteLine = success && result?.note
    ? `<p class="note">Note: ${escapeHtml(result.note)}</p>`
    : "";
  const templateList = success && Array.isArray(result?.templates) && result.templates.length
    ? `<div class="tasks"><h2>Included tasks</h2><ul>${result.templates
        .map(tpl => `<li><span class="points">+${escapeHtml(tpl.points)}</span> × ${escapeHtml(tpl.count)} — ${escapeHtml(tpl.title)}</li>`)
        .join("")}</ul></div>`
    : "";
  const errorLine = !success && error
    ? `<p class="message">${escapeHtml(error)}</p>`
    : "";
  const rawCodeLine = !success && rawCode
    ? `<p class="muted">(${escapeHtml(rawCode)})</p>`
    : "";

  return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)}</title>
    <style>
      body { font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; padding: 24px; background: #f6f7f9; color: #111; }
      main { max-width: 520px; margin: 40px auto; padding: 32px; background: #fff; border-radius: 16px; box-shadow: 0 12px 32px rgba(15, 23, 42, 0.12); }
      h1 { margin: 0 0 12px; font-size: 28px; color: ${accent}; }
      p { line-height: 1.5; margin: 12px 0; }
      .message { font-size: 18px; font-weight: 600; }
      .muted { color: #586174; font-size: 14px; }
      .note { background: rgba(10, 119, 92, 0.08); padding: 12px; border-radius: 12px; color: #0a7; font-size: 15px; }
      .tasks h2 { margin: 24px 0 12px; font-size: 18px; }
      .tasks ul { list-style: none; padding: 0; margin: 0; }
      .tasks li { padding: 10px 0; border-bottom: 1px solid #e5e7eb; font-size: 15px; display: flex; gap: 8px; align-items: baseline; }
      .tasks li:last-child { border-bottom: none; }
      .points { font-weight: 600; color: #0a7; }
    </style>
  </head>
  <body>
    <main>
      <h1>${escapeHtml(heading)}</h1>
      ${amountLine || errorLine || ""}
      ${balanceLine}
      ${noteLine}
      ${templateList}
      ${rawCodeLine}
    </main>
  </body>
</html>`;
}

function decodeBase64Url(str) {
  return Buffer.from(str, "base64url").toString("utf8");
}

function createToken(typ, data, ttl = TOKEN_TTL_SEC) {
  const now = nowSec();
  const payload = {
    typ,
    jti: randomId(),
    iat: now,
    exp: now + Math.max(10, ttl),
    data
  };
  const payloadStr = JSON.stringify(payload);
  const encoded = encodeBase64Url(payloadStr);
  const sig = crypto.createHmac("sha256", PARENT_SECRET).update(encoded).digest("base64url");
  return { token: `${encoded}.${sig}`, payload };
}

function verifyToken(token) {
  if (!token || typeof token !== "string" || !token.includes(".")) {
    throw new Error("BAD_TOKEN");
  }
  const [encoded, signature] = token.split(".");
  const expected = crypto.createHmac("sha256", PARENT_SECRET).update(encoded).digest("base64url");
  const sigBuf = Buffer.from(signature || "", "base64url");
  const expBuf = Buffer.from(expected, "base64url");
  if (sigBuf.length !== expBuf.length || !crypto.timingSafeEqual(sigBuf, expBuf)) {
    throw new Error("BAD_SIGNATURE");
  }
  const payloadStr = decodeBase64Url(encoded);
  const payload = JSON.parse(payloadStr);
  if (payload.exp < nowSec()) {
    throw new Error("EXPIRED");
  }
  return payload;
}

function buildQrUrl(req, token) {
  const host = req.headers["x-forwarded-host"] || req.headers.host;
  const proto = req.headers["x-forwarded-proto"] || (req.secure ? "https" : "http");
  return `${proto}://${host}/scan?t=${encodeURIComponent(token)}`;
}

function mapEarnTemplate(row) {
  return {
    id: row.id,
    title: row.title,
    points: row.points,
    description: row.description || "",
    youtube_url: row.youtube_url || null,
    active: !!row.active,
    sort_order: row.sort_order,
    created_at: row.created_at,
    updated_at: row.updated_at
  };
}

function mapPublicTask(row) {
  if (!row) return null;
  return {
    id: row.id,
    title: row.title || "",
    points: Number(row.points ?? 0) || 0,
    description: row.description || "",
    youtube_url: row.youtube_url || "",
    sort_order: Number(row.sort_order ?? 0) || 0
  };
}

app.get("/version", (_req, res) => {
  res.json({ build: BUILD });
});

app.get("/balance/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const familyId = resolveMemberFamilyId(userId);
  if (MULTITENANT_ENFORCE && !familyId) {
    return res.status(404).json({ error: "member_family_missing" });
  }
  res.json({ balance: getBalance(userId, familyId) });
});

app.get("/summary/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const familyId = resolveMemberFamilyId(userId);
  if (MULTITENANT_ENFORCE && !familyId) {
    return res.status(404).json({ error: "member_family_missing" });
  }
  const balance = getBalance(userId, familyId);
  const summarySql = LEDGER_HAS_FAMILY_COLUMN
    ? `
    SELECT
      SUM(CASE WHEN description LIKE 'earn_%' THEN amount ELSE 0 END) AS earned,
      SUM(CASE WHEN description LIKE 'spend_%' THEN ABS(amount) ELSE 0 END) AS spent
    FROM ledger
    WHERE user_id = @user_id
      AND (@family_id IS NULL OR family_id = @family_id)
  `
    : `
    SELECT
      SUM(CASE WHEN description LIKE 'earn_%' THEN amount ELSE 0 END) AS earned,
      SUM(CASE WHEN description LIKE 'spend_%' THEN ABS(amount) ELSE 0 END) AS spent
    FROM ledger
    WHERE user_id = @user_id
  `;
  const summaryParams = LEDGER_HAS_FAMILY_COLUMN
    ? { user_id: userId, family_id: familyId }
    : { user_id: userId };
  const sums = db.prepare(summarySql).get(summaryParams);
  res.json({
    userId,
    balance,
    earned: Number(sums?.earned || 0),
    spent: Number(sums?.spent || 0)
  });
});

function mapResolvedMember(row, fallbackFamilyId = null) {
  if (!row) return null;
  const familyId = row.family_id ?? row.familyId ?? fallbackFamilyId ?? null;
  return {
    id: row.id,
    name: row.name ?? "",
    familyId: familyId ?? null
  };
}

app.get("/api/admin/resolve-member", (req, res) => {
  const rawQuery = (req.query?.q ?? req.query?.user ?? "").toString().trim();
  if (!rawQuery) {
    res.status(400).json({ error: "q required" });
    return;
  }

  const requestedFamilyId = normalizeNullableString(req.query?.familyId ?? req.query?.family_id);
  const headerFamilyId = normalizeNullableString(
    req.get?.("x-family") ??
      req.headers?.["x-family"] ??
      req.get?.("x-act-as-family") ??
      req.headers?.["x-act-as-family"]
  );
  let scopeFamilyId = requestedFamilyId ?? headerFamilyId;
  let role = null;

  const adminKey = readAdminKey(req);
  if (adminKey) {
    const ctx = resolveAdminContext(db, adminKey);
    if (!ctx || ctx.role === "none") {
      res.status(403).json({ error: "invalid key" });
      return;
    }
    role = ctx.role;
    if (ctx.role === "family") {
      scopeFamilyId = ctx.familyId ?? ctx.family_id ?? scopeFamilyId ?? null;
      if (MULTITENANT_ENFORCE && !scopeFamilyId) {
        res.status(403).json({ error: "forbidden" });
        return;
      }
    } else if (ctx.role === "master") {
      scopeFamilyId = requestedFamilyId ?? ctx.familyId ?? ctx.family_id ?? null;
    } else {
      res.status(403).json({ error: "forbidden" });
      return;
    }
  } else if (req.headers["x-admin-key"]) {
    // An admin key header was supplied but did not validate.
    res.status(403).json({ error: "invalid key" });
    return;
  }

  if (!role && MULTITENANT_ENFORCE && !scopeFamilyId) {
    res.status(400).json({ error: "familyId required" });
    return;
  }

  const selectColumns = MEMBER_HAS_FAMILY_COLUMN
    ? "SELECT id, name, family_id FROM member"
    : "SELECT id, name FROM member";

  const normalizedId = normId(rawQuery);
  if (MEMBER_ID_PATTERN.test(rawQuery)) {
    let directRow = null;
    if (MEMBER_HAS_FAMILY_COLUMN && scopeFamilyId) {
      directRow = db
        .prepare(`${selectColumns} WHERE lower(id) = lower(@id) AND family_id = @familyId LIMIT 1`)
        .get({ id: rawQuery, familyId: scopeFamilyId });
    } else {
      directRow = db
        .prepare(`${selectColumns} WHERE lower(id) = lower(@id) LIMIT 1`)
        .get({ id: rawQuery });
    }
    if (!directRow && normalizedId && normalizedId !== rawQuery) {
      const altParams = MEMBER_HAS_FAMILY_COLUMN && scopeFamilyId
        ? { id: normalizedId, familyId: scopeFamilyId }
        : { id: normalizedId };
      const sql = MEMBER_HAS_FAMILY_COLUMN && scopeFamilyId
        ? `${selectColumns} WHERE id = @id AND family_id = @familyId LIMIT 1`
        : `${selectColumns} WHERE id = @id LIMIT 1`;
      directRow = db.prepare(sql).get(altParams);
    }
    if (directRow) {
      res.set("Cache-Control", "no-store");
      res.json([mapResolvedMember(directRow, scopeFamilyId)]);
      return;
    }
  }

  const nameParams = MEMBER_HAS_FAMILY_COLUMN && scopeFamilyId
    ? { name: rawQuery, nameLower: rawQuery.toLowerCase(), familyId: scopeFamilyId }
    : { name: rawQuery, nameLower: rawQuery.toLowerCase() };

  let nameSql = `${selectColumns} WHERE (name = @name OR lower(name) = @nameLower)`;
  if (MEMBER_HAS_FAMILY_COLUMN && scopeFamilyId) {
    nameSql += " AND family_id = @familyId";
  }
  nameSql += " ORDER BY name COLLATE NOCASE LIMIT 25";

  const rows = db.prepare(nameSql).all(nameParams).map((row) => mapResolvedMember(row, scopeFamilyId));
  res.set("Cache-Control", "no-store");
  res.json(rows);
});

app.get("/api/admin/members", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const search = (req.query?.search || "").toString().trim().toLowerCase();

  if (!MULTITENANT_ENFORCE) {
    try {
      res.json(listMembers(search));
    } catch (err) {
      console.error("listMembers failed", err);
      res.status(500).json({ error: "FAILED" });
    }
    return;
  }

  const familyId = req.scope?.family_id;
  if (!familyId) {
    res.status(400).json({ error: "family_id required" });
    return;
  }

  try {
    const like = `%${search}%`;
    const rows = search
      ? searchMembersByFamilyStmt.all({ family_id: familyId, like }).map(mapMember)
      : listMembersByFamilyStmt.all({ family_id: familyId }).map(mapMember);
    res.json(rows);
  } catch (err) {
    console.error("listMembers scoped failed", err);
    res.status(500).json({ error: "FAILED" });
  }
});

app.post("/api/admin/members", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const body = req.body || {};
  const userId = normId(body.userId);
  const name = (body.name || "").toString().trim();
  const dob = (body.dob || "").toString().trim();
  const sex = (body.sex || "").toString().trim();
  if (!userId) return res.status(400).json({ error: "userId required" });
  if (!name) return res.status(400).json({ error: "name required" });
  const now = Date.now();

  if (!MULTITENANT_ENFORCE) {
    try {
      insertMemberStmt.run({
        id: userId,
        family_id: "default",
        name,
        date_of_birth: dob || null,
        sex: sex || null,
        status: "active",
        created_at: now,
        updated_at: now
      });
    } catch (err) {
      if (err?.code === "SQLITE_CONSTRAINT_PRIMARYKEY") {
        return res.status(409).json({ error: "USER_EXISTS" });
      }
      console.error("insertMember failed", err);
      return res.status(500).json({ error: "FAILED" });
    }
    res.status(201).json({ ok: true, member: getMember(userId) });
    return;
  }

  const familyId = req.scope?.family_id;
  if (!familyId) {
    res.status(400).json({ error: "family_id required" });
    return;
  }

  try {
    insertMemberForFamilyStmt.run({
      id: userId,
      family_id: familyId,
      name,
      date_of_birth: dob || null,
      sex: sex || null,
      status: "active",
      created_at: now,
      updated_at: now
    });
  } catch (err) {
    if (err?.code === "SQLITE_CONSTRAINT_PRIMARYKEY") {
      return res.status(409).json({ error: "USER_EXISTS" });
    }
    console.error("insertMember scoped failed", err);
    return res.status(500).json({ error: "FAILED" });
  }
  res.status(201).json({ ok: true, member: getMember(userId) });
});

app.get("/api/admin/members/:userId", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) {
    return res.status(400).json({ error: "userId required" });
  }
  const existingRow = selectMemberWithFamilyStmt.get(userId);
  if (!existingRow) {
    return res.status(404).json({ error: "NOT_FOUND" });
  }

  if (MEMBER_HAS_FAMILY_COLUMN) {
    const scopedFamilyId = req.scope?.family_id ?? null;
    const existingFamilyId = existingRow.family_id ?? null;
    if (MULTITENANT_ENFORCE) {
      if (!scopedFamilyId) {
        return res.status(400).json({ error: "family_id required" });
      }
      if (existingFamilyId && scopedFamilyId !== existingFamilyId) {
        return res.status(404).json({ error: "NOT_FOUND" });
      }
    } else if (scopedFamilyId && existingFamilyId && scopedFamilyId !== existingFamilyId) {
      return res.status(404).json({ error: "NOT_FOUND" });
    }
  }

  res.json(mapMember(existingRow));
});

app.patch("/api/admin/members/:userId", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const existingRow = selectMemberWithFamilyStmt.get(userId);
  if (!existingRow) return res.status(404).json({ error: "NOT_FOUND" });

  const scopedFamilyId = req.scope?.family_id ?? null;
  const existingFamilyId = existingRow.family_id ?? null;
  if (MEMBER_HAS_FAMILY_COLUMN) {
    if (MULTITENANT_ENFORCE) {
      if (!scopedFamilyId) {
        return res.status(400).json({ error: "family_id required" });
      }
      if (existingFamilyId && scopedFamilyId !== existingFamilyId) {
        return res.status(404).json({ error: "NOT_FOUND" });
      }
    } else if (scopedFamilyId && existingFamilyId && scopedFamilyId !== existingFamilyId) {
      return res.status(404).json({ error: "NOT_FOUND" });
    }
  }

  const body = req.body || {};
  const name =
    body.name !== undefined
      ? (body.name || "").toString().trim()
      : (existingRow.name || "").toString().trim();
  const dob =
    body.dob !== undefined
      ? (body.dob || "").toString().trim()
      : (existingRow.date_of_birth || "").toString().trim();
  const sex =
    body.sex !== undefined
      ? (body.sex || "").toString().trim()
      : (existingRow.sex || "").toString().trim();
  if (!name) return res.status(400).json({ error: "name required" });

  const now = Date.now();
  const params = [
    name,
    dob ? dob : null,
    sex ? sex : null,
    (existingRow.status || "active").toString(),
    now,
    userId
  ];
  let sql =
    "UPDATE member SET name = ?, date_of_birth = ?, sex = ?, status = ?, updated_at = ? WHERE id = ?";

  if (MEMBER_HAS_FAMILY_COLUMN) {
    const guardFamilyId = MULTITENANT_ENFORCE
      ? scopedFamilyId
      : scopedFamilyId || existingFamilyId || null;
    if (MULTITENANT_ENFORCE && !guardFamilyId) {
      return res.status(400).json({ error: "family_id required" });
    }
    if (guardFamilyId) {
      sql += " AND family_id = ?";
      params.push(guardFamilyId);
    }
  }

  try {
    const info = db.prepare(sql).run(...params);
    if (!info.changes) {
      return res.status(404).json({ error: "NOT_FOUND" });
    }
  } catch (err) {
    console.error("updateMember failed", err);
    return res.status(500).json({ error: "FAILED" });
  }

  res.json({ ok: true, member: getMember(userId) });
});

app.delete("/api/admin/members/:userId", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });

  const existingRow = selectMemberWithFamilyStmt.get(userId);
  if (!existingRow) return res.status(404).json({ error: "NOT_FOUND" });

  const scopedFamilyId = req.scope?.family_id ?? null;
  const existingFamilyId = existingRow.family_id ?? null;
  if (MEMBER_HAS_FAMILY_COLUMN) {
    if (MULTITENANT_ENFORCE) {
      if (!scopedFamilyId) {
        return res.status(400).json({ error: "family_id required" });
      }
      if (existingFamilyId && scopedFamilyId !== existingFamilyId) {
        return res.status(404).json({ error: "NOT_FOUND" });
      }
    } else if (scopedFamilyId && existingFamilyId && scopedFamilyId !== existingFamilyId) {
      return res.status(404).json({ error: "NOT_FOUND" });
    }
  }

  let sql = "DELETE FROM member WHERE id = ?";
  const params = [userId];

  if (MEMBER_HAS_FAMILY_COLUMN) {
    const guardFamilyId = MULTITENANT_ENFORCE
      ? scopedFamilyId
      : scopedFamilyId || existingFamilyId || null;
    if (MULTITENANT_ENFORCE && !guardFamilyId) {
      return res.status(400).json({ error: "family_id required" });
    }
    if (guardFamilyId) {
      sql += " AND family_id = ?";
      params.push(guardFamilyId);
    }
  }

  try {
    const info = db.prepare(sql).run(...params);
    if (!info.changes) {
      return res.status(404).json({ error: "NOT_FOUND" });
    }
  } catch (err) {
    console.error("deleteMember failed", err);
    return res.status(500).json({ error: "FAILED" });
  }

  res.json({ ok: true });
});

app.get("/api/earn-templates", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const { active, sort } = req.query || {};
  const filters = [];
  const params = [];

  const scopedFamilyId = req.scope?.family_id ?? null;

  if (EARN_TEMPLATES_HAS_FAMILY_COLUMN && MULTITENANT_ENFORCE) {
    if (!scopedFamilyId) {
      res.status(400).json({ error: "family_id required" });
      return;
    }
    filters.push("family_id = ?");
    params.push(scopedFamilyId);
  } else if (EARN_TEMPLATES_HAS_FAMILY_COLUMN && scopedFamilyId) {
    filters.push("family_id = ?");
    params.push(scopedFamilyId);
  }

  if (active === "true") {
    filters.push("active = 1");
  } else if (active === "false") {
    filters.push("active = 0");
  }

  let sql = "SELECT * FROM earn_templates";
  if (filters.length) sql += ` WHERE ${filters.join(" AND ")}`;
  if (sort === "sort_order") {
    sql += " ORDER BY sort_order ASC, id ASC";
  } else {
    sql += " ORDER BY created_at DESC";
  }

  const stmt = db.prepare(sql);
  const rows = params.length ? stmt.all(...params) : stmt.all();
  res.json(rows.map(mapEarnTemplate));
});

app.post("/api/earn-templates", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const { title, points, description = "", youtube_url = null, active = true, sort_order = 0 } = req.body || {};
  if (!title || !Number.isFinite(Number(points))) {
    return res.status(400).json({ error: "invalid_template" });
  }
  if (MULTITENANT_ENFORCE && EARN_TEMPLATES_HAS_FAMILY_COLUMN && !req.scope?.family_id) {
    return res.status(400).json({ error: "family_id required" });
  }

  const columns = [
    "title",
    "points",
    "description",
    "youtube_url",
    "active",
    "sort_order",
    "created_at",
    "updated_at"
  ];
  const values = [
    String(title),
    Math.floor(Number(points)),
    String(description || ""),
    youtube_url ? String(youtube_url) : null,
    active ? 1 : 0,
    Number(sort_order) || 0
  ];
  const ts = nowSec();
  values.push(ts, ts);

  if (EARN_TEMPLATES_HAS_FAMILY_COLUMN) {
    const familyId = req.scope?.family_id || (!MULTITENANT_ENFORCE ? "default" : null);
    if (MULTITENANT_ENFORCE && !familyId) {
      return res.status(400).json({ error: "family_id required" });
    }
    columns.push("family_id");
    values.push(familyId);
  } else if (MULTITENANT_ENFORCE) {
    return res.status(500).json({ error: "earn_templates_missing_family_scope" });
  }

  const placeholders = columns.map(() => "?");
  const stmt = db.prepare(
    `INSERT INTO earn_templates (${columns.join(", ")}) VALUES (${placeholders.join(", ")})`
  );
  const info = stmt.run(...values);
  const row = db.prepare("SELECT * FROM earn_templates WHERE id = ?").get(info.lastInsertRowid);
  res.status(201).json(mapEarnTemplate(row));
});

app.patch("/api/earn-templates/:id", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid_id" });
  const fields = [];
  const params = [];
  const { title, points, description, youtube_url, active, sort_order } = req.body || {};
  if (typeof title === "string") { fields.push("title = ?"); params.push(title); }
  if (points !== undefined) {
    if (!Number.isFinite(Number(points))) return res.status(400).json({ error: "invalid_points" });
    fields.push("points = ?"); params.push(Math.floor(Number(points)));
  }
  if (description !== undefined) { fields.push("description = ?"); params.push(description); }
  if (youtube_url !== undefined) { fields.push("youtube_url = ?"); params.push(youtube_url || null); }
  if (active !== undefined) { fields.push("active = ?"); params.push(active ? 1 : 0); }
  if (sort_order !== undefined) { fields.push("sort_order = ?"); params.push(Number(sort_order) || 0); }
  if (!fields.length) return res.status(400).json({ error: "no_fields" });
  fields.push("updated_at = ?"); params.push(nowSec());

  let sql = `UPDATE earn_templates SET ${fields.join(", ")} WHERE id = ?`;
  const whereParams = [id];

  if (EARN_TEMPLATES_HAS_FAMILY_COLUMN && MULTITENANT_ENFORCE) {
    if (!req.scope?.family_id) {
      return res.status(400).json({ error: "family_id required" });
    }
    sql += " AND family_id = ?";
    whereParams.push(req.scope.family_id);
  } else if (EARN_TEMPLATES_HAS_FAMILY_COLUMN && req.scope?.family_id) {
    sql += " AND family_id = ?";
    whereParams.push(req.scope.family_id);
  }

  const info = db.prepare(sql).run(...params, ...whereParams);
  if (!info.changes) return res.status(404).json({ error: "not_found" });
  const row = db.prepare("SELECT * FROM earn_templates WHERE id = ?").get(id);
  res.json(mapEarnTemplate(row));
});

app.delete("/api/earn-templates/:id", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid_id" });
  let sql = "DELETE FROM earn_templates WHERE id = ?";
  const params = [id];

  if (EARN_TEMPLATES_HAS_FAMILY_COLUMN && MULTITENANT_ENFORCE) {
    if (!req.scope?.family_id) {
      return res.status(400).json({ error: "family_id required" });
    }
    sql += " AND family_id = ?";
    params.push(req.scope.family_id);
  } else if (EARN_TEMPLATES_HAS_FAMILY_COLUMN && req.scope?.family_id) {
    sql += " AND family_id = ?";
    params.push(req.scope.family_id);
  }

  const info = db.prepare(sql).run(...params);
  if (!info.changes) return res.status(404).json({ error: "not_found" });
  res.json({ ok: true });
});

app.post("/ck/refund", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const body = req.body || {};
  const actorId = (req.headers["x-admin-actor"] || "").toString().trim() || "admin";
  const idempotencyKey = body.idempotency_key ? String(body.idempotency_key).trim() : null;
  const normalizedUser = normId(body.user_id);
  const familyId = req.scope?.family_id ?? null;
  if (!familyId && MULTITENANT_ENFORCE) {
    return res.status(400).json(buildErrorResponse({ err: { message: "family_id required" }, userId: normalizedUser }));
  }
  if (!FEATURE_FLAGS.refunds) {
    return res.status(403).json(buildErrorResponse({ err: { message: "FEATURE_DISABLED" }, userId: normalizedUser }));
  }
  const started = Date.now();
  try {
    assertRefundRateLimit(actorId);
    console.info("[refund] attempt", {
      userId: normalizedUser,
      redeemTxId: body.redeem_tx_id,
      amount: body.amount,
      reason: body.reason,
      actorId,
      idempotencyKey
    });
    const result = createRefundTransaction({
      userId: body.user_id,
      redeemTxId: body.redeem_tx_id,
      amount: body.amount,
      reason: body.reason,
      notes: body.notes,
      actorId,
      idempotencyKey: resolveIdempotencyKey(req, idempotencyKey),
      req,
      familyId
    });
    recordTelemetry("refund", { ok: true, durationMs: Date.now() - started });
    console.info("[refund] success", {
      userId: normalizedUser,
      redeemTxId: body.redeem_tx_id,
      refundId: result.refund?.id,
      amount: result.refund?.delta,
      remaining: result.remaining,
      actorId
    });
    const response = buildActionResponse({
      userId: normalizedUser,
      familyId,
      txRow: result.refund,
      extras: {
        ok: true,
        refund: result.refund,
        remaining_refundable: result.remaining,
        verb: "refund"
      }
    });
    res.json(response);
  } catch (err) {
    const status = err?.status || 500;
    if (status === 409 && err?.existing) {
      console.warn("[refund] idempotent", {
        userId: normalizedUser,
        redeemTxId: body.redeem_tx_id,
        actorId,
        idempotencyKey
      });
      recordTelemetry("refund", { ok: false, error: err?.message, durationMs: Date.now() - started });
      res.status(409).json(buildErrorResponse({ err, userId: normalizedUser, fallback: "REFUND_EXISTS" }));
      return;
    }
    recordTelemetry("refund", { ok: false, error: err?.message, durationMs: Date.now() - started });
    const payload = buildErrorResponse({ err, userId: normalizedUser, fallback: "REFUND_FAILED" });
    console.error("[refund] failed", {
      userId: normalizedUser,
      redeemTxId: body.redeem_tx_id,
      error: err?.message,
      status,
      actorId
    });
    res.status(status).json(payload);
  }
});

app.get("/ck/ledger/:userId", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const familyId = req.scope?.family_id ?? null;
  if (!familyId && MULTITENANT_ENFORCE) {
    return res.status(400).json({ error: "family_id required" });
  }
  const data = getLedgerViewForUser(userId, familyId);
  const hints = getStateHints(userId, familyId);
  res.json({ ...data, hints });
});

app.post("/ck/earn", authenticateAdmin, resolveFamilyScope, express.json(), (req, res) => {
  const userId = normId(req.body?.user_id ?? req.body?.userId);
  const amount = Math.floor(Number(req.body?.amount ?? 0));
  const note = req.body?.note ? String(req.body.note).slice(0, 200) : null;
  const actorLabel = (req.headers["x-admin-actor"] || "").toString().trim() || "admin_manual";
  const action = req.body?.action ? String(req.body.action) : "earn_manual";
  const familyId = req.scope?.family_id ?? null;
  if (!userId || !Number.isFinite(amount) || amount <= 0) {
    return res.status(400).json(buildErrorResponse({ err: { message: "INVALID_AMOUNT" }, userId }));
  }
  if (!familyId && MULTITENANT_ENFORCE) {
    return res.status(400).json(buildErrorResponse({ err: { message: "family_id required" }, userId }));
  }
  const started = Date.now();
  try {
    const result = applyLedger({
      userId,
      delta: amount,
      action,
      note,
      actor: actorLabel,
      req,
      idempotencyKey: resolveIdempotencyKey(req, req.body?.idempotency_key),
      returnRow: true,
      verb: "earn",
      familyId
    });
    recordTelemetry("earn", { ok: true, durationMs: Date.now() - started });
    const response = buildActionResponse({
      userId,
      familyId,
      txRow: result.row,
      extras: { ok: true, amount, verb: "earn", action }
    });
    res.json(response);
  } catch (err) {
    recordTelemetry("earn", { ok: false, error: err?.message, durationMs: Date.now() - started });
    const status = err?.status || (err?.message === "INSUFFICIENT_FUNDS" ? 409 : 400);
    res.status(status).json(buildErrorResponse({ err, userId, fallback: "EARN_FAILED" }));
  }
});

app.post("/ck/redeem", authenticateAdmin, resolveFamilyScope, express.json(), (req, res) => {
  const userId = normId(req.body?.user_id ?? req.body?.userId);
  const amount = Math.floor(Number(req.body?.amount ?? 0));
  const note = req.body?.note ? String(req.body.note).slice(0, 200) : null;
  const actorLabel = (req.headers["x-admin-actor"] || "").toString().trim() || "admin_redeem_manual";
  const action = req.body?.action ? String(req.body.action) : "spend_manual";
  const familyId = req.scope?.family_id ?? null;
  if (!userId || !Number.isFinite(amount) || amount <= 0) {
    return res.status(400).json(buildErrorResponse({ err: { message: "INVALID_AMOUNT" }, userId }));
  }
  if (!familyId && MULTITENANT_ENFORCE) {
    return res.status(400).json(buildErrorResponse({ err: { message: "family_id required" }, userId }));
  }
  const started = Date.now();
  try {
    const result = applyLedger({
      userId,
      delta: -amount,
      action,
      note,
      actor: actorLabel,
      req,
      idempotencyKey: resolveIdempotencyKey(req, req.body?.idempotency_key),
      returnRow: true,
      verb: "redeem",
      familyId
    });
    recordTelemetry("redeem", { ok: true, durationMs: Date.now() - started });
    const response = buildActionResponse({
      userId,
      familyId,
      txRow: result.row,
      extras: { ok: true, amount, verb: "redeem", action }
    });
    res.json(response);
  } catch (err) {
    recordTelemetry("redeem", { ok: false, error: err?.message, durationMs: Date.now() - started });
    const status = err?.message === "INSUFFICIENT_FUNDS" ? 409 : err?.status || 400;
    res.status(status).json(buildErrorResponse({ err, userId, fallback: "REDEEM_FAILED" }));
  }
});

app.post("/ck/adjust", authenticateAdmin, resolveFamilyScope, express.json(), (req, res) => {
  const userId = normId(req.body?.user_id ?? req.body?.userId);
  const deltaRaw = Number(req.body?.delta ?? 0);
  const delta = Math.trunc(deltaRaw);
  const note = req.body?.note ? String(req.body.note).slice(0, 200) : null;
  const actorLabel = (req.headers["x-admin-actor"] || "").toString().trim() || "admin_adjust";
  const familyId = req.scope?.family_id ?? null;
  if (!userId || !Number.isFinite(delta) || delta === 0) {
    return res.status(400).json(buildErrorResponse({ err: { message: "INVALID_DELTA" }, userId }));
  }
  if (!familyId && MULTITENANT_ENFORCE) {
    return res.status(400).json(buildErrorResponse({ err: { message: "family_id required" }, userId }));
  }
  const started = Date.now();
  try {
    const action = req.body?.action ? String(req.body.action) : delta > 0 ? "adjust_credit" : "adjust_debit";
    const result = applyLedger({
      userId,
      delta,
      action,
      note,
      actor: actorLabel,
      req,
      idempotencyKey: resolveIdempotencyKey(req, req.body?.idempotency_key),
      returnRow: true,
      verb: "adjust",
      familyId
    });
    recordTelemetry("adjust", { ok: true, durationMs: Date.now() - started });
    const response = buildActionResponse({
      userId,
      familyId,
      txRow: result.row,
      extras: { ok: true, delta, verb: "adjust", action }
    });
    res.json(response);
  } catch (err) {
    recordTelemetry("adjust", { ok: false, error: err?.message, durationMs: Date.now() - started });
    const status = err?.status || (err?.message === "INSUFFICIENT_FUNDS" ? 409 : 400);
    res.status(status).json(buildErrorResponse({ err, userId, fallback: "ADJUST_FAILED" }));
  }
});

app.get("/api/admin/rewards", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const filters = [];
  const params = [];
  const query = req.query || {};
  const scopedFamilyId = req.scope?.family_id ?? null;

  if (REWARD_HAS_FAMILY_COLUMN) {
    if (MULTITENANT_ENFORCE) {
      if (!scopedFamilyId) {
        res.status(400).json({ error: "family_id required" });
        return;
      }
      filters.push("r.family_id = ?");
      params.push(scopedFamilyId);
    } else if (scopedFamilyId) {
      filters.push("r.family_id = ?");
      params.push(scopedFamilyId);
    }
  } else if (MULTITENANT_ENFORCE) {
    res.status(500).json({ error: "reward_missing_family_scope" });
    return;
  }

  if (query.status !== undefined && query.status !== null && query.status !== "") {
    filters.push("r.status = ?");
    params.push(String(query.status).trim().toLowerCase());
  } else if (query.active !== undefined) {
    const raw = String(query.active).trim().toLowerCase();
    const isActive = raw === "" || raw === "1" || raw === "true" || raw === "yes" || raw === "active";
    filters.push("r.status = ?");
    params.push(isActive ? "active" : "disabled");
  }

  filters.push("(mr.id IS NULL OR mr.status = 'active')");

  let sql = `
    SELECT r.id, r.name, r.cost, r.description, r.image_url, r.youtube_url, r.status, r.tags, r.campaign_id, r.source,
           r.created_at, r.updated_at${REWARD_HAS_FAMILY_COLUMN ? ", r.family_id" : ""},
           r.master_reward_id, r.source_template_id, r.source_version, r.is_customized,
           mr.youtube_url AS master_youtube
    FROM reward r
    LEFT JOIN master_reward mr ON mr.id = r.master_reward_id
  `;
  if (filters.length) {
    sql += " WHERE " + filters.join(" AND ");
  }
  sql += " ORDER BY r.cost ASC, r.name ASC";
  const rows = db.prepare(sql).all(...params).map(mapRewardRow);
  res.json(rows);
});

app.get("/api/child/rewards", (req, res) => {
  if (req.query?.name || req.query?.userId || req.query?.user_id) {
    res.status(400).json({ error: "memberId required" });
    return;
  }
  const memberId = normId(req.query?.memberId ?? req.query?.member_id ?? "");
  if (!memberId) {
    res.status(400).json({ error: "memberId required" });
    return;
  }
  const stmt = ensureChildRewardsStmt();
  if (!stmt) {
    res.status(500).json({ error: "rewards_unavailable" });
    return;
  }
  const familyId = familyIdByMember(memberId);
  if (!familyId) {
    res.status(404).json({ error: "member not found" });
    return;
  }
  try {
    res.set("Cache-Control", "no-store");
    const rows = stmt.all({ family_id: familyId });
    res.json(rows);
  } catch (err) {
    console.error("[child.rewards] failed", err);
    res.status(500).json({ error: "failed" });
  }
});

app.get("/api/child/tasks", (req, res) => {
  if (req.query?.name || req.query?.userId || req.query?.user_id) {
    res.status(400).json({ error: "memberId required" });
    return;
  }
  const memberId = normId(req.query?.memberId ?? req.query?.member_id ?? "");
  if (!memberId) {
    res.status(400).json({ error: "memberId required" });
    return;
  }
  const stmt = ensureChildTasksStmt();
  if (!stmt) {
    res.status(500).json({ error: "tasks_unavailable" });
    return;
  }
  const familyId = familyIdByMember(memberId);
  if (!familyId) {
    res.status(404).json({ error: "member not found" });
    return;
  }
  try {
    res.set("Cache-Control", "no-store");
    const rows = stmt.all({ family_id: familyId });
    res.json(rows);
  } catch (err) {
    console.error("[child.tasks] failed", err);
    res.status(500).json({ error: "failed" });
  }
});

app.get("/api/features", (_req, res) => {
  res.json({ ...FEATURE_FLAGS });
});

app.post("/api/admin/rewards", authenticateAdmin, resolveFamilyScope, (req, res) => {
  try {
    const body = req.body || {};
    const name = (body.name || "").toString().trim();
    const costRaw = body.cost ?? body.price;
    const imageUrl = body.imageUrl ?? body.image_url ?? null;
    const youtubeUrl = body.youtubeUrl ?? body.youtube_url ?? null;
    const description = (body.description ?? "").toString().trim();
    const tagsValue = body.tags;
    const campaignId = body.campaign_id ?? body.campaignId ?? null;
    const source = body.source ?? null;
    const statusRaw = body.status ?? (body.active === 0 || body.active === false ? "disabled" : "active");
    if (!name) return res.status(400).json({ error: "name_required" });
    const numericCost = Number(costRaw);
    if (!Number.isFinite(numericCost)) return res.status(400).json({ error: "invalid_cost" });
    const cost = Math.trunc(numericCost);
    const rewardId = (body.id ? String(body.id).trim() : "") || crypto.randomUUID();
    const now = Date.now();

    let familyId = null;
    if (REWARD_HAS_FAMILY_COLUMN) {
      familyId = req.scope?.family_id || (!MULTITENANT_ENFORCE ? "default" : null);
      if (MULTITENANT_ENFORCE && !familyId) {
        return res.status(400).json({ error: "family_id required" });
      }
    } else if (MULTITENANT_ENFORCE) {
      return res.status(500).json({ error: "reward_missing_family_scope" });
    }

    let encodedTags = null;
    if (tagsValue !== undefined && tagsValue !== null) {
      if (typeof tagsValue === "string") {
        encodedTags = tagsValue.trim() || null;
      } else {
        try {
          encodedTags = JSON.stringify(tagsValue);
        } catch {
          return res.status(400).json({ error: "invalid_tags" });
        }
      }
    }
    const columns = [
      "id",
      "name",
      "cost",
      "description",
      "image_url",
      "youtube_url",
      "status",
      "tags",
      "campaign_id",
      "source"
    ];
    const values = [
      rewardId,
      name,
      cost,
      description,
      imageUrl ? String(imageUrl).trim() || null : null,
      youtubeUrl ? String(youtubeUrl).trim() || null : null,
      String(statusRaw || "active").trim().toLowerCase() || "active",
      encodedTags,
      campaignId ? String(campaignId).trim() || null : null,
      source ? String(source).trim() || null : null
    ];

    if (REWARD_HAS_FAMILY_COLUMN) {
      columns.push("family_id");
      values.push(familyId);
    }

    columns.push("created_at", "updated_at");
    values.push(now, now);

    const placeholders = columns.map(() => "?");
    const insert = db.prepare(
      `INSERT INTO reward (${columns.join(", ")}) VALUES (${placeholders.join(", ")})`
    );
    insert.run(...values);
    const row = db.prepare("SELECT * FROM reward WHERE id = ?").get(rewardId);
    res.status(201).json(mapRewardRow(row));
  } catch (e) {
    console.error("create reward", e);
    const status = e?.code === "SQLITE_CONSTRAINT" ? 409 : 500;
    res.status(status).json({ error: "create_reward_failed" });
  }
});

app.patch("/api/admin/rewards/:id", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const id = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ error: "invalid_id" });
  const body = req.body || {};
  const fields = [];
  const params = [];
  const rewardColumns = getTableColumns("reward");
  const hasFamilyColumn = rewardColumns.includes("family_id");
  const scopedFamilyId = req.scope?.family_id ?? null;
  let guardFamilyId = null;
  if (hasFamilyColumn) {
    if (MULTITENANT_ENFORCE) {
      if (!scopedFamilyId) {
        return res.status(400).json({ error: "family_id required" });
      }
      guardFamilyId = scopedFamilyId;
    } else if (scopedFamilyId) {
      guardFamilyId = scopedFamilyId;
    }
  } else if (MULTITENANT_ENFORCE) {
    return res.status(500).json({ error: "reward_missing_family_scope" });
  }

  const selectSql = "SELECT * FROM reward WHERE id = ?";
  const selectParams = [id];
  const existing = db.prepare(selectSql).get(...selectParams);
  if (!existing) {
    return res.status(404).json({ error: "not_found" });
  }

  const existingFamilyId = hasFamilyColumn ? existing.family_id ?? null : null;
  if (hasFamilyColumn) {
    if (MULTITENANT_ENFORCE) {
      if (!scopedFamilyId) {
        return res.status(400).json({ error: "family_id required" });
      }
      if (existingFamilyId && scopedFamilyId !== existingFamilyId) {
        return res.status(404).json({ error: "not_found" });
      }
    } else if (scopedFamilyId && existingFamilyId && scopedFamilyId !== existingFamilyId) {
      return res.status(404).json({ error: "not_found" });
    }
  }

  if (hasFamilyColumn) {
    if (MULTITENANT_ENFORCE) {
      guardFamilyId = scopedFamilyId || existingFamilyId || null;
    } else if (scopedFamilyId) {
      guardFamilyId = scopedFamilyId;
    } else {
      guardFamilyId = existingFamilyId;
    }
  }

  const isMasterLinked = !!(existing.master_reward_id && String(existing.master_reward_id).trim());
  const hasCustomizationFlag = rewardColumns.includes("is_customized");
  let markCustomized = false;

  if (!isMasterLinked && body.name !== undefined) {
    fields.push("name = ?");
    params.push(String(body.name).trim());
  }
  const costRaw = body.cost ?? body.price;
  if (costRaw !== undefined) {
    const numeric = Number(costRaw);
    if (!Number.isFinite(numeric)) return res.status(400).json({ error: "invalid_cost" });
    fields.push("cost = ?");
    params.push(Math.trunc(numeric));
    if (isMasterLinked) {
      markCustomized = true;
    }
  }
  if (!isMasterLinked && body.description !== undefined) {
    fields.push("description = ?");
    params.push(String(body.description));
  }
  if (!isMasterLinked && (body.imageUrl !== undefined || body.image_url !== undefined)) {
    const imageUrl = body.imageUrl ?? body.image_url;
    fields.push("image_url = ?");
    params.push(imageUrl ? String(imageUrl).trim() || null : null);
  }
  if (!isMasterLinked && (body.youtubeUrl !== undefined || body.youtube_url !== undefined)) {
    const youtubeUrl = body.youtubeUrl ?? body.youtube_url;
    fields.push("youtube_url = ?");
    params.push(youtubeUrl ? String(youtubeUrl).trim() || null : null);
  }
  if (body.status !== undefined) {
    fields.push("status = ?");
    params.push(String(body.status).trim().toLowerCase());
  } else if (body.active !== undefined) {
    const isActive = body.active === 1 || body.active === true || body.active === "1" || body.active === "true";
    fields.push("status = ?");
    params.push(isActive ? "active" : "disabled");
  }
  if (body.tags !== undefined) {
    const tagsValue = body.tags;
    if (tagsValue === null) {
      fields.push("tags = ?");
      params.push(null);
    } else if (typeof tagsValue === "string") {
      fields.push("tags = ?");
      params.push(tagsValue.trim() || null);
    } else {
      try {
        fields.push("tags = ?");
        params.push(JSON.stringify(tagsValue));
      } catch {
        return res.status(400).json({ error: "invalid_tags" });
      }
    }
  }
  if (body.campaign_id !== undefined || body.campaignId !== undefined) {
    const campaignId = body.campaign_id ?? body.campaignId;
    fields.push("campaign_id = ?");
    params.push(campaignId ? String(campaignId).trim() || null : null);
  }
  if (!isMasterLinked && body.source !== undefined) {
    const source = body.source;
    fields.push("source = ?");
    params.push(source ? String(source).trim() || null : null);
  }
  if (!fields.length) {
    res.json({ ok: true, reward: mapRewardRow(existing) });
    return;
  }
  if (markCustomized && hasCustomizationFlag) {
    fields.push("is_customized = 1");
  }
  fields.push("updated_at = ?");
  const now = Date.now();
  params.push(now);

  let sql = `UPDATE reward SET ${fields.join(", ")} WHERE id = ?`;
  const updateParams = [...params, id];
  if (guardFamilyId) {
    sql += " AND family_id = ?";
    updateParams.push(guardFamilyId);
  }

  const info = db.prepare(sql).run(...updateParams);
  if (!info.changes) return res.status(404).json({ error: "not_found" });
  const fetchSql = guardFamilyId ? `${selectSql} AND family_id = ?` : selectSql;
  const fetchParams = guardFamilyId ? [...selectParams, guardFamilyId] : selectParams;
  const updated = db.prepare(fetchSql).get(...fetchParams);
  res.json({ ok: true, reward: mapRewardRow(updated) });
});

app.delete("/api/admin/rewards/:id", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const id = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ error: "invalid_id" });
  try {
    let sql = "DELETE FROM reward WHERE id = ?";
    const params = [id];

    if (REWARD_HAS_FAMILY_COLUMN) {
      if (MULTITENANT_ENFORCE) {
        if (!req.scope?.family_id) {
          return res.status(400).json({ error: "family_id required" });
        }
        sql += " AND family_id = ?";
        params.push(req.scope.family_id);
      } else if (req.scope?.family_id) {
        sql += " AND family_id = ?";
        params.push(req.scope.family_id);
      }
    } else if (MULTITENANT_ENFORCE) {
      return res.status(500).json({ error: "reward_missing_family_scope" });
    }

    const info = db.prepare(sql).run(...params);
    if (!info.changes) return res.status(404).json({ error: "not_found" });
    res.json({ ok: true });
  } catch (err) {
    console.error("delete reward", err);
    const code = err?.code === "SQLITE_CONSTRAINT_FOREIGNKEY" ? 409 : 500;
    const error = code === 409 ? "reward_in_use" : "delete_reward_failed";
    res.status(code).json({ error });
  }
});

app.get("/api/public/rewards", (req, res) => {
  if (!REWARD_HAS_FAMILY_COLUMN || !listPublicRewardsStmt) {
    res.status(500).json({ error: "reward_missing_family_scope" });
    return;
  }
  const familyId = (req.query?.family_id ?? "").toString().trim();
  if (!familyId) {
    res.status(400).json({ error: "family_id required" });
    return;
  }
  if (familyId.toLowerCase() === "default") {
    res.status(400).json({ error: "invalid family" });
    return;
  }
  try {
    res.set("Cache-Control", "no-store");
    const rows = listPublicRewardsStmt.all({ family_id: familyId }).map(mapPublicReward);
    res.json(rows);
  } catch (err) {
    console.error("public rewards query failed", err);
    res.status(500).json({ error: "FAILED" });
  }
});

app.get("/api/public/tasks", (req, res) => {
  if (!EARN_TEMPLATES_HAS_FAMILY_COLUMN || !listPublicTasksStmt) {
    res.status(500).json({ error: "tasks_missing_family_scope" });
    return;
  }
  const familyId = (req.query?.family_id ?? "").toString().trim();
  if (!familyId) {
    res.status(400).json({ error: "family_id required" });
    return;
  }
  if (familyId.toLowerCase() === "default") {
    res.status(400).json({ error: "invalid family" });
    return;
  }
  try {
    res.set("Cache-Control", "no-store");
    const rows = listPublicTasksStmt.all({ family_id: familyId }).map(mapPublicTask);
    res.json(rows);
  } catch (err) {
    console.error("public tasks query failed", err);
    res.status(500).json({ error: "FAILED" });
  }
});

app.get("/api/public/members/:memberId", (req, res) => {
  if (!MEMBER_HAS_FAMILY_COLUMN) {
    res.status(500).json({ error: "member_missing_family_scope" });
    return;
  }
  const familyId = (req.query?.family_id ?? "").toString().trim();
  if (!familyId) {
    res.status(400).json({ error: "family_id required" });
    return;
  }
  const userId = normId(req.params.memberId);
  if (!userId) {
    res.status(400).json({ error: "userId required" });
    return;
  }
  try {
    const row = selectMemberWithFamilyStmt.get(userId);
    if (!row || (row.family_id ?? null) !== familyId) {
      res.status(404).json({ error: "NOT_FOUND" });
      return;
    }
    res.set("Cache-Control", "no-store");
    res.json({ userId: row.id, name: row.name || "", status: row.status || "active" });
  } catch (err) {
    console.error("public member lookup failed", err);
    res.status(500).json({ error: "FAILED" });
  }
});

app.post("/api/tokens/earn", (req, res) => {
  try {
    const userId = normId(req.body?.userId);
    const templates = Array.isArray(req.body?.templates) ? req.body.templates : [];
    const note = req.body?.note ? String(req.body.note).slice(0, 200) : null;
    if (!userId || !templates.length) {
      return res.status(400).json({ error: "invalid_payload" });
    }
    const ids = templates.map(t => Number(t.id ?? t));
    const placeholders = ids.map(() => "?").join(",");
    const templateRows = db.prepare(`SELECT * FROM earn_templates WHERE id IN (${placeholders})`).all(...ids);
    if (!templateRows.length) {
      return res.status(400).json({ error: "invalid_templates" });
    }
    const templatesById = new Map(templateRows.map(r => [Number(r.id), r]));
    const normalizedTemplates = templates.map(entry => {
      const key = Number(entry.id ?? entry);
      const tpl = templatesById.get(key);
      if (!tpl) throw new Error("TEMPLATE_MISSING");
      const count = Math.max(1, Number(entry.count || 1));
      return { id: tpl.id, title: tpl.title, points: tpl.points, count };
    });
    const total = normalizedTemplates.reduce((sum, t) => sum + t.points * t.count, 0);
    const { token, payload } = createToken("earn", { userId, templates: normalizedTemplates, note, total });
    res.json({ token, qrText: buildQrUrl(req, token), expiresAt: payload.exp, total });
  } catch (err) {
    res.status(400).json({ error: err.message || "invalid_templates" });
  }
});

app.post("/api/tokens/give", requireAdminKey, (req, res) => {
  const userId = normId(req.body?.userId);
  const amount = Number(req.body?.amount || 0);
  const note = req.body?.note ? String(req.body.note).slice(0, 200) : null;
  if (!userId || !Number.isFinite(amount) || amount <= 0) {
    return res.status(400).json({ error: "invalid_payload" });
  }
  const ctx = req.auth ?? {};
  if (ctx.role === 'family') {
    const memberFamily = resolveMemberFamilyId(userId);
    if (memberFamily && memberFamily !== (ctx.familyId ?? ctx.family_id ?? null)) {
      return res.status(403).json({ error: 'forbidden' });
    }
  }
  const { token, payload } = createToken("give", { userId, amount: Math.floor(amount), note });
  res.json({ token, qrText: buildQrUrl(req, token), expiresAt: payload.exp, amount: Math.floor(amount) });
});

app.post("/api/earn/scan", (req, res) => {
  const started = Date.now();
  try {
    const rawKey = readAdminKey(req);
    const ctx = rawKey ? resolveAdminContext(db, rawKey) : { role: "none", familyId: null };
    const isAdmin = ctx.role === "master" || ctx.role === "family";
    const result = redeemToken({
      token: (req.body?.token || "").toString(),
      req,
      actor: isAdmin ? "admin_scan" : "child_scan",
      isAdmin,
      allowEarnWithoutAdmin: isAdmin
    });
    recordTelemetry("earn", { ok: true, durationMs: Date.now() - started });
    const response = buildActionResponse({
      userId: result.userId,
      txRow: result.tx,
      extras: {
        ok: true,
        userId: result.userId,
        amount: result.amount,
        action: result.action,
        note: result.note ?? undefined,
        templates: result.templates ?? undefined,
        tokenType: result.tokenType
      }
    });
    res.json(response);
  } catch (err) {
    const message = err?.message || "scan_failed";
    const status = err?.status || (message === "TOKEN_USED" ? 409 : message === "ADMIN_REQUIRED" ? 403 : 400);
    recordTelemetry("earn", { ok: false, error: message, durationMs: Date.now() - started });
    res.status(status).json(buildErrorResponse({ err, userId: null, fallback: message }));
  }
});

app.post("/api/earn/quick", authenticateAdmin, resolveFamilyScope, (req, res) => {
  const userId = normId(req.body?.userId);
  const templateId = Number(req.body?.templateId);
  const familyId = req.scope?.family_id ?? null;
  if (!userId || !templateId) {
    return res.status(400).json(buildErrorResponse({ err: { message: "invalid_payload" }, userId }));
  }
  if (!familyId && MULTITENANT_ENFORCE) {
    return res.status(400).json(buildErrorResponse({ err: { message: "family_id required" }, userId }));
  }
  const tpl = db.prepare("SELECT * FROM earn_templates WHERE id = ?").get(templateId);
  if (!tpl) {
    return res.status(404).json(buildErrorResponse({ err: { message: "template_not_found" }, userId }));
  }
  const started = Date.now();
  try {
    const result = applyLedger({
      userId,
      delta: tpl.points,
      action: "earn_admin_quick",
      note: tpl.title,
      templates: [{ id: tpl.id, title: tpl.title, points: tpl.points, count: 1 }],
      actor: "admin_quick",
      req,
      idempotencyKey: resolveIdempotencyKey(req, req.body?.idempotency_key),
      returnRow: true,
      familyId
    });
    const response = buildActionResponse({
      userId,
      familyId,
      txRow: result.row,
      extras: {
        ok: true,
        userId,
        amount: tpl.points,
        verb: "earn",
        action: "earn_admin_quick",
        templates: result.row?.templates || [{ id: tpl.id, title: tpl.title, points: tpl.points, count: 1 }]
      }
    });
    recordTelemetry("earn", { ok: true, durationMs: Date.now() - started });
    res.json(response);
  } catch (err) {
    recordTelemetry("earn", { ok: false, error: err?.message, durationMs: Date.now() - started });
    const status = err?.status || (err?.message === "TOKEN_USED" ? 409 : 400);
    res.status(status).json(buildErrorResponse({ err, userId, fallback: "EARN_FAILED" }));
  }
});

app.post("/api/holds", express.json(), (req, res) => {
  const started = Date.now();
  try {
    const userId = normId(req.body?.userId ?? req.body?.user_id);
    const rewardIdRaw = req.body?.itemId ?? req.body?.rewardId ?? req.body?.reward_id;
    const noteRaw = req.body?.note;
    if (!userId || rewardIdRaw === undefined || rewardIdRaw === null) {
      return res.status(400).json(buildErrorResponse({ err: { message: "invalid_payload" }, userId }));
    }
    const rewardId = String(rewardIdRaw).trim();
    const reward = db.prepare(`
      SELECT id, name, cost, image_url
      FROM reward
      WHERE id = ? AND status = 'active'
    `).get(rewardId);
    if (!reward) {
      return res.status(404).json(buildErrorResponse({ err: { message: "reward_not_found" }, userId }));
    }
    const id = randomId();
    const now = Date.now();
    const quoted = Number(reward.cost ?? reward.price ?? 0) || 0;
    const sourceLabel = req.body?.source ? String(req.body.source) : null;
    db.prepare(`
      INSERT INTO hold (
        id,
        user_id,
        reward_id,
        reward_name,
        reward_image_url,
        status,
        quoted_amount,
        final_amount,
        note,
        source,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, 'pending', ?, NULL, ?, ?, ?, ?)
    `).run(
      id,
      userId,
      reward.id,
      reward.name,
      reward.image_url || '',
      quoted,
      noteRaw ? String(noteRaw) : null,
      sourceLabel,
      now,
      now
    );
    const insertedHold = mapHoldRow(getHoldRow(id));
    const ledgerResult = applyLedger({
      userId,
      delta: 0,
      action: 'spend_hold',
      note: insertedHold.rewardName || reward.name,
      holdId: id,
      itemId: insertedHold.rewardId || reward.id,
      templates: null,
      actor: 'child',
      req,
      idempotencyKey: resolveIdempotencyKey(req, req.body?.idempotency_key),
      returnRow: true
    });
    const { token } = createToken('spend', { holdId: id, cost: insertedHold.quotedCost });
    const qrText = buildQrUrl(req, token);
    const response = buildActionResponse({
      userId,
      txRow: ledgerResult.row,
      extras: {
        ok: true,
        holdId: id,
        token,
        qrText,
        verb: 'hold.reserve',
        quotedCost: insertedHold.quotedCost
      }
    });
    recordTelemetry('hold.reserve', { ok: true, durationMs: Date.now() - started });
    res.status(201).json(response);
  } catch (e) {
    console.error('create hold', e);
    const userId = normId(req.body?.userId ?? req.body?.user_id);
    recordTelemetry('hold.reserve', { ok: false, error: e?.message, durationMs: Date.now() - started });
    res.status(500).json(buildErrorResponse({ err: e, userId, fallback: 'hold_failed' }));
  }
});

app.get('/api/holds', requireAdminKey, (req, res) => {
  const status = (req.query?.status || 'pending').toString().toLowerCase();
  const userId = normId(req.query?.userId ?? req.query?.user_id);
  const allowed = ['pending', 'redeemed', 'released', 'all'];
  if (!allowed.includes(status)) {
    return res.status(400).json({ error: 'invalid_status' });
  }
  const ctx = req.auth ?? {};
  const familyId = ctx.role === 'family' ? ctx.familyId ?? ctx.family_id ?? null : null;
  let sql = `
    SELECT h.*, m.family_id AS member_family_id
    FROM hold h
    LEFT JOIN member m ON m.id = h.user_id
  `;
  const filters = [];
  const params = [];
  if (status !== 'all') {
    filters.push('h.status = ?');
    params.push(status);
  }
  if (userId) {
    filters.push('LOWER(h.user_id) = ?');
    params.push(userId);
  }
  if (familyId) {
    filters.push('m.family_id = ?');
    params.push(familyId);
  }
  if (filters.length) {
    sql += ' WHERE ' + filters.join(' AND ');
  }
  sql += ' ORDER BY h.created_at DESC';
  const rows = db.prepare(sql).all(...params).map(mapHoldRow);
  res.json(rows);
});

app.post('/api/holds/:id/approve', authenticateAdmin, resolveFamilyScope, (req, res) => {
  const started = Date.now();
  let hold = null;
  try {
    const id = String(req.params.id || '');
    const token = String(req.body?.token || '');
    const override = req.body?.finalCost ?? req.body?.final_cost;
    const familyId = req.scope?.family_id ?? null;
    if (!familyId && MULTITENANT_ENFORCE) {
      return res.status(400).json(buildErrorResponse({ err: { message: 'family_id required' } }));
    }
    if (!id || !token) {
      return res.status(400).json(buildErrorResponse({ err: { message: 'invalid_payload' } }));
    }
    const payload = verifyToken(token);
    if (payload.typ !== 'spend') {
      return res.status(400).json(buildErrorResponse({ err: { message: 'unsupported_token' } }));
    }
    if (payload.data?.holdId !== id) {
      return res.status(400).json(buildErrorResponse({ err: { message: 'hold_mismatch' } }));
    }
    if (checkTokenStmt.get(payload.jti)) {
      return res.status(409).json(buildErrorResponse({ err: { message: 'TOKEN_USED' } }));
    }
    hold = mapHoldRow(getHoldRow(id));
    if (!hold || hold.status !== 'pending') {
      return res.status(404).json(buildErrorResponse({ err: { message: 'hold_not_pending' }, userId: hold?.userId }));
    }
    if (familyId && hold.familyId && hold.familyId !== familyId) {
      return res.status(403).json(buildErrorResponse({ err: { message: 'forbidden' }, userId: hold?.userId }));
    }
    const cost = override !== undefined && override !== null
      ? Math.max(0, Math.floor(Number(override)))
      : hold.quotedCost;
    const result = applyLedger({
      userId: hold.userId,
      delta: -cost,
      action: 'spend_redeemed',
      note: hold.rewardName || hold.note,
      itemId: hold.rewardId,
      holdId: hold.id,
      finalCost: cost,
      actor: 'admin_redeem',
      req,
      tokenInfo: { jti: payload.jti, typ: payload.typ, token, source: 'hold.approve' },
      returnRow: true,
      familyId
    });
    const now = Date.now();
    db.prepare(`
      UPDATE hold
      SET status = 'redeemed',
          final_amount = ?,
          note = ?,
          updated_at = ?,
          redeemed_at = ?,
          source = COALESCE(source, 'admin')
      WHERE id = ?
    `).run(cost, hold.note || null, now, now, id);
    hold = mapHoldRow(getHoldRow(id));
    const response = buildActionResponse({
      userId: hold.userId,
      familyId,
      txRow: result.row,
      extras: { ok: true, holdId: id, finalCost: cost, verb: 'hold.redeem' }
    });
    recordTelemetry('hold.redeem', { ok: true, durationMs: Date.now() - started });
    res.json(response);
  } catch (err) {
    recordTelemetry('hold.redeem', { ok: false, error: err?.message, durationMs: Date.now() - started });
    const code = err.message === 'TOKEN_USED' ? 409 : err.status || 400;
    res.status(code).json(buildErrorResponse({ err, userId: hold?.userId, fallback: 'approve_failed' }));
  }
});

app.post('/api/holds/:id/cancel', authenticateAdmin, resolveFamilyScope, (req, res) => {
  const started = Date.now();
  const id = String(req.params.id || '');
  let hold = mapHoldRow(getHoldRow(id));
  const familyId = req.scope?.family_id ?? null;
  if (!familyId && MULTITENANT_ENFORCE) {
    return res.status(400).json(buildErrorResponse({ err: { message: 'family_id required' }, userId: hold?.userId }));
  }
  if (!hold || hold.status !== 'pending') {
    return res.status(404).json(buildErrorResponse({ err: { message: 'hold_not_pending' }, userId: hold?.userId }));
  }
  if (familyId && hold.familyId && hold.familyId !== familyId) {
    return res.status(403).json(buildErrorResponse({ err: { message: 'forbidden' }, userId: hold?.userId }));
  }
  const now = Date.now();
  db.prepare(`
    UPDATE hold
    SET status = 'released',
        final_amount = 0,
        updated_at = ?,
        released_at = ?,
        note = COALESCE(note, ?)
    WHERE id = ?
  `).run(now, now, hold.note || null, id);
  hold = mapHoldRow(getHoldRow(id));
  const result = applyLedger({
    userId: hold.userId,
    delta: 0,
    action: 'spend_released',
    note: hold.rewardName || hold.note,
    holdId: hold.id,
    actor: 'admin_cancel',
    req,
    returnRow: true,
    idempotencyKey: resolveIdempotencyKey(req, req.body?.idempotency_key),
    familyId
  });
  recordTelemetry('hold.release', { ok: true, durationMs: Date.now() - started });
  const response = buildActionResponse({
    userId: hold.userId,
    familyId,
    txRow: result.row,
    extras: { ok: true, holdId: id, verb: 'hold.release' }
  });
  res.json(response);
});

function buildHistoryQuery(params) {
  const where = [];
  const sqlParams = [];
  if (params.familyId && LEDGER_HAS_FAMILY_COLUMN) {
    where.push("family_id = ?");
    sqlParams.push(params.familyId);
  }
  if (params.userId) {
    where.push("user_id = ?");
    sqlParams.push(normId(params.userId));
  }
  if (params.type === 'earn') {
    where.push("verb = 'earn'");
  } else if (params.type === 'spend') {
    where.push("verb = 'redeem'");
  } else if (params.type === 'refund') {
    where.push("verb = 'refund'");
  }
  if (params.verb) {
    where.push("verb = ?");
    sqlParams.push(params.verb);
  }
  if (params.source === 'task') {
    where.push("description = 'earn_qr'");
  } else if (params.source === 'admin') {
    where.push("description IN ('earn_admin_give','earn_admin_quick')");
  }
  if (params.actor) {
    where.push("actor_id = ?");
    sqlParams.push(params.actor);
  }
  if (params.from) {
    where.push("created_at >= ?");
    sqlParams.push(params.from);
  }
  if (params.to) {
    where.push("created_at <= ?");
    sqlParams.push(params.to);
  }
  const limit = Math.min(500, Math.max(1, Number(params.limit) || 50));
  const offset = Math.max(0, Number(params.offset) || 0);
  let sql = 'SELECT * FROM ledger';
  if (where.length) sql += ` WHERE ${where.join(' AND ')}`;
  sql += ' ORDER BY created_at DESC, id DESC';
  sql += ' LIMIT ? OFFSET ?';
  sqlParams.push(limit, offset);
  return { sql, params: sqlParams, limit, offset };
}

app.get("/api/history", requireAdminKey, (req, res) => {
  const from = req.query.from ? Number(new Date(req.query.from).getTime()) : undefined;
  const to = req.query.to ? Number(new Date(req.query.to).getTime()) : undefined;
  const ctx = req.auth ?? {};
  const familyId = ctx.role === 'family' ? ctx.familyId ?? ctx.family_id ?? null : null;
  const userIdParam = req.query.userId ? normId(req.query.userId) : null;
  if (familyId && userIdParam) {
    const memberFamily = resolveMemberFamilyId(userIdParam);
    if (memberFamily && memberFamily !== familyId) {
      return res.status(403).json({ error: 'forbidden' });
    }
  }
  const query = buildHistoryQuery({
    userId: req.query.userId,
    type: req.query.type,
    source: req.query.source,
    verb: req.query.verb,
    actor: req.query.actor,
    from,
    to,
    limit: req.query.limit,
    offset: req.query.offset,
    familyId
  });
  let rows = db.prepare(query.sql).all(...query.params).map(mapLedgerRow);
  if (familyId && !LEDGER_HAS_FAMILY_COLUMN) {
    rows = rows.filter((row) => {
      const memberFamily = resolveMemberFamilyId(row.userId ?? null);
      return !memberFamily || memberFamily === familyId;
    });
  }
  if (req.query.format === "csv") {
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", "attachment; filename=history.csv");
    const header = "at,userId,verb,action,delta,balance_after,itemId,holdId,parent_tx_id,finalCost,note,notes,templates,refund_reason,refund_notes,actor,idempotency_key\n";
    const escape = value => {
      if (value === null || value === undefined) return "";
      const str = String(value).replace(/"/g, '""');
      return `"${str}"`;
    };
    const body = rows
      .map(r => [
        escape(r.at),
        escape(r.userId),
        escape(r.verb),
        escape(r.action),
        escape(r.delta),
        escape(r.balance_after),
        escape(r.itemId ?? ""),
        escape(r.holdId ?? ""),
        escape(r.parent_tx_id ?? ""),
        escape(r.finalCost ?? ""),
        escape(r.note ?? ""),
        escape(r.notes ?? ""),
        escape(r.templates ? JSON.stringify(r.templates) : ""),
        escape(r.refund_reason ?? ""),
        escape(r.refund_notes ?? ""),
        escape(r.actor ?? ""),
        escape(r.idempotency_key ?? "")
      ].join(","))
      .join("\n");
    res.send(header + body);
    return;
  }
  res.json({ rows, limit: query.limit, offset: query.offset });
});

app.get("/api/admin/telemetry/core-health", requireAdminKey, (req, res) => {
  if (req.auth?.role !== 'master') {
    return res.status(403).json({ error: 'forbidden' });
  }
  res.json(summarizeTelemetry());
});

app.get("/api/history/user/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const familyId = resolveMemberFamilyId(userId);
  if (MULTITENANT_ENFORCE && !familyId) {
    return res.status(404).json({ error: "member_family_missing" });
  }
  const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 100));
  const rows = LEDGER_HAS_FAMILY_COLUMN
    ? historyPreviewByUserStmt.all({ user_id: userId, family_id: familyId, limit })
    : historyPreviewByUserStmt.all({ user_id: userId, limit });
  res.json({ rows });
});

app.get("/api/history.csv/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).send("userId required");
  const familyId = resolveMemberFamilyId(userId);
  if (MULTITENANT_ENFORCE && !familyId) {
    return res.status(404).send("member_family_missing");
  }
  const rows = listLedgerRowsForUser(userId, familyId).map(mapLedgerRow);
  const header = "at,userId,verb,action,delta,balance_after,itemId,holdId,parent_tx_id,finalCost,note,notes,templates,refund_reason,refund_notes,actor,idempotency_key\n";
  const escape = value => {
    if (value === null || value === undefined) return "";
    const str = String(value).replace(/"/g, '""');
    return `"${str}"`;
  };
  const body = rows
    .map(r => [
      escape(r.at),
      escape(r.userId),
      escape(r.verb),
      escape(r.action),
      escape(r.delta),
      escape(r.balance_after),
      escape(r.itemId ?? ""),
      escape(r.holdId ?? ""),
      escape(r.parent_tx_id ?? ""),
      escape(r.finalCost ?? ""),
      escape(r.note ?? ""),
      escape(r.notes ?? ""),
      escape(r.templates ? JSON.stringify(r.templates) : ""),
      escape(r.refund_reason ?? ""),
      escape(r.refund_notes ?? ""),
      escape(r.actor ?? ""),
      escape(r.idempotency_key ?? "")
    ].join(","))
    .join("\n");
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename=history_${userId}.csv`);
  res.send(header + body + (body ? "\n" : ""));
});

app.post("/admin/upload-image64", requireAdminKey, (req, res) => {
  const { image64 } = req.body || {};
  if (!image64) return res.status(400).json({ error: "missing_image" });
  const match = String(image64).match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,(.*)$/);
  if (!match) return res.status(400).json({ error: "bad_dataurl" });
  const mime = match[1];
  const data = match[2];
  const buffer = Buffer.from(data, "base64");
  const ext = ({ "image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif" })[mime] || "png";
  const hash = crypto.createHash("sha256").update(buffer).digest("hex").slice(0, 16);
  const filename = `rw_${hash}.${ext}`;
  const filePath = join(UPLOAD_DIR, filename);
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, buffer);
  }
  res.json({ url: `/uploads/${filename}` });
});

app.get("/index.html", (_req, res) => {
  sendVersioned(res, "index.html");
});

app.get("/", (_req, res) => {
  res.redirect(`/admin.html?v=${BUILD}`);
});

app.use((err, req, res, next) => {
  console.error("[api error]", err);
  const status = err?.status || 500;
  res.status(status).json({ error: err?.message || "Internal Server Error" });
});

if (process.env.NODE_ENV !== "test") {
  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Parents Shop API listening on http://0.0.0.0:${PORT}`);
  });
}

export {
  app,
  applyLedger,
  createRefundTransaction,
  getLedgerViewForUser,
  mapLedgerRow,
  getBalance,
  normId,
  getStateHints,
  ensureSchema,
  __resetRefundRateLimiter
};
