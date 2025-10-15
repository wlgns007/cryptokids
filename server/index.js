// CryptoKids Parents Shop API (refactored)
import express from "express";
import { fileURLToPath } from "node:url";
import { dirname, join, sep } from "node:path";
import fs from "node:fs";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import QRCode from "qrcode";
import db, { DATA_DIR, resolveAdminContext } from "./db.js";
import { MULTITENANT_ENFORCE } from "./config.js";
import ledgerRoutes from "./routes/ledger.js";
import { balanceOf, recordLedgerEntry } from "./ledger/core.js";
import { generateIcon, knownIcon } from "./iconFactory.js";
import { readAdminKey } from "./auth.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootPackage = JSON.parse(
  fs.readFileSync(new URL("../package.json", import.meta.url), "utf8")
);

const UPLOAD_DIR = process.env.UPLOAD_DIR || join(DATA_DIR, "uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });
const PARENT_SECRET = (process.env.PARENT_SECRET || "dev-secret-change-me").trim();

function applyAdminContext(req, ctx) {
  req.auth = {
    role: ctx.role,
    familyId: ctx.familyId,
    family_id: ctx.familyId ?? null
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
      req.scope = { family_id: familyId };
      next();
      return;
    }

    res.status(403).json({ error: "unsupported role" });
  };
}

const resolveFamilyScope = createFamilyScopeResolver({ requireFamilyId: true });

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

const insertFamilyStmt = db.prepare(
  `INSERT INTO family (id, name, status, admin_key, created_at, updated_at)
   VALUES (@id, @name, @status, @admin_key, @now, @now)`
);
const selectFamilyByIdStmt = db.prepare(
  "SELECT id, name, status, admin_key FROM family WHERE id = ? LIMIT 1"
);
const listFamiliesStmt = db.prepare(
  "SELECT id, name, status, admin_key, created_at, updated_at FROM family ORDER BY created_at DESC, id DESC"
);
const updateFamilyStmt = db.prepare(
  `UPDATE family SET name = @name, status = @status, updated_at = @now WHERE id = @id`
);
const updateFamilyAdminKeyStmt = db.prepare(
  `UPDATE family SET admin_key = @admin_key, updated_at = @now WHERE id = @id`
);

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

const app = express();
app.use(express.json({ limit: "4mb" }));
app.use(express.urlencoded({ extended: false }));
app.use("/api", ledgerRoutes);

app.get("/api/whoami", authenticateAdmin, (req, res) => {
  const role = req.auth?.role ?? null;
  const familyId = req.auth?.familyId ?? req.auth?.family_id ?? null;
  if (role === "master") {
    res.json({ role: "master", familyId: null, family_id: null });
    return;
  }
  if (role === "family") {
    res.json({ role: "family", familyId, family_id: familyId });
    return;
  }
  res.json({ role, familyId, family_id: familyId });
});

app.post("/api/families", authenticateAdmin, requireMaster, (req, res) => {
  const body = req.body ?? {};
  const name = (body.name ?? "").toString().trim();
  if (!name) {
    res.status(400).json({ error: "name required" });
    return;
  }

  const providedKey = body.adminKey === undefined || body.adminKey === null ? "" : String(body.adminKey);
  const trimmedKey = providedKey.trim();
  const adminKey = trimmedKey || makeKey();
  const id = crypto.randomUUID();
  const now = Date.now();

  try {
    insertFamilyStmt.run({ id, name, status: "active", admin_key: adminKey, now });
  } catch (err) {
    const message = String(err?.message || "");
    if (message.includes("UNIQUE") && message.includes("admin_key")) {
      res.status(409).json({ error: "adminKey already in use" });
      return;
    }
    throw err;
  }

  res.status(201).json({ id, name, adminKey });
});

app.get("/api/families", authenticateAdmin, requireMaster, (_req, res) => {
  const families = listFamiliesStmt.all().map(({ admin_key, ...rest }) => rest);
  res.json(families);
});

app.post("/api/families/:id/rotate-key", authenticateAdmin, requireMaster, (req, res) => {
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
  const now = Date.now();
  updateFamilyAdminKeyStmt.run({ id, admin_key: adminKey, now });
  res.json({ id, adminKey });
});

app.patch("/api/families/:id", authenticateAdmin, requireMaster, (req, res) => {
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
  const now = Date.now();
  updateFamilyStmt.run({ id, name, status, now });
  const updated = selectFamilyByIdStmt.get(id);
  if (updated) {
    const { admin_key, ...rest } = updated;
    res.json(rest);
    return;
  }
  res.json({ id, name, status });
});

app.get(["/", "/index.html", "/child", "/child.html"], (_req, res) => {
  sendVersioned(res, "child.html");
});

app.get(["/admin", "/admin.html"], (_req, res) => {
  sendVersioned(res, "admin.html");
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
    CREATE TABLE IF NOT EXISTS reward (
      id TEXT PRIMARY KEY,
      family_id TEXT NOT NULL,
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

function ensureSchema() {
  // Base tables + seeds run inside a transaction for atomicity.
  ensureBaseSchema();

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
  return {
    id: row.id,
    name: row.name || "",
    title: row.name || "",
    cost,
    price: cost,
    description: row.description || "",
    image_url: row.image_url || "",
    imageUrl: row.image_url || "",
    youtube_url: row.youtube_url || "",
    youtubeUrl: row.youtube_url || "",
    status,
    active: status === "active",
    tags: parsedTags,
    campaign_id: row.campaign_id || null,
    source: row.source || null,
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
      : `<p class="muted">Approve or cancel this reward in the <a href="/admin" target="_blank" rel="noopener">CryptoKids admin console</a>.</p>`
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
      filters.push("family_id = ?");
      params.push(scopedFamilyId);
    } else if (scopedFamilyId) {
      filters.push("family_id = ?");
      params.push(scopedFamilyId);
    }
  } else if (MULTITENANT_ENFORCE) {
    res.status(500).json({ error: "reward_missing_family_scope" });
    return;
  }

  if (query.status !== undefined && query.status !== null && query.status !== "") {
    filters.push("status = ?");
    params.push(String(query.status).trim().toLowerCase());
  } else if (query.active !== undefined) {
    const raw = String(query.active).trim().toLowerCase();
    const isActive = raw === "" || raw === "1" || raw === "true" || raw === "yes" || raw === "active";
    filters.push("status = ?");
    params.push(isActive ? "active" : "disabled");
  }

  let sql = `
    SELECT id, name, cost, description, image_url, youtube_url, status, tags, campaign_id, source, created_at, updated_at${
      REWARD_HAS_FAMILY_COLUMN ? ", family_id" : ""
    }
    FROM reward
  `;
  if (filters.length) {
    sql += " WHERE " + filters.join(" AND ");
  }
  sql += " ORDER BY cost ASC, name ASC";
  const rows = db.prepare(sql).all(...params).map(mapRewardRow);
  res.json(rows);
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
  if (body.name !== undefined) {
    fields.push("name = ?");
    params.push(String(body.name).trim());
  }
  const costRaw = body.cost ?? body.price;
  if (costRaw !== undefined) {
    const numeric = Number(costRaw);
    if (!Number.isFinite(numeric)) return res.status(400).json({ error: "invalid_cost" });
    fields.push("cost = ?");
    params.push(Math.trunc(numeric));
  }
  if (body.description !== undefined) {
    fields.push("description = ?");
    params.push(String(body.description));
  }
  if (body.imageUrl !== undefined || body.image_url !== undefined) {
    const imageUrl = body.imageUrl ?? body.image_url;
    fields.push("image_url = ?");
    params.push(imageUrl ? String(imageUrl).trim() || null : null);
  }
  if (body.youtubeUrl !== undefined || body.youtube_url !== undefined) {
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
  if (body.source !== undefined) {
    const source = body.source;
    fields.push("source = ?");
    params.push(source ? String(source).trim() || null : null);
  }
  if (!fields.length) return res.status(400).json({ error: "no_fields" });
  fields.push("updated_at = ?");
  const now = Date.now();
  params.push(now);

  let sql = `UPDATE reward SET ${fields.join(", ")} WHERE id = ?`;
  const whereParams = [id];

  if (REWARD_HAS_FAMILY_COLUMN) {
    if (MULTITENANT_ENFORCE) {
      if (!req.scope?.family_id) {
        return res.status(400).json({ error: "family_id required" });
      }
      sql += " AND family_id = ?";
      whereParams.push(req.scope.family_id);
    } else if (req.scope?.family_id) {
      sql += " AND family_id = ?";
      whereParams.push(req.scope.family_id);
    }
  } else if (MULTITENANT_ENFORCE) {
    return res.status(500).json({ error: "reward_missing_family_scope" });
  }

  const info = db.prepare(sql).run(...params, ...whereParams);
  if (!info.changes) return res.status(404).json({ error: "not_found" });
  const updated = db.prepare("SELECT * FROM reward WHERE id = ?").get(id);
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
