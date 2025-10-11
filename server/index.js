// CryptoKids Parents Shop API (refactored)
import express from "express";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import fs from "node:fs";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import db, { DATA_DIR } from "./db.js";
import ledgerRoutes from "./routes/ledger.js";
import { balanceOf, recordLedgerEntry } from "./ledger/core.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootPackage = JSON.parse(
  fs.readFileSync(new URL("../package.json", import.meta.url), "utf8")
);

const UPLOAD_DIR = process.env.UPLOAD_DIR || join(DATA_DIR, "uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });
const PARENT_SECRET = (process.env.PARENT_SECRET || "dev-secret-change-me").trim();
const ADMIN_KEY = (process.env.ADMIN_KEY || "Mamapapa").trim();

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
  refunds: (process.env.FEATURE_REFUNDS ?? "true").toString().toLowerCase() !== "false"
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

function sendVersioned(res, file, type = "text/html") {
  if (type) res.type(type);
  res.set("Cache-Control", "no-cache");
  res.send(loadVersioned(file));
}

const app = express();
app.use(express.json({ limit: "4mb" }));
app.use(express.urlencoded({ extended: false }));
app.use("/api", ledgerRoutes);

app.get(["/admin", "/admin.html"], (_req, res) => {
  sendVersioned(res, "admin.html");
});

app.get(["/child", "/child.html"], (_req, res) => {
  sendVersioned(res, "child.html");
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

    const holdRow = db.prepare("SELECT * FROM hold WHERE id = ?").get(holdId);
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

app.get("/manifest.webmanifest", (_req, res) => {
  sendVersioned(res, "manifest.webmanifest", "application/manifest+json");
});

app.get("/sw.js", (_req, res) => {
  sendVersioned(res, "sw.js", "application/javascript");
});

app.use(express.static(PUBLIC_DIR, {
  maxAge: "1h",
  setHeaders(res, filePath) {
    if (filePath.endsWith(".html")) {
      res.setHeader("Cache-Control", "no-cache");
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


async function ensureSchema() {
  const tableExists = name =>
    !!db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?").get(name);
  const backupTable = name => {
    const legacyName = `${name}_legacy_${Date.now()}`;
    db.exec(`ALTER TABLE ${name} RENAME TO ${legacyName}`);
    return legacyName;
  };
  const getColumns = name =>
    db.prepare("PRAGMA table_info('" + name.replace(/'/g, "''") + "')").all().map(col => col.name);

  const dropTable = name => {
    if (!name) return false;
    db.exec(`DROP TABLE IF EXISTS ${name}`);
    return true;
  };

  const migrate = async () => {
    // member table
    if (!tableExists("member")) {
      let legacyMembers = null;
      if (tableExists("members")) {
        legacyMembers = backupTable("members");
      }
      db.exec(`
        CREATE TABLE IF NOT EXISTS member (
          id TEXT PRIMARY KEY,
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
      if (legacyMembers) {
        const rows = db.prepare("SELECT * FROM " + legacyMembers).all();
        const insertMember = db.prepare(`
          INSERT INTO member (id, name, date_of_birth, sex, status, tags, campaign_id, source, created_at, updated_at)
          VALUES (@id, @name, @date_of_birth, @sex, @status, @tags, @campaign_id, @source, @created_at, @updated_at)
        `);
        for (const row of rows) {
          const id = normId(row.userId || row.id || "");
          if (!id) continue;
          insertMember.run({
            id,
            name: row.name || id,
            date_of_birth: row.dob || row.date_of_birth || null,
            sex: row.sex || null,
            status: "active",
            tags: null,
            campaign_id: null,
            source: null,
            created_at: normalizeTimestamp(row.createdAt),
            updated_at: normalizeTimestamp(row.updatedAt ?? row.createdAt)
          });
        }
      }
    } else {
      ensureColumn(
db, "member", "status", "TEXT");
      ensureColumn(
db, "member", "tags", "TEXT");
      ensureColumn(
db, "member", "campaign_id", "TEXT");
      ensureColumn(
db, "member", "source", "TEXT");
      ensureColumn(
db, "member", "date_of_birth", "TEXT");
    }
    db.exec(
      "UPDATE member SET status = 'active' WHERE status IS NULL OR status = ''"
    );
    db.exec("CREATE INDEX IF NOT EXISTS idx_member_status ON member(status)");

    // reward table
    if (!tableExists("reward")) {
      let legacyRewards = null;
      if (tableExists("rewards")) {
        legacyRewards = backupTable("rewards");
      }
      db.exec(`
        CREATE TABLE IF NOT EXISTS reward (
          id TEXT PRIMARY KEY,
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
      if (legacyRewards) {
        const rows = db.prepare("SELECT * FROM " + legacyRewards).all();
        const insertReward = db.prepare(`
          INSERT INTO reward (id, name, cost, description, image_url, youtube_url, status, tags, campaign_id, source, created_at, updated_at)
          VALUES (@id, @name, @cost, @description, @image_url, @youtube_url, @status, @tags, @campaign_id, @source, @created_at, @updated_at)
        `);
        for (const row of rows) {
          const id = String(row.id ?? "").trim();
          if (!id) continue;
          const status = Number(row.active ?? 1) === 1 ? "active" : "disabled";
          const created = normalizeTimestamp(row.created_at ?? row.createdAt);
          insertReward.run({
            id,
            name: row.name || id,
            cost: Number(row.price ?? row.cost ?? 0) || 0,
            description: row.description || "",
            image_url: row.image_url || row.imageUrl || "",
            youtube_url: row.youtube_url || row.youtubeUrl || null,
            status,
            tags: null,
            campaign_id: null,
            source: null,
            created_at: created,
            updated_at: normalizeTimestamp(row.updated_at ?? row.updatedAt ?? created)
          });
        }
      }
    } else {
      ensureColumn(
db, "reward", "status", "TEXT");
      ensureColumn(
db, "reward", "tags", "TEXT");
      ensureColumn(
db, "reward", "campaign_id", "TEXT");
      ensureColumn(
db, "reward", "source", "TEXT");
      ensureColumn(
db, "reward", "cost", "INTEGER");
      ensureColumn(
db, "reward", "updated_at", "INTEGER");
    }
    db.exec(
      "UPDATE reward SET status = 'active' WHERE status IS NULL OR status = ''"
    );
    db.exec(
      "UPDATE reward SET cost = 0 WHERE cost IS NULL"
    );
    db.exec(
      "UPDATE reward SET updated_at = COALESCE(NULLIF(updated_at, 0), COALESCE(NULLIF(created_at, 0), strftime('%s','now')*1000)) WHERE updated_at IS NULL OR updated_at = 0"
    );
    db.exec("CREATE INDEX IF NOT EXISTS idx_reward_status ON reward(status)");

    // hold table
    if (!tableExists("hold")) {
      let legacyHolds = null;
      if (tableExists("holds")) {
        legacyHolds = backupTable("holds");
      }
      db.exec(`
        CREATE TABLE IF NOT EXISTS hold (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          actor_id TEXT,
          reward_id TEXT,
          reward_name TEXT,
          reward_image_url TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          quoted_amount INTEGER NOT NULL,
          final_amount INTEGER,
          note TEXT,
          metadata TEXT,
          source TEXT,
          tags TEXT,
          campaign_id TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          released_at INTEGER,
          redeemed_at INTEGER,
          expires_at INTEGER,
          FOREIGN KEY (reward_id) REFERENCES reward(id)
        );
      `);
      if (legacyHolds) {
        const rows = db.prepare("SELECT * FROM " + legacyHolds).all();
        const insertHold = db.prepare(`
          INSERT INTO hold (
            id,
            user_id,
            actor_id,
            reward_id,
            reward_name,
            reward_image_url,
            status,
            quoted_amount,
            final_amount,
            note,
            metadata,
            source,
            tags,
            campaign_id,
            created_at,
            updated_at,
            released_at,
            redeemed_at,
            expires_at
          ) VALUES (@id,@user_id,@actor_id,@reward_id,@reward_name,@reward_image_url,@status,@quoted_amount,@final_amount,@note,@metadata,@source,@tags,@campaign_id,@created_at,@updated_at,@released_at,@redeemed_at,@expires_at)
        `);
        for (const row of rows) {
          const id = String(row.id ?? "").trim();
          if (!id) continue;
          const userId = normId(row.userId || row.user_id || "");
          if (!userId) continue;
          const rawStatus = String(row.status || "pending").trim().toLowerCase();
          const status =
            rawStatus === "redeemed"
              ? "redeemed"
              : rawStatus === "canceled" || rawStatus === "released"
              ? "released"
              : "pending";
          const approvedAt = row.approvedAt ?? row.approved_at ?? null;
          const createdAt = normalizeTimestamp(row.createdAt ?? row.created_at);
          insertHold.run({
            id,
            user_id: userId,
            actor_id: null,
            reward_id: row.itemId || row.reward_id || null,
            reward_name: row.itemName || row.reward_name || null,
            reward_image_url: row.itemImage || row.reward_image_url || null,
            status,
            quoted_amount: Number(row.quotedCost ?? row.quoted_amount ?? row.points ?? 0) || 0,
            final_amount:
              row.finalCost !== undefined && row.finalCost !== null
                ? Number(row.finalCost)
                : row.final_amount !== undefined && row.final_amount !== null
                ? Number(row.final_amount)
                : null,
            note: row.note || null,
            metadata: null,
            source: null,
            tags: null,
            campaign_id: null,
            created_at: createdAt,
            updated_at: normalizeTimestamp(approvedAt ?? createdAt),
            released_at: status === "released" ? normalizeTimestamp(approvedAt) : null,
            redeemed_at: status === "redeemed" ? normalizeTimestamp(approvedAt) : null,
            expires_at: null
          });
        }
      }
    } else {
      ensureColumn(
db, "hold", "actor_id", "TEXT");
      ensureColumn(
db, "hold", "reward_name", "TEXT");
      ensureColumn(
db, "hold", "reward_image_url", "TEXT");
      ensureColumn(
db, "hold", "quoted_amount", "INTEGER");
      ensureColumn(
db, "hold", "final_amount", "INTEGER");
      ensureColumn(
db, "hold", "metadata", "TEXT");
      ensureColumn(
db, "hold", "source", "TEXT");
      ensureColumn(
db, "hold", "tags", "TEXT");
      ensureColumn(
db, "hold", "campaign_id", "TEXT");
      ensureColumn(
db, "hold", "released_at", "INTEGER");
      ensureColumn(
db, "hold", "redeemed_at", "INTEGER");
      ensureColumn(
db, "hold", "expires_at", "INTEGER");
      ensureColumn(
db, "hold", "updated_at", "INTEGER");
      ensureColumn(
db, "hold", "created_at", "INTEGER");
    }
    db.exec("UPDATE hold SET quoted_amount = COALESCE(quoted_amount, 0)");
    db.exec("CREATE INDEX IF NOT EXISTS idx_hold_user_status ON hold(user_id, status)");

    // ledger table
    let legacyLedger = null;
    if (tableExists("ledger")) {
      const info = db.prepare("PRAGMA table_info('ledger')").all();
      const idColumn = info.find(col => col.name === "id");
      const hasTextId = idColumn && typeof idColumn.type === "string" && idColumn.type.toUpperCase().includes("TEXT");
      const legacyColumns = new Set(["delta", "reason", "kind", "nonce", "ts", "meta"]);
      const hasLegacy = info.some(col => legacyColumns.has(col.name));
      if (!hasTextId || hasLegacy) {
        legacyLedger = backupTable("ledger");
      }
    }

    if (!tableExists("ledger") || legacyLedger) {
      db.exec(`
        CREATE TABLE IF NOT EXISTS ledger (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          actor_id TEXT,
          reward_id TEXT,
          parent_hold_id TEXT,
          parent_ledger_id TEXT,
          verb TEXT NOT NULL,
          description TEXT,
          amount INTEGER NOT NULL,
          balance_after INTEGER NOT NULL,
          status TEXT NOT NULL DEFAULT 'posted',
          note TEXT,
          notes TEXT,
          template_ids TEXT,
          final_amount INTEGER,
          metadata TEXT,
          refund_reason TEXT,
          refund_notes TEXT,
          idempotency_key TEXT UNIQUE,
          source TEXT,
          tags TEXT,
          campaign_id TEXT,
          ip_address TEXT,
          user_agent TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          FOREIGN KEY (user_id) REFERENCES member(id),
          FOREIGN KEY (reward_id) REFERENCES reward(id),
          FOREIGN KEY (parent_hold_id) REFERENCES hold(id),
          FOREIGN KEY (parent_ledger_id) REFERENCES ledger(id)
        );
      `);
      if (legacyLedger) {
        const rows = db.prepare("SELECT * FROM " + legacyLedger).all();
        const insertLedger = db.prepare(`
          INSERT INTO ledger (
            id,
            user_id,
            actor_id,
            reward_id,
            parent_hold_id,
            parent_ledger_id,
            verb,
            description,
            amount,
            balance_after,
            status,
            note,
            notes,
            template_ids,
            final_amount,
            metadata,
            refund_reason,
            refund_notes,
            idempotency_key,
            source,
            tags,
            campaign_id,
            ip_address,
            user_agent,
            created_at,
            updated_at
          ) VALUES (@id,@user_id,@actor_id,@reward_id,@parent_hold_id,@parent_ledger_id,@verb,@description,@amount,@balance_after,@status,@note,@notes,@template_ids,@final_amount,@metadata,@refund_reason,@refund_notes,@idempotency_key,@source,@tags,@campaign_id,@ip_address,@user_agent,@created_at,@updated_at)
        `);
        for (const row of rows) {
          const id = String(row.id ?? row.ID ?? crypto.randomUUID()).trim();
          const userId = normId(row.user_id ?? row.userId ?? "");
          if (!id || !userId) continue;
          const amount = Number(row.amount ?? row.delta ?? 0) || 0;
          const balanceAfter = Number(row.balance_after ?? row.balanceAfter ?? row.balance ?? 0) || 0;
          const templateIds = row.template_ids ?? row.templateIds ?? null;
          const metadata = row.metadata ?? row.meta ?? null;
          const tags = row.tags ?? null;
          const createdAt = normalizeTimestamp(row.created_at ?? row.createdAt ?? row.ts);
          const updatedAt = normalizeTimestamp(row.updated_at ?? row.updatedAt ?? createdAt);
          insertLedger.run({
            id,
            user_id: userId,
            actor_id: row.actor_id || row.actorId || row.actor || null,
            reward_id: row.reward_id || row.itemId || null,
            parent_hold_id: row.parent_hold_id || row.holdId || null,
            parent_ledger_id: row.parent_ledger_id || row.parent_tx_id || null,
            verb:
              (row.verb || row.kind || "")
                .toString()
                .trim() || (amount > 0 ? "earn" : amount < 0 ? "redeem" : "adjust"),
            description: row.description || row.reason || row.action || null,
            amount,
            balance_after: balanceAfter,
            status: (row.status || row.state || "posted").toString().trim().toLowerCase() || "posted",
            note: row.note || null,
            notes: row.notes || null,
            template_ids: templateIds ? JSON.stringify(templateIds) : null,
            final_amount:
              row.final_amount !== undefined && row.final_amount !== null
                ? Number(row.final_amount)
                : row.finalCost !== undefined && row.finalCost !== null
                ? Number(row.finalCost)
                : null,
            metadata: metadata ? JSON.stringify(metadata) : null,
            refund_reason: row.refund_reason || null,
            refund_notes: row.refund_notes || null,
            idempotency_key: row.idempotency_key || row.nonce || null,
            source: row.source || null,
            tags: tags ? JSON.stringify(tags) : null,
            campaign_id: row.campaign_id || row.campaignId || null,
            ip_address: row.ip_address || row.ip || null,
            user_agent: row.user_agent || row.ua || null,
            created_at: createdAt,
            updated_at: updatedAt
          });
        }
      }
    } else {
      ensureColumn(
db, "ledger", "user_id", "TEXT");
      ensureColumn(
db, "ledger", "actor_id", "TEXT");
      ensureColumn(
db, "ledger", "reward_id", "TEXT");
      ensureColumn(
db, "ledger", "parent_hold_id", "TEXT");
      ensureColumn(
db, "ledger", "parent_ledger_id", "TEXT");
      ensureColumn(
db, "ledger", "description", "TEXT");
      ensureColumn(
db, "ledger", "verb", "TEXT");
      ensureColumn(
db, "ledger", "amount", "INTEGER");
      ensureColumn(
db, "ledger", "balance_after", "INTEGER");
      ensureColumn(
db, "ledger", "status", "TEXT");
      ensureColumn(
db, "ledger", "idempotency_key", "TEXT");
      ensureColumn(
db, "ledger", "template_ids", "TEXT");
      ensureColumn(
db, "ledger", "final_amount", "INTEGER");
      ensureColumn(
db, "ledger", "metadata", "TEXT");
      ensureColumn(
db, "ledger", "note", "TEXT");
      ensureColumn(
db, "ledger", "notes", "TEXT");
      ensureColumn(
db, "ledger", "refund_reason", "TEXT");
      ensureColumn(
db, "ledger", "refund_notes", "TEXT");
      ensureColumn(
db, "ledger", "source", "TEXT");
      ensureColumn(
db, "ledger", "tags", "TEXT");
      ensureColumn(
db, "ledger", "campaign_id", "TEXT");
      ensureColumn(
db, "ledger", "ip_address", "TEXT");
      ensureColumn(
db, "ledger", "user_agent", "TEXT");
      ensureColumn(
db, "ledger", "created_at", "INTEGER");
      ensureColumn(
db, "ledger", "updated_at", "INTEGER");
    }
    db.exec("UPDATE ledger SET verb = 'adjust' WHERE verb IS NULL OR verb = ''");
    db.exec("UPDATE ledger SET amount = 0 WHERE amount IS NULL");
    db.exec("UPDATE ledger SET balance_after = 0 WHERE balance_after IS NULL");
    db.exec("UPDATE ledger SET status = 'posted' WHERE status IS NULL OR status = ''");
    db.exec(
      "UPDATE ledger SET created_at = COALESCE(NULLIF(created_at, 0), strftime('%s','now')*1000) WHERE created_at IS NULL OR created_at = 0"
    );
    db.exec(
      "UPDATE ledger SET updated_at = COALESCE(NULLIF(updated_at, 0), COALESCE(NULLIF(created_at, 0), strftime('%s','now')*1000)) WHERE updated_at IS NULL OR updated_at = 0"
    );


const sqliteTransaction = handler => db.transaction(handler);

function rebuildLedgerTableIfLegacy() {
  const info = db.prepare("PRAGMA table_info('ledger')").all();
  if (!info.length) return;
  const idColumn = info.find(col => col.name === "id");
  const hasTextId = idColumn && typeof idColumn.type === "string" && idColumn.type.toUpperCase().includes("TEXT");
  const legacyColumns = new Set(["delta", "reason", "kind", "nonce", "ts", "meta"]);
  const hasLegacy = info.some(col => legacyColumns.has(col.name));
  if (hasTextId && !hasLegacy) return;

  const legacyName = `ledger_legacy_${Date.now()}`;
  db.exec(`ALTER TABLE ledger RENAME TO ${legacyName}`);
  db.exec(`
    CREATE TABLE IF NOT EXISTS ledger (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      actor_id TEXT,
      reward_id TEXT,
      parent_hold_id TEXT,
      parent_ledger_id TEXT,
      verb TEXT NOT NULL,
      description TEXT,
      amount INTEGER NOT NULL,
      balance_after INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'posted',
      note TEXT,
      notes TEXT,
      template_ids TEXT,
      final_amount INTEGER,
      metadata TEXT,
      refund_reason TEXT,
      refund_notes TEXT,
      idempotency_key TEXT UNIQUE,
      source TEXT,
      tags TEXT,
      campaign_id TEXT,
      ip_address TEXT,
      user_agent TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      FOREIGN KEY (user_id) REFERENCES member(id),
      FOREIGN KEY (reward_id) REFERENCES reward(id),
      FOREIGN KEY (parent_hold_id) REFERENCES hold(id),
      FOREIGN KEY (parent_ledger_id) REFERENCES ledger(id)
    );
  `);

  const rows = db.prepare(`SELECT * FROM ${legacyName}`).all();
  if (rows.length) {
    const insert = db.prepare(`
      INSERT INTO ledger (
        id,
        user_id,
        actor_id,
        reward_id,
        parent_hold_id,
        parent_ledger_id,
        verb,
        description,
        amount,
        balance_after,
        status,
        note,
        notes,
        template_ids,
        final_amount,
        metadata,
        refund_reason,
        refund_notes,
        idempotency_key,
        source,
        tags,
        campaign_id,
        ip_address,
        user_agent,
        created_at,
        updated_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);

    // spend_request table
    if (!tableExists("spend_request")) {
      let legacySpend = null;
      if (tableExists("spend_requests")) {
        legacySpend = backupTable("spend_requests");
      }
      db.exec(`
        CREATE TABLE IF NOT EXISTS spend_request (
          id TEXT PRIMARY KEY,
          token TEXT UNIQUE NOT NULL,
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
      if (legacySpend) {
        const rows = db.prepare(`SELECT * FROM ${legacySpend}`).all();

        const insertSpend = db.prepare(`
          INSERT INTO spend_request (
            id,
            token,
            user_id,
            reward_id,
            status,
            amount,
            title,
            image_url,
            actor_id,
            source,
            tags,
            campaign_id,
            created_at,
            updated_at
          ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        `);

        const insertMany = db.transaction((items) => {
          for (const r of items) {
            insertSpend.run(
              r.id,
              r.token,
              r.user_id,
              r.reward_id ?? null,
              r.status ?? 'pending',
              r.amount ?? null,
              r.title ?? null,
              r.image_url ?? null,
              r.actor_id ?? null,
              r.source ?? null,
              r.tags ?? null,
              r.campaign_id ?? null,
              r.created_at ?? Date.now(),
              r.updated_at ?? Date.now()
            );
          }
        });

        insertMany(rows);
        dropTable(legacySpend);
      }

      }
    } else {
      ensureColumn(
db, "spend_request", "actor_id", "TEXT");
      ensureColumn(
db, "spend_request", "source", "TEXT");
      ensureColumn(
db, "spend_request", "tags", "TEXT");
      ensureColumn(
db, "spend_request", "campaign_id", "TEXT");
      ensureColumn(
db, "spend_request", "amount", "INTEGER");
      ensureColumn(
db, "spend_request", "created_at", "INTEGER");
      ensureColumn(
db, "spend_request", "updated_at", "INTEGER");
    }
    db.exec(
      "UPDATE spend_request SET status = 'pending' WHERE status IS NULL OR status = ''"
    );
    db.exec(
      "UPDATE spend_request SET created_at = COALESCE(NULLIF(created_at, 0), strftime('%s','now')*1000) WHERE created_at IS NULL OR created_at = 0"
    );
    db.exec(
      "UPDATE spend_request SET updated_at = COALESCE(NULLIF(updated_at, 0), COALESCE(NULLIF(created_at, 0), strftime('%s','now')*1000)) WHERE updated_at IS NULL OR updated_at = 0"
    );
    db.exec("CREATE INDEX IF NOT EXISTS idx_spend_request_status ON spend_request(status)");
    db.exec("CREATE INDEX IF NOT EXISTS idx_spend_request_user ON spend_request(user_id)");

    // consumed tokens
    let consumedCols = [];
    if (!tableExists("consumed_tokens")) {
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
      consumedCols = [
        "id",
        "token",
        "typ",
        "request_id",
        "user_id",
        "reward_id",
        "source",
        "created_at",
        "updated_at"
      ];
    } else {
      consumedCols = getColumns("consumed_tokens");
      if (!consumedCols.includes("id") && consumedCols.includes("jti")) {
        db.exec("ALTER TABLE consumed_tokens RENAME COLUMN jti TO id");
        consumedCols = getColumns("consumed_tokens");
      }
      if (!consumedCols.includes("created_at") && consumedCols.includes("consumed_at")) {
        db.exec("ALTER TABLE consumed_tokens RENAME COLUMN consumed_at TO created_at");
        consumedCols = getColumns("consumed_tokens");
      }
      ensureColumn(
db, "consumed_tokens", "token", "TEXT");
      ensureColumn(
db, "consumed_tokens", "typ", "TEXT");
      ensureColumn(
db, "consumed_tokens", "request_id", "TEXT");
      ensureColumn(
db, "consumed_tokens", "user_id", "TEXT");
      ensureColumn(
db, "consumed_tokens", "reward_id", "TEXT");
      ensureColumn(
db, "consumed_tokens", "source", "TEXT");
      ensureColumn(
db, "consumed_tokens", "created_at", "INTEGER");
      ensureColumn(
db, "consumed_tokens", "updated_at", "INTEGER");
    }
  }

  db.exec(`DROP TABLE IF EXISTS ${legacyName}`);
}

const ensureTables = sqliteTransaction(() => {
  db.exec(`
    CREATE TABLE IF NOT EXISTS member (
      id TEXT PRIMARY KEY,
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

  db.exec(`
    CREATE TABLE IF NOT EXISTS reward (
      id TEXT PRIMARY KEY,
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

  db.exec(`
    CREATE TABLE IF NOT EXISTS hold (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      actor_id TEXT,
      reward_id TEXT,
      reward_name TEXT,
      reward_image_url TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      quoted_amount INTEGER NOT NULL,
      final_amount INTEGER,
      note TEXT,
      metadata TEXT,
      source TEXT,
      tags TEXT,
      campaign_id TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      released_at INTEGER,
      redeemed_at INTEGER,
      expires_at INTEGER,
      FOREIGN KEY (reward_id) REFERENCES reward(id)
    );
  `);
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
  db.exec("CREATE INDEX IF NOT EXISTS idx_hold_user_status ON hold(user_id, status)");

  db.exec(`
    CREATE TABLE IF NOT EXISTS ledger (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      actor_id TEXT,
      reward_id TEXT,
      parent_hold_id TEXT,
      parent_ledger_id TEXT,
      verb TEXT NOT NULL,
      description TEXT,
      amount INTEGER NOT NULL,
      balance_after INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'posted',
      note TEXT,
      notes TEXT,
      template_ids TEXT,
      final_amount INTEGER,
      metadata TEXT,
      refund_reason TEXT,
      refund_notes TEXT,
      idempotency_key TEXT UNIQUE,
      source TEXT,
      tags TEXT,
      campaign_id TEXT,
      ip_address TEXT,
      user_agent TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      FOREIGN KEY (user_id) REFERENCES member(id),
      FOREIGN KEY (reward_id) REFERENCES reward(id),
      FOREIGN KEY (parent_hold_id) REFERENCES hold(id),
      FOREIGN KEY (parent_ledger_id) REFERENCES ledger(id)
    );
  `);
  rebuildLedgerTableIfLegacy();
  ensureColumn(db, "ledger", "actor_id", "TEXT");
  ensureColumn(db, "ledger", "reward_id", "TEXT");
  ensureColumn(db, "ledger", "parent_hold_id", "TEXT");
  ensureColumn(db, "ledger", "parent_ledger_id", "TEXT");
  ensureColumn(db, "ledger", "verb", "TEXT");
  ensureColumn(db, "ledger", "description", "TEXT");
  ensureColumn(db, "ledger", "amount", "INTEGER");
  ensureColumn(db, "ledger", "balance_after", "INTEGER");
  ensureColumn(db, "ledger", "status", "TEXT");
  ensureColumn(db, "ledger", "note", "TEXT");
  ensureColumn(db, "ledger", "notes", "TEXT");
  ensureColumn(db, "ledger", "template_ids", "TEXT");
  ensureColumn(db, "ledger", "final_amount", "INTEGER");
  ensureColumn(db, "ledger", "metadata", "TEXT");
  ensureColumn(db, "ledger", "refund_reason", "TEXT");
  ensureColumn(db, "ledger", "refund_notes", "TEXT");
  ensureColumn(db, "ledger", "idempotency_key", "TEXT");
  ensureColumn(db, "ledger", "source", "TEXT");
  ensureColumn(db, "ledger", "tags", "TEXT");
  ensureColumn(db, "ledger", "campaign_id", "TEXT");
  ensureColumn(db, "ledger", "ip_address", "TEXT");
  ensureColumn(db, "ledger", "user_agent", "TEXT");
  ensureColumn(db, "ledger", "created_at", "INTEGER");
  ensureColumn(db, "ledger", "updated_at", "INTEGER");
  db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_idempotency
    ON ledger(idempotency_key)
    WHERE idempotency_key IS NOT NULL
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_ledger_user_verb_created_at
    ON ledger(user_id, verb, created_at, id)
  `);
  db.exec("CREATE INDEX IF NOT EXISTS idx_ledger_parent_hold ON ledger(parent_hold_id)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_ledger_parent_ledger ON ledger(parent_ledger_id)");

  const existingSpendRequest = db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'spend_request'")
    .get();
  if (!existingSpendRequest) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS spend_request (
        id TEXT PRIMARY KEY,
        token TEXT UNIQUE NOT NULL,
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
    db.exec("CREATE INDEX IF NOT EXISTS idx_consumed_tokens_user ON consumed_tokens(user_id)");
    db.exec("CREATE INDEX IF NOT EXISTS idx_consumed_tokens_reward ON consumed_tokens(reward_id)");
    db.exec("CREATE INDEX IF NOT EXISTS idx_consumed_tokens_request ON consumed_tokens(request_id)");
  }
});

  migrate();
}

ensureSchema();
ensureTables();
function nowSec() {
  return Math.floor(Date.now() / 1000);
}

const selectMemberStmt = db.prepare(`
  SELECT id, name, date_of_birth, sex, status, created_at, updated_at
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

const insertMemberStmt = db.prepare(`
  INSERT INTO member (id, name, date_of_birth, sex, status, created_at, updated_at)
  VALUES (@id, @name, @date_of_birth, @sex, @status, @created_at, @updated_at)
`);

const selectMemberExistsStmt = db.prepare(`
  SELECT 1
  FROM member
  WHERE id = ?
`);

const updateMemberStmt = db.prepare(`
  UPDATE member
  SET name = @name,
      date_of_birth = @date_of_birth,
      sex = @sex,
      status = @status,
      updated_at = @updated_at
  WHERE id = @id
`);

const deleteMemberStmt = db.prepare(`
  DELETE FROM member
  WHERE id = ?
`);

function ensureDefaultMembers() {
  const defaults = [
    { id: "leo", name: "Leo", date_of_birth: null, sex: null, status: "active" }
  ];

  const insertMissing = db.transaction(members => {
    for (const member of members) {
      if (!selectMemberExistsStmt.get(member.id)) {
        const ts = Date.now();
        insertMemberStmt.run({
          id: member.id,
          name: member.name,
          date_of_birth: member.date_of_birth,
          sex: member.sex,
          status: member.status || "active",
          created_at: ts,
          updated_at: ts
        });
      }
    }
  });

  insertMissing(defaults);
}

ensureDefaultMembers();

function getBalance(userId) {
  return Number(balanceOf(userId) || 0);
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

const selectLedgerByIdStmt = db.prepare("SELECT * FROM ledger WHERE id = ?");
const selectLedgerByKeyStmt = db.prepare("SELECT * FROM ledger WHERE idempotency_key = ?");
const sumRefundsByParentStmt = db.prepare(
  "SELECT COALESCE(SUM(amount), 0) AS total FROM ledger WHERE parent_ledger_id = ? AND verb = 'refund'"
);
const listLedgerByUserStmt = db.prepare(`
  SELECT *
  FROM ledger
  WHERE user_id = ?
  ORDER BY created_at DESC, id DESC
`);
const checkTokenStmt = db.prepare("SELECT 1 FROM consumed_tokens WHERE id = ?");
const consumeTokenStmt = db.prepare(
  "INSERT INTO consumed_tokens (id, token, typ, source, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
);
const listRecentRedeemsStmt = db.prepare(`
  SELECT id, created_at, amount
  FROM ledger
  WHERE user_id = ?
    AND verb = 'redeem'
  ORDER BY created_at DESC, id DESC
  LIMIT 50
`);
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
    metadata: row.metadata || null
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
    updated_at: Number(row.updated_at ?? 0) || null
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

function getStateHints(userId) {
  const normalized = normId(userId);
  if (!normalized) {
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

  const balance = getBalance(normalized);
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
    const redeemRows = listRecentRedeemsStmt.all(normalized);
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

function buildActionResponse({ userId, txRow = null, extras = {} }) {
  const hints = getStateHints(userId);
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
  returnRow = false
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

    const ledgerResult = recordLedgerEntry({
      userId,
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
  const key = (req.headers["x-admin-key"] || "").toString().trim();
  if (!key || key !== ADMIN_KEY) {
    return res.status(401).json({ error: "UNAUTHORIZED" });
  }
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

function redeemToken({ token, req, actor, isAdmin = false, allowEarnWithoutAdmin = false }) {
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
      returnRow: true
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
      returnRow: true
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
  req
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
        conflict.balance = getBalance(normalizedUser);
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
      returnRow: true
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

function getLedgerViewForUser(userId) {
  const normalized = normId(userId);
  if (!normalized) return [];
  const rows = listLedgerByUserStmt.all(normalized).map(mapLedgerRow);
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
    balance: getBalance(userId),
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
      : normalizedStatus.charAt(0).toUpperCase() + normalizedStatus.slice(1);
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

app.get("/version", (_req, res) => {
  res.json({ build: BUILD });
});

app.get("/balance/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  res.json({ balance: getBalance(userId) });
});

app.get("/summary/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const balance = getBalance(userId);
  const sums = db.prepare(`
    SELECT
      SUM(CASE WHEN description LIKE 'earn_%' THEN amount ELSE 0 END) AS earned,
      SUM(CASE WHEN description LIKE 'spend_%' THEN ABS(amount) ELSE 0 END) AS spent
    FROM ledger
    WHERE user_id = ?
  `).get(userId);
  res.json({
    userId,
    balance,
    earned: Number(sums?.earned || 0),
    spent: Number(sums?.spent || 0)
  });
});

app.get("/api/members", requireAdminKey, (req, res) => {
  const search = (req.query?.search || "").toString().trim().toLowerCase();
  try {
    res.json(listMembers(search));
  } catch (err) {
    console.error("listMembers failed", err);
    res.status(500).json({ error: "FAILED" });
  }
});

app.post("/api/members", requireAdminKey, (req, res) => {
  const body = req.body || {};
  const userId = normId(body.userId);
  const name = (body.name || "").toString().trim();
  const dob = (body.dob || "").toString().trim();
  const sex = (body.sex || "").toString().trim();
  if (!userId) return res.status(400).json({ error: "userId required" });
  if (!name) return res.status(400).json({ error: "name required" });
  const now = Date.now();
  try {
    insertMemberStmt.run({
      userId,
      name,
      dob: dob || null,
      sex: sex || null,
      createdAt: now,
      updatedAt: now
    });
  } catch (err) {
    if (err?.code === "SQLITE_CONSTRAINT_PRIMARYKEY") {
      return res.status(409).json({ error: "USER_EXISTS" });
    }
    console.error("insertMember failed", err);
    return res.status(500).json({ error: "FAILED" });
  }
  res.status(201).json({ ok: true, member: getMember(userId) });
});

app.get("/api/members/:userId", requireAdminKey, (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const member = getMember(userId);
  if (!member) return res.status(404).json({ error: "NOT_FOUND" });
  res.json(member);
});

app.patch("/api/members/:userId", requireAdminKey, (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const existing = getMember(userId);
  if (!existing) return res.status(404).json({ error: "NOT_FOUND" });
  const body = req.body || {};
  const name = body.name !== undefined ? (body.name || "").toString().trim() : existing.name;
  const dob = body.dob !== undefined ? (body.dob || "").toString().trim() : (existing.dob || "");
  const sex = body.sex !== undefined ? (body.sex || "").toString().trim() : (existing.sex || "");
  if (!name) return res.status(400).json({ error: "name required" });
  try {
    updateMemberStmt.run({
      userId,
      name,
      dob: dob || null,
      sex: sex || null,
      updatedAt: Date.now()
    });
  } catch (err) {
    console.error("updateMember failed", err);
    return res.status(500).json({ error: "FAILED" });
  }
  res.json({ ok: true, member: getMember(userId) });
});

app.delete("/api/members/:userId", requireAdminKey, (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const existing = getMember(userId);
  if (!existing) return res.status(404).json({ error: "NOT_FOUND" });
  try {
    deleteMemberStmt.run(userId);
  } catch (err) {
    console.error("deleteMember failed", err);
    return res.status(500).json({ error: "FAILED" });
  }
  res.json({ ok: true });
});

app.get("/api/earn-templates", (req, res) => {
  const { active, sort } = req.query;
  const filters = [];
  const params = [];
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
  const rows = db.prepare(sql).all(...params);
  res.json(rows.map(mapEarnTemplate));
});

app.post("/api/earn-templates", requireAdminKey, (req, res) => {
  const { title, points, description = "", youtube_url = null, active = true, sort_order = 0 } = req.body || {};
  if (!title || !Number.isFinite(Number(points))) {
    return res.status(400).json({ error: "invalid_template" });
  }
  const stmt = db.prepare(`
    INSERT INTO earn_templates (title, points, description, youtube_url, active, sort_order, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const ts = nowSec();
  const info = stmt.run(String(title), Math.floor(Number(points)), String(description || ""), youtube_url ? String(youtube_url) : null, active ? 1 : 0, Number(sort_order) || 0, ts, ts);
  const row = db.prepare("SELECT * FROM earn_templates WHERE id = ?").get(info.lastInsertRowid);
  res.status(201).json(mapEarnTemplate(row));
});

app.patch("/api/earn-templates/:id", requireAdminKey, (req, res) => {
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
  const sql = `UPDATE earn_templates SET ${fields.join(", ")} WHERE id = ?`;
  params.push(id);
  const info = db.prepare(sql).run(...params);
  if (!info.changes) return res.status(404).json({ error: "not_found" });
  const row = db.prepare("SELECT * FROM earn_templates WHERE id = ?").get(id);
  res.json(mapEarnTemplate(row));
});

app.delete("/api/earn-templates/:id", requireAdminKey, (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid_id" });
  const info = db.prepare("DELETE FROM earn_templates WHERE id = ?").run(id);
  if (!info.changes) return res.status(404).json({ error: "not_found" });
  res.json({ ok: true });
});

app.post("/ck/refund", requireAdminKey, (req, res) => {
  const body = req.body || {};
  const actorId = (req.headers["x-admin-actor"] || "").toString().trim() || "admin";
  const idempotencyKey = body.idempotency_key ? String(body.idempotency_key).trim() : null;
  const normalizedUser = normId(body.user_id);
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
      req
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

app.get("/ck/ledger/:userId", requireAdminKey, (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const data = getLedgerViewForUser(userId);
  const hints = getStateHints(userId);
  res.json({ ...data, hints });
});

app.post("/ck/earn", requireAdminKey, requireRole("admin"), express.json(), (req, res) => {
  const userId = normId(req.body?.user_id ?? req.body?.userId);
  const amount = Math.floor(Number(req.body?.amount ?? 0));
  const note = req.body?.note ? String(req.body.note).slice(0, 200) : null;
  const actorLabel = (req.headers["x-admin-actor"] || "").toString().trim() || "admin_manual";
  const action = req.body?.action ? String(req.body.action) : "earn_manual";
  if (!userId || !Number.isFinite(amount) || amount <= 0) {
    return res.status(400).json(buildErrorResponse({ err: { message: "INVALID_AMOUNT" }, userId }));
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
      verb: "earn"
    });
    recordTelemetry("earn", { ok: true, durationMs: Date.now() - started });
    const response = buildActionResponse({
      userId,
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

app.post("/ck/redeem", requireAdminKey, requireRole("admin"), express.json(), (req, res) => {
  const userId = normId(req.body?.user_id ?? req.body?.userId);
  const amount = Math.floor(Number(req.body?.amount ?? 0));
  const note = req.body?.note ? String(req.body.note).slice(0, 200) : null;
  const actorLabel = (req.headers["x-admin-actor"] || "").toString().trim() || "admin_redeem_manual";
  const action = req.body?.action ? String(req.body.action) : "spend_manual";
  if (!userId || !Number.isFinite(amount) || amount <= 0) {
    return res.status(400).json(buildErrorResponse({ err: { message: "INVALID_AMOUNT" }, userId }));
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
      verb: "redeem"
    });
    recordTelemetry("redeem", { ok: true, durationMs: Date.now() - started });
    const response = buildActionResponse({
      userId,
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

app.post("/ck/adjust", requireAdminKey, requireRole("admin"), express.json(), (req, res) => {
  const userId = normId(req.body?.user_id ?? req.body?.userId);
  const deltaRaw = Number(req.body?.delta ?? 0);
  const delta = Math.trunc(deltaRaw);
  const note = req.body?.note ? String(req.body.note).slice(0, 200) : null;
  const actorLabel = (req.headers["x-admin-actor"] || "").toString().trim() || "admin_adjust";
  if (!userId || !Number.isFinite(delta) || delta === 0) {
    return res.status(400).json(buildErrorResponse({ err: { message: "INVALID_DELTA" }, userId }));
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
      verb: "adjust"
    });
    recordTelemetry("adjust", { ok: true, durationMs: Date.now() - started });
    const response = buildActionResponse({
      userId,
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

app.get("/api/rewards", (req, res) => {
  const filters = [];
  const params = [];
  const query = req.query || {};
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
    SELECT id, name, cost, description, image_url, youtube_url, status, tags, campaign_id, source, created_at, updated_at
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

app.post("/api/rewards", requireAdminKey, express.json(), (req, res) => {
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
    const insert = db.prepare(`
      INSERT INTO reward (
        id, name, cost, description, image_url, youtube_url, status, tags, campaign_id, source, created_at, updated_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    `);
    insert.run(
      rewardId,
      name,
      cost,
      description,
      imageUrl ? String(imageUrl).trim() || null : null,
      youtubeUrl ? String(youtubeUrl).trim() || null : null,
      String(statusRaw || "active").trim().toLowerCase() || "active",
      encodedTags,
      campaignId ? String(campaignId).trim() || null : null,
      source ? String(source).trim() || null : null,
      now,
      now
    );
    const row = db.prepare("SELECT * FROM reward WHERE id = ?").get(rewardId);
    res.status(201).json(mapRewardRow(row));
  } catch (e) {
    console.error("create reward", e);
    const status = e?.code === "SQLITE_CONSTRAINT" ? 409 : 500;
    res.status(status).json({ error: "create_reward_failed" });
  }
});

app.patch("/api/rewards/:id", requireAdminKey, (req, res) => {
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
  params.push(Date.now());
  const sql = `UPDATE reward SET ${fields.join(", ")} WHERE id = ?`;
  params.push(id);
  const info = db.prepare(sql).run(...params);
  if (!info.changes) return res.status(404).json({ error: "not_found" });
  const updated = db.prepare("SELECT * FROM reward WHERE id = ?").get(id);
  res.json({ ok: true, reward: mapRewardRow(updated) });
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
  const { token, payload } = createToken("give", { userId, amount: Math.floor(amount), note });
  res.json({ token, qrText: buildQrUrl(req, token), expiresAt: payload.exp, amount: Math.floor(amount) });
});

app.post("/api/earn/scan", (req, res) => {
  const started = Date.now();
  try {
    const isAdmin = (req.headers["x-admin-key"] || "").toString().trim() === ADMIN_KEY;
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

app.post("/api/earn/quick", requireAdminKey, (req, res) => {
  const userId = normId(req.body?.userId);
  const templateId = Number(req.body?.templateId);
  if (!userId || !templateId) {
    return res.status(400).json(buildErrorResponse({ err: { message: "invalid_payload" }, userId }));
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
      returnRow: true
    });
    const response = buildActionResponse({
      userId,
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
    const insertedHold = mapHoldRow(db.prepare('SELECT * FROM hold WHERE id = ?').get(id));
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
  const allowed = ['pending', 'redeemed', 'released', 'all'];
  if (!allowed.includes(status)) {
    return res.status(400).json({ error: 'invalid_status' });
  }
  let sql = 'SELECT * FROM hold';
  const params = [];
  if (status !== 'all') {
    sql += ' WHERE status = ?';
    params.push(status);
  }
  sql += ' ORDER BY created_at DESC';
  const rows = db.prepare(sql).all(...params).map(mapHoldRow);
  res.json(rows);
});

app.post('/api/holds/:id/approve', requireAdminKey, (req, res) => {
  const started = Date.now();
  let hold = null;
  try {
    const id = String(req.params.id || '');
    const token = String(req.body?.token || '');
    const override = req.body?.finalCost ?? req.body?.final_cost;
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
    hold = mapHoldRow(db.prepare('SELECT * FROM hold WHERE id = ?').get(id));
    if (!hold || hold.status !== 'pending') {
      return res.status(404).json(buildErrorResponse({ err: { message: 'hold_not_pending' }, userId: hold?.userId }));
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
      returnRow: true
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
    hold = mapHoldRow(db.prepare('SELECT * FROM hold WHERE id = ?').get(id));
    const response = buildActionResponse({
      userId: hold.userId,
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

app.post('/api/holds/:id/cancel', requireAdminKey, (req, res) => {
  const started = Date.now();
  const id = String(req.params.id || '');
  let hold = mapHoldRow(db.prepare('SELECT * FROM hold WHERE id = ?').get(id));
  if (!hold || hold.status !== 'pending') {
    return res.status(404).json(buildErrorResponse({ err: { message: 'hold_not_pending' }, userId: hold?.userId }));
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
  hold = mapHoldRow(db.prepare('SELECT * FROM hold WHERE id = ?').get(id));
  const result = applyLedger({
    userId: hold.userId,
    delta: 0,
    action: 'spend_released',
    note: hold.rewardName || hold.note,
    holdId: hold.id,
    actor: 'admin_cancel',
    req,
    returnRow: true,
    idempotencyKey: resolveIdempotencyKey(req, req.body?.idempotency_key)
  });
  recordTelemetry('hold.release', { ok: true, durationMs: Date.now() - started });
  const response = buildActionResponse({
    userId: hold.userId,
    txRow: result.row,
    extras: { ok: true, holdId: id, verb: 'hold.release' }
  });
  res.json(response);
});

function buildHistoryQuery(params) {
  const where = [];
  const sqlParams = [];
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
  const query = buildHistoryQuery({
    userId: req.query.userId,
    type: req.query.type,
    source: req.query.source,
    verb: req.query.verb,
    actor: req.query.actor,
    from,
    to,
    limit: req.query.limit,
    offset: req.query.offset
  });
  const rows = db.prepare(query.sql).all(...query.params).map(mapLedgerRow);
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

app.get("/api/admin/telemetry/core-health", requireAdminKey, (_req, res) => {
  res.json(summarizeTelemetry());
});

app.get("/api/history/user/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 100));
  const rows = db.prepare(`
    SELECT created_at AS at, description AS action, amount AS delta, balance_after, note
    FROM ledger
    WHERE user_id = ?
    ORDER BY created_at DESC, id DESC
    LIMIT ?
  `).all(userId, limit);
  res.json({ rows });
});

app.get("/api/history.csv/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).send("userId required");
  const rows = listLedgerByUserStmt.all(userId).map(mapLedgerRow);
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
