// CryptoKids Parents Shop API (refactored)
import express from "express";
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const Database = require("better-sqlite3");

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.resolve(process.cwd(), "data");
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(DATA_DIR, "uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const DB_PATH = process.env.DB_PATH || path.join(DATA_DIR, "cryptokids.db");
const PARENT_SECRET = (process.env.PARENT_SECRET || "dev-secret-change-me").trim();
const ADMIN_KEY = (process.env.ADMIN_KEY || "Mamapapa").trim();

const TOKEN_TTL_SEC = Number(process.env.QR_TTL_SEC || 120);
const PORT = process.env.PORT || 4000;

let BUILD = (process.env.BUILD_VERSION || process.env.RENDER_GIT_COMMIT || process.env.COMMIT_HASH || "").trim();
if (!BUILD) {
  try {
    BUILD = execSync("git rev-parse --short HEAD").toString().trim();
  } catch {
    BUILD = "";
  }
}
if (!BUILD) {
  const pkg = require("../package.json");
  BUILD = pkg.version || "dev";
}

const PUBLIC_DIR = path.join(__dirname, "public");
const versionCache = new Map();
function loadVersioned(file) {
  if (!versionCache.has(file)) {
    const raw = fs.readFileSync(path.join(PUBLIC_DIR, file), "utf8");
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

app.get(["/admin", "/admin.html"], (_req, res) => {
  sendVersioned(res, "admin.html");
});

app.get(["/child", "/child.html"], (_req, res) => {
  sendVersioned(res, "child.html");
});

app.get(["/scan", "/scan.html"], (req, res) => {
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

    const hold = db.prepare("SELECT * FROM holds WHERE id = ?").get(holdId);
    if (!hold) {
      res.status(404).send(renderSpendApprovalPage({ message: "We couldn't find this reward request. Please ask the child to generate a new QR code." }));
      return;
    }

    let balance = null;
    if (hold.userId) {
      const row = selectBalanceStmt.get(hold.userId);
      if (row && row.balance !== undefined && row.balance !== null) {
        const parsed = Number(row.balance);
        if (Number.isFinite(parsed)) balance = parsed;
      }
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
    const result = redeemToken({
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

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

let holdColumnNames = new Set();

function refreshHoldColumnNames() {
  holdColumnNames = new Set(
    db.prepare("PRAGMA table_info('holds')").all().map(col => col.name)
  );
}

function randomId() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return crypto.randomBytes(16).toString("hex");
}

function ensureSchema() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS balances (
      user_id TEXT PRIMARY KEY,
      balance INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);

  const balanceCols = db.prepare(`PRAGMA table_info('balances')`).all();
  const hasUpdatedAt = balanceCols.some(c => c.name === "updated_at");
  if (!hasUpdatedAt) {
    db.exec("ALTER TABLE balances ADD COLUMN updated_at INTEGER DEFAULT 0");
  }
  db.exec(`
    UPDATE balances
    SET updated_at = strftime('%s','now')
    WHERE updated_at IS NULL OR updated_at = 0;
  `);

  const ledgerCols = db.prepare(`PRAGMA table_info(ledger)`).all();
  const requiredLedgerCols = new Set([
    "id", "at", "userId", "action", "delta", "balance_after",
    "itemId", "holdId", "templates", "finalCost", "note",
    "actor", "ip", "ua"
  ]);
  const hasLedger = ledgerCols.length && [...requiredLedgerCols].every(col => ledgerCols.some(c => c.name === col));
  if (!hasLedger) {
    if (ledgerCols.length) {
      const backupName = `ledger_legacy_${Date.now()}`;
      db.exec(`ALTER TABLE ledger RENAME TO ${backupName}`);
    }
    db.exec(`
      CREATE TABLE IF NOT EXISTS ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        at INTEGER NOT NULL,
        userId TEXT NOT NULL,
        action TEXT NOT NULL,
        delta INTEGER NOT NULL,
        balance_after INTEGER NOT NULL,
        itemId TEXT,
        holdId TEXT,
        templates TEXT,
        finalCost INTEGER,
        note TEXT,
        actor TEXT,
        ip TEXT,
        ua TEXT
      );
    `);
  }

  const rewardsCols = db.prepare(`PRAGMA table_info('rewards')`).all();
  if (!rewardsCols.length) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS rewards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        price INTEGER NOT NULL,
        description TEXT DEFAULT '',
        image_url TEXT DEFAULT '',
        active INTEGER NOT NULL DEFAULT 1,
        created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
      );
    `);
  } else {
    const hasCreatedAt = rewardsCols.some(c => c.name === "created_at");
    const hasCreatedAtCamel = rewardsCols.some(c => c.name === "createdAt");
    if (!hasCreatedAt) {
      db.exec(`ALTER TABLE rewards ADD COLUMN created_at INTEGER DEFAULT 0`);
    }

    const rowsNeedingBackfill = db
      .prepare("SELECT COUNT(*) as cnt FROM rewards WHERE created_at IS NULL OR created_at = 0")
      .get().cnt;
    if (rowsNeedingBackfill) {
      if (hasCreatedAtCamel) {
        db.exec(`
          UPDATE rewards
          SET created_at = CASE
            WHEN created_at IS NOT NULL AND created_at != 0 THEN created_at
            WHEN createdAt IS NOT NULL THEN createdAt
            ELSE strftime('%s','now')
          END
          WHERE created_at IS NULL OR created_at = 0;
        `);
      } else {
        db.exec(`
          UPDATE rewards
          SET created_at = CASE
            WHEN created_at IS NOT NULL AND created_at != 0 THEN created_at
            ELSE strftime('%s','now')
          END
          WHERE created_at IS NULL OR created_at = 0;
        `);
      }
    }
  }

  db.exec(`
    CREATE TABLE IF NOT EXISTS earn_templates (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      points INTEGER NOT NULL,
      description TEXT DEFAULT '',
      youtube_url TEXT,
      active INTEGER NOT NULL DEFAULT 1,
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
      updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
    );
  `);

  const holdCols = db.prepare(`PRAGMA table_info(holds)`).all();
  if (!holdCols.length) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS holds (
        id TEXT PRIMARY KEY,
        userId TEXT NOT NULL,
        status TEXT NOT NULL,
        itemId TEXT,
        itemName TEXT,
        itemImage TEXT,
        quotedCost INTEGER NOT NULL,
        finalCost INTEGER,
        note TEXT,
        createdAt INTEGER NOT NULL,
        approvedAt INTEGER
      );
    `);
  } else {
    const ensure = (name, sql) => {
      if (!holdCols.some(c => c.name === name)) {
        db.exec(sql);
        return true;
      }
      return false;
    };
    ensure("itemImage", "ALTER TABLE holds ADD COLUMN itemImage TEXT");
    const addedQuoted = ensure("quotedCost", "ALTER TABLE holds ADD COLUMN quotedCost INTEGER NOT NULL DEFAULT 0");
    ensure("finalCost", "ALTER TABLE holds ADD COLUMN finalCost INTEGER");
    ensure("note", "ALTER TABLE holds ADD COLUMN note TEXT");
    ensure("approvedAt", "ALTER TABLE holds ADD COLUMN approvedAt INTEGER");
    if (!holdCols.some(c => c.name === "status")) {
      db.exec("ALTER TABLE holds ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'");
    }
    if (!holdCols.some(c => c.name === "createdAt")) {
      db.exec("ALTER TABLE holds ADD COLUMN createdAt INTEGER NOT NULL DEFAULT (strftime('%s','now') * 1000)");
    }
    if (addedQuoted && holdCols.some(c => c.name === 'points')) {
      db.exec("UPDATE holds SET quotedCost = points WHERE quotedCost = 0");
    }
  }

  refreshHoldColumnNames();

  db.exec(`
    CREATE TABLE IF NOT EXISTS members (
      userId TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      dob TEXT,
      sex TEXT,
      createdAt INTEGER NOT NULL DEFAULT (strftime('%s','now') * 1000),
      updatedAt INTEGER NOT NULL DEFAULT (strftime('%s','now') * 1000)
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS consumed_tokens (
      jti TEXT PRIMARY KEY,
      typ TEXT NOT NULL,
      consumed_at INTEGER NOT NULL
    );
  `);
}

ensureSchema();

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

const selectBalanceStmt = db.prepare("SELECT balance FROM balances WHERE user_id = ?");
const upsertBalanceStmt = db.prepare(`
  INSERT INTO balances (user_id, balance, updated_at)
  VALUES (@user_id, @balance, @updated_at)
  ON CONFLICT(user_id) DO UPDATE SET balance = excluded.balance, updated_at = excluded.updated_at
`);

const selectMemberStmt = db.prepare(`
  SELECT userId, name, dob, sex, createdAt, updatedAt
  FROM members
  WHERE userId = ?
`);

const listMembersStmt = db.prepare(`
  SELECT userId, name, dob, sex, createdAt, updatedAt
  FROM members
  ORDER BY userId ASC
  LIMIT 200
`);

const searchMembersStmt = db.prepare(`
  SELECT userId, name, dob, sex, createdAt, updatedAt
  FROM members
  WHERE userId LIKE @like OR LOWER(name) LIKE @like
  ORDER BY userId ASC
  LIMIT 200
`);

const insertMemberStmt = db.prepare(`
  INSERT INTO members (userId, name, dob, sex, createdAt, updatedAt)
  VALUES (@userId, @name, @dob, @sex, @createdAt, @updatedAt)
`);

const selectMemberExistsStmt = db.prepare(`
  SELECT 1
  FROM members
  WHERE userId = ?
`);

const updateMemberStmt = db.prepare(`
  UPDATE members
  SET name = @name,
      dob = @dob,
      sex = @sex,
      updatedAt = @updatedAt
  WHERE userId = @userId
`);

const deleteMemberStmt = db.prepare(`
  DELETE FROM members
  WHERE userId = ?
`);

function ensureDefaultMembers() {
  const defaults = [
    { userId: "leo", name: "Leo", dob: null, sex: null }
  ];

  const insertMissing = db.transaction(members => {
    for (const member of members) {
      if (!selectMemberExistsStmt.get(member.userId)) {
        const ts = Date.now();
        insertMemberStmt.run({
          userId: member.userId,
          name: member.name,
          dob: member.dob,
          sex: member.sex,
          createdAt: ts,
          updatedAt: ts
        });
      }
    }
  });

  insertMissing(defaults);
}

ensureDefaultMembers();

function getBalance(userId) {
  const row = selectBalanceStmt.get(userId);
  return row ? Number(row.balance || 0) : 0;
}

function setBalance(userId, balance) {
  upsertBalanceStmt.run({ user_id: userId, balance, updated_at: nowSec() });
}

function mapMember(row) {
  if (!row) return null;
  return {
    userId: row.userId,
    name: row.name,
    dob: row.dob || null,
    sex: row.sex || null,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt
  };
}

function getMember(userId) {
  const row = selectMemberStmt.get(userId);
  return mapMember(row);
}

function listMembers(search) {
  if (search) {
    const like = `%${search}%`;
    return searchMembersStmt.all({ like }).map(mapMember);
  }
  return listMembersStmt.all().map(mapMember);
}

const insertLedgerStmt = db.prepare(`
  INSERT INTO ledger (at, userId, action, delta, balance_after, itemId, holdId, templates, finalCost, note, actor, ip, ua)
  VALUES (@at, @userId, @action, @delta, @balance_after, @itemId, @holdId, @templates, @finalCost, @note, @actor, @ip, @ua)
`);
const checkTokenStmt = db.prepare("SELECT 1 FROM consumed_tokens WHERE jti = ?");
const consumeTokenStmt = db.prepare("INSERT INTO consumed_tokens (jti, typ, consumed_at) VALUES (?, ?, ?)");

function applyLedger({ userId, delta, action, note = null, itemId = null, holdId = null, templates = null, finalCost = null, actor = null, req = null, tokenInfo = null }) {
  const ip = req?.ip || null;
  const ua = req?.headers?.["user-agent"] || null;
  const at = Date.now();
  const templatesJson = templates ? JSON.stringify(templates) : null;

  return db.transaction(() => {
    if (tokenInfo?.jti) {
      if (checkTokenStmt.get(tokenInfo.jti)) {
        throw new Error("TOKEN_USED");
      }
    }
    const current = getBalance(userId);
    const next = current + delta;
    if (next < 0) {
      throw new Error("INSUFFICIENT_FUNDS");
    }
    setBalance(userId, next);
    insertLedgerStmt.run({
      at,
      userId,
      action,
      delta,
      balance_after: next,
      itemId,
      holdId,
      templates: templatesJson,
      finalCost: finalCost ?? null,
      note,
      actor,
      ip,
      ua
    });
    if (tokenInfo?.jti) {
      consumeTokenStmt.run(tokenInfo.jti, tokenInfo.typ, at);
    }
    return next;
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
    const next = applyLedger({
      userId,
      delta: total,
      action: "earn_qr",
      note: data.note || null,
      templates: normalized,
      actor: resolvedActor || null,
      req,
      tokenInfo: { jti: payload.jti, typ: payload.typ }
    });
    return {
      ok: true,
      userId,
      amount: total,
      balance: next,
      action: "earn_qr",
      note: data.note || null,
      templates: normalized,
      tokenType: payload.typ
    };
  }

  if (payload.typ === "give") {
    const data = payload.data || {};
    const userId = normId(data.userId);
    const amount = Math.floor(Number(data.amount || 0));
    if (!userId || amount <= 0) {
      throw createHttpError(400, "invalid_payload");
    }
    const next = applyLedger({
      userId,
      delta: amount,
      action: "earn_admin_give",
      note: data.note || null,
      actor: resolvedActor || null,
      req,
      tokenInfo: { jti: payload.jti, typ: payload.typ }
    });
    return {
      ok: true,
      userId,
      amount,
      balance: next,
      action: "earn_admin_give",
      note: data.note || null,
      templates: null,
      tokenType: payload.typ
    };
  }

  throw createHttpError(400, "unsupported_token");
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
  } else if (normalizedStatus === "canceled") {
    statusLabel = "Canceled";
    statusDescription = message || "This reward request was canceled. Ask the child to generate a new QR code if needed.";
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
      SUM(CASE WHEN action LIKE 'earn_%' THEN delta ELSE 0 END) AS earned,
      SUM(CASE WHEN action LIKE 'spend_%' THEN ABS(delta) ELSE 0 END) AS spent
    FROM ledger
    WHERE userId = ?
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

app.get("/api/rewards", (_req, res) => {
  const rows = db.prepare("SELECT id, name, price, description, image_url, active FROM rewards WHERE active = 1 ORDER BY price ASC, name ASC").all();
  res.json(rows.map(r => ({
    id: r.id,
    name: r.name,
    title: r.name,
    cost: r.price,
    price: r.price,
    description: r.description || "",
    image_url: r.image_url || "",
    imageUrl: r.image_url || "",
    active: r.active
  })));
});

app.post("/api/rewards", requireAdminKey, express.json(), (req, res) => {
  try {
    const { name, cost, imageUrl = null, description = "" } = req.body || {};
    if (!name || Number.isNaN(Number(cost))) return res.status(400).json({ error: "name and cost required" });
    const stmt = db.prepare("INSERT INTO rewards (name, price, image_url, description, active, created_at) VALUES (?, ?, ?, ?, 1, ?)");
    const info = stmt.run(String(name), Math.floor(Number(cost)), imageUrl ? String(imageUrl) : null, String(description || ""), nowSec());
    const row = db.prepare("SELECT id, name, price AS cost, image_url, description, active FROM rewards WHERE id = ?").get(info.lastInsertRowid);
    if (!row) return res.status(500).json({ error: "create reward failed" });
    res.status(201).json({
      ...row,
      price: row.cost,
      imageUrl: row.image_url
    });
  } catch (e) {
    console.error("create reward", e);
    res.status(500).json({ error: "create reward failed" });
  }
});

app.patch("/api/rewards/:id", requireAdminKey, (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid_id" });
  const { name, price, description, imageUrl, active } = req.body || {};
  const fields = [];
  const params = [];
  if (name !== undefined) { fields.push("name = ?"); params.push(name); }
  if (price !== undefined) {
    if (!Number.isFinite(Number(price))) return res.status(400).json({ error: "invalid_price" });
    fields.push("price = ?"); params.push(Math.floor(Number(price)));
  }
  if (description !== undefined) { fields.push("description = ?"); params.push(description); }
  if (imageUrl !== undefined) { fields.push("image_url = ?"); params.push(imageUrl); }
  if (active !== undefined) { fields.push("active = ?"); params.push(active ? 1 : 0); }
  if (!fields.length) return res.status(400).json({ error: "no_fields" });
  const sql = `UPDATE rewards SET ${fields.join(", ")} WHERE id = ?`;
  params.push(id);
  const info = db.prepare(sql).run(...params);
  if (!info.changes) return res.status(404).json({ error: "not_found" });
  res.json({ ok: true });
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
  try {
    const isAdmin = (req.headers["x-admin-key"] || "").toString().trim() === ADMIN_KEY;
    const result = redeemToken({
      token: (req.body?.token || "").toString(),
      req,
      actor: isAdmin ? "admin_scan" : "child_scan",
      isAdmin,
      allowEarnWithoutAdmin: isAdmin
    });
    res.json({
      ok: true,
      userId: result.userId,
      amount: result.amount,
      balance: result.balance,
      action: result.action,
      note: result.note ?? undefined,
      templates: result.templates ?? undefined
    });
  } catch (err) {
    const message = err?.message || "scan_failed";
    const status = err?.status || (message === "TOKEN_USED" ? 409 : message === "ADMIN_REQUIRED" ? 403 : 400);
    res.status(status).json({ error: message });
  }
});

app.post("/api/earn/quick", requireAdminKey, (req, res) => {
  const userId = normId(req.body?.userId);
  const templateId = Number(req.body?.templateId);
  if (!userId || !templateId) return res.status(400).json({ error: "invalid_payload" });
  const tpl = db.prepare("SELECT * FROM earn_templates WHERE id = ?").get(templateId);
  if (!tpl) return res.status(404).json({ error: "template_not_found" });
  const next = applyLedger({
    userId,
    delta: tpl.points,
    action: "earn_admin_quick",
    note: tpl.title,
    templates: [{ id: tpl.id, title: tpl.title, points: tpl.points, count: 1 }],
    actor: "admin_quick",
    req
  });
  res.json({ ok: true, userId, amount: tpl.points, balance: next });
});

app.post("/api/holds", express.json(), (req, res) => {
  try {
    const userId = normId(req.body?.userId);
    const itemId = Number(req.body?.itemId);
    if (!userId || !itemId) return res.status(400).json({ error: "userId and itemId required" });

    const reward = db.prepare("SELECT id, name, price, image_url FROM rewards WHERE id = ? AND active = 1").get(itemId);
    if (!reward) return res.status(404).json({ error: "reward not found" });

    const id = randomId();
    const createdAt = Date.now();
    if (!holdColumnNames.size) {
      refreshHoldColumnNames();
    }

    const columns = [];
    const values = [];

    const pushIf = (name, value) => {
      if (holdColumnNames.has(name)) {
        columns.push(name);
        values.push(value);
      }
    };

    pushIf("id", id);
    pushIf("userId", userId);
    pushIf("status", "pending");
    pushIf("itemId", String(reward.id));
    pushIf("itemName", reward.name);
    pushIf("itemImage", reward.image_url || "");
    pushIf("quotedCost", reward.price);
    pushIf("points", reward.price);
    pushIf("createdAt", createdAt);

    if (!columns.length) {
      throw new Error("holds_table_invalid");
    }

    const placeholders = columns.map(() => "?").join(", ");
    const sql = `INSERT INTO holds (${columns.join(", ")}) VALUES (${placeholders})`;
    db.prepare(sql).run(...values);

    applyLedger({
      userId,
      delta: 0,
      action: "spend_hold",
      note: reward.name,
      holdId: id,
      itemId: String(reward.id),
      templates: null,
      actor: "child",
      req
    });

    const { token } = createToken("spend", { holdId: id, cost: reward.price });
    const qrText = buildQrUrl(req, token);
    res.status(201).json({ holdId: id, token, qrText });
  } catch (e) {
    console.error("create hold", e);
    res.status(500).json({ error: "create hold failed" });
  }
});

app.get("/api/holds", requireAdminKey, (req, res) => {
  const status = (req.query?.status || "pending").toString();
  const allowed = ["pending", "redeemed", "canceled", "all"];
  if (!allowed.includes(status)) {
    return res.status(400).json({ error: "invalid_status" });
  }
  let sql = "SELECT * FROM holds";
  const params = [];
  if (status !== "all") {
    sql += " WHERE status = ?";
    params.push(status);
  }
  sql += " ORDER BY createdAt DESC";
  const rows = db.prepare(sql).all(...params);
  res.json(rows);
});

app.post("/api/holds/:id/approve", requireAdminKey, (req, res) => {
  try {
    const id = String(req.params.id || "");
    const token = String(req.body?.token || "");
    const override = req.body?.finalCost;
    if (!id || !token) return res.status(400).json({ error: "invalid_payload" });
    const payload = verifyToken(token);
    if (payload.typ !== "spend") {
      return res.status(400).json({ error: "unsupported_token" });
    }
    if (payload.data?.holdId !== id) {
      return res.status(400).json({ error: "hold_mismatch" });
    }
    if (checkTokenStmt.get(payload.jti)) {
      return res.status(409).json({ error: "TOKEN_USED" });
    }
    const hold = db.prepare("SELECT * FROM holds WHERE id = ?").get(id);
    if (!hold || hold.status !== "pending") {
      return res.status(404).json({ error: "hold_not_pending" });
    }
    const cost = override !== undefined && override !== null ? Math.max(0, Math.floor(Number(override))) : Number(hold.quotedCost || 0);
    const next = applyLedger({
      userId: hold.userId,
      delta: -cost,
      action: "spend_redeemed",
      note: hold.itemName,
      itemId: hold.itemId,
      holdId: hold.id,
      finalCost: cost,
      actor: "admin_redeem",
      req,
      tokenInfo: { jti: payload.jti, typ: payload.typ }
    });
    db.prepare("UPDATE holds SET status = 'redeemed', finalCost = ?, approvedAt = ?, note = ?, quotedCost = quotedCost WHERE id = ?")
      .run(cost, Date.now(), hold.note || null, hold.id);
    res.json({ ok: true, balance: next, finalCost: cost });
  } catch (err) {
    const code = err.message === "TOKEN_USED" ? 409 : 400;
    res.status(code).json({ error: err.message || "approve_failed" });
  }
});

app.post("/api/holds/:id/cancel", requireAdminKey, (req, res) => {
  const id = String(req.params.id || "");
  const hold = db.prepare("SELECT * FROM holds WHERE id = ?").get(id);
  if (!hold || hold.status !== "pending") {
    return res.status(404).json({ error: "hold_not_pending" });
  }
  db.prepare("UPDATE holds SET status = 'canceled', finalCost = 0, approvedAt = ? WHERE id = ?").run(Date.now(), id);
  applyLedger({
    userId: hold.userId,
    delta: 0,
    action: "spend_canceled",
    note: hold.itemName,
    holdId: hold.id,
    actor: "admin_cancel",
    req
  });
  res.json({ ok: true });
});

function buildHistoryQuery(params) {
  const where = [];
  const sqlParams = [];
  if (params.userId) {
    where.push("userId = ?");
    sqlParams.push(normId(params.userId));
  }
  if (params.type === "earn") {
    where.push("action LIKE 'earn_%'");
  } else if (params.type === "spend") {
    where.push("action LIKE 'spend_%'");
  }
  if (params.source === "task") {
    where.push("action = 'earn_qr'");
  } else if (params.source === "admin") {
    where.push("action IN ('earn_admin_give','earn_admin_quick')");
  }
  if (params.from) {
    where.push("at >= ?");
    sqlParams.push(params.from);
  }
  if (params.to) {
    where.push("at <= ?");
    sqlParams.push(params.to);
  }
  const limit = Math.min(500, Math.max(1, Number(params.limit) || 50));
  const offset = Math.max(0, Number(params.offset) || 0);
  let sql = "SELECT * FROM ledger";
  if (where.length) sql += ` WHERE ${where.join(" AND ")}`;
  sql += " ORDER BY at DESC, id DESC";
  sql += " LIMIT ? OFFSET ?";
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
    from,
    to,
    limit: req.query.limit,
    offset: req.query.offset
  });
  const rows = db.prepare(query.sql).all(...query.params).map(row => ({
    ...row,
    templates: row.templates ? JSON.parse(row.templates) : null
  }));
  if (req.query.format === "csv") {
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", "attachment; filename=history.csv");
    const header = "at,userId,action,delta,balance_after,itemId,holdId,finalCost,note,actor\n";
    const body = rows.map(r => [r.at, r.userId, r.action, r.delta, r.balance_after, r.itemId ?? "", r.holdId ?? "", r.finalCost ?? "", (r.note ?? "").replace(/"/g, ""), r.actor ?? ""].join(",")).join("\n");
    res.send(header + body);
    return;
  }
  res.json({ rows, limit: query.limit, offset: query.offset });
});

app.get("/api/history/user/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });
  const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 100));
  const rows = db.prepare(`
    SELECT at, action, delta, balance_after, note
    FROM ledger
    WHERE userId = ?
    ORDER BY at DESC, id DESC
    LIMIT ?
  `).all(userId, limit);
  res.json({ rows });
});

app.get("/api/history.csv/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).send("userId required");
  const rows = db.prepare(`
    SELECT at, action, delta, balance_after, note
    FROM ledger
    WHERE userId = ?
    ORDER BY at DESC, id DESC
  `).all(userId);
  const header = "at,action,delta,balance_after,note\n";
  const csv = rows.map(r => {
    const note = String(r.note ?? '').replace(/"/g, '""');
    return `"${r.at}","${r.action}","${r.delta}","${r.balance_after}","${note}"`;
  }).join("\n");
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename=history_${userId}.csv`);
  res.send(header + csv + (csv ? "\n" : ""));
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
  const filePath = path.join(UPLOAD_DIR, filename);
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

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Parents Shop API listening on http://0.0.0.0:${PORT}`);
});
