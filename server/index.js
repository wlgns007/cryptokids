// index.js — CryptoKids / Parents Shop API (ESM)
// Node 22+
// npm i express better-sqlite3

import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import Database from "better-sqlite3";
import fs from "node:fs";
const DATA_DIR   = process.env.DATA_DIR   || '/data';
const UPLOAD_DIR = process.env.UPLOAD_DIR || `${DATA_DIR}/uploads`;
const DB_PATH    = process.env.DB_PATH    || `${DATA_DIR}/ck.sqlite`;
fs.mkdirSync(UPLOAD_DIR, { recursive: true });
import multer from "multer";

// ===== config =====
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const PORT          = process.env.PORT || 4000;
const DB_FILE       = process.env.DB_FILE || path.join(__dirname, "parentshop.db");
const PARENT_SECRET = (process.env.PARENT_SECRET || "dev-secret-change-me").trim();
const ADMIN_KEY     = (process.env.ADMIN_KEY || "adminkey").trim();
// ADD (dev only): show which key is active (masked)
const mask = ADMIN_KEY.length > 4 ? '*'.repeat(ADMIN_KEY.length - 4) + ADMIN_KEY.slice(-4) : ADMIN_KEY;
console.log(`[ck] ADMIN_KEY active: ${mask}`);

const QR_TTL_SEC    = 5 * 60; // 5 minutes
const BUILD         = "ck3-r1";

// ===== app =====
const app = express();
app.use(express.json({ limit: '2mb' })); // allow image data-url payload

// static: serve /public (admin.html, child.html, admin.js, qrcode libs, etc.)
const PUBLIC_DIR = path.join(__dirname, "public");
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });
app.use(express.static(PUBLIC_DIR, { maxAge: '1h' }));
app.use('/uploads', express.static(UPLOAD_DIR, { maxAge: '1y', fallthrough: true }));

// Image upload (1 MB max, images only) -> files go into /public/uploads
const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, UPLOAD_DIR),
  filename: (_req, file, cb) => {
    const ext = (file.originalname || "").split(".").pop()?.toLowerCase() || "bin";
    const safeExt = ext.replace(/[^a-z0-9]/g, "") || "bin";
    const name = `rw_${Date.now()}_${Math.random().toString(36).slice(2,8)}.${safeExt}`;
    cb(null, name);
  },
});

const uploadAny = multer({
  storage,
  limits: { fileSize: 1 * 1024 * 1024 }, // 1 MB
  fileFilter: (_req, file, cb) => {
    cb(/^image\/(png|jpe?g|webp|gif)$/i.test(file.mimetype) ? null : new Error("Only PNG/JPG/WebP/GIF allowed"));
  },
}).any(); // <-- accept ANY field name

// make req.protocol honor X-Forwarded-Proto (needed for ngrok => https)
app.set('trust proxy', 1);

// normalize all user IDs to lowercase/trimmed
function normId(s) {
  return String(s || '').trim().toLowerCase();
}
// ===== utils =====
const b64url = {
  enc: (bufOrStr) =>
    Buffer.from(bufOrStr)
      .toString("base64")
      .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, ""),
  dec: (s) =>
    Buffer.from(s.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((s.length + 3) % 4), "base64"),
};
function nowSec() { return Math.floor(Date.now() / 1000); }
function sign(payloadStr) {
  return crypto.createHmac("sha256", PARENT_SECRET).update(payloadStr).digest("hex");
}
function parseTokenFromQueryOrParam(req) {
  const raw = (req.query.t ?? req.params.token ?? "").toString();
  return decodeURIComponent(raw).trim();
}
function requireAdminKey(req, res, next) {
  const k = (req.headers["x-admin-key"] || "").toString().trim();
  if (!k || k !== ADMIN_KEY) return res.status(401).json({ error: "UNAUTHORIZED" });
  next();
}

// ===== DB init & helpers =====
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

function ensureSchema() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS balances (
      user_id    TEXT PRIMARY KEY,
      balance    INTEGER NOT NULL,
      updated_at INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS ledger (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id    TEXT NOT NULL,
      delta      INTEGER NOT NULL,              -- +earn / -spend
      reason     TEXT DEFAULT '',
      kind       TEXT NOT NULL DEFAULT 'earn',  -- 'earn' | 'spend'
      nonce      TEXT,
      ts         INTEGER NOT NULL DEFAULT 0,    -- event time (sec)
      meta       TEXT,
      created_at INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS nonces (
      nonce      TEXT PRIMARY KEY,
      used_at    INTEGER NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_nonce
      ON ledger(nonce) WHERE nonce IS NOT NULL;
  `);

  db.prepare(`UPDATE balances SET updated_at = ? WHERE updated_at = 0`).run(nowSec());
  db.prepare(`UPDATE ledger   SET ts         = ? WHERE ts         = 0`).run(nowSec());
  db.prepare(`UPDATE ledger   SET created_at = ? WHERE created_at = 0`).run(nowSec());
}
ensureSchema();
// === Rewards table (Parents Shop) ===
// Schema: rewards(id, name, price, active)

db.exec(`
  CREATE TABLE IF NOT EXISTS rewards (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    name   TEXT NOT NULL,
    price  INTEGER NOT NULL CHECK(price > 0),
    active INTEGER NOT NULL DEFAULT 1
  );
`);

// ADD: one-time migration for image_url on rewards
{
  const cols = db.prepare(`PRAGMA table_info(rewards)`).all();
  if (!cols.some(c => c.name === 'image_url')) {
    db.exec(`ALTER TABLE rewards ADD COLUMN image_url TEXT DEFAULT ''`);
  }
}

// ADD: one-time migration for description column
const cols = db.prepare(`PRAGMA table_info(rewards)`).all();
if (!cols.some(c => c.name === 'description')) {
  db.exec(`ALTER TABLE rewards ADD COLUMN description TEXT DEFAULT ''`);
}

// Prepared statements
const listRewardsStmt = db.prepare(`
  SELECT id, name, price, description, image_url
  FROM rewards
  WHERE active = 1
  ORDER BY price ASC, name ASC
`);
const insertRewardStmt = db.prepare(`
  INSERT INTO rewards (name, price, active, description, image_url)
  VALUES (?, ?, 1, ?, ?)
`);
const deactivateRewardStmt = db.prepare(`
  UPDATE rewards SET active = 0 WHERE id = ?
`);

function listRewards() { return listRewardsStmt.all(); }
function addReward(name, price, description = '', imageUrl = '') {
  insertRewardStmt.run(String(name), Math.floor(price), String(description || ''), String(imageUrl || ''));
}
function deactivateReward(id){ deactivateRewardStmt.run(Number(id)); }


const upsertBalance = db.prepare(`
  INSERT INTO balances (user_id, balance, updated_at)
  VALUES (@user_id, @balance, @updated_at)
  ON CONFLICT(user_id) DO UPDATE SET
    balance    = excluded.balance,
    updated_at = excluded.updated_at
`);
const getBalanceStmt = db.prepare(`SELECT balance FROM balances WHERE user_id = ?`);
const addLedgerStmt  = db.prepare(`
  INSERT INTO ledger (user_id, delta, reason, kind, nonce, ts, meta, created_at)
  VALUES (@user_id, @delta, @reason, @kind, @nonce, @ts, @meta, @created_at)
`);
const hasNonce  = db.prepare(`SELECT 1 FROM nonces WHERE nonce = ?`);
const markNonce = db.prepare(`INSERT INTO nonces (nonce, used_at) VALUES (?, ?)`);

function getBalance(userId) {
  const row = getBalanceStmt.get(userId);
  return row ? row.balance : 0;
}
function setBalance(userId, newBal) {
  upsertBalance.run({ user_id: userId, balance: newBal, updated_at: nowSec() });
}
function addLedger({ userId, delta, reason = "", kind = "earn", nonce = null, meta = null }) {
  const ts = nowSec();
  addLedgerStmt.run({
    user_id: userId,
    delta,
    reason,
    kind,
    nonce,
    ts,
    meta: meta ? JSON.stringify(meta) : null,
    created_at: ts,
  });
}
function applyDelta(userId, delta, reason, kind, nonce, meta = null) {
  return db.transaction(() => {
    if (nonce) {
      if (hasNonce.get(nonce)) throw new Error("NONCE_REPLAY");
      markNonce.run(nonce, nowSec());
    }
    const current = getBalance(userId);
    const next = current + delta;
    if (next < 0) throw new Error("INSUFFICIENT_FUNDS");
    setBalance(userId, next);
    addLedger({ userId, delta, reason, kind, nonce, meta });
    return next;
  })();
}


// ===== routes =====
app.get("/version", (_req, res) => res.json({ build: BUILD }));
app.get("/healthz", (_req, res) => res.json({ ok: true }));
// === Earn templates (admin-configured) ===
// Example static; later wire to your DB/Admin UI
app.get('/api/earn-templates', async (req, res) => {
  // TODO: fetch from DB "earn_templates" table
  const templates = [
    { id: 'tid_homework', label: 'Homework done', amount: 2 },
    { id: 'tid_cleanroom', label: 'Cleaned room', amount: 1 },
    { id: 'tid_reading',  label: 'Reading 20min', amount: 1 },
  ];
  res.json(templates);
});

// === Rewards (shop) listing ===
app.get('/api/rewards', (_req, res) => {
  const items = listRewards().map(r => ({
    id:   r.id,
    title: r.name,
    price: r.price,
    imageUrl: r.image_url || '',
    desc: r.description || ''
  }));
  res.json(items);
});

// Mint signed QR (admin)
app.post('/earn', requireAdminKey, (req, res) => {
  const userId = normId(req.body.userId);
  const amt    = Number(req.body.amount || 0);
  const task   = String(req.body.task || '');
  if (!userId || !Number.isFinite(amt) || amt <= 0) {
    return res.status(400).json({ error: 'bad_request' });
  }
  const payload = {
    v: 1,
    userId: String(userId),
    amount: Math.floor(amt),
    task: (task || "").slice(0, 120),
    exp: nowSec() + QR_TTL_SEC,
    n:   crypto.randomUUID(),
  };
  const payloadStr = JSON.stringify(payload);
  const token = `${b64url.enc(payloadStr)}.${sign(payloadStr)}`;
  const host  = req.headers["x-forwarded-host"] || req.headers.host;
  const proto = req.headers["x-forwarded-proto"] || (req.secure ? "https" : "http");
  const base  = `${proto}://${host}`;
  const url   = `${base}/r/${token}`;

  res.json({ url, expiresAt: payload.exp, payload });
});

// Admin: upload image (returns public URL under /uploads/...)

// ADD: Admin — upload image via JSON data URL (≤ 1 MB)
app.post("/admin/upload-image64", requireAdminKey, express.json({ limit: "10mb" }), (req, res) => {
  const { image64, ext } = req.body || {};
  if (!image64) return res.status(400).json({ error: "Missing image64" });

  // accept either data URL or raw base64
  const m = String(image64).match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,(.*)$/);
  const mime = m ? m[1] : `image/${(ext || "png").toLowerCase()}`;
  const b64  = m ? m[2] : String(image64).replace(/^base64,?/, "");

const ok = String(mime || '').toLowerCase();
if (!ok.startsWith('image/')) {
  return res.status(400).json({ error: "Unsupported image type" });
}

  const buf = Buffer.from(b64, "base64");
  const outExt = ({ "image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif" })[mime] || "png";

  // content-hash filename (write into disk-backed UPLOAD_DIR)
  const digest  = crypto.createHash("sha256").update(buf).digest("hex").slice(0, 16);
  const fname   = `rw_${digest}.${outExt}`;
  const filePath = path.join(UPLOAD_DIR, fname);   // <- use UPLOAD_DIR + fname

  const existed = fs.existsSync(filePath);
  if (!existed) fs.writeFileSync(filePath, buf);

  console.log(`[ck] upload64 ${fname} ${existed ? "(dedup: existed)" : "(new)"}`);
  return res.json({ url: `/uploads/${fname}` });

});

// hard block old uploader to prevent random duplicate files
app.post("/admin/upload-image", requireAdminKey, (_req, res) => {
  return res.status(410).json({ error: "DEPRECATED — use /admin/upload-image64" });
});

// Multer error handler (keeps JSON shape)
app.use((err, _req, res, next) => {
  if (err && (err.name === "MulterError" || (err.message && err.message.startsWith("Only ")))) {
    return res.status(400).json({ error: err.message });
  }
  return next(err);
});


// ADD: apply the spend after explicit approval
app.post("/s/approve", express.urlencoded({ extended: false }), (req, res) => {
  try {
    const token = String((req.body?.token) || "");
    const parts = token.split(".");
    if (parts.length !== 2) throw new Error("BAD_TOKEN");
    const [b64, macRaw] = parts;

    const payloadStr = b64url.dec(b64).toString("utf8");
    const expectedHex = sign(payloadStr);

    const macBuf = Buffer.from(macRaw.trim(), "hex");
    const expBuf = Buffer.from(expectedHex, "hex");
    if (macBuf.length !== expBuf.length) throw new Error("BAD_SIG_LEN");
    if (!crypto.timingSafeEqual(macBuf, expBuf)) throw new Error("BAD_SIG");

    const data = JSON.parse(payloadStr);
    if (data.exp < nowSec()) throw new Error("EXPIRED");
    if (data.kind !== "spend") throw new Error("WRONG_KIND");

    const next = applyDelta(
      String(data.userId),
      -Math.floor(data.price),
      String(data.item || ""),
      "spend",
      String(data.n || ""),
      { via: "shop-qr" }
    );

    res.type("html").send(`
      <!doctype html>
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <body style="font-family:system-ui; padding:24px;">
        <h2>✅ Approved: ${data.item} (−${data.price} RT)</h2>
        <p>For: <b>${data.userId}</b></p>
        <p>New balance: <b>${next}</b> RT</p>
      </body>
    `);
  } catch (e) {
    res.status(400).type("html").send(`
      <!doctype html>
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <body style="font-family:system-ui; padding:24px;">
        <h2>❌ Approval failed</h2>
        <p>${e.message}</p>
      </body>
    `);
  }
});

// Shop: mint a spend token (no admin key; parent approval happens by scanning)
app.post("/shop/mintSpend", (req, res) => {
  const userIdRaw = req.body?.userId;
  const rewardId  = req.body?.rewardId;
  if (!userIdRaw || !rewardId) return res.status(400).json({ error: "Missing userId/rewardId" });

  const userId = normId(userIdRaw);

  const reward = db.prepare(
    `SELECT id, name, price FROM rewards WHERE id = ? AND active = 1`
  ).get(Number(rewardId));
  if (!reward) return res.status(404).json({ error: "REWARD_NOT_FOUND" });

  const current = getBalance(userId);
  if (current < reward.price) {
    return res.status(400).json({ error: "INSUFFICIENT_FUNDS", balance: current, price: reward.price });
  }

  const payload = {
    v: 1,
    kind:  "spend",
    userId,                                   // normalized
    price: Math.floor(reward.price),
    item:  reward.name.slice(0, 120),
    exp:   nowSec() + (5 * 60),
    n:     crypto.randomUUID(),
  };

  const payloadStr = JSON.stringify(payload);
  const token = `${b64url.enc(payloadStr)}.${sign(payloadStr)}`;

  const host  = req.headers["x-forwarded-host"] || req.headers.host;
  const proto = req.headers["x-forwarded-proto"] || (req.secure ? "https" : "http");
  const url   = `${proto}://${host}/s/${token}`;

  res.json({
    url,
    expiresAt: payload.exp,
    payload: { userId: payload.userId, item: reward.name, price: reward.price }
  });
});


// Parent scans this to approve the spend
// REPLACE: confirmation page (no debit yet)
app.get("/s/:token", (req, res) => {
  try {
    const token = String(req.params.token || "");
    const parts = token.split(".");
    if (parts.length !== 2) throw new Error("BAD_TOKEN");
    const [b64, macRaw] = parts;

    const payloadStr = b64url.dec(b64).toString("utf8");
    const expectedHex = sign(payloadStr);

    const macBuf = Buffer.from(macRaw.trim(), "hex");
    const expBuf = Buffer.from(expectedHex, "hex");
    if (macBuf.length !== expBuf.length) throw new Error("BAD_SIG_LEN");
    if (!crypto.timingSafeEqual(macBuf, expBuf)) throw new Error("BAD_SIG");

    const data = JSON.parse(payloadStr);
    if (data.exp < nowSec()) throw new Error("EXPIRED");
    if (data.kind !== "spend") throw new Error("WRONG_KIND");

    // Look up current balance for preview
    const currentBal = getBalance(String(data.userId));
    const newBal = currentBal - Math.floor(data.price);

    res.type("html").send(`
      <!doctype html>
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <body style="font-family:system-ui; padding:24px; line-height:1.5">
        <h2>🛍️ Approve purchase?</h2>
        <p><b>Child:</b> ${data.userId}</p>
        <p><b>Item:</b> ${data.item} &nbsp; <b>Price:</b> ${data.price} RT</p>
        <p><b>Balance now:</b> ${currentBal} RT &nbsp; → &nbsp; <b>After:</b> ${newBal < 0 ? 0 : newBal} RT</p>
        <p id="exp" style="opacity:.8"></p>

        <form method="POST" action="/s/approve" style="margin-top:16px">
          <input type="hidden" name="token" value="${token}">
          <button type="submit" style="padding:10px 14px; font-size:16px">Approve</button>
        </form>

        <script>
          (function(){
            var exp=${Number(data.exp)||0};
            var el=document.getElementById('exp');
            function tick(){
              var now=Math.floor(Date.now()/1000);
              var remain=Math.max(0, exp-now);
              if(!el) return;
              if(remain<=0){ el.textContent='QR expired'; return; }
              var m=Math.floor(remain/60), s=String(remain%60).padStart(2,'0');
              el.textContent='Expires in '+m+':'+s;
              requestAnimationFrame(tick);
            }
            tick();
          })();
        </script>
      </body>
    `);
  } catch (e) {
    res.status(400).type("html").send(`
      <!doctype html>
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <body style="font-family:system-ui; padding:24px;">
        <h2>❌ Spend token invalid</h2>
        <p>${e.message}</p>
      </body>
    `);
  }
});


// Redeem QR (scan opens this)
app.get("/r/:token", (req, res) => {
  try {
    const token = parseTokenFromQueryOrParam(req) || req.params.token;
    const parts = String(token).split(".");
    if (parts.length !== 2) throw new Error("BAD_TOKEN");
    const [b64, macRaw] = parts;

    const payloadStr = b64url.dec(b64).toString("utf8");
    const expectedHex = sign(payloadStr);

    const macBuf = Buffer.from(macRaw.trim(), "hex");
    const expBuf = Buffer.from(expectedHex, "hex");
    if (macBuf.length !== expBuf.length) throw new Error("BAD_SIG_LEN");
    if (!crypto.timingSafeEqual(macBuf, expBuf)) throw new Error("BAD_SIG");

    const data = JSON.parse(payloadStr);
    if (data.exp < nowSec()) throw new Error("EXPIRED");

    const newBal = applyDelta(
      data.userId,
      Math.floor(data.amount),
      data.task,
      "earn",
      data.n,
      { v: data.v }
    );

    res.type("html").send(`
      <!doctype html>
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <body style="font-family:system-ui; padding:24px;">
        <h2>👍 ${data.amount} RT credited to ${data.userId}</h2>
        <p>Task: <b>${(data.task || "No task").replace(/[<>]/g, "")}</b></p>
        <p>New balance: <b>${newBal}</b> RT</p>
      </body>
    `);
  } catch (e) {
    res.status(400).type("html").send(`
      <!doctype html>
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <body style="font-family:system-ui; padding:24px;">
        <h2>❌ Redeem failed</h2>
        <p>${e.message}</p>
      </body>
    `);
  }
});

// Spend (admin)
app.post('/spend', requireAdminKey, (req, res) => {
  try {
    const userId = normId(req.body.userId);
    const amt    = Number(req.body.amount);
    const reason = String(req.body.item || req.body.reason || '');

    if (!userId || !Number.isFinite(amt) || amt <= 0) {
      return res.status(400).json({ error: 'Invalid userId/amount' });
    }

    const next = applyDelta(
      userId,
      -Math.floor(amt),
      reason,
      'spend',
      null,
      { via: 'admin-spend' }
    );

    res.json({ balance: next });
  } catch (e) {
    if (e.message === 'INSUFFICIENT_FUNDS') {
      return res.status(400).json({ error: 'INSUFFICIENT_FUNDS' });
    }
    res.status(400).json({ error: e.message });
  }
});


// Public: check balance by userId (used by admin UI "Check Balance")
app.get("/balance/:userId", (req, res) => {
  try {
    const userId = normId(req.params.userId);
    if (!userId) return res.status(400).json({ error: "Missing userId" });
    const row = getBalanceStmt.get(userId);
    const bal = row ? Number(row.balance || 0) : 0;
    res.json({ balance: bal });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// History for child view
app.get("/history/:userId", (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).json({ error: "userId required" });

  const rows = db.prepare(
    `SELECT kind, delta, reason, created_at
       FROM ledger
      WHERE user_id = ?
      ORDER BY id DESC
      LIMIT 200`
  ).all(userId);

  const history = rows.map(r => ({
    type:   r.kind,
    amount: Math.abs(Number(r.delta || 0)),
    reason: String(r.reason || ""),
    date:   new Date(((r.created_at ?? Math.floor(Date.now()/1000)) * 1000)).toISOString(),
  }));

  res.json(history);
});

// ADD: quick summary — balance, total earned, total spent
app.get('/summary/:userId', (req, res) => {
  const userId = normId(req.params.userId);  // <- normalize
  if (!userId) return res.status(400).json({ error: 'userId required' });

  const bal = getBalance(userId);
  const row = db.prepare(`
    SELECT
      SUM(CASE WHEN kind='earn'  THEN ABS(delta) ELSE 0 END) AS earned,
      SUM(CASE WHEN kind='spend' THEN ABS(delta) ELSE 0 END) AS spent
    FROM ledger
    WHERE user_id = ?
  `).get(userId);

  res.json({
    userId,
    balance: bal,
    earned: Number(row?.earned || 0),
    spent:  Number(row?.spent  || 0)
  });
});


app.get('/history.csv/:userId', (req, res) => {
  const userId = normId(req.params.userId);
  if (!userId) return res.status(400).send('userId required');

  // ledger: (user_id, delta, reason, kind, created_at)
  const rows = db.prepare(
    `SELECT kind, delta, COALESCE(reason,'') AS reason, created_at
       FROM ledger
      WHERE user_id = ?
      ORDER BY id DESC`
  ).all(userId);

  const header = 'type,amount,reason,date\n';
  const csv = rows.map(r => {
    const reason = String(r.reason).replace(/"/g, '""');
    const amount = Math.abs(Number(r.delta || 0));
    const iso    = new Date((r.created_at || Math.floor(Date.now()/1000)) * 1000).toISOString();
    return `"${r.kind}","${amount}","${reason}","${iso}"`;
  }).join('\n');

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="history_${userId}.csv"`);
  res.send(header + csv + (csv ? '\n' : ''));
});

// ADD: direct credit (admin; no QR)
app.post("/admin/credit", requireAdminKey, (req, res) => {
  try {
    const userId = normId(req.body.userId);    // <- normalize
    const amt    = Number(req.body.amount);
    const reason = String(req.body.reason || '');
    if (!userId || !Number.isFinite(amt) || amt <= 0) {
      return res.status(400).json({ error: "Invalid userId/amount" });
    }
    const next = applyDelta(userId, Math.floor(amt), reason, "earn", null, { via: "admin-credit" });
    res.json({ balance: next });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// Public: list rewards
app.get("/rewards", (_req, res) => {
  res.json({ items: listRewards() });
});

// Admin: add a reward and accept optional imageUrl
app.post("/admin/rewards", requireAdminKey, (req, res) => {
  const { name, price, description = "", imageUrl = "" } = req.body || {};
  const p = Number(price);
  if (!name || !Number.isFinite(p) || p <= 0) {
    return res.status(400).json({ error: "Invalid name/price" });
  }
  addReward(name, p, description, imageUrl);
  res.json({ ok: true });
});

// Admin: deactivate a reward
app.post("/admin/rewards/deactivate", requireAdminKey, (req, res) => {
  const { id } = req.body || {};
  if (!id) return res.status(400).json({ error: "Missing id" });
  deactivateReward(id);
  res.json({ ok: true });
});

// ADD: Admin — update a reward (partial updates allowed)
app.post("/admin/rewards/update", requireAdminKey, (req, res) => {
  const { id, name, price, description, imageUrl } = req.body || {};
  const rid = Number(id);
  if (!rid) return res.status(400).json({ error: "Missing id" });

  // Build dynamic SET clause for provided fields
  const sets = [];
  const params = [];
  if (typeof name === 'string')        { sets.push("name = ?");        params.push(name); }
  if (Number.isFinite(Number(price)))  { sets.push("price = ?");       params.push(Math.floor(Number(price))); }
  if (typeof description === 'string') { sets.push("description = ?"); params.push(description); }
  if (typeof imageUrl === 'string')    { sets.push("image_url = ?");   params.push(imageUrl); }

  if (!sets.length) return res.status(400).json({ error: "No fields to update" });

  const sql = `UPDATE rewards SET ${sets.join(", ")} WHERE id = ? AND active = 1`;
  params.push(rid);
  const info = db.prepare(sql).run(...params);
  if (!info.changes) return res.status(404).json({ error: "REWARD_NOT_FOUND" });

  res.json({ ok: true });
});

// serve admin at /
app.get("/", (_req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, "admin.html"));
});

// ===== start =====
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Parents Shop API listening on http://0.0.0.0:${PORT}`);
});

