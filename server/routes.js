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
    const row = db.prepare(`SELECT id, name, email, status, created_at, updated_at FROM "family" WHERE id = ?`).get(id);
    if (!row) return res.status(404).json({ error: "not found" });
    return res.json(row);
  }
  const rows = db.prepare(`SELECT id, name, email, status, created_at, updated_at FROM "family" ORDER BY created_at DESC`).all();
  res.json(rows);
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
