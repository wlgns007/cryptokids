#!/usr/bin/env node
import crypto from "node:crypto";
import db, { DB_PATH } from "../server/db.js";

const start = Date.now();

if (!process.env.NODE_ENV) {
  process.env.NODE_ENV = "test";
}

function log(message) {
  console.log(`[unified-schema] ${message}`);
}

function normId(value) {
  return String(value ?? "").trim().toLowerCase();
}

function normalizeTimestamp(value, fallback = Date.now()) {
  if (value === null || value === undefined) return fallback;
  const num = Number(value);
  if (Number.isFinite(num)) {
    if (num >= 1e12) return Math.trunc(num);
    if (num >= 1e9) return Math.trunc(num * 1000);
    if (num > 0) return Math.trunc(num);
  }
  const str = String(value || "").trim();
  if (!str) return fallback;
  const parsed = Date.parse(str);
  if (Number.isNaN(parsed)) return fallback;
  return parsed;
}

function quoteId(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

function tableExists(name) {
  return !!db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?").get(name);
}

function foreignKeyColumns(table) {
  const safe = table.replace(/"/g, '"');
  const rows = db.prepare(`PRAGMA foreign_key_list('${safe}')`).all();
  return new Set(rows.map(row => row.from));
}

function backupTable(name, suffix = Date.now()) {
  if (!tableExists(name)) return null;
  const legacy = `${name}_legacy_${suffix}`;
  log(`Backing up ${name} -> ${legacy}`);
  db.exec(`ALTER TABLE ${quoteId(name)} RENAME TO ${quoteId(legacy)}`);
  return legacy;
}

function fillTimestamps(table, columns = ["created_at", "updated_at"]) {
  const now = Date.now();
  for (const col of columns) {
    const sql = `UPDATE ${quoteId(table)} SET ${quoteId(col)} = ? WHERE ${quoteId(col)} IS NULL OR ${quoteId(col)} = '' OR ${quoteId(col)} = 0`;
    db.prepare(sql).run(now);
  }
}

function encodeJson(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    return trimmed;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return null;
  }
}

function ensureRewardTable() {
  let legacyTable = null;
  if (!tableExists("reward")) {
    if (tableExists("rewards")) {
      legacyTable = backupTable("rewards");
    }
  } else {
    const fks = foreignKeyColumns("reward");
    if (fks.size === 0) {
      legacyTable = backupTable("reward");
    }
  }

  if (legacyTable) {
    db.exec(`
      CREATE TABLE reward (
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
  } else if (!tableExists("reward")) {
    db.exec(`
      CREATE TABLE reward (
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
  }

  const rewardCols = tableExists("reward") ? db.prepare("PRAGMA table_info('reward')").all().map(col => col.name) : [];
  if (!rewardCols.length) return;

  if (!rewardCols.includes("cost") && rewardCols.includes("price")) {
    db.exec("ALTER TABLE reward RENAME COLUMN price TO cost");
  }
  if (!rewardCols.includes("status")) {
    db.exec("ALTER TABLE reward ADD COLUMN status TEXT NOT NULL DEFAULT 'active'");
  }
  if (!rewardCols.includes("tags")) {
    db.exec("ALTER TABLE reward ADD COLUMN tags TEXT");
  }
  if (!rewardCols.includes("campaign_id")) {
    db.exec("ALTER TABLE reward ADD COLUMN campaign_id TEXT");
  }
  if (!rewardCols.includes("source")) {
    db.exec("ALTER TABLE reward ADD COLUMN source TEXT");
  }
  if (!rewardCols.includes("created_at")) {
    db.exec("ALTER TABLE reward ADD COLUMN created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
  }
  if (!rewardCols.includes("updated_at")) {
    db.exec("ALTER TABLE reward ADD COLUMN updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
  }

  if (legacyTable) {
    const insert = db.prepare(`
      INSERT INTO reward (id, name, cost, description, image_url, youtube_url, status, tags, campaign_id, source, created_at, updated_at)
      VALUES (@id,@name,@cost,@description,@image_url,@youtube_url,@status,@tags,@campaign_id,@source,@created_at,@updated_at)
    `);
    const rows = db.prepare(`SELECT * FROM ${quoteId(legacyTable)}`).all();
    for (const row of rows) {
      const id = String(row.id ?? row.ID ?? crypto.randomUUID()).trim();
      if (!id) continue;
      const status = Number(row.active ?? 1) === 1 ? "active" : "disabled";
      const created = normalizeTimestamp(row.created_at ?? row.createdAt);
      insert.run({
        id,
        name: row.name || id,
        cost: Number(row.cost ?? row.price ?? 0) || 0,
        description: row.description || "",
        image_url: row.image_url || row.imageUrl || "",
        youtube_url: row.youtube_url || row.youtubeUrl || null,
        status,
        tags: encodeJson(row.tags),
        campaign_id: row.campaign_id || row.campaignId || null,
        source: row.source || null,
        created_at: created,
        updated_at: normalizeTimestamp(row.updated_at ?? row.updatedAt ?? created)
      });
    }
  }

  fillTimestamps("reward");
  db.exec("CREATE INDEX IF NOT EXISTS idx_reward_status ON reward(status)");
}

function ensureHoldTable() {
  let legacyTable = null;
  if (!tableExists("hold")) {
    if (tableExists("holds")) {
      legacyTable = backupTable("holds");
    }
  } else {
    const fks = foreignKeyColumns("hold");
    if (!fks.has("reward_id")) {
      legacyTable = backupTable("hold");
    }
  }

  if (legacyTable) {
    db.exec(`
      CREATE TABLE hold (
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
  } else if (!tableExists("hold")) {
    db.exec(`
      CREATE TABLE hold (
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
  }

  if (legacyTable) {
    const insert = db.prepare(`
      INSERT INTO hold (
        id,user_id,actor_id,reward_id,reward_name,reward_image_url,status,quoted_amount,final_amount,note,metadata,source,tags,campaign_id,created_at,updated_at,released_at,redeemed_at,expires_at
      ) VALUES (@id,@user_id,@actor_id,@reward_id,@reward_name,@reward_image_url,@status,@quoted_amount,@final_amount,@note,@metadata,@source,@tags,@campaign_id,@created_at,@updated_at,@released_at,@redeemed_at,@expires_at)
    `);
    const rows = db.prepare(`SELECT * FROM ${quoteId(legacyTable)}`).all();
    for (const row of rows) {
      const id = String(row.id ?? row.ID ?? "").trim();
      if (!id) continue;
      const userId = normId(row.user_id ?? row.userId ?? row.member_id ?? row.memberId ?? "");
      if (!userId) continue;
      const rawStatus = String(row.status || "pending").trim().toLowerCase();
      const status = rawStatus === "redeemed" ? "redeemed" : rawStatus === "released" || rawStatus === "canceled" ? "released" : "pending";
      const approvedAt = row.approvedAt ?? row.approved_at ?? null;
      const createdAt = normalizeTimestamp(row.created_at ?? row.createdAt);
      const updatedAt = normalizeTimestamp(row.updated_at ?? row.updatedAt ?? approvedAt ?? createdAt);
      insert.run({
        id,
        user_id: userId,
        actor_id: row.actor_id || row.actorId || null,
        reward_id: row.reward_id || row.itemId || null,
        reward_name: row.reward_name || row.itemName || null,
        reward_image_url: row.reward_image_url || row.itemImage || null,
        status,
        quoted_amount: Number(row.quoted_amount ?? row.quotedCost ?? row.points ?? row.cost ?? 0) || 0,
        final_amount:
          row.final_amount !== undefined && row.final_amount !== null
            ? Number(row.final_amount)
            : row.finalCost !== undefined && row.finalCost !== null
            ? Number(row.finalCost)
            : null,
        note: row.note || null,
        metadata: encodeJson(row.metadata),
        source: row.source || null,
        tags: encodeJson(row.tags),
        campaign_id: row.campaign_id || row.campaignId || null,
        created_at: createdAt,
        updated_at: updatedAt,
        released_at: status === "released" ? normalizeTimestamp(approvedAt ?? row.released_at ?? row.releasedAt) : null,
        redeemed_at: status === "redeemed" ? normalizeTimestamp(approvedAt ?? row.redeemed_at ?? row.redeemedAt) : null,
        expires_at: row.expires_at ? normalizeTimestamp(row.expires_at) : null
      });
    }
  } else {
    const holdCols = db.prepare("PRAGMA table_info('hold')").all().map(col => col.name);
    if (!holdCols.includes("actor_id")) db.exec("ALTER TABLE hold ADD COLUMN actor_id TEXT");
    if (!holdCols.includes("reward_name")) db.exec("ALTER TABLE hold ADD COLUMN reward_name TEXT");
    if (!holdCols.includes("reward_image_url")) db.exec("ALTER TABLE hold ADD COLUMN reward_image_url TEXT");
    if (!holdCols.includes("metadata")) db.exec("ALTER TABLE hold ADD COLUMN metadata TEXT");
    if (!holdCols.includes("source")) db.exec("ALTER TABLE hold ADD COLUMN source TEXT");
    if (!holdCols.includes("tags")) db.exec("ALTER TABLE hold ADD COLUMN tags TEXT");
    if (!holdCols.includes("campaign_id")) db.exec("ALTER TABLE hold ADD COLUMN campaign_id TEXT");
    if (!holdCols.includes("released_at")) db.exec("ALTER TABLE hold ADD COLUMN released_at INTEGER");
    if (!holdCols.includes("redeemed_at")) db.exec("ALTER TABLE hold ADD COLUMN redeemed_at INTEGER");
    if (!holdCols.includes("expires_at")) db.exec("ALTER TABLE hold ADD COLUMN expires_at INTEGER");
    if (!holdCols.includes("created_at")) db.exec("ALTER TABLE hold ADD COLUMN created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
    if (!holdCols.includes("updated_at")) db.exec("ALTER TABLE hold ADD COLUMN updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
  }

  fillTimestamps("hold");
  db.exec("CREATE INDEX IF NOT EXISTS idx_hold_user_status ON hold(user_id, status)");
}

function ensureLedgerTable() {
  let legacyTable = null;
  if (!tableExists("ledger")) {
    legacyTable = backupTable("ledger_tx") || backupTable("ledger_entries") || null;
  } else {
    const fks = foreignKeyColumns("ledger");
    const required = ["user_id", "reward_id", "parent_hold_id", "parent_ledger_id"];
    const missing = required.some(col => !fks.has(col));
    if (missing) {
      legacyTable = backupTable("ledger");
    }
  }

  if (legacyTable) {
    db.exec(`
      CREATE TABLE ledger (
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
    const insert = db.prepare(`
      INSERT INTO ledger (
        id,user_id,actor_id,reward_id,parent_hold_id,parent_ledger_id,verb,description,amount,balance_after,status,note,notes,template_ids,final_amount,metadata,refund_reason,refund_notes,idempotency_key,source,tags,campaign_id,ip_address,user_agent,created_at,updated_at
      ) VALUES (@id,@user_id,@actor_id,@reward_id,@parent_hold_id,@parent_ledger_id,@verb,@description,@amount,@balance_after,@status,@note,@notes,@template_ids,@final_amount,@metadata,@refund_reason,@refund_notes,@idempotency_key,@source,@tags,@campaign_id,@ip_address,@user_agent,@created_at,@updated_at)
    `);
    const rows = db.prepare(`SELECT * FROM ${quoteId(legacyTable)}`).all();
    for (const row of rows) {
      const id = String(row.id ?? row.ID ?? crypto.randomUUID()).trim();
      const userId = normId(row.user_id ?? row.userId ?? row.member_id ?? row.memberId ?? "");
      if (!id || !userId) continue;
      const amountRaw = row.amount ?? row.delta ?? 0;
      const amount = Math.trunc(Number(amountRaw) || 0);
      const balanceAfter = Number(row.balance_after ?? row.balanceAfter ?? row.balance ?? 0) || 0;
      const templateIds = row.template_ids ?? row.templateIds ?? null;
      const metadata = row.metadata ?? row.meta ?? null;
      const tags = row.tags ?? null;
      const verb = (row.verb || "").toString().trim() || (amount > 0 ? "earn" : amount < 0 ? "redeem" : "adjust");
      const status = (row.status || "posted").toString().trim().toLowerCase() || "posted";
      const createdAt = normalizeTimestamp(row.created_at ?? row.createdAt);
      const updatedAt = normalizeTimestamp(row.updated_at ?? row.updatedAt ?? createdAt);
      insert.run({
        id,
        user_id: userId,
        actor_id: row.actor_id || row.actorId || null,
        reward_id: row.reward_id || row.itemId || null,
        parent_hold_id: row.parent_hold_id || row.holdId || null,
        parent_ledger_id: row.parent_ledger_id || row.parent_tx_id || null,
        verb,
        description: row.description || row.action || null,
        amount,
        balance_after: balanceAfter,
        status,
        note: row.note || null,
        notes: row.notes || null,
        template_ids: templateIds ? encodeJson(templateIds) : null,
        final_amount:
          row.final_amount !== undefined && row.final_amount !== null
            ? Number(row.final_amount)
            : row.finalCost !== undefined && row.finalCost !== null
            ? Number(row.finalCost)
            : null,
        metadata: metadata ? encodeJson(metadata) : null,
        refund_reason: row.refund_reason || null,
        refund_notes: row.refund_notes || null,
        idempotency_key: row.idempotency_key || null,
        source: row.source || null,
        tags: tags ? encodeJson(tags) : null,
        campaign_id: row.campaign_id || row.campaignId || null,
        ip_address: row.ip_address || row.ip || null,
        user_agent: row.user_agent || row.ua || null,
        created_at: createdAt,
        updated_at: updatedAt
      });
    }
  } else {
    const ledgerCols = db.prepare("PRAGMA table_info('ledger')").all().map(col => col.name);
    if (!ledgerCols.includes("user_id")) db.exec("ALTER TABLE ledger ADD COLUMN user_id TEXT");
    if (!ledgerCols.includes("actor_id")) db.exec("ALTER TABLE ledger ADD COLUMN actor_id TEXT");
    if (!ledgerCols.includes("reward_id")) db.exec("ALTER TABLE ledger ADD COLUMN reward_id TEXT");
    if (!ledgerCols.includes("parent_hold_id")) db.exec("ALTER TABLE ledger ADD COLUMN parent_hold_id TEXT");
    if (!ledgerCols.includes("parent_ledger_id")) db.exec("ALTER TABLE ledger ADD COLUMN parent_ledger_id TEXT");
    if (!ledgerCols.includes("description")) db.exec("ALTER TABLE ledger ADD COLUMN description TEXT");
    if (!ledgerCols.includes("amount")) db.exec("ALTER TABLE ledger ADD COLUMN amount INTEGER NOT NULL DEFAULT 0");
    if (!ledgerCols.includes("status")) db.exec("ALTER TABLE ledger ADD COLUMN status TEXT NOT NULL DEFAULT 'posted'");
    if (!ledgerCols.includes("template_ids")) db.exec("ALTER TABLE ledger ADD COLUMN template_ids TEXT");
    if (!ledgerCols.includes("final_amount")) db.exec("ALTER TABLE ledger ADD COLUMN final_amount INTEGER");
    if (!ledgerCols.includes("metadata")) db.exec("ALTER TABLE ledger ADD COLUMN metadata TEXT");
    if (!ledgerCols.includes("source")) db.exec("ALTER TABLE ledger ADD COLUMN source TEXT");
    if (!ledgerCols.includes("tags")) db.exec("ALTER TABLE ledger ADD COLUMN tags TEXT");
    if (!ledgerCols.includes("campaign_id")) db.exec("ALTER TABLE ledger ADD COLUMN campaign_id TEXT");
    if (!ledgerCols.includes("ip_address")) db.exec("ALTER TABLE ledger ADD COLUMN ip_address TEXT");
    if (!ledgerCols.includes("user_agent")) db.exec("ALTER TABLE ledger ADD COLUMN user_agent TEXT");
    if (!ledgerCols.includes("created_at")) db.exec("ALTER TABLE ledger ADD COLUMN created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
    if (!ledgerCols.includes("updated_at")) db.exec("ALTER TABLE ledger ADD COLUMN updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
    const oldCols = new Set(ledgerCols);
    if (!oldCols.has("amount") && oldCols.has("delta")) db.exec("ALTER TABLE ledger RENAME COLUMN delta TO amount");
    if (!oldCols.has("user_id") && oldCols.has("userId")) db.exec("ALTER TABLE ledger RENAME COLUMN userId TO user_id");
    if (!oldCols.has("description") && oldCols.has("action")) db.exec("ALTER TABLE ledger RENAME COLUMN action TO description");
    if (!oldCols.has("reward_id") && oldCols.has("itemId")) db.exec("ALTER TABLE ledger RENAME COLUMN itemId TO reward_id");
    if (!oldCols.has("parent_hold_id") && oldCols.has("holdId")) db.exec("ALTER TABLE ledger RENAME COLUMN holdId TO parent_hold_id");
    if (!oldCols.has("final_amount") && oldCols.has("finalCost")) db.exec("ALTER TABLE ledger RENAME COLUMN finalCost TO final_amount");
    if (!oldCols.has("actor_id") && oldCols.has("actor")) db.exec("ALTER TABLE ledger RENAME COLUMN actor TO actor_id");
    if (!oldCols.has("ip_address") && oldCols.has("ip")) db.exec("ALTER TABLE ledger RENAME COLUMN ip TO ip_address");
    if (!oldCols.has("user_agent") && oldCols.has("ua")) db.exec("ALTER TABLE ledger RENAME COLUMN ua TO user_agent");
    if (!oldCols.has("parent_ledger_id") && oldCols.has("parent_tx_id")) db.exec("ALTER TABLE ledger RENAME COLUMN parent_tx_id TO parent_ledger_id");
  }

  fillTimestamps("ledger");
  db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_idempotency
    ON ledger(idempotency_key)
    WHERE idempotency_key IS NOT NULL
  `);
  db.exec("CREATE INDEX IF NOT EXISTS idx_ledger_user_verb_created_at ON ledger(user_id, verb, created_at, id)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_ledger_parent_hold ON ledger(parent_hold_id)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_ledger_parent_ledger ON ledger(parent_ledger_id)");
}

function ensureSpendRequestTable() {
  let legacyTable = null;
  if (!tableExists("spend_request")) {
    if (tableExists("spend_requests")) {
      legacyTable = backupTable("spend_requests");
    }
  } else {
    const fks = foreignKeyColumns("spend_request");
    if (!fks.has("user_id")) {
      legacyTable = backupTable("spend_request");
    }
  }

  if (legacyTable) {
    db.exec(`
      CREATE TABLE spend_request (
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
    const insert = db.prepare(`
      INSERT INTO spend_request (
        id,token,user_id,reward_id,status,amount,title,image_url,actor_id,source,tags,campaign_id,created_at,updated_at
      ) VALUES (@id,@token,@user_id,@reward_id,@status,@amount,@title,@image_url,@actor_id,@source,@tags,@campaign_id,@created_at,@updated_at)
    `);
    const rows = db.prepare(`SELECT * FROM ${quoteId(legacyTable)}`).all();
    for (const row of rows) {
      const id = String(row.id ?? crypto.randomUUID()).trim();
      const userId = normId(row.user_id ?? row.userId ?? "");
      if (!id || !userId) continue;
      insert.run({
        id,
        token: row.token || crypto.randomUUID(),
        user_id: userId,
        reward_id: row.reward_id || row.itemId || null,
        status: String(row.status || "pending").trim().toLowerCase(),
        amount: row.amount ?? row.price ?? null,
        title: row.title || null,
        image_url: row.image_url || row.imageUrl || null,
        actor_id: row.actor_id || row.actorId || null,
        source: row.source || null,
        tags: encodeJson(row.tags),
        campaign_id: row.campaign_id || row.campaignId || null,
        created_at: normalizeTimestamp(row.created_at ?? row.createdAt),
        updated_at: normalizeTimestamp(row.updated_at ?? row.updatedAt ?? row.created_at ?? row.createdAt)
      });
    }
  } else {
    const cols = db.prepare("PRAGMA table_info('spend_request')").all().map(col => col.name);
    if (!cols.includes("actor_id")) db.exec("ALTER TABLE spend_request ADD COLUMN actor_id TEXT");
    if (!cols.includes("source")) db.exec("ALTER TABLE spend_request ADD COLUMN source TEXT");
    if (!cols.includes("tags")) db.exec("ALTER TABLE spend_request ADD COLUMN tags TEXT");
    if (!cols.includes("campaign_id")) db.exec("ALTER TABLE spend_request ADD COLUMN campaign_id TEXT");
    if (!cols.includes("amount")) db.exec("ALTER TABLE spend_request ADD COLUMN amount INTEGER");
    if (!cols.includes("created_at")) db.exec("ALTER TABLE spend_request ADD COLUMN created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
    if (!cols.includes("updated_at")) db.exec("ALTER TABLE spend_request ADD COLUMN updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
  }

  fillTimestamps("spend_request");
}

function ensureConsumedTokensTable() {
  let legacyTable = null;
  if (!tableExists("consumed_tokens")) {
    if (tableExists("consumed_token")) {
      legacyTable = backupTable("consumed_token");
    }
  } else {
    const cols = db.prepare("PRAGMA table_info('consumed_tokens')").all().map(col => col.name);
    const needsRebuild = !cols.includes("id") || cols.includes("jti") || !cols.includes("created_at");
    if (needsRebuild) {
      legacyTable = backupTable("consumed_tokens");
    }
  }

  if (legacyTable) {
    db.exec(`
      CREATE TABLE consumed_tokens (
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
    const insert = db.prepare(`
      INSERT INTO consumed_tokens (id, token, typ, request_id, user_id, reward_id, source, created_at, updated_at)
      VALUES (@id,@token,@typ,@request_id,@user_id,@reward_id,@source,@created_at,@updated_at)
    `);
    const rows = db.prepare(`SELECT * FROM ${quoteId(legacyTable)}`).all();
    for (const row of rows) {
      const id = String(row.id ?? row.jti ?? row.token ?? crypto.randomUUID()).trim();
      if (!id) continue;
      insert.run({
        id,
        token: row.token || null,
        typ: row.typ || row.type || null,
        request_id: row.request_id || row.requestId || null,
        user_id: normId(row.user_id ?? row.userId ?? "") || null,
        reward_id: row.reward_id || row.itemId || null,
        source: row.source || null,
        created_at: normalizeTimestamp(row.created_at ?? row.consumed_at ?? row.createdAt),
        updated_at: normalizeTimestamp(row.updated_at ?? row.consumed_at ?? row.updatedAt ?? row.created_at ?? row.createdAt)
      });
    }
  } else if (!tableExists("consumed_tokens")) {
    db.exec(`
      CREATE TABLE consumed_tokens (
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
    const cols = db.prepare("PRAGMA table_info('consumed_tokens')").all().map(col => col.name);
    if (!cols.includes("id") && cols.includes("jti")) {
      db.exec("ALTER TABLE consumed_tokens RENAME COLUMN jti TO id");
    }
    if (!cols.includes("created_at") && cols.includes("consumed_at")) {
      db.exec("ALTER TABLE consumed_tokens RENAME COLUMN consumed_at TO created_at");
    }
    if (!cols.includes("token")) db.exec("ALTER TABLE consumed_tokens ADD COLUMN token TEXT");
    if (!cols.includes("typ")) db.exec("ALTER TABLE consumed_tokens ADD COLUMN typ TEXT");
    if (!cols.includes("request_id")) db.exec("ALTER TABLE consumed_tokens ADD COLUMN request_id TEXT");
    if (!cols.includes("user_id")) db.exec("ALTER TABLE consumed_tokens ADD COLUMN user_id TEXT");
    if (!cols.includes("reward_id")) db.exec("ALTER TABLE consumed_tokens ADD COLUMN reward_id TEXT");
    if (!cols.includes("source")) db.exec("ALTER TABLE consumed_tokens ADD COLUMN source TEXT");
    if (!cols.includes("created_at")) db.exec("ALTER TABLE consumed_tokens ADD COLUMN created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
    if (!cols.includes("updated_at")) db.exec("ALTER TABLE consumed_tokens ADD COLUMN updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
  }

  fillTimestamps("consumed_tokens");
  db.exec("CREATE INDEX IF NOT EXISTS idx_consumed_tokens_user ON consumed_tokens(user_id)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_consumed_tokens_reward ON consumed_tokens(reward_id)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_consumed_tokens_request ON consumed_tokens(request_id)");
}

async function main() {
  log(`Migrating database at ${DB_PATH}`);
  const { ensureSchema } = await import("../server/index.js");
  await ensureSchema();

  const migration = db.transaction(() => {
    ensureRewardTable();
    ensureHoldTable();
    ensureLedgerTable();
    ensureSpendRequestTable();
    ensureConsumedTokensTable();
    fillTimestamps("member");
  });

  migration();

  log(`Migration completed in ${Date.now() - start}ms`);
}

main().catch(err => {
  console.error("[unified-schema] migration failed", err);
  process.exit(1);
});
