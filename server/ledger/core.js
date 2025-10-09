// server/ledger/core.js — unified ledger helpers
import crypto from "node:crypto";
import db from "../db.js";

const LEDGER_VERBS = new Set(["earn", "redeem", "refund", "adjust"]);

function normalizeId(value) {
  return String(value ?? "").trim().toLowerCase();
}

function nowMs() {
  return Date.now();
}

function encodeJson(value) {
  if (value === undefined || value === null) return null;
  try {
    return JSON.stringify(value);
  } catch {
    return null;
  }
}

function resolveVerb(verb, amount) {
  if (verb && LEDGER_VERBS.has(verb)) return verb;
  if (amount > 0) return "earn";
  if (amount < 0) return "redeem";
  return "adjust";
}

function fetchBalance(userId) {
  const row = db.prepare("SELECT COALESCE(SUM(amount), 0) AS balance FROM ledger WHERE user_id = ?").get(userId);
  return Number(row?.balance ?? 0);
}

export function balanceOf(memberId) {
  const normalized = normalizeId(memberId);
  if (!normalized) return 0;
  return fetchBalance(normalized);
}

export function ensureMemberAccount(memberId) {
  const normalized = normalizeId(memberId);
  if (!normalized) throw new Error("member_required");
  return normalized;
}

function coerceAmount(rawAmount, verb) {
  const value = Number(rawAmount);
  if (!Number.isFinite(value) || !Number.isInteger(value)) {
    throw new Error("amount_must_be_integer");
  }
  if (verb === "redeem") {
    return value <= 0 ? value : -Math.abs(value);
  }
  if (verb === "earn" || verb === "refund") {
    return value >= 0 ? value : Math.abs(value);
  }
  return value;
}

export function recordLedgerEntry({
  userId,
  amount,
  verb,
  description = null,
  rewardId = null,
  parentHoldId = null,
  parentLedgerId = null,
  note = null,
  notes = null,
  templateIds = null,
  finalAmount = null,
  metadata = null,
  actorId = null,
  refundReason = null,
  refundNotes = null,
  idempotencyKey = null,
  status = "posted",
  source = null,
  tags = null,
  campaignId = null,
  ipAddress = null,
  userAgent = null,
  createdAt = null,
  updatedAt = null
} = {}) {
  const normalizedUser = ensureMemberAccount(userId);
  const trimmedActor = actorId ? normalizeId(actorId) : null;
  const trimmedReward = rewardId ? String(rewardId).trim() : null;
  const trimmedHold = parentHoldId ? String(parentHoldId).trim() : null;
  const trimmedParentLedger = parentLedgerId ? String(parentLedgerId).trim() : null;
  const cleanStatus = String(status ?? "posted").trim().toLowerCase() || "posted";
  const resolvedVerb = resolveVerb(verb, Number(amount));
  const signedAmount = coerceAmount(amount, resolvedVerb);

  const ledgerKey = idempotencyKey ? String(idempotencyKey).trim() : null;
  if (ledgerKey) {
    const existing = db.prepare("SELECT * FROM ledger WHERE idempotency_key = ?").get(ledgerKey);
    if (existing) {
      return {
        id: existing.id,
        balance_after: Number(existing.balance_after),
        row: existing
      };
    }
  }

  const balanceBefore = fetchBalance(normalizedUser);
  const balanceAfter = balanceBefore + signedAmount;
  if (balanceAfter < 0) {
    const err = new Error("INSUFFICIENT_FUNDS");
    err.status = 400;
    throw err;
  }

  const id = crypto.randomUUID();
  const ts = nowMs();
  const created = Number.isFinite(createdAt) ? Number(createdAt) : ts;
  const updated = Number.isFinite(updatedAt) ? Number(updatedAt) : created;

  const insert = db.prepare(
    `INSERT INTO ledger (
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
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
  );

  insert.run(
    id,
    normalizedUser,
    trimmedActor,
    trimmedReward,
    trimmedHold,
    trimmedParentLedger,
    resolvedVerb,
    description ? String(description) : null,
    signedAmount,
    balanceAfter,
    cleanStatus,
    note ? String(note) : null,
    notes ? String(notes) : null,
    templateIds ? encodeJson(templateIds) : null,
    finalAmount !== null && finalAmount !== undefined ? Number(finalAmount) : null,
    metadata ? encodeJson(metadata) : null,
    refundReason ? String(refundReason).trim() : null,
    refundNotes ? String(refundNotes).trim() : null,
    ledgerKey,
    source ? String(source).trim() : null,
    tags ? encodeJson(tags) : null,
    campaignId ? String(campaignId).trim() : null,
    ipAddress ? String(ipAddress) : null,
    userAgent ? String(userAgent) : null,
    created,
    updated
  );

  return { id, balance_after: balanceAfter };
}

export function earn({ memberId, amount, reason = null, actorId = null, idempotencyKey = null, source = null, tags = null, campaignId = null, metadata = null, sourceId = null } = {}) {
  const entryMetadata = metadata ?? (sourceId ? { sourceId } : null);
  const result = recordLedgerEntry({
    userId: memberId,
    amount,
    verb: 'earn',
    description: reason || 'earn',
    actorId,
    idempotencyKey,
    source,
    tags,
    campaignId,
    metadata: entryMetadata
  });
  return result.id;
}

export function redeem({ memberId, amount, rewardId = null, actorId = null, holdId = null, idempotencyKey = null, source = null, tags = null, campaignId = null, metadata = null } = {}) {
  const result = recordLedgerEntry({
    userId: memberId,
    amount,
    verb: 'redeem',
    rewardId,
    parentHoldId: holdId,
    actorId,
    description: 'redeem',
    idempotencyKey,
    source,
    tags,
    campaignId,
    metadata
  });
  return result.id;
}
