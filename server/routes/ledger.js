// server/routes/ledger.js
import express from "express";
import { earn, redeem, balanceOf } from "../ledger/core.js";
import { familyAccessChain } from "../routes.js";

const r = express.Router();

r.use(express.json());

function sendLedgerError(res, err, fallback) {
  const status = Number.isInteger(err?.status) ? err.status : 400;
  const rawMessage = typeof err?.message === "string" ? err.message.trim() : "";
  const normalized = rawMessage ? rawMessage.toLowerCase() : "";
  const safeMessage = /^[a-z0-9_]+$/.test(normalized) ? normalized : fallback;
  res.status(status >= 400 ? status : 400).json({ error: safeMessage || fallback });
}

r.post("/ledger/earn", ...familyAccessChain, async (req, res) => {
  try {
    const { memberId, amount, reason, sourceId } = req.body || {};
    const familyId = req.family?.id || null;
    if (!memberId || amount === undefined || amount === null) {
      return res.status(400).json({ error: "invalid_payload" });
    }
    const txId = await earn({ memberId, amount, reason, sourceId, familyId });
    res.json({ ok: true, txId });
  } catch (err) {
    sendLedgerError(res, err, "earn_failed");
  }
});

r.post("/ledger/redeem", ...familyAccessChain, async (req, res) => {
  try {
    const { memberId, amount, rewardId } = req.body || {};
    const familyId = req.family?.id || null;
    if (!memberId || amount === undefined || amount === null) {
      return res.status(400).json({ error: "invalid_payload" });
    }
    const txId = await redeem({ memberId, amount, rewardId, familyId });
    res.json({ ok: true, txId });
  } catch (err) {
    sendLedgerError(res, err, "redeem_failed");
  }
});

r.get("/ledger/balance/:memberId", ...familyAccessChain, async (req, res) => {
  try {
    const memberId = req.params.memberId;
    if (!memberId) {
      return res.status(400).json({ error: "invalid_member" });
    }
    const familyId = req.family?.id || null;
    const balance = await balanceOf(memberId, familyId);
    res.json({ memberId, balance });
  } catch (err) {
    sendLedgerError(res, err, "balance_failed");
  }
});

export default r;
