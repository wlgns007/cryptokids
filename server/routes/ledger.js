// server/routes/ledger.js
import express from "express";
import { earn, redeem, balanceOf } from "../ledger/core.js";

const r = express.Router();

r.post("/ledger/earn", async (req, res, next) => {
  try {
    const { memberId, amount, reason, sourceId } = req.body || {};
    const txId = await earn({ memberId, amount, reason, sourceId });
    res.json({ ok: true, txId });
  } catch (err) {
    next(err);
  }
});

r.post("/ledger/redeem", async (req, res, next) => {
  try {
    const { memberId, amount, rewardId } = req.body || {};
    const txId = await redeem({ memberId, amount, rewardId });
    res.json({ ok: true, txId });
  } catch (err) {
    next(err);
  }
});

r.get("/ledger/balance/:memberId", async (req, res, next) => {
  try {
    const memberId = req.params.memberId;
    const balance = await balanceOf(memberId);
    res.json({ memberId, balance });
  } catch (err) {
    next(err);
  }
});

export default r;
