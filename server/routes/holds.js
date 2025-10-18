import { db } from "../db.js";
import { tableExists, hasColumn } from "../lib/dbUtil.js";
import { requireFamilyScope } from "../middleware/requireFamilyScope.js";

export const listHolds = [
  requireFamilyScope,
  (req, res, next) => {
    try {
      const candidates = ["holds", "reward_holds", "reward_hold", "pending_holds"];
      const table = candidates.find((name) => tableExists(name));
      if (!table) return res.json([]);

      const famCol = hasColumn(table, "family_id") ? "family_id" : null;
      const idCol = hasColumn(table, "id") ? "id" : null;
      const memberCol = hasColumn(table, "member_id")
        ? "member_id"
        : hasColumn(table, "kid_id")
          ? "kid_id"
          : null;
      const rewardCol = hasColumn(table, "reward_id") ? "reward_id" : null;
      const statusCol = hasColumn(table, "status") ? "status" : null;
      const pointsCol = hasColumn(table, "points")
        ? "points"
        : hasColumn(table, "cost")
          ? "cost"
          : null;
      const createdCol = hasColumn(table, "created_at") ? "created_at" : null;

      const selectList = [
        idCol ? `"${idCol}" AS id` : "NULL AS id",
        famCol ? `"${famCol}" AS family_id` : "? AS family_id",
        memberCol ? `"${memberCol}" AS member_id` : "NULL AS member_id",
        rewardCol ? `"${rewardCol}" AS reward_id` : "NULL AS reward_id",
        statusCol ? `"${statusCol}" AS status` : "'pending' AS status",
        pointsCol ? `"${pointsCol}" AS points` : "0 AS points",
        createdCol
          ? `"${createdCol}" AS created_at`
          : "strftime('%s','now') AS created_at"
      ].join(", ");

      const whereClause = famCol ? `WHERE "${famCol}" = ?` : "";
      const orderColumn = createdCol || idCol || "1";
      const orderExpr = orderColumn === "1" ? "1" : `"${orderColumn}" DESC`;
      const sql = `SELECT ${selectList} FROM "${table}" ${whereClause} ORDER BY ${orderExpr} LIMIT 200`;

      const rows = db.prepare(sql).all(req.family.id);
      res.json(rows);
    } catch (error) {
      next(error);
    }
  }
];

export default listHolds;
