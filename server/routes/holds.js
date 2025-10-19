import { db } from "../db.js";
import { tableExists, hasColumn } from "../lib/dbUtil.js";

export function listHolds(req, res) {
  const familyId =
    (res.locals?.family?.id ? String(res.locals.family.id).trim() : "") ||
    (req.family?.id ? String(req.family.id).trim() : "");

  if (!familyId) {
    return res.status(400).json({ error: "family_scope_required" });
  }

  try {
    const table = ["holds", "reward_holds", "reward_hold", "pending_holds"].find((name) =>
      tableExists(name)
    );

    if (!table) {
      return res.json([]);
    }

    const familyColumn = hasColumn(table, "family_id") ? "family_id" : null;
    const idColumn = hasColumn(table, "id") ? "id" : null;
    const memberColumn = hasColumn(table, "member_id")
      ? "member_id"
      : hasColumn(table, "kid_id")
        ? "kid_id"
        : null;
    const rewardColumn = hasColumn(table, "reward_id") ? "reward_id" : null;
    const statusColumn = hasColumn(table, "status") ? "status" : null;
    const pointsColumn = hasColumn(table, "points")
      ? "points"
      : hasColumn(table, "cost")
        ? "cost"
        : null;
    const createdColumn = hasColumn(table, "created_at") ? "created_at" : null;

    const projections = [
      idColumn ? `"${idColumn}" AS id` : "NULL AS id",
      familyColumn ? `"${familyColumn}" AS family_id` : "? AS family_id",
      memberColumn ? `"${memberColumn}" AS member_id` : "NULL AS member_id",
      rewardColumn ? `"${rewardColumn}" AS reward_id` : "NULL AS reward_id",
      statusColumn ? `"${statusColumn}" AS status` : "'pending' AS status",
      pointsColumn ? `"${pointsColumn}" AS points` : "0 AS points",
      createdColumn
        ? `"${createdColumn}" AS created_at`
        : "strftime('%s','now') AS created_at",
    ];

    const whereClause = familyColumn ? `WHERE "${familyColumn}" = ?` : "";
    const orderColumn = createdColumn || idColumn || "1";
    const sql = `SELECT ${projections.join(", ")} FROM "${table}" ${whereClause} ORDER BY ${orderColumn} DESC LIMIT 200`;
    const database = req.db || db;
    const rows = database.prepare(sql).all(familyId);
    return res.json(rows);
  } catch (error) {
    console.error("[admin.holds] failed to list holds", error?.message || error);
    return res.status(500).json({ error: "server_error", detail: "holds_query_failed" });
  }
}

export default listHolds;
