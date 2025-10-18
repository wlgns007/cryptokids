import { db } from "../db.js";
import { hasColumn, tableColumns } from "../lib/dbUtil.js";
import { requireFamilyScope } from "../middleware/requireFamilyScope.js";

const MEMBER_TABLE_CANDIDATES = ["members", "member"];

function resolveMemberTable() {
  for (const candidate of MEMBER_TABLE_CANDIDATES) {
    const columns = tableColumns(candidate);
    if (columns.length) {
      return { table: candidate, columns };
    }
  }
  return { table: null, columns: [] };
}

function allowFamilyOrMaster(req, familyId) {
  const role = req.admin?.role || req.auth?.role || null;
  if (role === "master") return true;
  if (role === "family") {
    const scopedId =
      req.admin?.family_id ||
      req.admin?.familyId ||
      req.auth?.familyId ||
      req.auth?.family_id ||
      null;
    return scopedId != null && String(scopedId) === String(familyId);
  }
  return false;
}

export const listMembers = [
  requireFamilyScope,
  (req, res, next) => {
    try {
      const familyId = req.family?.id;
      if (!familyId) {
        return res.status(400).json({ error: "Missing family scope (x-family)" });
      }

      if (!allowFamilyOrMaster(req, familyId)) {
        return res.sendStatus(403);
      }

      const { table, columns } = resolveMemberTable();
      if (!table) {
        return res.json([]);
      }

      const hasCol = (column) => columns.includes(column) || hasColumn(table, column);

      // Pick the first name-like column that actually exists
      const nameCols = ["nickname", "display_name", "name", "full_name", "first_name"];
      const present = nameCols.find((column) => hasCol(column));
      const nameExpr = present ? `"${present}"` : "'Member'";

      // Optional avatar column mapping
      const avatarCol = hasCol("avatar_url")
        ? "avatar_url"
        : hasCol("image_url")
          ? "image_url"
          : null;
      const avatarExpr = avatarCol ? `"${avatarCol}"` : "NULL";

      // Status fallback
      const statusCol = hasCol("status") ? "status" : null;
      const statusExpr = statusCol ? `"${statusCol}"` : "'active'";

      // Build SQL dynamically to avoid referencing missing cols
      const sql = `
        SELECT id,
               family_id,
               ${nameExpr}         AS name,
               ${avatarExpr}       AS avatar_url,
               ${statusExpr}       AS status,
               created_at,
               updated_at
        FROM ${table}
        WHERE family_id = ?
        ORDER BY ${present ? `"${present}" COLLATE NOCASE` : "created_at DESC"}
      `;

      const rows = db.prepare(sql).all(familyId);
      res.json(rows);
    } catch (error) {
      next(error);
    }
  },
];

export default listMembers;
