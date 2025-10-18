import { db } from "../db.js";
import { hasColumn, tableExists } from "../lib/dbUtil.js";
import { requireFamilyScope } from "../middleware/requireFamilyScope.js";

export const listMembers = [
  requireFamilyScope,
  (req, res, next) => {
    try {
      if (tableExists("members")) {
        if (!hasColumn("members", "family_id")) {
          return res.json([]);
        }
        const nameCols = ["nickname", "display_name", "name", "full_name", "first_name"];
        const present = nameCols.find((column) => hasColumn("members", column));
        const nameExpr = present ? `"${present}"` : "'Member'";
        const avatarCol = hasColumn("members", "avatar_url")
          ? "avatar_url"
          : hasColumn("members", "image_url")
            ? "image_url"
            : null;
        const avatarExpr = avatarCol ? `"${avatarCol}"` : "NULL";
        const statusExpr = hasColumn("members", "status") ? '"status"' : "'active'";
        const orderExpr = present ? `"${present}" COLLATE NOCASE` : "created_at DESC";

        const sql = `
          SELECT id, family_id,
                 ${nameExpr}   AS name,
                 ${avatarExpr} AS avatar_url,
                 ${statusExpr} AS status,
                 created_at, updated_at
          FROM members
          WHERE family_id = ?
          ORDER BY ${orderExpr}
          LIMIT 500
        `;
        const rows = db.prepare(sql).all(req.family.id);
        return res.json(rows);
      }

      if (tableExists("kids")) {
        return res.json([]);
      }

      return res.json([]);
    } catch (error) {
      next(error);
    }
  }
];

export default listMembers;
