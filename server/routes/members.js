import { db } from "../db.js";
import { hasColumn, tableExists } from "../lib/dbUtil.js";
import { requireFamilyScope } from "../middleware/requireFamilyScope.js";

export const listMembers = [
  requireFamilyScope,
  (req, res, next) => {
    try {
      const table = ["members", "member", "kids", "children"].find((name) => tableExists(name));
      if (!table) {
        res.json([]);
        return;
      }

      const nameCols = ["nickname", "display_name", "name", "full_name", "first_name"];
      const present = nameCols.find((c) => hasColumn(table, c));
      const nameExpr = present ? `"${present}"` : "'Member'";
      const avatarCol = hasColumn(table, "avatar_url")
        ? "avatar_url"
        : hasColumn(table, "image_url")
          ? "image_url"
          : null;
      const avatarExpr = avatarCol ? `"${avatarCol}"` : "NULL";
      const statusExpr = hasColumn(table, "status") ? '"status"' : "'active'";

      const sql = `
        SELECT id, family_id, ${nameExpr} AS name, ${avatarExpr} AS avatar_url,
               ${statusExpr} AS status, created_at, updated_at
        FROM "${table}"
        WHERE family_id = ?
        ORDER BY ${present ? `"${present}" COLLATE NOCASE` : "created_at DESC"}
      `;
      res.json(db.prepare(sql).all(req.family.id));
    } catch (e) {
      next(e);
    }
  }
];

export default listMembers;
