import { db } from "../db.js";
import { hasColumn } from "../lib/dbUtil.js";
import { requireFamilyScope } from "../middleware/requireFamilyScope.js";

export const listMembers = [
  requireFamilyScope,
  (req, res, next) => {
    try {
      const nameCols = ["nickname", "display_name", "name", "full_name", "first_name"];
      const present = nameCols.find((c) => hasColumn("members", c));
      const nameExpr = present ? `"${present}"` : "'Member'";
      const avatarCol = hasColumn("members", "avatar_url")
        ? "avatar_url"
        : hasColumn("members", "image_url")
          ? "image_url"
          : null;
      const avatarExpr = avatarCol ? `"${avatarCol}"` : "NULL";
      const statusExpr = hasColumn("members", "status") ? '"status"' : "'active'";

      const sql = `
        SELECT id, family_id, ${nameExpr} AS name, ${avatarExpr} AS avatar_url,
               ${statusExpr} AS status, created_at, updated_at
        FROM members
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
