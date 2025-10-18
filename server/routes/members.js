import { db } from '../db.js';
import { tableExists, hasColumn } from '../lib/dbUtil.js';
import { requireFamilyScope } from '../middleware/requireFamilyScope.js';

export const listMembers = [
  requireFamilyScope,
  (req, res, next) => {
    try {
      if (tableExists('members')) {
        const nameCol = ['nickname','display_name','name','full_name','first_name']
          .find(c => hasColumn('members', c));
        const avatarCol = hasColumn('members','avatar_url') ? 'avatar_url'
                        : hasColumn('members','image_url')  ? 'image_url' : null;
        const statusCol = hasColumn('members','status') ? 'status' : null;

        const sql = `
          SELECT id, family_id,
                 ${nameCol ? `"${nameCol}"` : `'Member'`}       AS name,
                 ${avatarCol ? `"${avatarCol}"` : 'NULL'}       AS avatar_url,
                 ${statusCol ? `"${statusCol}"` : `'active'`}   AS status,
                 created_at, updated_at
          FROM members
          WHERE family_id = ?
          ORDER BY ${nameCol ? `"${nameCol}" COLLATE NOCASE` : 'created_at DESC'}
          LIMIT 500
        `;
        return res.json(db.prepare(sql).all(req.family.id));
      }

      // No members table in this DB → don’t error
      return res.json([]);
    } catch (e) { next(e); }
  }
];
