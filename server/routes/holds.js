import { db } from '../db.js';
import { tableExists, hasColumn } from '../lib/dbUtil.js';
import { requireFamilyScope } from '../middleware/requireFamilyScope.js';

export const listHolds = [
  requireFamilyScope,
  (req, res, next) => {
    try {
      const T = ['holds','reward_holds','reward_hold','pending_holds']
        .find(t => tableExists(t));
      if (!T) return res.json([]);

      const famCol    = hasColumn(T,'family_id') ? 'family_id' : null;
      const idCol     = hasColumn(T,'id') ? 'id' : null;
      const memberCol = hasColumn(T,'member_id') ? 'member_id'
                      : hasColumn(T,'kid_id')    ? 'kid_id' : null;
      const rewardCol = hasColumn(T,'reward_id') ? 'reward_id' : null;
      const statusCol = hasColumn(T,'status') ? 'status' : null;
      const pointsCol = hasColumn(T,'points') ? 'points'
                      : hasColumn(T,'cost')   ? 'cost'   : null;
      const createdCol= hasColumn(T,'created_at') ? 'created_at' : null;

      const sel = [
        idCol     ? `"${idCol}" AS id`             : 'NULL AS id',
        famCol    ? `"${famCol}" AS family_id`     : '? AS family_id',
        memberCol ? `"${memberCol}" AS member_id`  : 'NULL AS member_id',
        rewardCol ? `"${rewardCol}" AS reward_id`  : 'NULL AS reward_id',
        statusCol ? `"${statusCol}" AS status`     : `'pending' AS status`,
        pointsCol ? `"${pointsCol}" AS points`     : '0 AS points',
        createdCol? `"${createdCol}" AS created_at`: `strftime('%s','now') AS created_at`,
      ].join(', ');

      const where = famCol ? `WHERE "${famCol}" = ?` : '';
      const order = createdCol || idCol || '1';
      const sql = `SELECT ${sel} FROM "${T}" ${where} ORDER BY ${order} DESC LIMIT 200`;
      const rows = db.prepare(sql).all(req.family.id);
      res.json(rows);
    } catch (e) { next(e); }
  }
];
