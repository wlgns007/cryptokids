import { db } from "../db.js";
import { tableExists, hasColumn } from "../lib/dbUtil.js";

function coalesceFamilyValue(row, preferred) {
  for (const key of preferred) {
    if (key && Object.prototype.hasOwnProperty.call(row, key)) {
      const value = row[key];
      if (value !== undefined) {
        return value ?? null;
      }
    }
  }
  return null;
}

function loadFamilyRow(familyId) {
  if (!tableExists("families")) return null;
  if (!hasColumn("families", "id")) return null;

  return db.prepare('SELECT * FROM "families" WHERE "id" = ? LIMIT 1').get(familyId) || null;
}

export function familyForCurrentAdmin(req, res, next) {
  try {
    const scopedFamily = req.family || null;
    const key = req.cookies?.ck_admin_key || req.header("x-admin-key") || null;

    if (key && process.env.MASTER_ADMIN_KEY && key === process.env.MASTER_ADMIN_KEY && scopedFamily?.id) {
      return res.json({
        id: scopedFamily.id,
        key: scopedFamily.key ?? scopedFamily.family_key ?? scopedFamily.admin_key ?? null,
        name: scopedFamily.name ?? null,
        status: scopedFamily.status ?? null
      });
    }

    if (!key) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    let familyId = null;

    if (tableExists("family_admins") && hasColumn("family_admins", "admin_key") && hasColumn("family_admins", "family_id")) {
      const row = db
        .prepare(
          'SELECT "family_id" FROM "family_admins" WHERE "admin_key" = ? LIMIT 1'
        )
        .get(key);
      if (row?.family_id) {
        familyId = row.family_id;
      }
    }

    if (!familyId && tableExists("families") && hasColumn("families", "admin_key")) {
      const row = db
        .prepare('SELECT "id" FROM "families" WHERE "admin_key" = ? LIMIT 1')
        .get(key);
      if (row?.id) {
        familyId = row.id;
      }
    }

    if (
      !familyId &&
      tableExists("admins") &&
      hasColumn("admins", "key") &&
      hasColumn("admins", "family_id")
    ) {
      const row = db
        .prepare('SELECT "family_id" FROM "admins" WHERE "key" = ? LIMIT 1')
        .get(key);
      if (row?.family_id) {
        familyId = row.family_id;
      }
    }

    if (!familyId) {
      return res.status(404).json({ error: "No family found for admin key" });
    }

    const familyRow = loadFamilyRow(familyId);
    if (!familyRow) {
      return res.status(404).json({ error: "No family found for admin key" });
    }

    const keyValue = coalesceFamilyValue(familyRow, ["key", "family_key", "admin_key"]);
    const nameValue = coalesceFamilyValue(familyRow, ["name", "title", "display_name"]);
    const statusValue = coalesceFamilyValue(familyRow, ["status", "state"]);

    res.json({
      id: familyRow.id ?? familyId,
      key: keyValue,
      name: nameValue,
      status: statusValue
    });
  } catch (error) {
    next(error);
  }
}

export default familyForCurrentAdmin;
