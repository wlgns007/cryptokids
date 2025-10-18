import { db as defaultDb } from "../db.js";

export function listFamilies(req, res, next) {
  try {
    if (req.admin?.role !== "master" && req.auth?.role !== "master") {
      return res.status(403).json({ error: "Forbidden" });
    }

    const db = req.db || defaultDb;
    const status = (req.query?.status || "active").toString().toLowerCase();
    const rows = db
      .prepare(
        `SELECT id,
                admin_key AS key,
                name,
                status,
                email,
                created_at,
                updated_at
           FROM family
          WHERE id <> 'default' AND (? = 'all' OR LOWER(status) = ?)
          ORDER BY name COLLATE NOCASE`
      )
      .all(status, status);

    res.json(
      rows.map((row) => {
        const key = row.key ?? null;
        return {
          id: row.id,
          key,
          admin_key: key,
          family_key: key ?? "",
          name: row.name ?? "",
          status: (row.status || "active").toString(),
          email: row.email ?? null,
          created_at: row.created_at ?? null,
          updated_at: row.updated_at ?? null,
        };
      })
    );
  } catch (error) {
    next(error);
  }
}

export default listFamilies;
