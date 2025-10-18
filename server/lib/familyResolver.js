export default function makeFamilyResolver(db) {
  const byIdStmt = db.prepare(`SELECT id, family_key, name FROM family WHERE id = ?`);
  const byKeyStmt = db.prepare(`SELECT id, family_key, name FROM family WHERE family_key = ?`);

  function looksLikeUuid(value) {
    return typeof value === 'string' && value.length >= 32 && /[a-f0-9-]{32,}/i.test(value);
  }

  return function resolveFamily(param) {
    if (!param) return null;
    const row = looksLikeUuid(param) ? byIdStmt.get(param) : byKeyStmt.get(param);
    return row || null;
  };
}
