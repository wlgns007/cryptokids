export function readAdminKey(req) {
  const h = req.headers['x-admin-key'];
  if (typeof h === 'string' && h.trim()) return h.trim();
  const q = req.query?.adminKey;
  if (typeof q === 'string' && q.trim()) return q.trim();
  return '';
}
