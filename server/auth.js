export function readAdminKey(req) {
  const headerKey =
    (typeof req.get === 'function' && (req.get('x-admin-key') || req.get('X-Admin-Key')))
      || req.headers['x-admin-key']
      || req.headers['X-Admin-Key'];
  if (typeof headerKey === 'string' && headerKey.trim()) return headerKey.trim();
  const q = req.query?.adminKey;
  if (typeof q === 'string' && q.trim()) return q.trim();
  return '';
}
