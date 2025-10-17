export function readAdminKey(req) {
  const headerKey =
    (typeof req.get === 'function' && (req.get('x-admin-key') || req.get('X-Admin-Key')))
      || req.headers['x-admin-key']
      || req.headers['X-Admin-Key'];
  if (typeof headerKey === 'string') {
    const trimmed = headerKey.trim();
    if (trimmed) return trimmed;
  }
  const rawCookie = req.headers?.cookie || '';
  if (typeof rawCookie === 'string' && rawCookie) {
    const match = rawCookie.match(/(?:^|;\s*)ck_admin_key=([^;]+)/);
    if (match && match[1]) {
      try {
        const value = decodeURIComponent(match[1]);
        if (value && value.trim()) return value.trim();
      } catch {
        // ignore malformed cookie
      }
    }
  }
  return '';
}
