export function readAdminKey(req) {
  const headerValue = req.headers?.['x-admin-key'];
  if (typeof headerValue === 'string' && headerValue.trim()) {
    return headerValue.trim();
  }
  const queryValue = req.query?.adminKey ?? req.query?.adminkey;
  if (typeof queryValue === 'string' && queryValue.trim()) {
    return queryValue.trim();
  }
  return '';
}
