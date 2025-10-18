export function requireFamilyScope(req, res, next) {
  if (!req.family?.id) return res.status(400).json({ error: 'Missing family scope (x-family)' });
  return next();
}

export default requireFamilyScope;
