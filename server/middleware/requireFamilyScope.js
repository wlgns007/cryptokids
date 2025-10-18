export function requireFamilyScope(req, res, next) {
  if (!req.family) return res.status(400).json({ error: 'Missing family scope (x-family).' });
  next();
}

export default requireFamilyScope;
