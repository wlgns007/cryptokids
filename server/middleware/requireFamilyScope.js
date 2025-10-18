export function requireFamilyScope(req, res, next) {
  if (req.family?.id) {
    return next();
  }

  if (req.familyScopeError?.code === 'family_not_found') {
    return res.status(404).json({ error: 'family_not_found' });
  }

  return res.status(400).json({ error: 'Missing family scope (x-family)' });
}

export default requireFamilyScope;
