import { COOKIE_NAME } from '../config.js';

const MASTER = process.env.MASTER_ADMIN_KEY;

export default function adminAuth(req, res, next) {
  try {
    const key = req.cookies?.[COOKIE_NAME] || req.header('x-admin-key') || null;
    if (!key) return res.status(401).json({ error: 'Unauthorized' });

    // Master always allowed
    if (key === MASTER) {
      req.admin = { role: 'master' };
      return next();
    }

    // TEMP: if a family scope is present, allow (we already require scope on the route)
    // Later, replace this with a real DB check (key belongs to req.family.id).
    if (req.family?.id) {
      req.admin = { role: 'family' };
      return next();
    }

    return res.status(403).json({ error: 'Family scope required' });
  } catch (e) {
    next(e);
  }
}
