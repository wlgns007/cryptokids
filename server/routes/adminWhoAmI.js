export function whoAmI(req, res) {
  const fam = req.family || null;
  const role = req.admin?.role || req.auth?.role || 'unknown';
  res.json({
    role,
    family_uuid: fam?.id ?? null,
    family_key: fam?.key ?? null,
    family_name: fam?.name ?? null,
    family_status: fam?.status ?? null,
  });
}

export default whoAmI;
