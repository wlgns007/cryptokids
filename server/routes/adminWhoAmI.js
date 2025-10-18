export function whoAmI(req, res) {
  const role = req.admin?.role || "none";
  res.json({
    role,
    family_uuid: req.family?.id ?? null,
    family_key: req.family?.key ?? null,
    family_name: req.family?.name ?? null,
    family_status: req.family?.status ?? null
  });
}

export default whoAmI;
