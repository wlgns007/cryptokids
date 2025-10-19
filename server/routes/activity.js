export function listActivity(req, res) {
  // TODO: replace with a UNION over ledger/holds/events
  // For now, avoid hitting a non-existent "activity" table.
  return res.json([]);
}

export default listActivity;
