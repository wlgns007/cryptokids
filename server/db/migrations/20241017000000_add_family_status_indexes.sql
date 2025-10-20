CREATE INDEX IF NOT EXISTS idx_task_family_status ON task(family_id, status);
CREATE INDEX IF NOT EXISTS idx_member_family ON member(family_id);
CREATE INDEX IF NOT EXISTS idx_ledger_family ON ledger(family_id);
