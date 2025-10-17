ALTER TABLE task   ADD COLUMN source_template_id TEXT;
ALTER TABLE task   ADD COLUMN source_version INTEGER DEFAULT 1;
ALTER TABLE task   ADD COLUMN is_customized INTEGER DEFAULT 0;

ALTER TABLE reward ADD COLUMN source_template_id TEXT;
ALTER TABLE reward ADD COLUMN source_version INTEGER DEFAULT 1;
ALTER TABLE reward ADD COLUMN is_customized INTEGER DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_task_source   ON task(source_template_id, is_customized);
CREATE INDEX IF NOT EXISTS idx_reward_source ON reward(source_template_id, is_customized);
