module.exports = function up({
  db,
  addColumnIfMissing,
  createIndexIfMissing,
  hasColumn,
  tableInfo,
}) {
  const tableExists = (table) => {
    const info = typeof tableInfo === "function" ? tableInfo(table) : [];
    return Array.isArray(info) && info.length > 0;
  };

  const addColumn = (table, definition) => {
    if (!tableExists(table)) {
      return;
    }
    if (typeof addColumnIfMissing === "function") {
      addColumnIfMissing(table, definition);
    } else {
      const [column] = definition.trim().split(/\s+/, 1);
      const exists = typeof hasColumn === "function" ? hasColumn(table, column) : false;
      if (!exists) {
        db.exec(`ALTER TABLE "${table}" ADD COLUMN ${definition}`);
      }
    }
  };

  addColumn("task", "source_template_id TEXT");
  addColumn("task", "source_version INTEGER DEFAULT 1");
  addColumn("task", "is_customized INTEGER DEFAULT 0");

  addColumn("reward", "source_template_id TEXT");
  addColumn("reward", "source_version INTEGER DEFAULT 1");
  addColumn("reward", "is_customized INTEGER DEFAULT 0");

  if (typeof hasColumn === "function" && hasColumn("task", "source_version")) {
    db.exec("UPDATE task SET source_version = 1 WHERE source_version IS NULL");
  }
  if (typeof hasColumn === "function" && hasColumn("reward", "source_version")) {
    db.exec("UPDATE reward SET source_version = 1 WHERE source_version IS NULL");
  }
  if (typeof hasColumn === "function" && hasColumn("task", "is_customized")) {
    db.exec("UPDATE task SET is_customized = 0 WHERE is_customized IS NULL");
  }
  if (typeof hasColumn === "function" && hasColumn("reward", "is_customized")) {
    db.exec("UPDATE reward SET is_customized = 0 WHERE is_customized IS NULL");
  }

  const ensureIndex = (name, ddl, table, columns) => {
    if (!tableExists(table)) {
      return;
    }
    const missing = columns.filter((column) => !hasColumn?.(table, column));
    if (missing.length) {
      return;
    }
    if (typeof createIndexIfMissing === "function") {
      createIndexIfMissing(name, ddl);
    } else {
      db.exec(ddl);
    }
  };

  ensureIndex(
    "idx_task_source",
    "CREATE INDEX IF NOT EXISTS idx_task_source ON task(source_template_id, is_customized)",
    "task",
    ["source_template_id", "is_customized"]
  );
  ensureIndex(
    "idx_reward_source",
    "CREATE INDEX IF NOT EXISTS idx_reward_source ON reward(source_template_id, is_customized)",
    "reward",
    ["source_template_id", "is_customized"]
  );
};
