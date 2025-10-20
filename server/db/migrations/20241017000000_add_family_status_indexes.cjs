module.exports = function up({ db, createIndexIfMissing, hasColumn, tableInfo }) {
  const ensureIndex = (name, ddl, table, requiredColumns = []) => {
    const columns = requiredColumns.length
      ? requiredColumns
      : (tableInfo?.(table) || []).map((col) => col?.name).filter(Boolean);
    const hasColumns = requiredColumns.every((column) =>
      typeof hasColumn === "function" ? hasColumn(table, column) : columns.includes(column)
    );
    if (!hasColumns) {
      return;
    }
    if (typeof createIndexIfMissing === "function") {
      createIndexIfMissing(name, ddl);
    } else {
      db.exec(ddl);
    }
  };

  ensureIndex(
    "idx_task_family_status",
    "CREATE INDEX IF NOT EXISTS idx_task_family_status ON task(family_id, status)",
    "task",
    ["family_id", "status"]
  );

  ensureIndex(
    "idx_member_family",
    "CREATE INDEX IF NOT EXISTS idx_member_family ON member(family_id)",
    "member",
    ["family_id"]
  );

  ensureIndex(
    "idx_ledger_family",
    "CREATE INDEX IF NOT EXISTS idx_ledger_family ON ledger(family_id)",
    "ledger",
    ["family_id"]
  );
};
