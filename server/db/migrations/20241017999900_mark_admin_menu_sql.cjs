module.exports = function up({ db, hasColumn }) {
  const taskHasScope = typeof hasColumn === "function" ? hasColumn("task", "scope") : false;
  const rewardHasScope = typeof hasColumn === "function" ? hasColumn("reward", "scope") : false;

  if (!taskHasScope || !rewardHasScope) {
    return;
  }

  const insert = db.prepare(
    "INSERT OR IGNORE INTO schema_migrations (name, applied_at) VALUES (?, ?)"
  );
  const timestamp = Date.now();
  insert.run("20241018000100_create_admin_menus.sql", timestamp);
};
