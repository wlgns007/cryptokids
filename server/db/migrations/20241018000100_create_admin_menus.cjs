const TASK_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS task (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL DEFAULT 'family' CHECK(scope IN ('global','family')),
    family_id TEXT,
    title TEXT NOT NULL,
    description TEXT,
    icon TEXT,
    points INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    master_task_id TEXT,
    source_template_id TEXT,
    source_version INTEGER NOT NULL DEFAULT 1,
    is_customized INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
`;

const REWARD_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS reward (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL DEFAULT 'family' CHECK(scope IN ('global','family')),
    family_id TEXT,
    name TEXT NOT NULL,
    cost INTEGER NOT NULL,
    description TEXT DEFAULT '',
    image_url TEXT DEFAULT '',
    youtube_url TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    tags TEXT,
    campaign_id TEXT,
    source TEXT,
    master_reward_id TEXT,
    source_template_id TEXT,
    source_version INTEGER NOT NULL DEFAULT 1,
    is_customized INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
`;

const MASTER_TASK_SQL = `
  CREATE TABLE IF NOT EXISTS master_task (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    base_points INTEGER NOT NULL DEFAULT 0,
    icon TEXT,
    youtube_url TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    version INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
`;

const MASTER_REWARD_SQL = `
  CREATE TABLE IF NOT EXISTS master_reward (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    base_cost INTEGER NOT NULL DEFAULT 0,
    icon TEXT,
    youtube_url TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    version INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
`;

function ensureTables(db) {
  db.exec(TASK_TABLE_SQL);
  db.exec(REWARD_TABLE_SQL);
  db.exec(MASTER_TASK_SQL);
  db.exec(MASTER_REWARD_SQL);
}

function backfillDefaults(db) {
  if (columnExists(db, "task", "scope")) {
    db.exec("UPDATE task SET scope = 'family' WHERE scope IS NULL OR TRIM(scope) = ''");
  }
  if (columnExists(db, "reward", "scope")) {
    db.exec("UPDATE reward SET scope = 'family' WHERE scope IS NULL OR TRIM(scope) = ''");
  }
  if (columnExists(db, "task", "source_version")) {
    db.exec("UPDATE task SET source_version = 1 WHERE source_version IS NULL");
  }
  if (columnExists(db, "reward", "source_version")) {
    db.exec("UPDATE reward SET source_version = 1 WHERE source_version IS NULL");
  }
  if (columnExists(db, "task", "is_customized")) {
    db.exec("UPDATE task SET is_customized = 0 WHERE is_customized IS NULL");
  }
  if (columnExists(db, "reward", "is_customized")) {
    db.exec("UPDATE reward SET is_customized = 0 WHERE is_customized IS NULL");
  }
}

function columnExists(db, table, column) {
  try {
    const info = db.prepare(`PRAGMA table_info(\"${table}\")`).all();
    return info.some((row) => row?.name === column);
  } catch {
    return false;
  }
}

module.exports = function up({ db, addColumnIfMissing, createIndexIfMissing, hasColumn }) {
  ensureTables(db);

  const addColumn = (table, definition) => {
    if (typeof addColumnIfMissing === "function") {
      addColumnIfMissing(table, definition);
    } else {
      const [name] = definition.trim().split(/\s+/, 1);
      if (!columnExists(db, table, name)) {
        db.exec(`ALTER TABLE "${table}" ADD COLUMN ${definition}`);
      }
    }
  };

  addColumn("task", "scope TEXT DEFAULT 'family' CHECK(scope IN ('global','family'))");
  addColumn("task", "source_template_id TEXT");
  addColumn("task", "source_version INTEGER DEFAULT 1");
  addColumn("task", "is_customized INTEGER DEFAULT 0");

  addColumn("reward", "scope TEXT DEFAULT 'family' CHECK(scope IN ('global','family'))");
  addColumn("reward", "source_template_id TEXT");
  addColumn("reward", "source_version INTEGER DEFAULT 1");
  addColumn("reward", "is_customized INTEGER DEFAULT 0");

  backfillDefaults(db);

  const createIndex = (name, ddl) => {
    if (typeof createIndexIfMissing === "function") {
      createIndexIfMissing(name, ddl);
    } else {
      db.exec(ddl);
    }
  };

  createIndex(
    "task_uq_family_title",
    "CREATE UNIQUE INDEX IF NOT EXISTS task_uq_family_title ON task(family_id, title)"
  );
  createIndex(
    "reward_uq_family_name",
    "CREATE UNIQUE INDEX IF NOT EXISTS reward_uq_family_name ON reward(family_id, name)"
  );
};
