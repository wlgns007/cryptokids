import { randomUUID } from "node:crypto";

function quoteIdent(identifier) {
  return `"${String(identifier).replaceAll('"', '""')}"`;
}

function hasTable(db, name) {
  try {
    return Boolean(
      db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1").get(name)
    );
  } catch (error) {
    console.warn("[seed] unable to verify table", { table: name, error: error?.message || error });
    return false;
  }
}

function tableColumns(db, table) {
  try {
    return db.prepare(`PRAGMA table_info(${quoteIdent(table)})`).all().map((col) => col.name);
  } catch (error) {
    console.warn("[seed] unable to inspect columns", { table, error: error?.message || error });
    return [];
  }
}

function insertRecord(db, table, record) {
  const columns = tableColumns(db, table).filter((column) => Object.prototype.hasOwnProperty.call(record, column));
  if (!columns.length) return;
  const sql = `INSERT INTO ${quoteIdent(table)} (${columns.map(quoteIdent).join(", ")}) VALUES (${columns
    .map((column) => `@${column}`)
    .join(", ")})`;
  db.prepare(sql).run(record);
}

const MASTER_TASK_DEFAULTS = [
  {
    title: "Brush Teeth",
    description: "Brush your teeth in the morning and at night.",
    base_points: 5,
    icon: "brush-teeth",
    youtube_url: null,
  },
  {
    title: "Make Your Bed",
    description: "Tidy up your bed after waking up.",
    base_points: 5,
    icon: "make-bed",
    youtube_url: null,
  },
];

const MASTER_REWARD_DEFAULTS = [
  {
    name: "Ice Cream Treat",
    description: "Enjoy a scoop of ice cream.",
    base_cost: 25,
    icon: "ice-cream",
    youtube_url: null,
  },
  {
    name: "Screen Time (30 min)",
    description: "Earn 30 minutes of screen time.",
    base_cost: 50,
    icon: "screen-time",
    youtube_url: null,
  },
];

export function seedGlobalTemplates(db) {
  if (!hasTable(db, "master_task") || !hasTable(db, "master_reward")) {
    return { tasks: [], rewards: [] };
  }

  const createdTasks = [];
  const createdRewards = [];

  const run = db.transaction(() => {
    const masterTaskCheck = db.prepare(
      "SELECT id FROM master_task WHERE LOWER(title) = LOWER(?) LIMIT 1"
    );
    const masterRewardCheck = db.prepare(
      "SELECT id FROM master_reward WHERE LOWER(title) = LOWER(?) LIMIT 1"
    );

    for (const template of MASTER_TASK_DEFAULTS) {
      const existing = masterTaskCheck.get(template.title);
      if (existing) continue;
      const now = Date.now();
      const record = {
        id: randomUUID(),
        title: template.title,
        description: template.description,
        base_points: template.base_points,
        icon: template.icon,
        youtube_url: template.youtube_url,
        status: "active",
        version: 1,
        created_at: now,
        updated_at: now,
      };
      try {
        insertRecord(db, "master_task", record);
        createdTasks.push(template.title);
      } catch (error) {
        const message = error?.message || "";
        if (!/unique/i.test(message)) {
          throw error;
        }
      }
    }

    for (const reward of MASTER_REWARD_DEFAULTS) {
      const existing = masterRewardCheck.get(reward.name);
      if (existing) continue;
      const now = Date.now();
      const record = {
        id: randomUUID(),
        title: reward.name,
        description: reward.description,
        base_cost: reward.base_cost,
        icon: reward.icon,
        youtube_url: reward.youtube_url,
        status: "active",
        version: 1,
        created_at: now,
        updated_at: now,
      };
      try {
        insertRecord(db, "master_reward", record);
        createdRewards.push(reward.name);
      } catch (error) {
        const message = error?.message || "";
        if (!/unique/i.test(message)) {
          throw error;
        }
      }
    }
  });

  run();
  return { tasks: createdTasks, rewards: createdRewards };
}

export default seedGlobalTemplates;
