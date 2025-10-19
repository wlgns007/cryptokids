import { randomUUID } from 'node:crypto';
import { createTestContext } from './setup.js';

function tableExists(db, name) {
  try {
    return Boolean(
      db
        .prepare("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name = ?")
        .get(name)
    );
  } catch {
    return false;
  }
}

function ensureHoldsTable(db) {
  if (!tableExists(db, 'holds')) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS holds (
        id TEXT PRIMARY KEY,
        family_id TEXT,
        member_id TEXT,
        reward_id TEXT,
        status TEXT,
        points INTEGER,
        created_at INTEGER
      );
    `);
  }
}

export const MASTER_KEY = 'Murasaki';

export const families = {
  tang: {
    id: '11111111-1111-1111-1111-111111111111',
    name: 'Tang Family',
    adminKey: 'tang-admin-key',
    email: 'tang@example.com',
  },
  jang: {
    id: '22222222-2222-2222-2222-222222222222',
    name: 'Jang Family',
    adminKey: 'jang-admin-key',
    email: 'jang@example.com',
  },
};

export const members = {
  tangKid: {
    id: '33333333-3333-3333-3333-333333333333',
    name: 'Tang Kid',
  },
};

export const tasks = {
  tangTask: {
    id: '44444444-4444-4444-4444-444444444444',
    title: 'Wash dishes',
    points: 10,
  },
};

export const rewards = {
  tangReward: {
    id: '55555555-5555-5555-5555-555555555555',
    name: 'Ice Cream',
    cost: 25,
  },
};

export const holds = {
  tangHold: {
    id: '66666666-6666-6666-6666-666666666666',
    points: 25,
  },
};

export async function resetDatabase() {
  const { db } = await createTestContext();

  ensureHoldsTable(db);

  const tables = [
    'member',
    'task',
    'reward',
    'holds',
    'hold',
    'ledger',
    'family_admin',
    'family',
    'master_admin'
  ];

  for (const table of tables) {
    if (tableExists(db, table)) {
      db.exec(`DELETE FROM "${table}";`);
    }
  }
}

export async function seedBaselineData() {
  const { db } = await createTestContext();
  await resetDatabase();

  const nowIso = new Date().toISOString();
  const nowTs = Math.floor(Date.now() / 1000);

  const masterStmt = db.prepare(
    `INSERT INTO master_admin (id, admin_key, created_at) VALUES (@id, @key, @created)`
  );
  masterStmt.run({ id: randomUUID(), key: MASTER_KEY, created: nowIso });

  const familyStmt = db.prepare(
    `INSERT INTO family (id, name, email, status, admin_key, created_at, updated_at)
     VALUES (@id, @name, @email, 'active', @adminKey, @createdAt, @updatedAt)`
  );

  for (const family of Object.values(families)) {
    familyStmt.run({
      id: family.id,
      name: family.name,
      email: family.email,
      adminKey: family.adminKey,
      createdAt: nowIso,
      updatedAt: nowIso,
    });
  }

  const memberStmt = db.prepare(
    `INSERT INTO member (id, family_id, name, status, created_at, updated_at)
     VALUES (@id, @familyId, @name, 'active', @createdAt, @updatedAt)`
  );
  memberStmt.run({
    id: members.tangKid.id,
    familyId: families.tang.id,
    name: members.tangKid.name,
    createdAt: nowTs,
    updatedAt: nowTs,
  });

  const taskStmt = db.prepare(
    `INSERT INTO task (id, family_id, title, description, icon, points, status, master_task_id, created_at, updated_at)
     VALUES (@id, @familyId, @title, '', '', @points, 'active', NULL, @createdAt, @updatedAt)`
  );
  taskStmt.run({
    id: tasks.tangTask.id,
    familyId: families.tang.id,
    title: tasks.tangTask.title,
    points: tasks.tangTask.points,
    createdAt: nowTs,
    updatedAt: nowTs,
  });

  const rewardStmt = db.prepare(
    `INSERT INTO reward (id, family_id, name, cost, description, image_url, youtube_url, status, tags, campaign_id, source, created_at, updated_at)
     VALUES (@id, @familyId, @name, @cost, '', '', '', 'active', NULL, NULL, NULL, @createdAt, @updatedAt)`
  );
  rewardStmt.run({
    id: rewards.tangReward.id,
    familyId: families.tang.id,
    name: rewards.tangReward.name,
    cost: rewards.tangReward.cost,
    createdAt: nowTs,
    updatedAt: nowTs,
  });

  const holdStmt = db.prepare(
    `INSERT INTO holds (id, family_id, member_id, reward_id, status, points, created_at)
     VALUES (@id, @familyId, @memberId, @rewardId, 'pending', @points, @createdAt)`
  );
  holdStmt.run({
    id: holds.tangHold.id,
    familyId: families.tang.id,
    memberId: members.tangKid.id,
    rewardId: rewards.tangReward.id,
    points: holds.tangHold.points,
    createdAt: nowTs,
  });

  return {
    memberId: members.tangKid.id,
    taskId: tasks.tangTask.id,
    rewardId: rewards.tangReward.id,
    holdId: holds.tangHold.id,
  };
}
