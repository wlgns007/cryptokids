import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import { once } from "node:events";
import { dirname, join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import Database from "better-sqlite3";

import { MULTITENANT_ENFORCE } from "../server/config.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const REPO_ROOT = join(__dirname, "..");
const DATA_DIR = join(REPO_ROOT, "data");
const SMITH_KEY_FILE = join(DATA_DIR, "smoke-smith-key.txt");
const DEFAULT_FAMILY_ID = "default";
const JANG_FAMILY_ID = "JangJ6494";
const JANG_ADMIN_KEY = "Mamapapa";

const DEFAULT_PORT = process.env.SMOKE_PORT || "4100";
const BASE_URL = process.env.SMOKE_BASE_URL || `http://127.0.0.1:${DEFAULT_PORT}`;

function logStep(message) {
  console.log(`[testC1] ${message}`);
}

async function ensureOk(response, label) {
  if (response.ok) {
    return;
  }
  let detail = "";
  try {
    detail = await response.text();
  } catch {
    detail = "<no body>";
  }
  throw new Error(`${label} failed: ${response.status} ${response.statusText} ${detail}`.trim());
}

async function waitForServer(baseUrl, attempts = 40) {
  for (let i = 0; i < attempts; i += 1) {
    try {
      const res = await fetch(new URL("/version", baseUrl));
      if (res.ok) {
        return;
      }
    } catch {
      // ignore
    }
    await delay(250);
  }
  throw new Error(`server at ${baseUrl} did not become ready`);
}

async function withServer(baseUrl, extraEnv, fn) {
  const url = new URL(baseUrl);
  const env = {
    ...process.env,
    PORT: url.port || "80",
    ...extraEnv
  };

  await new Promise((resolve, reject) => {
    const prestart = spawn("node", ["scripts/prestart-check.mjs"], {
      cwd: REPO_ROOT,
      env,
      stdio: "inherit"
    });
    prestart.on("error", reject);
    prestart.on("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`prestart-check exited with code ${code}`));
      }
    });
  });

  const child = spawn("node", ["server/index.js"], {
    cwd: REPO_ROOT,
    env,
    stdio: ["ignore", "pipe", "pipe"]
  });

  child.stdout.on("data", (chunk) => {
    process.stdout.write(`[server] ${chunk}`);
  });
  child.stderr.on("data", (chunk) => {
    process.stderr.write(`[server-err] ${chunk}`);
  });

  try {
    await waitForServer(baseUrl);
    await fn();
  } finally {
    child.kill();
    try {
      await once(child, "exit");
    } catch {
      // ignore
    }
  }
}

async function ensureFamily(baseUrl, masterKey, familyName) {
  const listRes = await fetch(new URL("/api/families", baseUrl), {
    headers: { "X-ADMIN-KEY": masterKey }
  });
  await ensureOk(listRes, "list families");
  const families = await listRes.json();
  const existing = families.find(
    (family) => family.name?.toLowerCase() === familyName.toLowerCase()
  );
  if (existing) {
    return existing;
  }

  const createRes = await fetch(new URL("/api/families", baseUrl), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-ADMIN-KEY": masterKey
    },
    body: JSON.stringify({ name: familyName })
  });
  await ensureOk(createRes, "create family");
  return await createRes.json();
}

async function readStoredSmithKey() {
  try {
    const content = await fs.readFile(SMITH_KEY_FILE, "utf8");
    const trimmed = content.trim();
    return trimmed ? trimmed : null;
  } catch (err) {
    if (err && err.code === "ENOENT") {
      return null;
    }
    throw err;
  }
}

async function writeStoredSmithKey(key) {
  await fs.mkdir(DATA_DIR, { recursive: true });
  await fs.writeFile(SMITH_KEY_FILE, `${key}\n`, "utf8");
}

async function removeDbArtifacts(dbPath) {
  if (!dbPath) return;
  const suffixes = ["", "-shm", "-wal"];
  for (const suffix of suffixes) {
    const target = `${dbPath}${suffix}`;
    try {
      await fs.rm(target, { force: true });
    } catch {
      // ignore cleanup errors
    }
  }
}

async function validateAdminKey(baseUrl, adminKey, expectedFamilyId) {
  if (!adminKey) {
    return false;
  }
  try {
    const res = await fetch(new URL("/api/whoami", baseUrl), {
      headers: {
        "X-ADMIN-KEY": adminKey
      }
    });
    if (!res.ok) {
      return false;
    }
    const payload = await res.json();
    return payload.role === "family_admin" && payload.family_id === expectedFamilyId;
  } catch {
    return false;
  }
}

async function ensureFamilyAdminKey(baseUrl, masterKey, familyId) {
  const stored = await readStoredSmithKey();
  if (await validateAdminKey(baseUrl, stored, familyId)) {
    return stored;
  }
  if (stored) {
    await fs.rm(SMITH_KEY_FILE, { force: true });
  }

  const label = `smoke-smith-${Date.now()}`;
  const createRes = await fetch(new URL("/api/admin-keys", baseUrl), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-ADMIN-KEY": masterKey
    },
    body: JSON.stringify({ family_id: familyId, label })
  });
  await ensureOk(createRes, "create family admin key");
  const payload = await createRes.json();
  if (!payload?.plain_key) {
    throw new Error("family admin key creation did not return plain_key");
  }
  await writeStoredSmithKey(payload.plain_key);
  return payload.plain_key;
}

async function createScopedMember(baseUrl, adminKey, userId, name) {
  const res = await fetch(new URL("/api/admin/members", baseUrl), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-ADMIN-KEY": adminKey
    },
    body: JSON.stringify({ userId, name })
  });
  await ensureOk(res, "create scoped member");
  return await res.json();
}

async function listMembersForKey(baseUrl, adminKey, actAsFamily) {
  const headers = { "X-ADMIN-KEY": adminKey };
  if (actAsFamily) {
    headers["X-Act-As-Family"] = actAsFamily;
  }
  const res = await fetch(new URL("/api/admin/members", baseUrl), { headers });
  await ensureOk(res, "list members for key");
  return await res.json();
}

async function createEarnTemplate(baseUrl, adminKey, body, actAsFamily) {
  const headers = {
    "Content-Type": "application/json",
    "X-ADMIN-KEY": adminKey
  };
  if (actAsFamily) {
    headers["X-Act-As-Family"] = actAsFamily;
  }
  const res = await fetch(new URL("/api/earn-templates", baseUrl), {
    method: "POST",
    headers,
    body: JSON.stringify(body)
  });
  await ensureOk(res, "create earn template");
  return await res.json();
}

async function listEarnTemplates(baseUrl, adminKey, actAsFamily) {
  const headers = { "X-ADMIN-KEY": adminKey };
  if (actAsFamily) {
    headers["X-Act-As-Family"] = actAsFamily;
  }
  const res = await fetch(new URL("/api/earn-templates", baseUrl), { headers });
  await ensureOk(res, "list earn templates");
  return await res.json();
}

async function createReward(baseUrl, adminKey, body, actAsFamily) {
  const headers = {
    "Content-Type": "application/json",
    "X-ADMIN-KEY": adminKey
  };
  if (actAsFamily) {
    headers["X-Act-As-Family"] = actAsFamily;
  }
  const res = await fetch(new URL("/api/admin/rewards", baseUrl), {
    method: "POST",
    headers,
    body: JSON.stringify(body)
  });
  await ensureOk(res, "create reward");
  return await res.json();
}

async function listRewards(baseUrl, adminKey, actAsFamily) {
  const headers = { "X-ADMIN-KEY": adminKey };
  if (actAsFamily) {
    headers["X-Act-As-Family"] = actAsFamily;
  }
  const res = await fetch(new URL("/api/admin/rewards", baseUrl), { headers });
  await ensureOk(res, "list rewards");
  return await res.json();
}

export async function testC0() {
  console.log("[testC0] MULTITENANT_ENFORCE:", MULTITENANT_ENFORCE);
}

export async function testC1() {
  const masterKey = (process.env.MASTER_ADMIN_KEY || "").trim();
  if (!masterKey) {
    throw new Error("MASTER_ADMIN_KEY environment variable is required for testC1");
  }

  await withServer(
    BASE_URL,
    {
      MASTER_ADMIN_KEY: masterKey
    },
    async () => {
      logStep("ensuring smith family exists");
      const smithFamily = await ensureFamily(BASE_URL, masterKey, "smith");
      logStep(`smith family id: ${smithFamily.id}`);

      logStep("ensuring smith family admin key");
      const smithKey = await ensureFamilyAdminKey(BASE_URL, masterKey, smithFamily.id);
      logStep("smith admin key ready");

      const newMemberId = `kid-a-${Date.now()}`;
      logStep(`creating member ${newMemberId}`);
      await createScopedMember(BASE_URL, smithKey, newMemberId, "Kid A");

      logStep("listing members as smith family admin");
      const smithMembers = await listMembersForKey(BASE_URL, smithKey);
      const smithHasMember = smithMembers.some((member) => member.userId === newMemberId);
      console.log(`[testC1] smith family has new member: ${smithHasMember}`);

      logStep("listing members as master acting as default family");
      const defaultMembers = await listMembersForKey(BASE_URL, masterKey, "default");
      const defaultHasMember = defaultMembers.some((member) => member.userId === newMemberId);
      console.log(`[testC1] default family contains new smith member: ${defaultHasMember}`);

      if (!smithHasMember) {
        throw new Error("smith family did not contain the newly created member");
      }
      if (defaultHasMember) {
        throw new Error("default family unexpectedly contained the smith member");
      }
    }
  );
}

export async function testC2() {
  const masterKey = (process.env.MASTER_ADMIN_KEY || "").trim();
  if (!masterKey) {
    throw new Error("MASTER_ADMIN_KEY environment variable is required for testC2");
  }

  const log = (message) => console.log(`[testC2] ${message}`);

  await withServer(
    BASE_URL,
    {
      MASTER_ADMIN_KEY: masterKey
    },
    async () => {
      log("ensuring smith family exists");
      const smithFamily = await ensureFamily(BASE_URL, masterKey, "smith");
      log(`smith family id: ${smithFamily.id}`);

      log("ensuring smith family admin key");
      const smithKey = await ensureFamilyAdminKey(BASE_URL, masterKey, smithFamily.id);
      log("smith admin key ready");

      const memberId = `kid-ledger-${Date.now()}`;
      log(`creating member ${memberId}`);
      await createScopedMember(BASE_URL, smithKey, memberId, "Ledger Kid");

      log("posting earn transaction for smith member");
      const earnRes = await fetch(new URL("/ck/earn", BASE_URL), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-ADMIN-KEY": smithKey
        },
        body: JSON.stringify({ user_id: memberId, amount: 5, action: "earn_smoke" })
      });
      await ensureOk(earnRes, "family earn");
      await earnRes.json();

      log("fetching smith ledger view");
      const smithLedgerRes = await fetch(new URL(`/ck/ledger/${memberId}`, BASE_URL), {
        headers: { "X-ADMIN-KEY": smithKey }
      });
      await ensureOk(smithLedgerRes, "smith ledger fetch");
      const smithLedger = await smithLedgerRes.json();
      const smithRows = Array.isArray(smithLedger.rows) ? smithLedger.rows : [];
      const smithHasEarn = smithRows.some((row) => row.action === "earn_smoke");
      log(`smith ledger contains earn_smoke entry: ${smithHasEarn}`);
      if (!smithHasEarn) {
        throw new Error("smith ledger did not contain the expected earn_smoke entry");
      }

      log("fetching default family ledger via master");
      const masterLedgerRes = await fetch(new URL(`/ck/ledger/${memberId}`, BASE_URL), {
        headers: {
          "X-ADMIN-KEY": masterKey,
          "X-Act-As-Family": "default"
        }
      });
      await ensureOk(masterLedgerRes, "master default ledger fetch");
      const masterLedger = await masterLedgerRes.json();
      const masterRows = Array.isArray(masterLedger.rows) ? masterLedger.rows : [];
      const masterHasEarn = masterRows.some((row) => row.action === "earn_smoke");
      log(`default family saw smith earn_smoke entry: ${masterHasEarn}`);
      if (masterHasEarn) {
        throw new Error("default family unexpectedly saw smith ledger entry");
      }
    }
  );
}

export async function testC3() {
  const masterKey = (process.env.MASTER_ADMIN_KEY || "").trim();
  if (!masterKey) {
    throw new Error("MASTER_ADMIN_KEY environment variable is required for testC3");
  }

  const log = (message) => console.log(`[testC3] ${message}`);

  await withServer(
    BASE_URL,
    {
      MASTER_ADMIN_KEY: masterKey
    },
    async () => {
      log("ensuring smith family exists");
      const smithFamily = await ensureFamily(BASE_URL, masterKey, "smith");
      log(`smith family id: ${smithFamily.id}`);

      log("ensuring smith family admin key");
      const smithKey = await ensureFamilyAdminKey(BASE_URL, masterKey, smithFamily.id);
      log("smith admin key ready");

      const taskTitle = `Make Bed ${Date.now()}`;
      log(`creating smith task ${taskTitle}`);
      await createEarnTemplate(
        BASE_URL,
        smithKey,
        { title: taskTitle, points: 5, description: "Smoke task" }
      );

      log("listing tasks as smith family admin");
      const smithTasks = await listEarnTemplates(BASE_URL, smithKey);
      const smithHasTask = smithTasks.some((tpl) => tpl?.title === taskTitle);
      log(`smith family has new task: ${smithHasTask}`);
      if (!smithHasTask) {
        throw new Error("smith family did not contain the newly created task");
      }

      log("listing tasks as master acting as default family");
      const defaultTasks = await listEarnTemplates(BASE_URL, masterKey, "default");
      const defaultHasTask = defaultTasks.some((tpl) => tpl?.title === taskTitle);
      log(`default family contains smith task: ${defaultHasTask}`);
      if (defaultHasTask) {
        throw new Error("default family unexpectedly contained the smith task");
      }
    }
  );
}

export async function testF1() {
  const masterKey = (process.env.MASTER_ADMIN_KEY || "").trim();
  if (!masterKey) {
    throw new Error("MASTER_ADMIN_KEY environment variable is required for testF1");
  }

  const log = (message) => console.log(`[testF1] ${message}`);
  const dbPath = join(DATA_DIR, `smoke-f1-${Date.now()}.db`);

  await removeDbArtifacts(dbPath);
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    const seedDb = new Database(dbPath);
    try {
      const now = Date.now();
      seedDb.exec(`
        CREATE TABLE IF NOT EXISTS family (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS member (
          id TEXT PRIMARY KEY,
          name TEXT,
          date_of_birth TEXT,
          sex TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          family_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS task (
          id TEXT PRIMARY KEY,
          title TEXT,
          description TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          family_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reward (
          id TEXT PRIMARY KEY,
          name TEXT,
          description TEXT,
          cost INTEGER,
          status TEXT NOT NULL DEFAULT 'active',
          family_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ledger (
          id TEXT PRIMARY KEY,
          family_id TEXT NOT NULL,
          member_id TEXT,
          reward_id TEXT,
          amount INTEGER NOT NULL,
          verb TEXT,
          description TEXT,
          note TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
      `);
      seedDb.prepare(
        `INSERT INTO family (id, name, status, created_at, updated_at)
         VALUES (@id, @name, 'active', @now, @now)
         ON CONFLICT(id) DO UPDATE SET name=excluded.name, status=excluded.status, updated_at=excluded.updated_at`
      ).run({ id: DEFAULT_FAMILY_ID, name: "Default Family", now });
      const memberId = crypto.randomUUID();
      const taskId = crypto.randomUUID();
      const rewardId = crypto.randomUUID();
      const ledgerId = crypto.randomUUID();
      seedDb.prepare(
        `INSERT INTO member (id, name, status, family_id, created_at, updated_at)
         VALUES (@id, @name, 'active', @family_id, @now, @now)`
      ).run({ id: memberId, name: "Seed Kid", family_id: DEFAULT_FAMILY_ID, now });
      seedDb.prepare(
        `INSERT INTO task (id, title, description, status, family_id, created_at, updated_at)
         VALUES (@id, @title, @description, 'active', @family_id, @now, @now)`
      ).run({
        id: taskId,
        title: "Seed Task",
        description: "Ensure duplication",
        family_id: DEFAULT_FAMILY_ID,
        now
      });
      seedDb.prepare(
        `INSERT INTO reward (id, name, description, cost, status, family_id, created_at, updated_at)
         VALUES (@id, @name, @description, @cost, 'active', @family_id, @now, @now)`
      ).run({
        id: rewardId,
        name: "Seed Reward",
        description: "Ensure duplication",
        cost: 5,
        family_id: DEFAULT_FAMILY_ID,
        now
      });
      seedDb.prepare(
        `INSERT INTO ledger (id, family_id, member_id, reward_id, amount, verb, description, note, created_at, updated_at)
         VALUES (@id, @family_id, @member_id, @reward_id, @amount, @verb, @description, @note, @now, @now)`
      ).run({
        id: ledgerId,
        family_id: DEFAULT_FAMILY_ID,
        member_id: memberId,
        reward_id: rewardId,
        amount: 10,
        verb: "earn",
        description: "Seed duplication",
        note: "",
        now
      });
    } finally {
      seedDb.close();
    }

    await withServer(
      BASE_URL,
      {
        MASTER_ADMIN_KEY: masterKey,
        DB_PATH: dbPath
      },
      async () => {
        log("validating Jang family seed snapshot");
        const db = new Database(dbPath, { readonly: true });
        try {
          const familyRow = db.prepare("SELECT id FROM family WHERE id = ?").get(JANG_FAMILY_ID);
          if (!familyRow) {
            throw new Error("Jang family was not created by prestart seed");
          }
          const tables = ["member", "task", "reward", "ledger"];
          for (const table of tables) {
            const exists = db
              .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1")
              .get(table);
            if (!exists) {
              throw new Error(`expected table ${table} to exist for seed validation`);
            }
            const row = db
              .prepare(`SELECT COUNT(*) AS count FROM ${table} WHERE family_id = ?`)
              .get(JANG_FAMILY_ID);
            const rawCount = row?.count ?? 0;
            const count = typeof rawCount === "number" ? rawCount : Number(rawCount);
            log(`${table} rows for ${JANG_FAMILY_ID}: ${count}`);
            if (!Number.isFinite(count) || count <= 0) {
              throw new Error(`expected backup rows for ${table} in family ${JANG_FAMILY_ID}`);
            }
          }
        } finally {
          db.close();
        }

        log("verifying whoami for master key");
        const masterRes = await fetch(new URL("/api/whoami", BASE_URL), {
          headers: { "X-ADMIN-KEY": masterKey }
        });
        await ensureOk(masterRes, "master whoami");
        const masterPayload = await masterRes.json();
        if (masterPayload?.role !== "master") {
          throw new Error(`expected master role, received ${masterPayload?.role ?? "unknown"}`);
        }

        log("verifying whoami for seeded Jang admin key");
        const jangRes = await fetch(new URL("/api/whoami", BASE_URL), {
          headers: { "X-ADMIN-KEY": JANG_ADMIN_KEY }
        });
        await ensureOk(jangRes, "jang whoami");
        const jangPayload = await jangRes.json();
        if (
          jangPayload?.role !== "family_admin" ||
          (jangPayload?.family_id ?? null) !== JANG_FAMILY_ID
        ) {
          throw new Error("Jang admin key did not resolve to expected family scope");
        }

        log("verifying public rewards feed for Jang family");
        const jangRewardsRes = await fetch(
          new URL(`/api/public/rewards?family_id=${encodeURIComponent(JANG_FAMILY_ID)}`, BASE_URL)
        );
        await ensureOk(jangRewardsRes, "public rewards (jang)");
        const jangRewards = await jangRewardsRes.json();
        if (!Array.isArray(jangRewards) || jangRewards.length === 0) {
          throw new Error("expected public rewards for Jang family to be non-empty");
        }

        log("verifying public rewards feed for default family");
        const defaultRewardsRes = await fetch(
          new URL(`/api/public/rewards?family_id=${encodeURIComponent(DEFAULT_FAMILY_ID)}`, BASE_URL)
        );
        await ensureOk(defaultRewardsRes, "public rewards (default)");
        const defaultRewards = await defaultRewardsRes.json();
        if (!Array.isArray(defaultRewards) || defaultRewards.length === 0) {
          throw new Error("expected public rewards for default family to be non-empty");
        }

        const verifyDb = new Database(dbPath, { readonly: true });
        try {
          const jangRewardCountRow = verifyDb
            .prepare("SELECT COUNT(*) AS count FROM reward WHERE family_id = ?")
            .get(JANG_FAMILY_ID);
          const defaultRewardCountRow = verifyDb
            .prepare("SELECT COUNT(*) AS count FROM reward WHERE family_id = ?")
            .get(DEFAULT_FAMILY_ID);
          const jangRewardCount = Number(jangRewardCountRow?.count ?? 0);
          const defaultRewardCount = Number(defaultRewardCountRow?.count ?? 0);
          log(`public rewards returned ${jangRewards.length} Jang rows (expected ${jangRewardCount})`);
          log(`public rewards returned ${defaultRewards.length} default rows (expected ${defaultRewardCount})`);
          if (jangRewards.length !== jangRewardCount) {
            throw new Error("public rewards Jang count mismatch");
          }
          if (defaultRewards.length !== defaultRewardCount) {
            throw new Error("public rewards default count mismatch");
          }
        } finally {
          verifyDb.close();
        }
      }
    );
  } finally {
    await removeDbArtifacts(dbPath);
  }
}

export async function testC4() {
  const masterKey = (process.env.MASTER_ADMIN_KEY || "").trim();
  if (!masterKey) {
    throw new Error("MASTER_ADMIN_KEY environment variable is required for testC4");
  }

  const log = (message) => console.log(`[testC4] ${message}`);

  await withServer(
    BASE_URL,
    {
      MASTER_ADMIN_KEY: masterKey
    },
    async () => {
      log("ensuring smith family exists");
      const smithFamily = await ensureFamily(BASE_URL, masterKey, "smith");
      log(`smith family id: ${smithFamily.id}`);

      log("ensuring smith family admin key");
      const smithKey = await ensureFamilyAdminKey(BASE_URL, masterKey, smithFamily.id);
      log("smith admin key ready");

      const rewardName = `Smith Reward ${Date.now()}`;
      log(`creating smith reward ${rewardName}`);
      await createReward(
        BASE_URL,
        smithKey,
        { name: rewardName, cost: 25, description: "Smoke reward" }
      );

      log("listing rewards as smith family admin");
      const smithRewards = await listRewards(BASE_URL, smithKey);
      const smithHasReward = smithRewards.some(
        (reward) => reward?.name === rewardName || reward?.title === rewardName
      );
      log(`smith family has new reward: ${smithHasReward}`);
      if (!smithHasReward) {
        throw new Error("smith family did not contain the newly created reward");
      }

      log("listing rewards as master acting as default family");
      const defaultRewards = await listRewards(BASE_URL, masterKey, "default");
      const defaultHasReward = defaultRewards.some(
        (reward) => reward?.name === rewardName || reward?.title === rewardName
      );
      log(`default family contains smith reward: ${defaultHasReward}`);
      if (defaultHasReward) {
        throw new Error("default family unexpectedly contained the smith reward");
      }
    }
  );
}

export async function testC5() {
  const masterKey = (process.env.MASTER_ADMIN_KEY || "").trim();
  if (!masterKey) {
    throw new Error("MASTER_ADMIN_KEY environment variable is required for testC5");
  }

  const log = (message) => console.log(`[testC5] ${message}`);

  await withServer(
    BASE_URL,
    {
      MASTER_ADMIN_KEY: masterKey
    },
    async () => {
      log("ensuring smith family exists");
      const smithFamily = await ensureFamily(BASE_URL, masterKey, "smith");
      log(`smith family id: ${smithFamily.id}`);

      log("ensuring smith family admin key");
      const smithKey = await ensureFamilyAdminKey(BASE_URL, masterKey, smithFamily.id);
      log("smith admin key ready");

      const rewardName = `C5 Reward ${Date.now()}`;
      log(`creating smith reward ${rewardName}`);
      const reward = await createReward(
        BASE_URL,
        smithKey,
        { name: rewardName, cost: 15, description: "C5 guardrail" }
      );
      if (!reward?.id) {
        throw new Error("reward creation did not return id");
      }

      log("attempting to delete smith reward while scoped to default family");
      const deleteDefaultRes = await fetch(new URL(`/api/admin/rewards/${reward.id}`, BASE_URL), {
        method: "DELETE",
        headers: {
          "X-ADMIN-KEY": masterKey,
          "X-Act-As-Family": "default"
        }
      });
      if (deleteDefaultRes.ok) {
        throw new Error("default family unexpectedly deleted smith reward");
      }
      if (deleteDefaultRes.status !== 404) {
        const body = await deleteDefaultRes.text();
        throw new Error(
          `expected 404 when deleting across families, got ${deleteDefaultRes.status}: ${body}`
        );
      }
      log("cross-family delete correctly blocked");

      log("verifying reward still visible to smith family");
      const smithRewards = await listRewards(BASE_URL, smithKey);
      const smithStillHasReward = smithRewards.some(
        (entry) => entry?.id === reward.id || entry?.name === rewardName
      );
      if (!smithStillHasReward) {
        throw new Error("smith reward disappeared after blocked delete");
      }

      log("cleaning up reward via master scoped to smith");
      const deleteSmithRes = await fetch(new URL(`/api/admin/rewards/${reward.id}`, BASE_URL), {
        method: "DELETE",
        headers: {
          "X-ADMIN-KEY": masterKey,
          "X-Act-As-Family": smithFamily.id
        }
      });
      await ensureOk(deleteSmithRes, "delete smith reward with correct scope");
    }
  );
}

export async function testC6() {
  const masterKey = (process.env.MASTER_ADMIN_KEY || "").trim();
  if (!masterKey) {
    throw new Error("MASTER_ADMIN_KEY environment variable is required for testC6");
  }

  const log = (message) => console.log(`[testC6] ${message}`);

  await withServer(
    BASE_URL,
    {
      MASTER_ADMIN_KEY: masterKey
    },
    async () => {
      log("requesting /api/admin/members without family scope");
      const res = await fetch(new URL("/api/admin/members", BASE_URL), {
        headers: {
          "X-ADMIN-KEY": masterKey
        }
      });
      log(`status=${res.status}`);
      if (res.status !== 400) {
        const body = await res.text();
        throw new Error(`expected 400 when missing scope, got ${res.status}: ${body}`);
      }
      let payload;
      try {
        payload = await res.json();
      } catch {
        payload = null;
      }
      if (!payload?.error) {
        throw new Error("400 response did not include an error message");
      }
      if (payload.error !== "family_id required") {
        log(`warning: unexpected error payload ${JSON.stringify(payload)}`);
      }
    }
  );
}

const tests = {
  testC0,
  testC1,
  testC2,
  testC3,
  testC4,
  testC5,
  testC6,
  testF1
};

const isMainModule = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];

if (isMainModule) {
  const [, , ...args] = process.argv;
  const queue = args.length ? args : Object.keys(tests);
  let exitCode = 0;
  for (const name of queue) {
    const fn = tests[name];
    if (!fn) {
      console.error(`[smoke] Unknown test: ${name}`);
      exitCode = 1;
      continue;
    }
    try {
      await fn();
    } catch (err) {
      console.error(`[smoke] ${name} failed:`, err);
      exitCode = 1;
      if (args.length) {
        break;
      }
    }
  }
  if (exitCode) {
    process.exitCode = exitCode;
  }
}
