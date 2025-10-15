import crypto from "node:crypto";

console.log("[prestart] starting DB checks...");
const { default: db } = await import("../server/db.js");
console.log("[prestart] DB checks done.");

function sha256(input) {
  return crypto.createHash("sha256").update(String(input)).digest("hex");
}

function ensureDefaultFamily() {
  const now = Date.now();
  const familyCountRow = db.prepare("SELECT COUNT(*) as count FROM family").get();
  if (familyCountRow?.count > 0) {
    return;
  }
  db.prepare(
    `INSERT OR IGNORE INTO family (id, name, status, created_at, updated_at)
     VALUES (@id, @name, 'active', @now, @now)`
  ).run({ id: "default", name: "Default Family", now });
}

function ensureMasterAdminKey() {
  const envKey = (process.env.MASTER_ADMIN_KEY || "").trim();
  if (!envKey) {
    return false;
  }
  const existing = db.prepare("SELECT id FROM admin_key WHERE role = 'master' LIMIT 1").get();
  if (existing) {
    return true;
  }
  const now = Date.now();
  const id = crypto.randomUUID();
  db.prepare(
    `INSERT INTO admin_key (id, key_hash, role, family_id, label, status, created_at, updated_at)
     VALUES (@id, @hash, 'master', NULL, NULL, 'active', @now, @now)`
  ).run({ id, hash: sha256(envKey), now });
  return true;
}

function ensureDefaultFamilyAdminKey() {
  const existing = db
    .prepare(
      "SELECT id FROM admin_key WHERE role = 'family_admin' AND family_id = 'default' LIMIT 1"
    )
    .get();
  if (existing) {
    return null;
  }
  const plainKey = crypto.randomBytes(24).toString("hex");
  const now = Date.now();
  const id = crypto.randomUUID();
  db.prepare(
    `INSERT INTO admin_key (id, key_hash, role, family_id, label, status, created_at, updated_at)
     VALUES (@id, @hash, 'family_admin', 'default', 'Default Family Admin', 'active', @now, @now)`
  ).run({ id, hash: sha256(plainKey), now });
  return plainKey;
}

ensureDefaultFamily();
const masterKeyPresent = ensureMasterAdminKey();
const defaultFamilyAdminKey = ensureDefaultFamilyAdminKey();

const familyCount = db.prepare("SELECT COUNT(*) as count FROM family").get()?.count ?? 0;
const adminKeyCount = db.prepare("SELECT COUNT(*) as count FROM admin_key").get()?.count ?? 0;

console.log(`families: ${familyCount}`);
console.log(`admin_keys: ${adminKeyCount}`);
if (!masterKeyPresent) {
  console.warn("MASTER_ADMIN_KEY env var missing or blank; master admin key not created");
}
if (defaultFamilyAdminKey) {
  console.log("[DEV ONLY] Default family admin key:", defaultFamilyAdminKey);
}

process.exit(0);
