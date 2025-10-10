import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const serverPath = join(__dirname, "..", "server", "index.js");
const source = readFileSync(serverPath, "utf8");

const issues = [];

if (/\bawait\s+ensureColumn\b/.test(source)) {
  issues.push("ensureColumn is synchronous; remove await ensureColumn(...)");
}

if (/\bawait\s+ensureSchema\b/.test(source)) {
  issues.push("ensureSchema runs synchronously inside sqliteTransaction; drop await ensureSchema(...)");
}

if (/\bawait\s+ensureTables\b/.test(source)) {
  issues.push("ensureTables runs synchronously; drop await ensureTables(...)");
}

if (/sqliteTransaction\(\s*async\b/.test(source)) {
  issues.push("sqliteTransaction should wrap synchronous handlers; remove async handlers in schema helpers");
}

if (issues.length > 0) {
  console.error("Schema sync check failed:\n" + issues.map(issue => ` - ${issue}`).join("\n"));
  process.exit(1);
}
