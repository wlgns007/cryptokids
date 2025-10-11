import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { spawnSync } from "node:child_process";
import Database from "better-sqlite3";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const serverPath = join(__dirname, "..", "server", "index.js");
const startLine = 790;
const endLine = 835;
const source = readFileSync(serverPath, "utf8").split("\n");
const excerpt = source.slice(startLine - 1, endLine);

console.log(`\n--- server/index.js (lines ${startLine}-${endLine}) ---`);
console.log(excerpt.join("\n"));
console.log("--- end excerpt ---\n");

const db = new Database(process.env.DB_PATH || "./data.sqlite", { fileMustExist: false });
const cols = db.prepare("PRAGMA table_info(ledger)").all().map(c => `${c.name}:${c.type}`);
console.log("Ledger columns at prestart:", cols.join(", "));
db.close();

const result = spawnSync(process.execPath, ["--check", serverPath], {
  stdio: "inherit"
});

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}
