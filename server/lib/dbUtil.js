import { db } from "../db.js";

function quoteIdentifier(name) {
  return `"${String(name).replaceAll("\"", "\"\"")}"`;
}

export function tableExists(name) {
  return !!db
    .prepare(
      "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name = ?"
    )
    .get(name);
}

export function tableColumns(table) {
  if (!tableExists(table)) return [];
  const sql = `PRAGMA table_info(${quoteIdentifier(table)})`;
  return db.prepare(sql).all().map((column) => column.name);
}

export function hasColumn(table, column) {
  return tableColumns(table).includes(column);
}
