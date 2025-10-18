import { db } from "../db.js";

function quoteIdentifier(name) {
  return `"${String(name).replaceAll("\"", "\"\"")}"`;
}

export function tableColumns(table) {
  const sql = `PRAGMA table_info(${quoteIdentifier(table)})`;
  return db.prepare(sql).all().map((column) => column.name);
}

export function hasColumn(table, column) {
  return tableColumns(table).includes(column);
}
