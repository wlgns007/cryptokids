import { db } from '../db.js';

export function tableExists(name) {
  return !!db.prepare(
    `SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name=?`
  ).get(name);
}
export function hasColumn(table, col) {
  if (!tableExists(table)) return false;
  return db.prepare(`PRAGMA table_info(${table})`).all().some(c => c.name === col);
}
