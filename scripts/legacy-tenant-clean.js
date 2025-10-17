#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import Database from 'better-sqlite3';

const DEFAULT_DATA_DIR = path.resolve(process.cwd(), 'data');
const DEFAULT_DB = process.env.DB_FILE || process.env.DB_PATH || path.join(DEFAULT_DATA_DIR, 'cryptokids.db');

function usage() {
  console.log(`Usage: node scripts/legacy-tenant-clean.js [options]

Options:
  --legacy-family-pk=<value>    Primary key of the legacy family (family.id).
  --legacy-family-key=<value>   Legacy family key (family.family_id) if present.
  --archive / --no-archive      Copy rows into archive_* tables before delete (default: archive).
  --execute                     Apply the deletion. Without this flag the script is a dry run.
  --cleanup-orphans             Remove orphaned rows after deletion (requires --execute).
  --db=<path>                   Path to the SQLite database file (default: ${DEFAULT_DB}).
  -h, --help                    Show this message.
`);
  process.exit(0);
}

function parseArgs(argv) {
  const options = {
    legacyFamilyPk: undefined,
    legacyFamilyKey: undefined,
    archive: true,
    execute: false,
    cleanupOrphans: false,
    dbPath: DEFAULT_DB
  };

  for (let i = 0; i < argv.length; i += 1) {
    const raw = argv[i];
    if (!raw) continue;

    if (raw === '--help' || raw === '-h') {
      usage();
    }

    if (raw.startsWith('--legacy-family-pk=')) {
      options.legacyFamilyPk = raw.slice('--legacy-family-pk='.length).trim();
      continue;
    }
    if (raw === '--legacy-family-pk') {
      options.legacyFamilyPk = (argv[++i] ?? '').trim();
      continue;
    }
    if (raw.startsWith('--legacy-family-key=')) {
      options.legacyFamilyKey = raw.slice('--legacy-family-key='.length).trim();
      continue;
    }
    if (raw === '--legacy-family-key') {
      options.legacyFamilyKey = (argv[++i] ?? '').trim();
      continue;
    }
    if (raw === '--archive') {
      options.archive = true;
      continue;
    }
    if (raw === '--no-archive') {
      options.archive = false;
      continue;
    }
    if (raw === '--execute') {
      options.execute = true;
      continue;
    }
    if (raw === '--cleanup-orphans') {
      options.cleanupOrphans = true;
      continue;
    }
    if (raw.startsWith('--db=')) {
      options.dbPath = raw.slice('--db='.length).trim();
      continue;
    }
    if (raw === '--db') {
      options.dbPath = (argv[++i] ?? '').trim();
      continue;
    }

    console.error(`Unknown option: ${raw}`);
    process.exit(1);
  }

  if (!options.legacyFamilyPk && !options.legacyFamilyKey) {
    console.error('Error: --legacy-family-pk or --legacy-family-key is required.');
    process.exit(1);
  }

  return options;
}

function quoteIdent(name) {
  return `"${name.replaceAll('"', '""')}"`;
}

function tableExists(db, name) {
  if (!name) return false;
  const row = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name = ?`).get(name);
  return Boolean(row?.name);
}

function ensureArchiveTable(db, tableName) {
  if (!tableExists(db, tableName)) {
    return;
  }
  const archiveName = `archive_${tableName}`;
  if (tableExists(db, archiveName)) {
    return;
  }
  db.exec(`CREATE TABLE IF NOT EXISTS ${quoteIdent(archiveName)} AS SELECT * FROM ${quoteIdent(tableName)} WHERE 0`);
}

function getFamilyRow(db, pkColumn, pkValue) {
  const stmt = db.prepare(`SELECT * FROM family WHERE ${quoteIdent(pkColumn)} = ? LIMIT 1`);
  return stmt.get(pkValue);
}

function lookupFamilyByKey(db, keyColumn, value) {
  const stmt = db.prepare(`SELECT * FROM family WHERE ${quoteIdent(keyColumn)} = ? LIMIT 1`);
  return stmt.get(value);
}

function getFamilyColumns(db) {
  return db.prepare(`PRAGMA table_info('family')`).all().map((row) => row.name);
}

function getLedgerMemberColumn(db) {
  const columns = db.prepare(`PRAGMA table_info('ledger')`).all().map((row) => row.name);
  return columns.includes('member_id') ? 'member_id' : 'user_id';
}

function getMemberTaskMemberColumn(db) {
  if (!tableExists(db, 'member_task')) {
    return null;
  }
  const columns = db.prepare(`PRAGMA table_info('member_task')`).all().map((row) => row.name);
  if (columns.includes('member_id')) {
    return 'member_id';
  }
  if (columns.includes('memberId')) {
    return 'memberId';
  }
  return null;
}

function countRows(db, sql, params = []) {
  const stmt = db.prepare(sql);
  const row = Array.isArray(params) ? stmt.get(...params) : stmt.get(params);
  return Number(row?.count ?? 0);
}

function gatherCounts(db, familyId, ledgerMemberColumn, memberTaskMemberColumn) {
  const members = countRows(db, `SELECT COUNT(*) AS count FROM member WHERE family_id = ?`, [familyId]);
  const tasks = countRows(db, `SELECT COUNT(*) AS count FROM task WHERE family_id = ?`, [familyId]);
  const ledger = countRows(
    db,
    `SELECT COUNT(*) AS count FROM ledger WHERE family_id = ? OR ${ledgerMemberColumn} IN (SELECT id FROM member WHERE family_id = ?)` ,
    [familyId, familyId]
  );
  let memberTasks;
  let orphanMemberTasks;
  if (memberTaskMemberColumn) {
    memberTasks = countRows(
      db,
      `SELECT COUNT(*) AS count FROM member_task WHERE ${memberTaskMemberColumn} IN (SELECT id FROM member WHERE family_id = ?)` ,
      [familyId]
    );
    orphanMemberTasks = countRows(
      db,
      `SELECT COUNT(*) AS count FROM member_task WHERE ${memberTaskMemberColumn} NOT IN (SELECT id FROM member)` ,
      []
    );
  }

  const tasksWithoutFamily = countRows(db, `SELECT COUNT(*) AS count FROM task WHERE family_id IS NULL`, []);
  const ledgerWithoutFamily = countRows(db, `SELECT COUNT(*) AS count FROM ledger WHERE family_id IS NULL`, []);

  return {
    members,
    tasks,
    ledger,
    memberTasks,
    orphans: {
      tasksWithoutFamily,
      ledgerWithoutFamily,
      memberTasksWithoutMember: orphanMemberTasks
    }
  };
}

function archiveFamilyData(db, familyId, ledgerMemberColumn, memberTaskMemberColumn) {
  ensureArchiveTable(db, 'family');
  ensureArchiveTable(db, 'member');
  ensureArchiveTable(db, 'task');
  ensureArchiveTable(db, 'ledger');
  if (memberTaskMemberColumn) {
    ensureArchiveTable(db, 'member_task');
  }

  const archiveFamily = db.prepare(`INSERT INTO archive_family SELECT * FROM family WHERE id = ?`).run(familyId).changes;
  const archiveMembers = db.prepare(`INSERT INTO archive_member SELECT * FROM member WHERE family_id = ?`).run(familyId).changes;
  const archiveTasks = db.prepare(`INSERT INTO archive_task SELECT * FROM task WHERE family_id = ?`).run(familyId).changes;
  const archiveLedger = db
    .prepare(
      `INSERT INTO archive_ledger
       SELECT * FROM ledger
        WHERE family_id = ?
           OR ${ledgerMemberColumn} IN (SELECT id FROM member WHERE family_id = ?)`
    )
    .run(familyId, familyId).changes;

  let archiveMemberTasks;
  if (memberTaskMemberColumn) {
    archiveMemberTasks = db
      .prepare(
        `INSERT INTO archive_member_task
         SELECT * FROM member_task WHERE ${memberTaskMemberColumn} IN (SELECT id FROM member WHERE family_id = ?)`
      )
      .run(familyId).changes;
  }

  return {
    family: archiveFamily,
    members: archiveMembers,
    tasks: archiveTasks,
    ledger: archiveLedger,
    memberTasks: archiveMemberTasks
  };
}

function deleteFamilyCascade(db, familyId, ledgerMemberColumn, memberTaskMemberColumn) {
  const deleteMemberTaskStmt = memberTaskMemberColumn
    ? db.prepare(
        `DELETE FROM member_task WHERE ${memberTaskMemberColumn} IN (SELECT id FROM member WHERE family_id = @familyId)`
      )
    : null;
  const deleteLedgerStmt = db.prepare(
    `DELETE FROM ledger WHERE family_id = @familyId OR ${ledgerMemberColumn} IN (SELECT id FROM member WHERE family_id = @familyId)`
  );
  const deleteTasksStmt = db.prepare(`DELETE FROM task WHERE family_id = @familyId`);
  const deleteMembersStmt = db.prepare(`DELETE FROM member WHERE family_id = @familyId`);
  const deleteFamilyStmt = db.prepare(`DELETE FROM family WHERE id = @familyId`);

  const runCascade = db.transaction((payload) => {
    const removed = {
      family: 0,
      members: 0,
      tasks: 0,
      ledger: 0,
      memberTasks: memberTaskMemberColumn ? 0 : undefined
    };
    if (deleteMemberTaskStmt) {
      removed.memberTasks = deleteMemberTaskStmt.run(payload).changes;
    }
    removed.ledger = deleteLedgerStmt.run(payload).changes;
    removed.tasks = deleteTasksStmt.run(payload).changes;
    removed.members = deleteMembersStmt.run(payload).changes;
    removed.family = deleteFamilyStmt.run(payload).changes;
    return removed;
  });

  return runCascade({ familyId });
}

function cleanupOrphans(db, ledgerMemberColumn) {
  const removeTasks = db.prepare(`DELETE FROM task WHERE family_id IS NULL`).run().changes;
  const removeLedger = db
    .prepare(
      `DELETE FROM ledger WHERE family_id IS NULL AND (${ledgerMemberColumn} IS NULL OR ${ledgerMemberColumn} NOT IN (SELECT id FROM member))`
    )
    .run().changes;
  return { tasks: removeTasks, ledger: removeLedger };
}

function redactFamily(row) {
  const clone = { ...row };
  if (Object.prototype.hasOwnProperty.call(clone, 'admin_key')) {
    clone.admin_key = '<redacted>';
  }
  if (Object.prototype.hasOwnProperty.call(clone, 'adminKey')) {
    clone.adminKey = '<redacted>';
  }
  return clone;
}

function main() {
  try {
    const options = parseArgs(process.argv.slice(2));
    const dbPath = options.dbPath || DEFAULT_DB;
    const dbDir = path.dirname(dbPath);

    if (!fs.existsSync(dbDir)) {
      fs.mkdirSync(dbDir, { recursive: true });
    }

    if (!fs.existsSync(dbPath)) {
      console.error(`Database file not found at ${dbPath}`);
      process.exit(1);
    }

    const database = new Database(dbPath);
    database.pragma('foreign_keys = ON');

    const familyColumns = getFamilyColumns(database);
    const hasFamilyIdColumn = familyColumns.includes('family_id');
    const pkColumn = familyColumns.includes('id') ? 'id' : familyColumns[0];

    let targetRow;

    if (options.legacyFamilyPk) {
      targetRow = getFamilyRow(database, pkColumn, options.legacyFamilyPk);
      if (!targetRow) {
        console.error(`Family not found for ${pkColumn} = ${options.legacyFamilyPk}`);
        process.exit(1);
      }
    }

    if (options.legacyFamilyKey) {
      const lookupColumn = hasFamilyIdColumn ? 'family_id' : pkColumn;
      const byKey = lookupFamilyByKey(database, lookupColumn, options.legacyFamilyKey);
      if (!byKey) {
        console.error(`Family not found for ${lookupColumn} = ${options.legacyFamilyKey}`);
        process.exit(1);
      }
      if (targetRow && byKey?.id !== targetRow?.id) {
        console.error('Mismatch between --legacy-family-pk and --legacy-family-key. Aborting.');
        process.exit(1);
      }
      targetRow = byKey;
    }

    if (!targetRow) {
      console.error('Unable to resolve target family.');
      process.exit(1);
    }

    if (String(targetRow.id) === 'default') {
      console.error('Refusing to remove the default master family.');
      process.exit(1);
    }

    const familyId = String(targetRow.id);

    const ledgerMemberColumn = getLedgerMemberColumn(database);
    const memberTaskMemberColumn = getMemberTaskMemberColumn(database);
    const hasMemberTaskTable = Boolean(memberTaskMemberColumn);

    const preCounts = gatherCounts(database, familyId, ledgerMemberColumn, memberTaskMemberColumn);

    console.log('--- Legacy tenant cleanup ---');
    console.log(`Database: ${dbPath}`);
    console.log('Target family:');
    console.dir(redactFamily(targetRow), { depth: null });
    console.log('Pre-state counts:');
    console.log(`  members: ${preCounts.members}`);
    console.log(`  tasks: ${preCounts.tasks}`);
    console.log(`  ledger entries: ${preCounts.ledger}`);
    if (hasMemberTaskTable) {
      console.log(`  member_task rows: ${preCounts.memberTasks ?? 0}`);
    }
    console.log('  Orphans:');
    console.log(`    task.family_id IS NULL: ${preCounts.orphans.tasksWithoutFamily}`);
    console.log(`    ledger.family_id IS NULL: ${preCounts.orphans.ledgerWithoutFamily}`);
    if (hasMemberTaskTable) {
      const memberColumnLabel = memberTaskMemberColumn ?? 'member_id';
      console.log(
        `    member_task.${memberColumnLabel} NOT IN member: ${preCounts.orphans.memberTasksWithoutMember ?? 0}`
      );
    }

    let archiveSummary;
    let deleteSummary;
    let cleanupSummary;

    if (options.execute) {
      if (options.archive) {
        archiveSummary = archiveFamilyData(database, familyId, ledgerMemberColumn, memberTaskMemberColumn);
      }

      deleteSummary = deleteFamilyCascade(database, familyId, ledgerMemberColumn, memberTaskMemberColumn);
      database.exec('VACUUM');

      if (options.cleanupOrphans) {
        cleanupSummary = cleanupOrphans(database, ledgerMemberColumn);
      }
    } else if (options.cleanupOrphans) {
      console.log('Skipping orphan cleanup because --execute was not provided.');
    }

    const postCounts = gatherCounts(database, familyId, ledgerMemberColumn, memberTaskMemberColumn);

    if (archiveSummary) {
      console.log('Archived rows:');
      console.log(`  family: ${archiveSummary.family}`);
      console.log(`  members: ${archiveSummary.members}`);
      console.log(`  tasks: ${archiveSummary.tasks}`);
      console.log(`  ledger: ${archiveSummary.ledger}`);
      if (archiveSummary.memberTasks !== undefined) {
        console.log(`  member_task: ${archiveSummary.memberTasks}`);
      }
    }

    if (deleteSummary) {
      console.log('Deleted rows:');
      console.log(`  family: ${deleteSummary.family}`);
      console.log(`  members: ${deleteSummary.members}`);
      console.log(`  tasks: ${deleteSummary.tasks}`);
      console.log(`  ledger: ${deleteSummary.ledger}`);
      if (deleteSummary.memberTasks !== undefined) {
        console.log(`  member_task: ${deleteSummary.memberTasks}`);
      }
    }

    if (cleanupSummary) {
      console.log('Orphan cleanup removed:');
      console.log(`  task rows: ${cleanupSummary.tasks}`);
      console.log(`  ledger rows: ${cleanupSummary.ledger}`);
    }

    console.log('Post-state counts:');
    console.log(`  members: ${postCounts.members}`);
    console.log(`  tasks: ${postCounts.tasks}`);
    console.log(`  ledger entries: ${postCounts.ledger}`);
    if (hasMemberTaskTable) {
      console.log(`  member_task rows: ${postCounts.memberTasks ?? 0}`);
    }
    console.log('  Orphans:');
    console.log(`    task.family_id IS NULL: ${postCounts.orphans.tasksWithoutFamily}`);
    console.log(`    ledger.family_id IS NULL: ${postCounts.orphans.ledgerWithoutFamily}`);
    if (hasMemberTaskTable) {
      const memberColumnLabel = memberTaskMemberColumn ?? 'member_id';
      console.log(
        `    member_task.${memberColumnLabel} NOT IN member: ${postCounts.orphans.memberTasksWithoutMember ?? 0}`
      );
    }

    if (!options.execute) {
      console.log('Dry run complete. Re-run with --execute to apply changes.');
    }

    database.close();
  } catch (err) {
    console.error('Fatal error:', err instanceof Error ? err.message : err);
    if (err instanceof Error && err.stack) {
      console.error(err.stack);
    }
    process.exitCode = 1;
  }
}

main();

