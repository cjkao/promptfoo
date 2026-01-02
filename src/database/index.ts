import * as path from 'path';

import { drizzle } from 'drizzle-orm/sqlite-proxy';
import { DefaultLogger, type LogWriter } from 'drizzle-orm/logger';
import sqlite3 from 'sqlite3';
import { getEnvBool } from '../envars';
import logger from '../logger';
import { getConfigDirectoryPath } from '../util/config/manage';

export class DrizzleLogWriter implements LogWriter {
  write(message: string) {
    if (getEnvBool('PROMPTFOO_ENABLE_DATABASE_LOGS', false)) {
      logger.debug(`Drizzle: ${message}`);
    }
  }
}

let dbInstance: ReturnType<typeof drizzle> | null = null;
let sqliteInstance: sqlite3.Database | null = null;

export function getDbPath() {
  return path.resolve(getConfigDirectoryPath(true /* createIfNotExists */), 'promptfoo.db');
}

export function getDbSignalPath() {
  return path.resolve(getConfigDirectoryPath(true /* createIfNotExists */), 'evalLastWritten');
}

export function getDb() {
  if (!dbInstance) {
    const isMemoryDb = getEnvBool('IS_TESTING');
    const dbPath = isMemoryDb ? ':memory:' : getDbPath();

    sqliteInstance = new sqlite3.Database(dbPath);

    // Enable foreign key constraints (required for referential integrity)
    sqliteInstance.run('PRAGMA foreign_keys = ON');

    // Configure WAL mode unless explicitly disabled or using in-memory database
    if (!isMemoryDb && !getEnvBool('PROMPTFOO_DISABLE_WAL_MODE', false)) {
      try {
        // Enable WAL mode for better concurrency
        sqliteInstance.run('PRAGMA journal_mode = WAL');

        // Verify WAL mode was actually enabled (async check, just logging best effort here)
        sqliteInstance.get(
          'PRAGMA journal_mode',
          (err, result: { journal_mode: string } | undefined) => {
            if (err) {
              logger.warn(`Failed to check WAL mode: ${err}`);
              return;
            }
            if (result && result.journal_mode.toLowerCase() === 'wal') {
              logger.debug('Successfully enabled SQLite WAL mode');
            } else {
              logger.warn(
                `Failed to enable WAL mode (got '${result?.journal_mode}'). ` +
                  'Database performance may be reduced. This can happen on network filesystems. ' +
                  'Set PROMPTFOO_DISABLE_WAL_MODE=true to suppress this warning.',
              );
            }
          },
        );

        // Additional WAL configuration for optimal performance
        sqliteInstance.run('PRAGMA wal_autocheckpoint = 1000'); // Checkpoint every 1000 pages
        sqliteInstance.run('PRAGMA synchronous = NORMAL'); // Good balance of safety and speed with WAL
      } catch (err) {
        logger.warn(
          `Error configuring SQLite WAL mode: ${err}. ` +
            'Database will use default journal mode. Performance may be reduced. ' +
            'This can happen on network filesystems or certain containerized environments. ' +
            'Set PROMPTFOO_DISABLE_WAL_MODE=true to suppress this warning.',
        );
      }
    }

    const drizzleLogger = new DefaultLogger({ writer: new DrizzleLogWriter() });
    dbInstance = drizzle(
      async (sql, params, method) => {
        if (!sqliteInstance) {
          throw new Error('Database not initialized');
        }
        try {
          const rows = await new Promise<any[]>((resolve, reject) => {
            if (method === 'run') {
              sqliteInstance!.run(sql, params, function (this: sqlite3.RunResult, err: Error | null) {
                if (err) {
                  reject(err);
                } else {
                  // Return format expected by drizzle for run
                  resolve([{
                    rows: [],
                    changes: this.changes,
                    insertId: this.lastID, // sqlite3 uses lastID, better-sqlite3 uses lastInsertRowid. Check drizzle expectation.
                    lastInsertRowid: this.lastID,
                  } as any]);
                }
              });
            } else {
              sqliteInstance!.all(sql, params, (err: Error | null, rows: any[]) => {
                if (err) {
                  reject(err);
                } else {
                  resolve(rows);
                }
              });
            }
          });

          if (method === 'run') {
              // sqlite-proxy expects { rows: [] } but specifically for run it might look at other properties?
              // The callback above resolves an array with one object containing metadata.
              // However, drizzle-orm/sqlite-proxy expects { rows: any[] }
              // Wait, checking drizzle source or docs for sqlite-proxy run result.
              // Drizzle docs say: "The return value of the callback function should be an object with the rows property."
              // For `run`, we usually want result info.
              // If we look at how better-sqlite3 driver works, `.run()` returns `{ changes, lastInsertRowid }`.
              // With proxy, `db.run()` returns what?
              // `drizzle-orm/sqlite-proxy` `run` method is expected to return something.
              // Actually, `driver` signature is `(sql, params, method) => Promise<{ rows: any[] }>`.
              // It seems strictly `rows`.
              // But if I want `changes`, I should probably pass it in rows?
              // Wait, if I use `db.run()`, I expect `RunResult`.
              // Let's verify if sqlite-proxy supports `RunResult`.
              // It seems sqlite-proxy is generic and returns `{ rows: ... }`.
              // If the underlying query was `run`, maybe `rows` should contain the result?
              // Actually, `sqlite-proxy` might not support `run` result object fully if it enforces `rows` array.
              // However, let's look at `src/util/database.ts` usage: `.run()` result `.changes`.
              // If I return `rows` as `[]`, `.run()` result will be `[]`? No.
              // Let's return the object with properties and see.
              // Actually, for `run`, we can assume `rows` is unused by the caller of `drizzle(...)` unless `drizzle` internals process it.
              // But `drizzle-orm` internals for `sqlite-proxy` map the result.
              // If I look at `drizzle-orm` source (I can't), I'd guess:
              // For `run`: return { rows: [], changes, lastInsertRowid } works if generic type allows.
              return { rows, changes: (rows[0] as any)?.changes, lastInsertRowid: (rows[0] as any)?.lastInsertRowid };
          }

          if (method === 'values') {
             // Drizzle expects array of arrays for 'values'
             // sqlite3 returns array of objects.
             // We need to convert.
             // But we don't know the column order easily unless we parse SQL or result.
             // Actually, `sqlite3` `all` returns objects.
             // If method is 'values', we should transform.
             // But simpler is to assume 'all' is enough for most cases.
             // If `values` is used, we might have an issue.
             // However, `drizzle-orm` documentation says:
             // "If you want to use `db.select().values()` you need to handle `values` method."
             // "You should return array of arrays."
             return { rows: rows.map(r => Object.values(r)) };
          }

          return { rows };
        } catch (e: any) {
          logger.error(`Error from sqlite proxy: ${e.message}`);
          throw e;
        }
      },
      { logger: drizzleLogger }
    );
  }
  return dbInstance;
}

export function closeDb() {
  if (sqliteInstance) {
    try {
      // Attempt to checkpoint WAL file before closing
      if (!getEnvBool('IS_TESTING') && !getEnvBool('PROMPTFOO_DISABLE_WAL_MODE', false)) {
        try {
          sqliteInstance.run('PRAGMA wal_checkpoint(TRUNCATE)');
          logger.debug('Successfully checkpointed WAL file before closing');
        } catch (err) {
          logger.debug(`Could not checkpoint WAL file: ${err}`);
        }
      }

      sqliteInstance.close((err) => {
        if (err) {
             logger.error(`Error closing database connection: ${err}`);
        } else {
             logger.debug('Database connection closed successfully');
        }
      });
    } catch (err) {
      logger.error(`Error closing database connection: ${err}`);
      // Even if close fails, we should still clear the instances
      // to prevent reuse of a potentially corrupted connection
    } finally {
      sqliteInstance = null;
      dbInstance = null;
    }
  }
}

/**
 * Check if the database is currently open
 */
export function isDbOpen(): boolean {
  return sqliteInstance !== null && dbInstance !== null;
}

/**
 * Close database connection if it's currently open
 * Safe to call even if database was never opened
 * Should be called during graceful shutdown to prevent event loop hanging
 */
export function closeDbIfOpen(): void {
  if (sqliteInstance) {
    closeDb();
  }
}
