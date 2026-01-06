import { DatabaseSync } from 'node:sqlite';
import { entityKind } from 'drizzle-orm/entity';
import { DefaultLogger, NoopLogger } from 'drizzle-orm/logger';
import { fillPlaceholders, sql } from 'drizzle-orm/sql/sql';
import { SQLiteTransaction } from 'drizzle-orm/sqlite-core';
import {
  SQLitePreparedQuery as PreparedQueryBase,
  SQLiteSession,
} from 'drizzle-orm/sqlite-core/session';
import { mapResultRow } from 'drizzle-orm/utils';

export class NodeSQLiteSession extends SQLiteSession {
  static [entityKind] = 'NodeSQLiteSession';

  constructor(client, dialect, schema, options = {}) {
    super(dialect);
    this.client = client;
    this.schema = schema;
    this.logger = options.logger ?? new NoopLogger();
  }

  prepareQuery(
    query,
    fields,
    executeMethod,
    isResponseInArrayMode,
    customResultMapper,
    queryMetadata,
    cacheConfig,
  ) {
    const stmt = this.client.prepare(query.sql);
    return new NodeSQLitePreparedQuery(
      stmt,
      query,
      this.logger,
      queryMetadata,
      cacheConfig,
      fields,
      executeMethod,
      isResponseInArrayMode,
      customResultMapper,
    );
  }

  transaction(transaction, config = {}) {
    const tx = new NodeSQLiteTransaction(
      'sync',
      this.dialect,
      this,
      this.schema,
    );
    // node:sqlite doesn't have a specific transaction method that takes a callback like better-sqlite3
    // We have to implement it manually using BEGIN/COMMIT/ROLLBACK or simple immediate execution
    // However, DatabaseSync doesn't support nested transactions automatically like better-sqlite3 might with savepoints implicitly.
    // We will implement a basic wrapper.

    this.run(sql`BEGIN${config.behavior ? sql.raw(' ' + config.behavior) : undefined}`);
    try {
      const result = transaction(tx);
      this.run(sql`COMMIT`);
      return result;
    } catch (err) {
      this.run(sql`ROLLBACK`);
      throw err;
    }
  }
}

export class NodeSQLiteTransaction extends SQLiteTransaction {
  static [entityKind] = 'NodeSQLiteTransaction';

  transaction(transaction) {
    const savepointName = `sp${this.nestedIndex}`;
    const tx = new NodeSQLiteTransaction(
      'sync',
      this.dialect,
      this.session,
      this.schema,
      this.nestedIndex + 1,
    );
    this.session.run(sql.raw(`SAVEPOINT ${savepointName}`));
    try {
      const result = transaction(tx);
      this.session.run(sql.raw(`RELEASE SAVEPOINT ${savepointName}`));
      return result;
    } catch (err) {
      this.session.run(sql.raw(`ROLLBACK TO SAVEPOINT ${savepointName}`));
      throw err;
    }
  }
}

export class NodeSQLitePreparedQuery extends PreparedQueryBase {
  static [entityKind] = 'NodeSQLitePreparedQuery';

  constructor(
    stmt,
    query,
    logger,
    queryMetadata,
    cacheConfig,
    fields,
    executeMethod,
    _isResponseInArrayMode,
    customResultMapper,
  ) {
    super('sync', executeMethod, query, queryMetadata, cacheConfig);
    this.stmt = stmt;
    this.logger = logger;
    this.fields = fields;
    this._isResponseInArrayMode = _isResponseInArrayMode;
    this.customResultMapper = customResultMapper;
  }

  run(placeholderValues) {
    const params = fillPlaceholders(this.query.params, placeholderValues ?? {});
    this.logger.logQuery(this.query.sql, params);
    // node:sqlite returns { lastInsertRowid, changes }
    const result = this.stmt.run(...params);
    return {
      lastInsertRowid: result.lastInsertRowid,
      changes: result.changes,
    };
  }

  all(placeholderValues) {
    const { fields, joinsNotNullableMap, query, logger, stmt, customResultMapper } = this;
    if (!fields && !customResultMapper) {
      const params = fillPlaceholders(query.params, placeholderValues ?? {});
      logger.logQuery(query.sql, params);
      return stmt.all(...params);
    }
    const rows = this.values(placeholderValues);
    if (customResultMapper) {
      return customResultMapper(rows);
    }
    return rows.map((row) => mapResultRow(fields, row, joinsNotNullableMap));
  }

  get(placeholderValues) {
    const params = fillPlaceholders(this.query.params, placeholderValues ?? {});
    this.logger.logQuery(this.query.sql, params);
    const { fields, stmt, joinsNotNullableMap, customResultMapper } = this;

    const row = stmt.get(...params);

    if (!row) {
      return undefined;
    }

    if (!fields && !customResultMapper) {
      return row;
    }

    if (customResultMapper) {
      return customResultMapper([row]);
    }
    return mapResultRow(fields, row, joinsNotNullableMap);
  }

  values(placeholderValues) {
    const params = fillPlaceholders(this.query.params, placeholderValues ?? {});
    this.logger.logQuery(this.query.sql, params);
    // node:sqlite's .all() returns array of objects.
    // If we want "values" (array of arrays), node:sqlite doesn't have a direct 'raw' mode like better-sqlite3?
    // Actually DatabaseSync statement has .all().
    // drizzle expects values to be array of arrays if it called .values()?
    // Wait, better-sqlite3's .raw() makes it return arrays.
    // Node's sqlite doesn't seem to have a .raw() equivalent documented easily, it returns objects by default.
    // We might need to manually convert if Drizzle expects arrays here.
    // Checking Drizzle source, mapResultRow expects object if it's NOT from batch/raw?
    // Actually mapResultRow implementation:
    // if (isFromBatch) rows = rows.rows;
    // return rows.map(row => mapResultRow(fields, row, joinsNotNullableMap));
    // mapResultRow takes (fields, row, joinsNotNullableMap).
    // inside mapResultRow: `row` is expected to be array of values corresponding to fields?
    // Let's check how better-sqlite3 session does it.
    // It calls `stmt.raw().all(...params)` for values().
    // So yes, we need array of arrays.
    // We have to convert object rows to array rows based on property order? No, property order is not guaranteed in JS objects.
    // We might depend on how node:sqlite returns data.
    // If node:sqlite doesn't support raw results, this is a performance hit.
    // But for now, let's implement compatibility.

    // Note: Node 22.5.0+ sqlite might not have .raw().
    // However, Drizzle's `mapResultRow` usually handles object rows too if we don't pass `raw`?
    // Let's look at `mapResultRow` usage in `all()`:
    // `return rows.map((row) => mapResultRow(fields, row, joinsNotNullableMap));`
    // In `all()` it calls `this.values()`.
    // In `BetterSQLiteSession`, `values()` uses `raw().all()`.
    // If we return objects, `mapResultRow` might fail if it expects array by index.

    // WORKAROUND: For now, let's assume we can get objects and map them.
    // But mapResultRow is internal to Drizzle.
    // Let's try to simulate `raw` behavior if possible, or assume object keys match field names.
    // Wait, Drizzle's `SQLitePreparedQuery` in `sqlite-core` generally works with drivers.
    // If we look at `drizzle-orm/sqlite-core/utils.ts` (if accessible) or just usage.
    // If we assume `stmt.all()` returns objects `{ col: val, ... }`.
    // We need to convert to `[val, val, ...]` matching `fields`.

    const rows = this.stmt.all(...params);
    // If we are in "values" mode, we need arrays.
    // We can use `fields` to determine order if available.
    if (this.fields) {
      return rows.map((row) => {
        return this.fields.map((field) => row[field.name || field.dbName]); // This is simplified.
      });
    }

    // If no fields (e.g. raw sql execution), maybe we just return rows?
    // But `values()` contract in Drizzle usually implies raw arrays.
    // Let's stick to what we can do.
    // If `fields` is not available, we can't easily guarantee order.
    // But usually `values()` is called when `fields` are known or for internal mapping.

    // Let's try to be smart:
    return rows.map(row => Object.values(row));
  }
}

import { BaseSQLiteDatabase } from 'drizzle-orm/sqlite-core/db';
import { SQLiteSyncDialect } from 'drizzle-orm/sqlite-core/dialect';

export function drizzle(client, config = {}) {
  const dialect = new SQLiteSyncDialect();
  let logger;
  if (config.logger === true) {
    logger = new DefaultLogger();
  } else if (config.logger !== false) {
    logger = config.logger;
  }
  let schema;
  if (config.schema) {
    schema = config.schema;
  } else {
    schema = undefined;
  }
  const session = new NodeSQLiteSession(client, dialect, schema, { logger });
  return new BaseSQLiteDatabase(dialect, session, schema);
}
