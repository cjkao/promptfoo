import { DatabaseSync, StatementSync } from 'node:sqlite';
import { entityKind, fillPlaceholders } from 'drizzle-orm';
import { BaseSQLiteDatabase } from 'drizzle-orm/sqlite-core';
import { SQLiteSession, SQLiteTransaction, SQLitePreparedQuery } from 'drizzle-orm/sqlite-core';
import { SQLiteSyncDialect } from 'drizzle-orm/sqlite-core';
import type { RelationalSchemaConfig, TablesRelationalConfig } from 'drizzle-orm/relations';
import type { Query } from 'drizzle-orm/sql/sql';
import type { Logger } from 'drizzle-orm/logger';
import { DefaultLogger } from 'drizzle-orm/logger';
import type { SQLiteExecuteMethod, SQLiteTransactionConfig } from 'drizzle-orm/sqlite-core';
import type { SelectedFieldsOrdered } from 'drizzle-orm/sqlite-core/query-builders/select.types';
import type { PreparedQueryConfig as PreparedQueryConfigBase } from 'drizzle-orm/sqlite-core/session';

export interface NodeSQLiteRunResult {
  changes: number;
  lastInsertRowid: number | bigint;
}

export type NodeSQLiteDatabaseConfig = {
  client: DatabaseSync;
};

export class NodeSQLiteDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>
> extends BaseSQLiteDatabase<'sync', NodeSQLiteRunResult, TSchema> {
  static readonly [entityKind]: string = 'NodeSQLiteDatabase';
}

type PreparedQueryConfig = Omit<PreparedQueryConfigBase, 'statement' | 'run'>;

export class NodeSQLiteSession<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig
> extends SQLiteSession<'sync', NodeSQLiteRunResult, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'NodeSQLiteSession';

  private logger?: Logger;

  constructor(
    private client: DatabaseSync,
    dialect: SQLiteSyncDialect,
    schema: RelationalSchemaConfig<TSchema> | undefined,
    options?: { logger?: Logger }
  ) {
    super(dialect);
    this.logger = options?.logger;
  }

  prepareQuery<T extends Omit<PreparedQueryConfig, 'run'>>(
    query: Query,
    fields: SelectedFieldsOrdered | undefined,
    executeMethod: SQLiteExecuteMethod,
    isResponseInArrayMode: boolean,
    customResultMapper?: (rows: unknown[][], mapColumnValue?: (value: unknown) => unknown) => unknown,
    queryMetadata?: { type: string; tables: string[] },
    cacheConfig?: any
  ): PreparedQuery<T> {
    const stmt = this.client.prepare(query.sql);
    return new PreparedQuery(
      stmt,
      query,
      this.logger,
      fields,
      executeMethod,
      isResponseInArrayMode,
      customResultMapper
    ) as unknown as PreparedQuery<T>;
  }

  transaction<T>(
    transaction: (tx: NodeSQLiteTransaction<TFullSchema, TSchema>) => T,
    config?: SQLiteTransactionConfig
  ): T {
    const tx = new NodeSQLiteTransaction('sync', this.client, (this as any).dialect, this as any);

    let runResult: T;

    this.client.exec('BEGIN');
    try {
      runResult = transaction(tx as any);
      this.client.exec('COMMIT');
    } catch (err) {
      this.client.exec('ROLLBACK');
      throw err;
    }

    return runResult;
  }
}

export class NodeSQLiteTransaction<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig
> extends SQLiteTransaction<'sync', NodeSQLiteRunResult, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'NodeSQLiteTransaction';

  constructor(
    type: 'sync',
    private client: DatabaseSync,
    dialect: SQLiteSyncDialect,
    session: SQLiteSession<'sync', NodeSQLiteRunResult, TFullSchema, TSchema>
  ) {
    super('sync', dialect, session, undefined as any);
  }

  transaction<T>(
    transaction: (tx: NodeSQLiteTransaction<TFullSchema, TSchema>) => T
  ): T {
    return ((this as any).session as NodeSQLiteSession<TFullSchema, TSchema>).transaction(transaction);
  }
}

export class PreparedQuery<T extends PreparedQueryConfig = PreparedQueryConfig> extends SQLitePreparedQuery<{
  type: 'sync';
  run: NodeSQLiteRunResult;
  all: T['all'];
  get: T['get'];
  values: T['values'];
  execute: T['execute'];
}> {
  static readonly [entityKind]: string = 'NodeSQLitePreparedQuery';

  constructor(
    private stmt: StatementSync,
    query: Query,
    private logger: Logger | undefined,
    private fields: SelectedFieldsOrdered | undefined,
    executeMethod: SQLiteExecuteMethod,
    private _isResponseInArrayMode: boolean,
    private customResultMapper?: (rows: unknown[][]) => unknown
  ) {
    super('sync', executeMethod, query);
  }

  run(placeholderValues?: Record<string, unknown>): NodeSQLiteRunResult {
    const params = fillPlaceholders(this.query.params, placeholderValues ?? {});
    this.logger?.logQuery(this.query.sql, params);

    if (params.length > 0) {
      const mappedParams = params.map(p => {
         if (typeof p === 'boolean') return p ? 1 : 0;
         return p as any;
      });
      return this.stmt.run(...mappedParams) as NodeSQLiteRunResult;
    }
    return this.stmt.run() as NodeSQLiteRunResult;
  }

  all(placeholderValues?: Record<string, unknown>): T['all'] {
    const params = fillPlaceholders(this.query.params, placeholderValues ?? {});
    this.logger?.logQuery(this.query.sql, params);

    const { customResultMapper } = this;

    let result: unknown[];
    const mappedParams = params.map(p => {
       if (typeof p === 'boolean') return p ? 1 : 0;
       return p as any;
    });

    if (mappedParams.length > 0) {
      if (this._isResponseInArrayMode) {
        // We simulate array mode by calling all() which returns objects, then mapping to arrays
        const rows = this.stmt.all(...mappedParams) as Record<string, unknown>[];
        result = rows.map(r => Object.values(r));
      } else {
        result = this.stmt.all(...mappedParams) as unknown[];
      }
    } else {
      if (this._isResponseInArrayMode) {
        const rows = this.stmt.all() as Record<string, unknown>[];
        result = rows.map(r => Object.values(r));
      } else {
        result = this.stmt.all() as unknown[];
      }
    }

    if (customResultMapper) {
      return customResultMapper(result as unknown[][]) as T['all'];
    }

    return result as T['all'];
  }

  get(placeholderValues?: Record<string, unknown>): T['get'] {
    const params = fillPlaceholders(this.query.params, placeholderValues ?? {});
    this.logger?.logQuery(this.query.sql, params);

    let result: unknown;
    const mappedParams = params.map(p => {
       if (typeof p === 'boolean') return p ? 1 : 0;
       return p as any;
    });

    if (mappedParams.length > 0) {
      if (this._isResponseInArrayMode) {
        const row = this.stmt.get(...mappedParams) as Record<string, unknown> | undefined;
        result = row ? Object.values(row) : undefined;
      } else {
        result = this.stmt.get(...mappedParams);
      }
    } else {
      if (this._isResponseInArrayMode) {
        const row = this.stmt.get() as Record<string, unknown> | undefined;
        result = row ? Object.values(row) : undefined;
      } else {
        result = this.stmt.get();
      }
    }

    const { customResultMapper } = this;

    if (customResultMapper) {
      return customResultMapper([result as unknown[]]) as T['get'];
    }

    return result as T['get'];
  }

  values(placeholderValues?: Record<string, unknown>): T['values'] {
    const params = fillPlaceholders(this.query.params, placeholderValues ?? {});
    this.logger?.logQuery(this.query.sql, params);

    const mappedParams = params.map(p => {
       if (typeof p === 'boolean') return p ? 1 : 0;
       return p as any;
    });

    let result: unknown[];
    if (mappedParams.length > 0) {
      const rows = this.stmt.all(...mappedParams) as Record<string, unknown>[];
      result = rows.map(r => Object.values(r));
    } else {
      const rows = this.stmt.all() as Record<string, unknown>[];
      result = rows.map(r => Object.values(r));
    }

    return result as T['values'];
  }
}

export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>
>(
  client: DatabaseSync,
  config?: { logger?: Logger | boolean; schema?: TSchema }
): NodeSQLiteDatabase<TSchema> {
  const dialect = new SQLiteSyncDialect();
  let logger: Logger | undefined;
  if (config?.logger === true) {
    logger = new DefaultLogger();
  } else if (typeof config?.logger === 'object' && config.logger !== null) {
    logger = config.logger as Logger;
  }

  const session = new NodeSQLiteSession(client, dialect, config?.schema as any, { logger });
  return new NodeSQLiteDatabase('sync', dialect, session as any, config?.schema as any);
}
