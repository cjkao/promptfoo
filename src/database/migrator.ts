import { readMigrationFiles } from 'drizzle-orm/migrator';

export function migrate(db, config) {
  const migrations = readMigrationFiles(config);
  db.dialect.migrate(migrations, db.session, config);
}
