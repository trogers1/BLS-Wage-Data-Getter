import { getDbInstance } from "../db/index.ts";
import { migrateToLatest } from "../db/migrate.ts";

async function applyMigrations() {
  const db = getDbInstance();
  await migrateToLatest({ db });

  await db.destroy();
}

await applyMigrations();
