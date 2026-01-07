import { db } from "../db";
import { migrateToLatest } from "../db/migrate";

async function applyMigrations() {
  await migrateToLatest({ db });

  await db.destroy();
}

applyMigrations();
