import { db } from "./index.ts";
import { FileMigrationProvider, Migrator } from "kysely";
import { promises as fs } from "fs";
import path from "path";

async function migrateToLatest() {
  const migrator = new Migrator({
    db,
    provider: new FileMigrationProvider({
      fs,
      path,
      migrationFolder: path.join(import.meta.dirname, "migrations"),
    }),
  });

  const { error, results } = await migrator.migrateToLatest();
  let resultError: string | undefined = undefined;

  results?.forEach((it) => {
    if (it.status === "Success") {
      console.log(`Migration "${it.migrationName}" was executed successfully`);
    } else if (it.status === "Error") {
      console.error(`Failed to execute migration "${it.migrationName}"`);
      resultError = `${it.status}: ${it.migrationName}`;
    }
  });

  if (error || resultError) {
    console.error("Failed to migrate");
    console.error(error);
    console.error(resultError);
    process.exit(1);
  }

  await db.destroy();
}

migrateToLatest();
