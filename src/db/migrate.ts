import { getDbInstance } from "./index.ts";
import { FileMigrationProvider, Migrator } from "kysely";
import { promises as fs } from "fs";
import path from "path";
import { log } from "../utils/logger.ts";

export async function migrateToLatest({
  db,
  exitOnError = true,
}: {
  db: ReturnType<typeof getDbInstance>;
  exitOnError?: boolean;
}) {
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
      log(`Migration "${it.migrationName}" was executed successfully`);
    } else if (it.status === "Error") {
      console.error(`Failed to execute migration "${it.migrationName}"`);
      resultError = `${it.status}: ${it.migrationName}`;
    }
  });

  if (error || resultError) {
    console.error("Failed to migrate");
    console.error(error);
    console.error(resultError);
    if (exitOnError) {
      process.exit(1);
    } else {
      throw error || new Error(resultError);
    }
  }
}
