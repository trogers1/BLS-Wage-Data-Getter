import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { Pool } from "pg";
import { exportTableToCSV, getColumns } from "./db_backup_table.ts";

const DEFAULT_BATCH_SIZE = 5000;

// Tables must be exported in a consistent order
const TABLES_IN_ORDER = [
  // Reference tables (no FKs)
  "oe_occupations",
  "oe_industries",
  "oe_areatypes",
  "oe_datatypes",
  "oe_sectors",
  "oe_footnotes",
  "oe_releases",
  "oe_seasonal",
  // Dependent tables
  "oe_areas",
  "oe_series",
  "oe_data",
  "meaningfulness_scores",
];

async function backupAll() {
  const outputDir =
    process.env.BACKUP_DIR ?? path.resolve(process.cwd(), "data", "backup");
  const batchSize = Number(process.env.BACKUP_BATCH_SIZE ?? DEFAULT_BATCH_SIZE);
  if (!Number.isFinite(batchSize) || batchSize <= 0) {
    throw new Error(`Invalid BACKUP_BATCH_SIZE: ${batchSize}`);
  }

  await mkdir(outputDir, { recursive: true });

  // Write metadata file
  const metadata = {
    timestamp: new Date().toISOString(),
    tables: TABLES_IN_ORDER,
  };
  await writeFile(
    path.join(outputDir, "backup_metadata.json"),
    JSON.stringify(metadata, null, 2)
  );

  const pool = new Pool({
    connectionString:
      process.env.DATABASE_URL ?? "postgres://oews:oews@localhost:5432/oews",
  });

  console.log(`Starting backup to: ${outputDir}`);
  console.log("-".repeat(60));

  const results: { table: string; rows: number }[] = [];

  try {
    for (const table of TABLES_IN_ORDER) {
      process.stdout.write(`Exporting ${table}... `);
      const outputPath = path.join(outputDir, `${table}.csv`);
      const rowCount = await exportTableToCSV(
        pool,
        table,
        outputPath,
        batchSize
      );
      results.push({ table, rows: rowCount });
      console.log(`${rowCount} rows`);
    }
  } finally {
    await pool.end();
  }

  console.log("-".repeat(60));
  console.log("Backup complete!");
  console.log(
    `\nTotal rows exported: ${results.reduce((sum, r) => sum + r.rows, 0)}`
  );
  console.log(`\nBackup location: ${outputDir}`);
  console.log(`\nTo restore, run: npm run db:restore`);
}

await backupAll();
