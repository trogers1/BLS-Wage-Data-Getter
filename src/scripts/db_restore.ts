import { createReadStream } from "node:fs";
import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { createInterface } from "node:readline";
import { Pool, types } from "pg";

const DEFAULT_BATCH_SIZE = 1000;

// Tables must be restored in dependency order (FK constraints)
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

// Columns that should be parsed as JSON
const JSON_COLUMNS = ["source_inputs"];

// Disable default parsing for numeric types to preserve precision
// 1700 = NUMERIC
const NUMERIC_OID = 1700;
types.setTypeParser(NUMERIC_OID, (val) => val);

async function parseCSVLine(line: string): Promise<string[]> {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        // Escaped quote
        current += '"';
        i++; // Skip next quote
      } else {
        // Toggle quote state
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += char;
    }
  }

  // Don't forget the last field
  result.push(current);
  return result;
}

async function restoreTable(
  pool: Pool,
  table: string,
  backupDir: string,
  batchSize: number
) {
  const filePath = path.join(backupDir, `${table}.csv`);

  // Check if file exists
  try {
    await readFile(filePath);
  } catch {
    console.log(`  Skipping ${table} - no backup file found`);
    return 0;
  }

  const fileStream = createReadStream(filePath);
  const rl = createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let columns: string[] = [];
  let batch: string[][] = [];
  let totalRows = 0;
  let lineNumber = 0;

  // First, truncate the table
  await pool.query(`TRUNCATE TABLE ${table} CASCADE`);

  for await (const line of rl) {
    lineNumber++;

    if (lineNumber === 1) {
      // Header row - parse column names
      columns = await parseCSVLine(line);
      continue;
    }

    const values = await parseCSVLine(line);
    batch.push(values);

    if (batch.length >= batchSize) {
      await insertBatch(pool, table, columns, batch);
      totalRows += batch.length;
      batch = [];
    }
  }

  // Insert remaining rows
  if (batch.length > 0) {
    await insertBatch(pool, table, columns, batch);
    totalRows += batch.length;
  }

  return totalRows;
}

async function insertBatch(
  pool: Pool,
  table: string,
  columns: string[],
  batch: string[][]
) {
  // Build the INSERT query with parameterized values
  const valueRows: string[] = [];
  const values: (string | object | null)[] = [];
  let paramIndex = 1;

  for (const row of batch) {
    const placeholders: string[] = [];
    for (let i = 0; i < columns.length; i++) {
      const value = row[i];
      const columnName = columns[i];

      if (value === "" || value === undefined) {
        placeholders.push("NULL");
      } else if (JSON_COLUMNS.includes(columnName)) {
        // Parse JSON column
        try {
          const parsed = JSON.parse(value);
          placeholders.push(`$${paramIndex++}`);
          values.push(JSON.stringify(parsed));
        } catch {
          // If not valid JSON, store as string
          placeholders.push(`$${paramIndex++}`);
          values.push(value);
        }
      } else {
        placeholders.push(`$${paramIndex++}`);
        values.push(value);
      }
    }
    valueRows.push(`(${placeholders.join(", ")})`);
  }

  const query = `
    INSERT INTO ${table} (${columns.join(", ")})
    VALUES ${valueRows.join(", ")}
  `;

  await pool.query(query, values);
}

async function restoreAll() {
  const backupDir =
    process.env.BACKUP_DIR ?? path.resolve(process.cwd(), "data", "backup");
  const batchSize = Number(
    process.env.RESTORE_BATCH_SIZE ?? DEFAULT_BATCH_SIZE
  );
  if (!Number.isFinite(batchSize) || batchSize <= 0) {
    throw new Error(`Invalid RESTORE_BATCH_SIZE: ${batchSize}`);
  }

  // Verify backup directory exists
  try {
    await readdir(backupDir);
  } catch {
    throw new Error(`Backup directory not found: ${backupDir}`);
  }

  // Check for metadata file
  const metadataPath = path.join(backupDir, "backup_metadata.json");
  let tablesToRestore = TABLES_IN_ORDER;
  try {
    const metadata = JSON.parse(await readFile(metadataPath, "utf-8"));
    console.log(`Restoring from backup created: ${metadata.timestamp}`);
    if (metadata.tables) {
      tablesToRestore = metadata.tables.filter((t: string) =>
        TABLES_IN_ORDER.includes(t)
      );
    }
  } catch {
    console.log("No metadata file found, using default table order");
  }

  const pool = new Pool({
    connectionString:
      process.env.DATABASE_URL ?? "postgres://oews:oews@localhost:5432/oews",
  });

  console.log(`Starting restore from: ${backupDir}`);
  console.log("-".repeat(60));

  const results: { table: string; rows: number }[] = [];

  try {
    // Disable foreign key checks for faster loading
    await pool.query("SET session_replication_role = 'replica'");

    for (const table of tablesToRestore) {
      process.stdout.write(`Restoring ${table}... `);
      const rowCount = await restoreTable(pool, table, backupDir, batchSize);
      results.push({ table, rows: rowCount });
      console.log(`${rowCount} rows`);
    }

    // Re-enable foreign key checks
    await pool.query("SET session_replication_role = 'origin'");
  } finally {
    await pool.end();
  }

  console.log("-".repeat(60));
  console.log("Restore complete!");
  console.log(
    `\nTotal rows restored: ${results.reduce((sum, r) => sum + r.rows, 0)}`
  );
}

await restoreAll();
