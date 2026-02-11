import { mkdir, writeFile } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import path from "node:path";
import { Pool } from "pg";

const DEFAULT_BATCH_SIZE = 5000;

const ALLOWED_TABLES = new Set([
  "meaningfulness_scores",
  "oe_data",
  "oe_series",
  "oe_occupations",
  "oe_industries",
  "oe_areas",
  "oe_areatypes",
  "oe_datatypes",
  "oe_footnotes",
  "oe_releases",
  "oe_seasonal",
  "oe_sectors",
]);

export function csvEscape(value: unknown) {
  if (value === null || value === undefined) {
    return "";
  }

  const stringValue = String(value);
  if (/[",\n\r]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }

  return stringValue;
}

export async function getColumns(pool: Pool, table: string) {
  const result = await pool.query(
    `
      select column_name
      from information_schema.columns
      where table_schema = 'public' and table_name = $1
      order by ordinal_position
    `,
    [table]
  );

  return result.rows.map((row) => row.column_name as string);
}

export async function exportTableToCSV(
  pool: Pool,
  table: string,
  outputPath: string,
  batchSize: number
): Promise<number> {
  await mkdir(path.dirname(outputPath), { recursive: true });
  await writeFile(outputPath, "");

  const stream = createWriteStream(outputPath, { flags: "a" });

  try {
    const columns = await getColumns(pool, table);
    if (columns.length === 0) {
      throw new Error(`No columns found for table ${table}`);
    }

    stream.write(`${columns.join(",")}\n`);

    let offset = 0;
    let totalRows = 0;
    while (true) {
      const result = await pool.query(
        `select ${columns.join(", ")} from ${table} order by 1 limit $1 offset $2`,
        [batchSize, offset]
      );

      if (result.rows.length === 0) {
        break;
      }

      for (const row of result.rows) {
        const line = columns.map((col) => csvEscape(row[col])).join(",");
        stream.write(`${line}\n`);
      }

      offset += result.rows.length;
      totalRows += result.rows.length;
    }

    return totalRows;
  } finally {
    stream.end();
  }
}

async function main() {
  const table = process.env.BACKUP_TABLE ?? "meaningfulness_scores";
  if (!ALLOWED_TABLES.has(table)) {
    throw new Error(
      `Unsupported BACKUP_TABLE: ${table}. Allowed: ${[...ALLOWED_TABLES].join(
        ", "
      )}`
    );
  }

  const outputPath =
    process.env.BACKUP_PATH ??
    path.resolve(process.cwd(), "data", "exports", `${table}.csv`);
  const batchSize = Number(process.env.BACKUP_BATCH_SIZE ?? DEFAULT_BATCH_SIZE);
  if (!Number.isFinite(batchSize) || batchSize <= 0) {
    throw new Error(`Invalid BACKUP_BATCH_SIZE: ${batchSize}`);
  }

  const pool = new Pool({
    connectionString:
      process.env.DATABASE_URL ?? "postgres://oews:oews@localhost:5432/oews",
  });

  try {
    const rowCount = await exportTableToCSV(pool, table, outputPath, batchSize);
    console.log(`Exported ${rowCount} rows from ${table} to ${outputPath}`);
  } finally {
    await pool.end();
  }
}

await main();
