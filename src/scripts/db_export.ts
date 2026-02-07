import { mkdir, writeFile } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import path from "node:path";
import { Pool } from "pg";

const DEFAULT_TABLE = "meaningfulness_scores";
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

function csvEscape(value: unknown) {
  if (value === null || value === undefined) {
    return "";
  }

  const stringValue = String(value);
  if (/[",\n\r]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }

  return stringValue;
}

async function getColumns(pool: Pool, table: string) {
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

async function exportTable() {
  const table = process.env.EXPORT_TABLE ?? DEFAULT_TABLE;
  if (!ALLOWED_TABLES.has(table)) {
    throw new Error(
      `Unsupported EXPORT_TABLE: ${table}. Allowed: ${[...ALLOWED_TABLES].join(
        ", "
      )}`
    );
  }

  const outputPath =
    process.env.EXPORT_PATH ??
    path.resolve(process.cwd(), "data", "exports", `${table}.csv`);
  const batchSize = Number(process.env.EXPORT_BATCH_SIZE ?? DEFAULT_BATCH_SIZE);
  if (!Number.isFinite(batchSize) || batchSize <= 0) {
    throw new Error(`Invalid EXPORT_BATCH_SIZE: ${batchSize}`);
  }

  await mkdir(path.dirname(outputPath), { recursive: true });
  await writeFile(outputPath, "");

  const pool = new Pool({
    connectionString:
      process.env.DATABASE_URL ?? "postgres://oews:oews@localhost:5432/oews",
  });

  const stream = createWriteStream(outputPath, { flags: "a" });

  try {
    const columns = await getColumns(pool, table);
    if (columns.length === 0) {
      throw new Error(`No columns found for table ${table}`);
    }

    stream.write(`${columns.join(",")}\n`);

    let offset = 0;
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
    }
  } finally {
    stream.end();
    await pool.end();
  }

  console.log(`Exported ${table} to ${outputPath}`);
}

await exportTable();
