import { Type } from "@sinclair/typebox";
import { getDbInstance } from "../../db/index.ts";
import { DataRow } from "../../schemas/bulk.ts";
import { validate } from "../../schemas/validate.ts";
import { createLineReader, getBulkFilePath } from "./utils.ts";
import { parseDataLine } from "./parsers.ts";

const DATA_BATCH_SIZE = Number(process.env.INGEST_BATCH_SIZE) || 2000;

async function insertDataBatch(
  db: ReturnType<typeof getDbInstance>,
  batch: ReturnType<typeof parseDataLine>[]
) {
  const validated = validate(Type.Array(DataRow), batch, "oe.data.0.Current");

  await db
    .insertInto("oe_data")
    .values(validated)
    .onConflict((oc) => oc.columns(["series_id", "year", "period"]).doNothing())
    .execute();
}

export async function ingestDataFile() {
  const filePath = getBulkFilePath("oe.data.0.Current");

  // Load series IDs FIRST before opening the file stream
  // This prevents file stream buffering issues while waiting for DB query
  const db = getDbInstance();
  const seriesRows = await db
    .selectFrom("oe_series")
    .select("series_id")
    .execute();
  const validSeriesIds = new Set(seriesRows.map((r) => r.series_id));
  console.log(
    `Loaded ${validSeriesIds.size} series_ids from oe_series for filtering`
  );

  // NOW open the file stream after series are loaded
  const reader = createLineReader(filePath);

  let isHeader = true;
  let batch: ReturnType<typeof parseDataLine>[] = [];
  let skippedCount = 0;

  try {
    for await (const line of reader) {
      if (isHeader) {
        isHeader = false;
        if (!line.trim().startsWith("series_id")) {
          throw new Error(`Unexpected oe.data header: ${line}`);
        }
        continue;
      }

      if (!line.trim()) {
        continue;
      }

      const parsed = parseDataLine(line);

      // Filter: Only ingest data rows where series_id exists in oe_series
      if (!validSeriesIds.has(parsed.series_id)) {
        skippedCount++;
        continue;
      }

      batch.push(parsed);
      if (batch.length >= DATA_BATCH_SIZE) {
        await insertDataBatch(db, batch);
        batch = [];
      }
    }

    if (batch.length > 0) {
      await insertDataBatch(db, batch);
    }

    console.log(
      `Skipped ${skippedCount} data rows (series_id not in oe_series)`
    );
  } finally {
    await db.destroy();
  }
}
