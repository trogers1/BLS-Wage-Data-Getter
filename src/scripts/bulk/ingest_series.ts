import { Type } from "@sinclair/typebox";
import { getDbInstance } from "../../db/index.ts";
import { SeriesRow } from "../../schemas/bulk.ts";
import { validate } from "../../schemas/validate.ts";
import { createLineReader, getBulkFilePath } from "./utils.ts";
import { parseSeriesLine } from "./parsers.ts";

const SERIES_BATCH_SIZE = Number(process.env.INGEST_BATCH_SIZE) || 1000;

async function insertSeriesBatch(
  db: ReturnType<typeof getDbInstance>,
  batch: ReturnType<typeof parseSeriesLine>[]
) {
  const validated = validate(Type.Array(SeriesRow), batch, "oe.series");

  await db
    .insertInto("oe_series")
    .values(validated)
    .onConflict((oc) => oc.column("series_id").doNothing())
    .execute();
}

export async function ingestSeriesFile() {
  const filePath = getBulkFilePath("oe.series");
  const reader = createLineReader(filePath);
  const db = getDbInstance();

  let isHeader = true;
  let batch: ReturnType<typeof parseSeriesLine>[] = [];

  try {
    for await (const line of reader) {
      if (isHeader) {
        isHeader = false;
        if (!line.trim().startsWith("series_id")) {
          throw new Error(`Unexpected oe.series header: ${line}`);
        }
        continue;
      }

      if (!line.trim()) {
        continue;
      }

      const parsed = parseSeriesLine(line);

      // Filter to national-level data only (state_code === "00")
      if (parsed.state_code !== "00") {
        continue;
      }

      batch.push(parsed);
      if (batch.length >= SERIES_BATCH_SIZE) {
        await insertSeriesBatch(db, batch);
        batch = [];
      }
    }

    if (batch.length > 0) {
      await insertSeriesBatch(db, batch);
    }
  } finally {
    await db.destroy();
  }
}
