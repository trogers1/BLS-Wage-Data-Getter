import { Type } from "@sinclair/typebox";
import { getDbInstance } from "../../db/index.ts";
import { DataRow } from "../../schemas/bulk.ts";
import { validate } from "../../schemas/validate.ts";
import { createLineReader, getBulkFilePath } from "./utils.ts";
import { parseDataLine } from "./parsers.ts";

const DATA_BATCH_SIZE = 2000;

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
  const reader = createLineReader(filePath);
  const db = getDbInstance();

  let isHeader = true;
  let batch: ReturnType<typeof parseDataLine>[] = [];

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

      batch.push(parseDataLine(line));
      if (batch.length >= DATA_BATCH_SIZE) {
        await insertDataBatch(db, batch);
        batch = [];
      }
    }

    if (batch.length > 0) {
      await insertDataBatch(db, batch);
    }
  } finally {
    await db.destroy();
  }
}

await ingestDataFile();
