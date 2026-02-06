import { Type } from "@sinclair/typebox";
import { getDbInstance } from "../../db/index.ts";
import { SeriesRow } from "../../schemas/bulk.ts";
import { validate } from "../../schemas/validate.ts";
import { createLineReader, getBulkFilePath } from "./utils.ts";

const SERIES_BATCH_SIZE = 1000;

const PREFIX_FIELD_SPECS = [
  { name: "series_id", length: 30 },
  { name: "seasonal", length: 1 },
  { name: "areatype_code", length: 1 },
  { name: "industry_code", length: 6 },
  { name: "occupation_code", length: 6 },
  { name: "datatype_code", length: 2 },
  { name: "state_code", length: 2 },
  { name: "area_code", length: 7 },
  { name: "sector_code", length: 6 },
];

const SUFFIX_LENGTH = 10 + 4 + 3 + 4 + 3;

function parseSeriesLine(line: string) {
  let cursor = 0;
  const parsed: Record<string, string> = {};

  for (const spec of PREFIX_FIELD_SPECS) {
    parsed[spec.name] = line.slice(cursor, cursor + spec.length).trim();
    cursor += spec.length;
  }

  const suffixStart = Math.max(cursor, line.length - SUFFIX_LENGTH);
  const seriesTitle = line.slice(cursor, suffixStart).trim();
  const suffix = line.slice(suffixStart);

  if (suffix.length < SUFFIX_LENGTH) {
    throw new Error(`Invalid oe.series line length: ${line}`);
  }

  const footnoteCodes = suffix.slice(0, 10).trim();
  const beginYear = suffix.slice(10, 14).trim();
  const beginPeriod = suffix.slice(14, 17).trim();
  const endYear = suffix.slice(17, 21).trim();
  const endPeriod = suffix.slice(21, 24).trim();

  const beginYearValue = Number(beginYear);
  const endYearValue = Number(endYear);

  if (Number.isNaN(beginYearValue) || Number.isNaN(endYearValue)) {
    throw new Error(`Invalid year in oe.series line: ${line}`);
  }

  return {
    series_id: parsed.series_id,
    seasonal: parsed.seasonal,
    areatype_code: parsed.areatype_code,
    industry_code: parsed.industry_code,
    occupation_code: parsed.occupation_code,
    datatype_code: parsed.datatype_code,
    state_code: parsed.state_code,
    area_code: parsed.area_code,
    sector_code: parsed.sector_code,
    series_title: seriesTitle,
    footnote_codes: footnoteCodes.length > 0 ? footnoteCodes : null,
    begin_year: beginYearValue,
    begin_period: beginPeriod,
    end_year: endYearValue,
    end_period: endPeriod,
  };
}

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

      batch.push(parseSeriesLine(line));
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

await ingestSeriesFile();
