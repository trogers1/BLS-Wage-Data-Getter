import { getDbInstance } from "../../db/index.ts";
import type { Wages } from "../../db/generated/db.d.ts";
import { TimeseriesResponse } from "../../schemas/schemas.ts";
import { validateResponse } from "../../schemas/validate.ts";
import { API_BASE_URL, WAGE_API_PATH } from "../constants.ts";

// Helper type for creating series data - matches the pattern used in testDataSeeder
type SeriesData = {
  series_id: string;
  soc_code: string;
  naics_code: string;
  does_exist: boolean;
  last_checked: Date | string;
};

export type BLSWageRequestBody = {
  seriesid: string[];
  startyear: string;
  endyear: string;
  registrationkey: string;
};

export function createSeriesId({ soc, naics }: { soc: string; naics: string }) {
  // Remove hyphens and pad NAICS to 6 digits
  const naicsPart = naics.replace(/-/g, "").padStart(6, "0");
  const socPart = soc.replace("-", "");
  return `OEUN${naicsPart}${socPart}03`;
}

export async function getWages({
  db,
  startYear,
  endYear,
}: {
  db: ReturnType<typeof getDbInstance>;
  startYear: number;
  endYear: number;
}): Promise<{ wages: Wages[]; oewsSeries: SeriesData[] }> {
  const API_KEY = process.env.BLS_API_KEY;

  if (!API_KEY) {
    throw new Error("BLS_API_KEY environment variable is required");
  }

  const socs = await db.selectFrom("soc_codes").select("soc_code").execute();
  const BATCH_SIZE = 50;

  const wages: Wages[] = [];
  const oewsSeries: SeriesData[] = [];

  for (const { soc_code } of socs) {
    // Start with level 2 NAICS codes (top-level industries)
    let queue = await db
      .selectFrom("naics_codes")
      .selectAll()
      .where("level", "=", 2)
      .execute();

    while (queue.length) {
      const batch: typeof queue = [];
      const seriesIds: string[] = [];
      const naicsBatch: typeof queue = [];

      // Build a batch of up to BATCH_SIZE NAICS codes to query
      while (queue.length && batch.length < BATCH_SIZE) {
        const naics = queue.pop()!;
        const sid = createSeriesId({ soc: soc_code, naics: naics.naics_code });

        // Check if this series has already been processed
        const existingSeries = await db
          .selectFrom("oews_series")
          .select("series_id")
          .where("series_id", "=", sid)
          .executeTakeFirst();

        if (existingSeries) {
          // Already processed, skip it
          continue;
        }

        batch.push(naics);
        seriesIds.push(sid);
        naicsBatch.push(naics);
      }

      if (seriesIds.length === 0) {
        continue;
      }
      const wageRequestBody: BLSWageRequestBody = {
        seriesid: seriesIds,
        startyear: startYear.toString(),
        endyear: endYear.toString(),
        registrationkey: API_KEY,
      };
      const fullURL = API_BASE_URL + WAGE_API_PATH;
      console.log("making POST request to: ", API_BASE_URL + WAGE_API_PATH);
      console.log("with body: ", JSON.stringify(wageRequestBody));
      console.log("with headers: ", { "Content-Type": "application/json" });

      // Fetch wage data for this batch
      const res = await fetch(fullURL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(wageRequestBody),
      });

      const json = await res.json();
      const validated = validateResponse(
        TimeseriesResponse,
        res,
        json,
        `timeseries data for batch of ${seriesIds.length} series from ${startYear}-${endYear}`
      );

      const seriesResults = new Map(
        validated.Results.series.map((s) => [s.seriesID, s])
      );

      // Process each NAICS code in the batch
      for (let i = 0; i < naicsBatch.length; i++) {
        const naics = naicsBatch[i];
        const sid = seriesIds[i];
        const series = seriesResults.get(sid);
        const wasFound = !!series;

        // Record the series information
        oewsSeries.push({
          series_id: sid,
          soc_code,
          naics_code: naics.naics_code,
          does_exist: wasFound,
          last_checked: new Date(),
        });

        // If data was found, extract wage information
        if (wasFound && series.data.length) {
          for (const dataPoint of series.data) {
            wages.push({
              series_id: sid,
              year: Number(dataPoint.year),
              mean_annual_wage: Number(dataPoint.value),
            });
          }
        }

        // If this NAICS code has wage data and is not at the deepest level,
        // add its children to the queue for further exploration
        if (wasFound && naics.level < 6) {
          const children = await db
            .selectFrom("naics_codes")
            .selectAll()
            .where("parent_code", "=", naics.naics_code)
            .execute();

          queue.push(...children);
        }
      }
    }
  }

  return { wages, oewsSeries };
}

export async function insertWagesIntoDb({
  wages,
  oewsSeries,
  db,
}: {
  wages: Wages[];
  oewsSeries: SeriesData[];
  db: ReturnType<typeof getDbInstance>;
}): Promise<void> {
  await db.transaction().execute(async (trx) => {
    // Insert series records
    for (const series of oewsSeries) {
      await trx
        .insertInto("oews_series")
        .values({
          series_id: series.series_id,
          soc_code: series.soc_code,
          naics_code: series.naics_code,
          does_exist: series.does_exist,
          last_checked: series.last_checked,
        })
        .onConflict((oc) => oc.column("series_id").doNothing())
        .execute();
    }

    // Insert wage records
    for (const wage of wages) {
      await trx
        .insertInto("wages")
        .values(wage)
        .onConflict((oc) => oc.columns(["series_id", "year"]).doNothing())
        .execute();
    }
  });

  console.log(
    `Loaded ${oewsSeries.length} series and ${wages.length} wage records`
  );
}
