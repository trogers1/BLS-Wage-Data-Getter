import { db } from "../db";
import { TimeseriesResponse } from "../schemas";
import { validateResponse } from "../schemas/validate";

const API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/";
const API_KEY = process.env.BLS_API_KEY!;

if (!API_KEY) {
  throw new Error("BLS_API_KEY environment variable is required");
}

function createSeriesId({ soc, naics }: { soc: string; naics: string }) {
  return `OEUN0000000${naics}${soc.replace("-", "")}03`;
}

const socs = await db.selectFrom("soc_codes").select("soc_code").execute();

const BATCH_SIZE = 50;

for (const { soc_code } of socs) {
  let queue = await db
    .selectFrom("naics_codes")
    .selectAll()
    .where("level", "=", 2)
    .execute();

  while (queue.length) {
    const batch: typeof queue = [];
    const seriesIds: string[] = [];
    const naicsBatch: typeof queue = [];

    while (queue.length && batch.length < BATCH_SIZE) {
      const naics = queue.pop()!;
      const sid = createSeriesId({ soc: soc_code, naics: naics.naics_code });

      const known = await db
        .selectFrom("oews_series")
        .select("exists")
        .where("series_id", "=", sid)
        .executeTakeFirst();

      if (known) continue;

      batch.push(naics);
      seriesIds.push(sid);
      naicsBatch.push(naics);
    }

    if (seriesIds.length === 0) continue;

    const res = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        seriesid: seriesIds,
        startyear: "2023",
        endyear: "2023",
        registrationkey: API_KEY,
      }),
    });

    const json = await res.json();
    const validated = validateResponse(
      TimeseriesResponse,
      res,
      json,
      `timeseries data for batch of ${seriesIds.length} series`
    );

    const seriesResults = new Map(
      validated.Results.series.map((s) => [s.seriesID, s])
    );

    for (let i = 0; i < naicsBatch.length; i++) {
      const naics = naicsBatch[i];
      const sid = seriesIds[i];
      const series = seriesResults.get(sid);
      const found = !!series;

      await db
        .insertInto("oews_series")
        .values({
          series_id: sid,
          soc_code,
          naics_code: naics.naics_code,
          exists: found,
          last_checked: new Date().toISOString(),
        })
        .execute();

      if (found && series.data.length) {
        await db
          .insertInto("wages")
          .values({
            series_id: sid,
            year: Number(series.data[0].year),
            mean_annual_wage: Number(series.data[0].value),
          })
          .onConflict((oc) => oc.doNothing())
          .execute();
      }

      if (found && naics.level < 6) {
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

await db.destroy();
console.log("Crawling complete");
