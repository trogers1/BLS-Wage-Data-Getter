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

for (const { soc_code } of socs) {
  let queue = await db
    .selectFrom("naics_codes")
    .selectAll()
    .where("level", "=", 2)
    .execute();

  while (queue.length) {
    const naics = queue.pop()!;

    const sid = createSeriesId({ soc: soc_code, naics: naics.naics_code });

    const known = await db
      .selectFrom("oews_series")
      .select("exists")
      .where("series_id", "=", sid)
      .executeTakeFirst();

    if (known) continue;

    const res = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        seriesid: [sid],
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
      `timeseries data for ${sid}`
    );
    const found = validated.Results.series.length > 0;

    await db
      .insertInto("oews_series")
      .values({
        series_id: sid,
        soc_code,
        naics_code: naics.naics_code,
        exists: found ? 1 : 0,
        last_checked: new Date().toISOString(),
      })
      .execute();

    if (!found) continue;

    const data = validated.Results.series[0].data;
    if (data.length) {
      await db
        .insertInto("wages")
        .values({
          series_id: sid,
          year: Number(data[0].year),
          mean_annual_wage: Number(data[0].value),
        })
        .onConflict((oc) => oc.doNothing())
        .execute();
    }

    if (naics.level < 6) {
      const children = await db
        .selectFrom("naics_codes")
        .selectAll()
        .where("parent_code", "=", naics.naics_code)
        .execute();

      queue.push(...children);
    }
  }
}

await db.destroy();
console.log("Crawling complete");
