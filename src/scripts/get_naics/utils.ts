import { getDbInstance } from "../../db/index.ts";
import type { NaicsCodes } from "../../db/generated/db.d.ts";
import { NaicsResponse } from "../../schemas/index.ts";
import { validateResponse } from "../../schemas/validate.ts";

export async function getNaics(): Promise<NaicsCodes[]> {
  const res = await fetch(
    "https://api.bls.gov/publicAPI/v2/surveys/OEWS/industries/"
  );
  const json = await res.json();

  const validated = validateResponse(
    NaicsResponse,
    res,
    json,
    "NAICS codes API"
  );

  console.log("Naics request results:");
  console.log(validated, null, 2);

  const naics: NaicsCodes[] = [];
  for (const n of validated.industries) {
    const level = n.code.length;
    if (level >= 2 && level <= 6) {
      naics.push({
        naics_code: n.code,
        title: n.text,
        level,
        parent_code: level > 2 ? n.code.slice(0, level - 1) : null,
      });
    }
  }
  return naics;
}

export async function insertNaicsIntoDb({
  naics,
  db,
}: {
  naics: NaicsCodes[];
  db: ReturnType<typeof getDbInstance>;
}): Promise<void> {
  await db.transaction().execute(async (trx) => {
    for (const n of naics) {
      await trx
        .insertInto("naics_codes")
        .values(n)
        .onConflict((oc) => oc.column("naics_code").doNothing())
        .execute();
    }
  });
  console.log("NAICS codes loaded");
}
