import { SocResponse } from "../../schemas/schemas.ts";
import { validateResponse } from "../../schemas/validate.ts";
import { getDbInstance } from "../../db/index.ts";
import type { SocCodes } from "../../db/generated/db.d.ts";

export async function getSocs(): Promise<SocCodes[]> {
  const res = await fetch(
    "https://api.bls.gov/publicAPI/v2/surveys/OEWS/occupations/"
  );
  const json = await res.json();

  const validated = validateResponse(SocResponse, res, json, "SOC codes API");

  console.log("Soc request results:");
  console.log(validated, null, 2);

  const socs: SocCodes[] = [];
  for (const o of validated.occupations) {
    if (/^\d{2}-\d{4}$/.test(o.code)) {
      socs.push({
        soc_code: o.code,
        title: o.text,
      });
    }
  }
  return socs;
}

export async function insertSocsIntoDb({
  socs,
  db,
}: {
  socs: SocCodes[];
  db: ReturnType<typeof getDbInstance>;
}): Promise<void> {
  await db.transaction().execute(async (trx) => {
    for (const soc of socs) {
      await trx
        .insertInto("soc_codes")
        .values(soc)
        .onConflict((oc) => oc.column("soc_code").doNothing())
        .execute();
    }
  });
  console.log("SOC codes loaded");
}
