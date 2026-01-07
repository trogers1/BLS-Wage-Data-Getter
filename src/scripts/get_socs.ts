import { db } from "../db";
import { SocResponse } from "../schemas";
import { validateResponse } from "../schemas/validate";

const res = await fetch(
  "https://api.bls.gov/publicAPI/v2/surveys/OEWS/occupations/"
);
const json = await res.json();

const validated = validateResponse(SocResponse, res, json, "SOC codes API");

console.log("Soc request results:");
console.log(validated, null, 2);

for (const o of validated.occupations) {
  if (/^\d{2}-\d{4}$/.test(o.code)) {
    await db
      .insertInto("soc_codes")
      .values({
        soc_code: o.code,
        title: o.text,
      })
      .onConflict((oc) => oc.column("soc_code").doNothing())
      .execute();
  }
}

await db.destroy();
console.log("SOC codes loaded");
