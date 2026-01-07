import { db } from "../db";
import { NaicsResponse } from "../schemas";
import { validateResponse } from "../schemas/validate";

const res = await fetch(
  "https://api.bls.gov/publicAPI/v2/surveys/OEWS/industries/"
);
const json = await res.json();

const validated = validateResponse(NaicsResponse, res, json, "NAICS codes API");

console.log("Naics request results:");
console.log(validated, null, 2);

for (const n of validated.industries) {
  const level = n.code.length;
  if (level >= 2 && level <= 6) {
    await db
      .insertInto("naics_codes")
      .values({
        naics_code: n.code,
        title: n.text,
        level,
        parent_code: level > 2 ? n.code.slice(0, level - 1) : null,
      })
      .onConflict((oc) => oc.column("naics_code").doNothing())
      .execute();
  }
}

await db.destroy();
console.log("NAICS codes loaded");
