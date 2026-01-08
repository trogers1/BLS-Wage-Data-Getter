import { getDbInstance } from "../../db/index.ts";
import { getWages, insertWagesIntoDb } from "./utils.ts";

const db = getDbInstance();

// Get wages for the last 5 years
const currentYear = new Date().getFullYear();
const startYear = currentYear - 5;
const endYear = currentYear;

const { wages, oewsSeries } = await getWages({
  db,
  startYear,
  endYear,
});

await insertWagesIntoDb({ db, wages, oewsSeries });
await db.destroy();
console.log("Wage data loaded");
