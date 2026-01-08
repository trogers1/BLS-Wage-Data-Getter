import { getDbInstance } from "../../db/index.ts";
import { getWages, insertWagesIntoDb } from "./utils.ts";

const wages = await getWages();

const db = getDbInstance();
await insertWagesIntoDb({ db, wages });
await db.destroy();
console.log("SOC codes loaded");
