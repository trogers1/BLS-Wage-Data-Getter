import { getDbInstance } from "../../db/index.ts";
import { getNaics, insertNaicsIntoDb } from "./utils.ts";

const naics = await getNaics();

const db = getDbInstance();
await insertNaicsIntoDb({ db, naics });
await db.destroy();
