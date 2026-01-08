import { getDbInstance } from "../../db/index.ts";
import { getSocs, insertSocsIntoDb } from "./utils.ts";

const socs = await getSocs();

const db = getDbInstance();
await insertSocsIntoDb({ db, socs });
await db.destroy();
