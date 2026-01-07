import { db } from "../db";
import { migrateToLatest } from "../db/migrate";

migrateToLatest({ db });
