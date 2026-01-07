import { DB } from "./generated/db";
import { Pool } from "pg";
import { Kysely, PostgresDialect } from "kysely";

export const db = new Kysely<DB>({
  dialect: new PostgresDialect({
    pool: new Pool({
      database: "oews",
      host: "localhost",
      port: 5432,
      user: "oews",
    }),
  }),
});
