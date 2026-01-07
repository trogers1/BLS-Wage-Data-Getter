import { Database } from "./generated/types.ts";
import { Pool } from "pg";
import { Kysely, PostgresDialect } from "kysely";

export const db = new Kysely<Database>({
  dialect: new PostgresDialect({
    pool: new Pool({
      database: "oews",
      host: "localhost",
      port: 5432,
      user: "oews",
    }),
  }),
});
