import type { DB } from "./generated/db.d.ts";
import { Pool } from "pg";
import { Kysely, PostgresDialect } from "kysely";

export const getDbInstance = () =>
  new Kysely<DB>({
    dialect: new PostgresDialect({
      pool: new Pool({
        database: "oews",
        host: "localhost",
        port: 5432,
        user: "oews",
      }),
    }),
  });
