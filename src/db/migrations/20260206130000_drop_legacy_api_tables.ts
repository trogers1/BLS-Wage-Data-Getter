import { Kysely } from "kysely";

export async function up(db: Kysely<any>) {
  await db.schema.dropTable("wages").ifExists().execute();
  await db.schema.dropTable("oews_series").ifExists().execute();
}

export async function down(db: Kysely<any>) {
  await db.schema
    .createTable("oews_series")
    .ifNotExists()
    .addColumn("series_id", "text", (col) => col.primaryKey())
    .addColumn("soc_code", "text", (col) => col.notNull())
    .addColumn("naics_code", "text", (col) => col.notNull())
    .addColumn("does_exist", "boolean", (col) => col.notNull())
    .addColumn("last_checked", "timestamptz", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("wages")
    .ifNotExists()
    .addColumn("series_id", "text", (col) => col.notNull())
    .addColumn("year", "integer", (col) => col.notNull())
    .addColumn("mean_annual_wage", "integer", (col) => col.notNull())
    .addPrimaryKeyConstraint("wages_pk", ["series_id", "year"])
    .execute();
}
