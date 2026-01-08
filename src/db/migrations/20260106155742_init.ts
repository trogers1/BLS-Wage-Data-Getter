import { Kysely } from "kysely";

export async function up(db: Kysely<any>) {
  await db.schema
    .createTable("soc_codes")
    .ifNotExists()
    .addColumn("soc_code", "text", (col) => col.primaryKey())
    .addColumn("title", "text", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("naics_codes")
    .ifNotExists()
    .addColumn("naics_code", "text", (col) => col.primaryKey())
    .addColumn("title", "text", (col) => col.notNull())
    .addColumn("level", "integer", (col) => col.notNull())
    .addColumn("parent_code", "text")
    .execute();

  await db.schema
    .createTable("oews_series")
    .ifNotExists()
    .addColumn("series_id", "text", (col) => col.primaryKey())
    .addColumn("soc_code", "text", (col) => col.notNull())
    .addColumn("naics_code", "text", (col) => col.notNull())
    .addColumn("does_exist", "boolean", (col) => col.notNull())
    .addColumn("last_checked", "timestamptz", (col) => col.notNull())
    .addForeignKeyConstraint("fk_series_soc", ["soc_code"], "soc_codes", [
      "soc_code",
    ])
    .addForeignKeyConstraint("fk_series_naics", ["naics_code"], "naics_codes", [
      "naics_code",
    ])
    .execute();

  await db.schema
    .createTable("wages")
    .ifNotExists()
    .addColumn("series_id", "text", (col) => col.notNull())
    .addColumn("year", "integer", (col) => col.notNull())
    .addColumn("mean_annual_wage", "integer", (col) => col.notNull())
    .addPrimaryKeyConstraint("wages_pk", ["series_id", "year"])
    .addForeignKeyConstraint("fk_wages_series", ["series_id"], "oews_series", [
      "series_id",
    ])
    .execute();
}

export async function down(db: Kysely<any>) {
  await db.schema.dropTable("wages").ifExists().execute();
  await db.schema.dropTable("oews_series").ifExists().execute();
  await db.schema.dropTable("naics_codes").ifExists().execute();
  await db.schema.dropTable("soc_codes").ifExists().execute();
}
