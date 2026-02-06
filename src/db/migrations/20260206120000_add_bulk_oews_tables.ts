import { Kysely } from "kysely";

export async function up(db: Kysely<any>) {
  await db.schema
    .createTable("oe_occupations")
    .ifNotExists()
    .addColumn("occupation_code", "text", (col) => col.primaryKey())
    .addColumn("occupation_name", "text", (col) => col.notNull())
    .addColumn("display_level", "integer", (col) => col.notNull())
    .addColumn("selectable", "boolean", (col) => col.notNull())
    .addColumn("sort_sequence", "integer", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("oe_industries")
    .ifNotExists()
    .addColumn("industry_code", "text", (col) => col.primaryKey())
    .addColumn("industry_name", "text", (col) => col.notNull())
    .addColumn("display_level", "integer", (col) => col.notNull())
    .addColumn("selectable", "boolean", (col) => col.notNull())
    .addColumn("sort_sequence", "integer", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("oe_areatypes")
    .ifNotExists()
    .addColumn("areatype_code", "text", (col) => col.primaryKey())
    .addColumn("areatype_name", "text", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("oe_areas")
    .ifNotExists()
    .addColumn("state_code", "text", (col) => col.notNull())
    .addColumn("area_code", "text", (col) => col.notNull())
    .addColumn("areatype_code", "text", (col) => col.notNull())
    .addColumn("area_name", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("oe_areas_pk", ["state_code", "area_code"])
    .execute();

  await db.schema
    .createTable("oe_datatypes")
    .ifNotExists()
    .addColumn("datatype_code", "text", (col) => col.primaryKey())
    .addColumn("datatype_name", "text", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("oe_sectors")
    .ifNotExists()
    .addColumn("sector_code", "text", (col) => col.primaryKey())
    .addColumn("sector_name", "text", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("oe_footnotes")
    .ifNotExists()
    .addColumn("footnote_code", "text", (col) => col.primaryKey())
    .addColumn("footnote_text", "text", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("oe_releases")
    .ifNotExists()
    .addColumn("release_date", "text", (col) => col.primaryKey())
    .addColumn("description", "text", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("oe_seasonal")
    .ifNotExists()
    .addColumn("seasonal_code", "text", (col) => col.primaryKey())
    .addColumn("seasonal_text", "text", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("oe_series")
    .ifNotExists()
    .addColumn("series_id", "text", (col) => col.primaryKey())
    .addColumn("seasonal", "text", (col) => col.notNull())
    .addColumn("areatype_code", "text", (col) => col.notNull())
    .addColumn("industry_code", "text", (col) => col.notNull())
    .addColumn("occupation_code", "text", (col) => col.notNull())
    .addColumn("datatype_code", "text", (col) => col.notNull())
    .addColumn("state_code", "text", (col) => col.notNull())
    .addColumn("area_code", "text", (col) => col.notNull())
    .addColumn("sector_code", "text", (col) => col.notNull())
    .addColumn("series_title", "text", (col) => col.notNull())
    .addColumn("footnote_codes", "text")
    .addColumn("begin_year", "integer", (col) => col.notNull())
    .addColumn("begin_period", "text", (col) => col.notNull())
    .addColumn("end_year", "integer", (col) => col.notNull())
    .addColumn("end_period", "text", (col) => col.notNull())
    .execute();

  await db.schema
    .createTable("oe_data")
    .ifNotExists()
    .addColumn("series_id", "text", (col) => col.notNull())
    .addColumn("year", "integer", (col) => col.notNull())
    .addColumn("period", "text", (col) => col.notNull())
    .addColumn("value", "numeric", (col) => col.notNull())
    .addColumn("footnote_codes", "text")
    .addPrimaryKeyConstraint("oe_data_pk", ["series_id", "year", "period"])
    .addForeignKeyConstraint("oe_data_series_fk", ["series_id"], "oe_series", [
      "series_id",
    ])
    .execute();
}

export async function down(db: Kysely<any>) {
  await db.schema.dropTable("oe_data").ifExists().execute();
  await db.schema.dropTable("oe_series").ifExists().execute();
  await db.schema.dropTable("oe_seasonal").ifExists().execute();
  await db.schema.dropTable("oe_releases").ifExists().execute();
  await db.schema.dropTable("oe_footnotes").ifExists().execute();
  await db.schema.dropTable("oe_sectors").ifExists().execute();
  await db.schema.dropTable("oe_datatypes").ifExists().execute();
  await db.schema.dropTable("oe_areas").ifExists().execute();
  await db.schema.dropTable("oe_areatypes").ifExists().execute();
  await db.schema.dropTable("oe_industries").ifExists().execute();
  await db.schema.dropTable("oe_occupations").ifExists().execute();
}
