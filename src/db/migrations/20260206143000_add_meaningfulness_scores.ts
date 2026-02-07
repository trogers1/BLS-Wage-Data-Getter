import { Kysely, sql } from "kysely";

export async function up(db: Kysely<any>) {
  await db.schema
    .createTable("meaningfulness_scores")
    .ifNotExists()
    .addColumn("id", "bigserial", (col) => col.primaryKey())
    .addColumn("occupation_code", "text", (col) => col.notNull())
    .addColumn("industry_code", "text", (col) => col.notNull())
    .addColumn("score", "integer", (col) => col.notNull())
    .addColumn("reason", "text", (col) => col.notNull())
    .addColumn("model", "text", (col) => col.notNull())
    .addColumn("prompt_version", "text", (col) => col.notNull())
    .addColumn("source_inputs", "jsonb", (col) => col.notNull())
    .addColumn("created_at", "timestamptz", (col) =>
      col.notNull().defaultTo(sql`now()`)
    )
    .addColumn("updated_at", "timestamptz", (col) =>
      col.notNull().defaultTo(sql`now()`)
    )
    .addForeignKeyConstraint(
      "meaningfulness_occ_fk",
      ["occupation_code"],
      "oe_occupations",
      ["occupation_code"]
    )
    .addForeignKeyConstraint(
      "meaningfulness_ind_fk",
      ["industry_code"],
      "oe_industries",
      ["industry_code"]
    )
    .addUniqueConstraint("meaningfulness_unique", [
      "occupation_code",
      "industry_code",
      "prompt_version",
    ])
    .execute();
}

export async function down(db: Kysely<any>) {
  await db.schema.dropTable("meaningfulness_scores").ifExists().execute();
}
