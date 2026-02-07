import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { sql } from "kysely";
import { TestDbManager } from "../test-utils/testDBManager.ts";

type MeaningfulnessViewRow = {
  series_id: string;
  datatype_name: string;
  year: number;
  period: string;
  meaningfulness_score: number | null;
};

describe("Deterministic integration dataset", () => {
  let dbManager: TestDbManager;

  beforeAll(async () => {
    dbManager = new TestDbManager();
    await dbManager.start();
  });

  afterAll(async () => {
    await dbManager.stop();
  });

  it("seeds production-like counts and relationships", async () => {
    const db = await dbManager.createAndSeedTestDbDeterministic(
      "integration-counts",
      "integration-seed"
    );

    const occupations = await db
      .selectFrom("oe_occupations")
      .selectAll()
      .execute();
    const industries = await db
      .selectFrom("oe_industries")
      .selectAll()
      .execute();
    const datatypes = await db.selectFrom("oe_datatypes").selectAll().execute();
    const series = await db.selectFrom("oe_series").selectAll().execute();
    const data = await db.selectFrom("oe_data").selectAll().execute();

    expect(occupations.length).toBeGreaterThan(0);
    expect(industries.length).toBeGreaterThan(0);
    expect(datatypes.length).toBeGreaterThan(0);
    expect(series).toHaveLength(
      occupations.length * industries.length * datatypes.length
    );
    expect(data).toHaveLength(series.length * 2);
  });

  it("exposes the meaningfulness view with latest A01 data", async () => {
    const db = await dbManager.createAndSeedTestDbDeterministic(
      "integration-view",
      "integration-seed"
    );

    const result =
      await sql`select * from occupation_industry_meaningfulness`.execute(db);
    const rows = result.rows as MeaningfulnessViewRow[];

    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((row) => row.period === "A01")).toBe(true);
    expect(
      rows.every((row) => row.datatype_name.toLowerCase().includes("mean"))
    ).toBe(true);

    const sample = rows[0];
    const maxYear = await db
      .selectFrom("oe_data")
      .select(({ fn }) => fn.max("year").as("max_year"))
      .where("series_id", "=", sample.series_id)
      .executeTakeFirst();
    expect(sample.year).toBe(Number(maxYear?.max_year));

    const hasScored = rows.some((row) => row.meaningfulness_score !== null);
    const hasUnscored = rows.some((row) => row.meaningfulness_score === null);
    expect(hasScored).toBe(true);
    expect(hasUnscored).toBe(true);
  });

  it("filters the view to mean annual wage series only", async () => {
    const db = await dbManager.createAndSeedTestDbDeterministic(
      "integration-filter",
      "integration-seed"
    );

    const seriesCount = await db
      .selectFrom("oe_series")
      .select(({ fn }) => fn.countAll().as("count"))
      .where("datatype_code", "=", "15")
      .executeTakeFirst();

    const result =
      await sql`select * from occupation_industry_meaningfulness`.execute(db);
    const rows = result.rows as MeaningfulnessViewRow[];

    expect(rows).toHaveLength(Number(seriesCount?.count ?? 0));
  });
});
