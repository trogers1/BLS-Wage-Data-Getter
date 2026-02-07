import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { TestDbManager } from "../testDBManager.ts";
import { TestDataSeeder } from "./index.ts";

describe("TestDataSeeder", () => {
  let dbManager: TestDbManager;

  beforeAll(async () => {
    dbManager = new TestDbManager();
    await dbManager.start();
  });

  afterAll(async () => {
    await dbManager.stop();
  });

  describe("seedAll", () => {
    it("should seed mapping tables", async () => {
      const testId = "seed-all-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedAll();

      const occupations = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();
      const industries = await db
        .selectFrom("oe_industries")
        .selectAll()
        .execute();

      expect(occupations.length).toBeGreaterThan(0);
      expect(industries.length).toBeGreaterThan(0);
    });

    it("should not duplicate data on conflict", async () => {
      const testId = "seed-all-conflict-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedAll();
      const firstCount = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();

      await seeder.seedAll();
      const secondCount = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();

      expect(secondCount).toHaveLength(firstCount.length);
    });
  });

  describe("seedDeterministic", () => {
    it("should seed deterministic data with the same base seed", async () => {
      const testId1 = "deterministic-test-1";
      const testId2 = "deterministic-test-2";
      const db1 = await dbManager.getTestDb(testId1);
      const db2 = await dbManager.getTestDb(testId2);
      const seeder1 = new TestDataSeeder(db1);
      const seeder2 = new TestDataSeeder(db2);

      const baseSeed = "test-seed-123";
      await seeder1.seedDeterministic(baseSeed);
      await seeder2.seedDeterministic(baseSeed);

      const occ1 = await db1
        .selectFrom("oe_occupations")
        .selectAll()
        .orderBy("occupation_code")
        .execute();
      const occ2 = await db2
        .selectFrom("oe_occupations")
        .selectAll()
        .orderBy("occupation_code")
        .execute();
      const series1 = await db1
        .selectFrom("oe_series")
        .selectAll()
        .orderBy("series_id")
        .execute();
      const series2 = await db2
        .selectFrom("oe_series")
        .selectAll()
        .orderBy("series_id")
        .execute();
      const data1 = await db1
        .selectFrom("oe_data")
        .selectAll()
        .orderBy("series_id")
        .orderBy("year")
        .execute();
      const data2 = await db2
        .selectFrom("oe_data")
        .selectAll()
        .orderBy("series_id")
        .orderBy("year")
        .execute();

      expect(occ1).toEqual(occ2);
      expect(series1).toEqual(series2);
      expect(data1).toEqual(data2);
    });

    it("should seed different data with different base seeds", async () => {
      const testId = "deterministic-diff-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedDeterministic("seed-1");
      const occ1 = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .orderBy("occupation_code")
        .execute();
      const series1 = await db
        .selectFrom("oe_series")
        .selectAll()
        .orderBy("series_id")
        .execute();
      const data1 = await db
        .selectFrom("oe_data")
        .selectAll()
        .orderBy("series_id")
        .orderBy("year")
        .execute();

      await seeder.clearAll();
      await seeder.seedDeterministic("seed-2");
      const occ2 = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .orderBy("occupation_code")
        .execute();
      const series2 = await db
        .selectFrom("oe_series")
        .selectAll()
        .orderBy("series_id")
        .execute();
      const data2 = await db
        .selectFrom("oe_data")
        .selectAll()
        .orderBy("series_id")
        .orderBy("year")
        .execute();

      expect(occ1.map((o) => o.occupation_code)).not.toEqual(
        occ2.map((o) => o.occupation_code)
      );
      expect(series1.map((s) => s.series_id)).not.toEqual(
        series2.map((s) => s.series_id)
      );
      expect(data1.map((d) => d.value)).not.toEqual(data2.map((d) => d.value));
    });
  });

  describe("seedOeOccupations", () => {
    it("should seed OEWS occupations from constants", async () => {
      const testId = "seed-occupations-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedOeOccupations();

      const occupations = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();

      expect(occupations.length).toBeGreaterThan(0);
      expect(occupations.some((o) => o.occupation_code === "111011")).toBe(
        true
      );
    });
  });

  describe("seedOeIndustries", () => {
    it("should seed OEWS industries from constants", async () => {
      const testId = "seed-industries-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedOeIndustries();

      const industries = await db
        .selectFrom("oe_industries")
        .selectAll()
        .execute();

      expect(industries.length).toBeGreaterThan(0);
      expect(industries.some((i) => i.industry_code === "111110")).toBe(true);
    });
  });

  describe("clearAll", () => {
    it("should clear all seeded data", async () => {
      const testId = "clear-all-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedDeterministic("clear-all-seed");

      const occupationsBefore = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();
      expect(occupationsBefore.length).toBeGreaterThan(0);

      await seeder.clearAll();

      const occupationsAfter = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();
      const industriesAfter = await db
        .selectFrom("oe_industries")
        .selectAll()
        .execute();
      const seriesAfter = await db
        .selectFrom("oe_series")
        .selectAll()
        .execute();
      const dataAfter = await db.selectFrom("oe_data").selectAll().execute();
      const meaningfulnessAfter = await db
        .selectFrom("meaningfulness_scores")
        .selectAll()
        .execute();

      expect(occupationsAfter).toHaveLength(0);
      expect(industriesAfter).toHaveLength(0);
      expect(seriesAfter).toHaveLength(0);
      expect(dataAfter).toHaveLength(0);
      expect(meaningfulnessAfter).toHaveLength(0);
    });
  });
});
