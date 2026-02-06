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
    it("should seed all static data", async () => {
      const testId = "seed-all-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedAll();

      const socCodes = await db.selectFrom("soc_codes").selectAll().execute();
      const naicsCodes = await db
        .selectFrom("naics_codes")
        .selectAll()
        .execute();
      expect(socCodes).toHaveLength(6);
      expect(naicsCodes.length).toBeGreaterThan(0);
    });

    it("should not duplicate data on conflict", async () => {
      const testId = "seed-all-conflict-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedAll();
      const firstCount = await db.selectFrom("soc_codes").selectAll().execute();

      await seeder.seedAll();
      const secondCount = await db
        .selectFrom("soc_codes")
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

      const socCodes1 = await db1
        .selectFrom("soc_codes")
        .selectAll()
        .orderBy("soc_code")
        .execute();
      const socCodes2 = await db2
        .selectFrom("soc_codes")
        .selectAll()
        .orderBy("soc_code")
        .execute();

      expect(socCodes1).toEqual(socCodes2);
    });

    it("should seed different data with different base seeds", async () => {
      const testId = "deterministic-diff-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedDeterministic("seed-1");
      const socCodes1 = await db.selectFrom("soc_codes").selectAll().execute();

      await seeder.clearAll();
      await seeder.seedDeterministic("seed-2");
      const socCodes2 = await db.selectFrom("soc_codes").selectAll().execute();

      expect(socCodes1.map((s) => s.soc_code)).not.toEqual(
        socCodes2.map((s) => s.soc_code)
      );
    });
  });

  describe("seedSocCodes", () => {
    it("should seed SOC codes from constants", async () => {
      const testId = "seed-soc-codes-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedSocCodes();

      const socCodes = await db.selectFrom("soc_codes").selectAll().execute();

      expect(socCodes).toHaveLength(6);
      expect(socCodes.map((s) => s.soc_code)).toContain("11-1011");
      expect(socCodes.map((s) => s.soc_code)).toContain("15-1252");
    });
  });

  describe("seedNaicsCodes", () => {
    it("should seed NAICS codes from constants", async () => {
      const testId = "seed-naics-codes-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedNaicsCodes();

      const naicsCodes = await db
        .selectFrom("naics_codes")
        .selectAll()
        .execute();

      expect(naicsCodes.length).toBeGreaterThan(0);
      expect(naicsCodes.some((n) => n.naics_code === "11")).toBe(true);
      expect(naicsCodes.some((n) => n.naics_code === "23")).toBe(true);
    });
  });

  describe("clearAll", () => {
    it("should clear all data from all tables", async () => {
      const testId = "clear-all-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedAll();

      const socCodesBefore = await db
        .selectFrom("soc_codes")
        .selectAll()
        .execute();
      expect(socCodesBefore.length).toBeGreaterThan(0);

      await seeder.clearAll();

      const socCodesAfter = await db
        .selectFrom("soc_codes")
        .selectAll()
        .execute();
      const naicsCodesAfter = await db
        .selectFrom("naics_codes")
        .selectAll()
        .execute();
      expect(socCodesAfter).toHaveLength(0);
      expect(naicsCodesAfter).toHaveLength(0);
    });
  });

  describe("seedSocCodesDeterministic", () => {
    it("should seed 20 SOC codes deterministically", async () => {
      const testId = "seed-soc-deterministic-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedSocCodesDeterministic("test-seed");

      const socCodes = await db.selectFrom("soc_codes").selectAll().execute();

      expect(socCodes).toHaveLength(20);
      expect(socCodes.every((s) => /^\d{2}-\d{4}$/.test(s.soc_code))).toBe(
        true
      );
    });

    it("should produce same results with same seed", async () => {
      const testId1 = "soc-same-seed-1";
      const testId2 = "soc-same-seed-2";
      const db1 = await dbManager.getTestDb(testId1);
      const db2 = await dbManager.getTestDb(testId2);
      const seeder1 = new TestDataSeeder(db1);
      const seeder2 = new TestDataSeeder(db2);

      await seeder1.seedSocCodesDeterministic("same-seed");
      await seeder2.seedSocCodesDeterministic("same-seed");

      const socCodes1 = await db1
        .selectFrom("soc_codes")
        .selectAll()
        .orderBy("soc_code")
        .execute();
      const socCodes2 = await db2
        .selectFrom("soc_codes")
        .selectAll()
        .orderBy("soc_code")
        .execute();

      expect(socCodes1).toEqual(socCodes2);
    });
  });

  describe("seedNaicsCodesDeterministic", () => {
    it("should seed NAICS codes with hierarchical structure", async () => {
      const testId = "seed-naics-deterministic-test";
      const db = await dbManager.getTestDb(testId);
      const seeder = new TestDataSeeder(db);

      await seeder.seedNaicsCodesDeterministic("test-seed");

      const naicsCodes = await db
        .selectFrom("naics_codes")
        .selectAll()
        .execute();

      const level2Codes = naicsCodes.filter((n) => n.level === 2);
      const childCodes = naicsCodes.filter((n) => n.parent_code !== null);

      expect(level2Codes.length).toBe(10);
      expect(childCodes.length).toBeGreaterThan(0);
      expect(
        childCodes.every((c) =>
          level2Codes.some((p) => p.naics_code === c.parent_code)
        )
      ).toBe(true);
    });

    it("should produce same results with same seed", async () => {
      const testId1 = "naics-same-seed-1";
      const testId2 = "naics-same-seed-2";
      const db1 = await dbManager.getTestDb(testId1);
      const db2 = await dbManager.getTestDb(testId2);
      const seeder1 = new TestDataSeeder(db1);
      const seeder2 = new TestDataSeeder(db2);

      await seeder1.seedNaicsCodesDeterministic("same-seed");
      await seeder2.seedNaicsCodesDeterministic("same-seed");

      const naicsCodes1 = await db1
        .selectFrom("naics_codes")
        .selectAll()
        .orderBy("naics_code")
        .execute();
      const naicsCodes2 = await db2
        .selectFrom("naics_codes")
        .selectAll()
        .orderBy("naics_code")
        .execute();

      expect(naicsCodes1).toEqual(naicsCodes2);
    });
  });
});
