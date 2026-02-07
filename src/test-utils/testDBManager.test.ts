import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { TestDbManager } from "./testDBManager.ts";
import { log } from "../utils/logger.ts";

describe("TestDbManager", () => {
  let dbManager: TestDbManager;

  beforeAll(async () => {
    dbManager = new TestDbManager();
    await dbManager.start();
  });

  afterAll(async () => {
    await dbManager.stop();
  });

  describe("createAndSeedTestDb", () => {
    it("should create and seed a new database for a test ID", async () => {
      const testId = "test_1";
      const db = await dbManager.createAndSeedTestDb(testId);

      // Verify the database has been seeded
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

    it("should create isolated databases for different test IDs", async () => {
      const testId1 = "test_2";
      const testId2 = "test_3";

      const db1 = await dbManager.createAndSeedTestDb(testId1);

      // Add unique data to db1
      const uniqueOccupation = "999999";
      await db1
        .insertInto("oe_occupations")
        .values({
          occupation_code: uniqueOccupation,
          occupation_name: "Unique Test Occupation",
          display_level: 3,
          selectable: true,
          sort_sequence: 999,
        })
        .execute();

      // Verify db1 has the unique data
      const db1Occupations = await db1
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();

      expect(db1Occupations.length).toBeGreaterThan(0);
      expect(db1Occupations.map((c) => c.occupation_code)).toContain(
        uniqueOccupation
      );

      const db2 = await dbManager.createAndSeedTestDb(testId2);

      // Verify db2 does NOT have the unique data (should only have seeded data)
      const db2Occupations = await db2
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();

      expect(db2Occupations).toHaveLength(db1Occupations.length - 1);
      expect(db2Occupations.map((c) => c.occupation_code)).not.toContain(
        uniqueOccupation
      );
    });

    it("should return the same database instance for the same test ID", async () => {
      const testId = "test_4";
      const db1 = await dbManager.createAndSeedTestDb(testId);
      const db2 = await dbManager.createAndSeedTestDb(testId);

      // They should be the same instance (cached)
      expect(db1).toStrictEqual(db2);
    });
  });

  describe("getTestDb", () => {
    it("should create a database without seeding if not already created", async () => {
      const testId = "test_5";
      const db = await dbManager.getTestDb(testId);

      // Database should exist but not be seeded
      const occupations = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();

      expect(occupations).toHaveLength(0);
    });

    it("should return cached database for existing test ID", async () => {
      const testId = "test_6";
      const db1 = await dbManager.getTestDb(testId);
      const db2 = await dbManager.getTestDb(testId);

      expect(db1).toStrictEqual(db2);
    });
  });

  describe("resetTestDb", () => {
    it("should clear and re-seed a test database", async () => {
      const testId = "test_7";
      const db = await dbManager.createAndSeedTestDb(testId);

      // Add some extra data
      const uniqueOccupation = "888888";
      await db
        .insertInto("oe_occupations")
        .values({
          occupation_code: uniqueOccupation,
          occupation_name: "Extra Occupation",
          display_level: 3,
          selectable: true,
          sort_sequence: 888,
        })
        .execute();

      // Verify extra data exists
      const beforeReset = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();

      expect(beforeReset.length).toBeGreaterThan(0);

      // Reset the database
      await dbManager.resetTestDb(testId);

      // Verify only seeded data exists
      const afterReset = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();

      log({ afterReset });

      expect(afterReset).toHaveLength(beforeReset.length - 1);
      expect(afterReset.map((c) => c.occupation_code)).not.toContain(
        uniqueOccupation
      );
    });
  });

  describe("cleanupTestDb", () => {
    it("should cleanup a specific test database", async () => {
      const testId = "test_8";
      const db = await dbManager.createAndSeedTestDb(testId);

      // Database should exist
      const occupations = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();
      expect(occupations.length).toBeGreaterThan(0);

      // Cleanup
      await dbManager.cleanupTestDb(testId);

      // Should not be able to use the database after cleanup
      await expect(
        db.selectFrom("oe_occupations").selectAll().execute()
      ).rejects.toThrow();
    });
  });

  describe("clearTestData", () => {
    it("should clear all test data from a database", async () => {
      const testId = "test_9";
      const db = await dbManager.createAndSeedTestDb(testId);

      // Verify seeded data exists
      const beforeClear = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();
      expect(beforeClear.length).toBeGreaterThan(0);

      // Clear data
      await dbManager.clearTestData(db);

      // Verify data is cleared
      const afterClear = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();
      expect(afterClear).toHaveLength(0);
    });
  });

  describe("seedTestData", () => {
    it("should seed test data into a database", async () => {
      const testId = "test_10";
      const db = await dbManager.getTestDb(testId);

      // Database should be empty initially
      const beforeSeed = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();
      expect(beforeSeed).toHaveLength(0);

      // Seed data
      await dbManager.seedTestData(db);

      // Verify data is seeded
      const afterSeed = await db
        .selectFrom("oe_occupations")
        .selectAll()
        .execute();
      expect(afterSeed.length).toBeGreaterThan(0);
    });
  });
});
