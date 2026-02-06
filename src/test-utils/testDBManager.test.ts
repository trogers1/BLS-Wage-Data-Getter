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
      const socCodes = await db.selectFrom("soc_codes").selectAll().execute();
      const naicsCodes = await db
        .selectFrom("naics_codes")
        .selectAll()
        .execute();

      expect(socCodes.length).toBeGreaterThan(0);
      expect(naicsCodes.length).toBeGreaterThan(0);
    });

    it("should create isolated databases for different test IDs", async () => {
      const testId1 = "test_2";
      const testId2 = "test_3";

      const db1 = await dbManager.createAndSeedTestDb(testId1);

      // Add unique data to db1
      const uniqueSocCode = "this-should-never-be-there";
      await db1
        .insertInto("soc_codes")
        .values({
          soc_code: uniqueSocCode,
          title: "Unique Test SOC",
        })
        .execute();

      // Verify db1 has the unique data
      const db1SocCodes = await db1
        .selectFrom("soc_codes")
        .selectAll()
        .execute();

      expect(db1SocCodes.length).toBeGreaterThan(0);
      expect(db1SocCodes.map((c) => c.soc_code)).toContain(uniqueSocCode);

      const db2 = await dbManager.createAndSeedTestDb(testId2);

      // Verify db2 does NOT have the unique data (should only have seeded data)
      const db2SocCodes = await db2
        .selectFrom("soc_codes")
        .selectAll()
        .execute();

      expect(db2SocCodes).toHaveLength(db1SocCodes.length - 1);
      expect(db2SocCodes.map((c) => c.soc_code)).not.toContain(uniqueSocCode);
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
      const socCodes = await db.selectFrom("soc_codes").selectAll().execute();

      expect(socCodes).toHaveLength(0);
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
      const uniqueSocCode = "should-not-exist";
      await db
        .insertInto("soc_codes")
        .values({ soc_code: uniqueSocCode, title: "Extra SOC" })
        .execute();

      // Verify extra data exists
      const beforeReset = await db
        .selectFrom("soc_codes")
        .selectAll()
        .execute();

      expect(beforeReset.length).toBeGreaterThan(0);

      // Reset the database
      await dbManager.resetTestDb(testId);

      // Verify only seeded data exists
      const afterReset = await db.selectFrom("soc_codes").selectAll().execute();

      log({ afterReset });

      expect(afterReset).toHaveLength(beforeReset.length - 1);
      expect(afterReset.map((c) => c.soc_code)).not.toContain(uniqueSocCode);
    });
  });

  describe("cleanupTestDb", () => {
    it("should cleanup a specific test database", async () => {
      const testId = "test_8";
      const db = await dbManager.createAndSeedTestDb(testId);

      // Database should exist
      const socCodes = await db.selectFrom("soc_codes").selectAll().execute();
      expect(socCodes.length).toBeGreaterThan(0);

      // Cleanup
      await dbManager.cleanupTestDb(testId);

      // Should not be able to use the database after cleanup
      await expect(
        db.selectFrom("soc_codes").selectAll().execute()
      ).rejects.toThrow();
    });
  });

  describe("clearTestData", () => {
    it("should clear all test data from a database", async () => {
      const testId = "test_9";
      const db = await dbManager.createAndSeedTestDb(testId);

      // Verify seeded data exists
      const beforeClear = await db
        .selectFrom("soc_codes")
        .selectAll()
        .execute();
      expect(beforeClear.length).toBeGreaterThan(0);

      // Clear data
      await dbManager.clearTestData(db);

      // Verify data is cleared
      const afterClear = await db.selectFrom("soc_codes").selectAll().execute();
      expect(afterClear).toHaveLength(0);
    });
  });

  describe("seedTestData", () => {
    it("should seed test data into a database", async () => {
      const testId = "test_10";
      const db = await dbManager.getTestDb(testId);

      // Database should be empty initially
      const beforeSeed = await db.selectFrom("soc_codes").selectAll().execute();
      expect(beforeSeed).toHaveLength(0);

      // Seed data
      await dbManager.seedTestData(db);

      // Verify data is seeded
      const afterSeed = await db.selectFrom("soc_codes").selectAll().execute();
      expect(afterSeed).toHaveLength(6);
    });
  });
});
