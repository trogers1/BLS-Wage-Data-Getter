import {
  describe,
  it,
  expect,
  beforeEach,
  afterEach,
  vi,
  beforeAll,
  afterAll,
} from "vitest";
import nock from "nock";
import { TestDbManager } from "../../test-utils/testDBManager.ts";
import { getNaics, insertNaicsIntoDb } from "./utils.ts";
import { BLS_OE_BULK_BASE_URL } from "../constants.ts";

describe("get_naics/utils", () => {
  const API_URL = BLS_OE_BULK_BASE_URL;
  let dbManager: TestDbManager;

  beforeAll(async () => {
    dbManager = new TestDbManager();
    await dbManager.start();
  }, 120000);

  afterAll(async () => {
    await dbManager.stop();
  });

  beforeEach(() => {
    nock.cleanAll();
    vi.clearAllMocks();
  });

  afterEach(() => {
    nock.cleanAll();
  });

  describe("getNaics", () => {
    it("should fetch and parse NAICS codes from bulk file", async () => {
      const mockResponse = [
        "industry_code\tindustry_name\tdisplay_level\tselectable\tsort_sequence",
        "111110\tSoybean Farming\t6\tT\t001",
        "111120\tOilseed Farming\t6\tT\t002",
        "11\tAgriculture\t2\tT\t003",
        "999999\tInvalid\t7\tT\t004",
      ].join("\n");

      nock(API_URL).get("/oe.industry").reply(200, mockResponse);

      const naics = await getNaics();
      expect(naics).toEqual([
        {
          naics_code: "111110",
          title: "Soybean Farming",
          level: 6,
          parent_code: "11111",
        },
        {
          naics_code: "111120",
          title: "Oilseed Farming",
          level: 6,
          parent_code: "11112",
        },
      ]);
    });

    it("should throw error when file headers are invalid", async () => {
      const mockResponse = "industry_code\tindustry_name";
      nock(API_URL).get("/oe.industry").reply(200, mockResponse);

      await expect(getNaics()).rejects.toThrow("Unexpected industry headers");
    });

    it("should throw error when request fails", async () => {
      nock(API_URL).get("/oe.industry").reply(500, "Internal server error");

      await expect(getNaics()).rejects.toThrow("Failed getNaics request");
    });
  });

  describe("insertNaicsIntoDb", () => {
    it("should insert NAICS codes into database", async () => {
      const db = await dbManager.getTestDb("insert-naics-test");

      const testNaics = [
        {
          naics_code: "11",
          title: "Agriculture, Forestry, Fishing and Hunting",
          level: 2,
          parent_code: null,
        },
        {
          naics_code: "111",
          title: "Crop Production",
          level: 3,
          parent_code: "11",
        },
        {
          naics_code: "1111",
          title: "Oilseed and Grain Farming",
          level: 4,
          parent_code: "111",
        },
      ];

      await insertNaicsIntoDb({ naics: testNaics, db });

      const inserted = await db
        .selectFrom("naics_codes")
        .selectAll()
        .orderBy("naics_code")
        .execute();

      expect(inserted).toHaveLength(3);
      expect(inserted).toEqual(
        expect.arrayContaining(
          testNaics.map((naics) => expect.objectContaining(naics))
        )
      );
    });

    it("should handle duplicates with onConflict doNothing", async () => {
      const db = await dbManager.getTestDb("duplicate-naics-test");

      // Insert first batch
      const firstBatch = [
        {
          naics_code: "11",
          title: "Agriculture",
          level: 2,
          parent_code: null,
        },
        {
          naics_code: "23",
          title: "Construction",
          level: 2,
          parent_code: null,
        },
      ];

      await insertNaicsIntoDb({ naics: firstBatch, db });

      // Try to insert same NAICS codes with different titles (should be ignored)
      const secondBatch = [
        {
          naics_code: "11",
          title: "Different Title",
          level: 2,
          parent_code: null,
        }, // Should be ignored
        {
          naics_code: "31-33",
          title: "Manufacturing",
          level: 5,
          parent_code: null,
        }, // New
      ];

      await insertNaicsIntoDb({ naics: secondBatch, db });

      const inserted = await db
        .selectFrom("naics_codes")
        .selectAll()
        .orderBy("naics_code")
        .execute();

      expect(inserted).toHaveLength(3);

      // Check that the original title was preserved
      const agriculture = inserted.find(
        (n) => n.naics_code === firstBatch[0].naics_code
      );
      expect(agriculture?.title).toBe(firstBatch[0].title);

      // Check that new NAICS code was added
      expect(
        inserted.find((n) => n.naics_code === secondBatch[1].naics_code)
      ).toBeTruthy();
    });

    it("should insert NAICS codes in a transaction", async () => {
      const db = await dbManager.getTestDb("transaction-naics-test");

      const testNaics = [
        {
          naics_code: "11",
          title: "Agriculture",
          level: 2,
          parent_code: null,
        },
        {
          naics_code: "23",
          title: "Construction",
          level: 2,
          parent_code: null,
        },
      ];

      // Mock transaction to verify it's used
      const executeSpy = vi.spyOn(db, "transaction");

      await insertNaicsIntoDb({ naics: testNaics, db });

      expect(executeSpy).toHaveBeenCalled();

      const inserted = await db.selectFrom("naics_codes").selectAll().execute();

      expect(inserted).toHaveLength(2);
    });

    it("should work with deterministic random data", async () => {
      const db = await dbManager.createAndSeedTestDbDeterministic(
        "deterministic-naics-test",
        "test-seed-123"
      );

      // Use the deterministic seeder directly to get NAICS codes
      const seeder = new (
        await import("../../test-utils/testDataSeeder/index.ts")
      ).TestDataSeeder(db);
      await seeder.seedNaicsCodesDeterministic("test-seed-456");

      const inserted = await db.selectFrom("naics_codes").selectAll().execute();

      expect(inserted.length).toBeGreaterThan(0);

      // All NAICS codes should have valid format and relationships
      for (const naics of inserted) {
        expect(naics.naics_code).toBeTruthy();
        expect(naics.title).toBeTruthy();
        expect(naics.level).toBeGreaterThanOrEqual(2);
        expect(naics.level).toBeLessThanOrEqual(6);

        if (naics.parent_code !== null) {
          // Parent should exist in the database
          const parent = inserted.find(
            (n) => n.naics_code === naics.parent_code
          );
          expect(parent).toBeTruthy();
          expect(parent?.level).toBe(naics.level - 1);
        }
      }
    });
  });

  describe("integration", () => {
    it("should fetch from API and insert into database", async () => {
      const db = await dbManager.getTestDb("integration-test");

      const mockResponse = [
        "industry_code\tindustry_name\tdisplay_level\tselectable\tsort_sequence",
        "111110\tSoybean Farming\t6\tT\t001",
        "111120\tOilseed Farming\t6\tT\t002",
      ].join("\n");

      nock(API_URL).get("/oe.industry").reply(200, mockResponse);

      // Fetch NAICS codes
      const naics = await getNaics();
      expect(naics).toHaveLength(2);

      // Insert into database
      await insertNaicsIntoDb({ naics, db });

      // Verify insertion
      const inserted = await db.selectFrom("naics_codes").selectAll().execute();

      expect(inserted).toHaveLength(2);
      expect(inserted).toEqual(
        expect.arrayContaining(naics.map((n) => expect.objectContaining(n)))
      );
    });
  });
});
