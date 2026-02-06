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
import { NaicsResponseType } from "../../schemas/schemas.ts";
import { TestDbManager } from "../../test-utils/testDBManager.ts";
import { getNaics, insertNaicsIntoDb } from "./utils.ts";

describe("get_naics/utils", () => {
  const API_URL = "https://api.bls.gov";
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
    const buildExpectedNaics = (
      industry: NaicsResponseType["industries"][0]
    ) => {
      const isRange = /^\d{2}-\d{2}$/.test(industry.code);
      const level = isRange ? 2 : industry.code.length;
      return {
        naics_code: industry.code,
        title: industry.text,
        level,
        parent_code: level > 2 ? industry.code.slice(0, level - 1) : null,
      };
    };

    it("should fetch and parse NAICS codes from BLS API", async () => {
      const mockResponse: NaicsResponseType = {
        industries: [
          { code: "11", text: "Agriculture, Forestry, Fishing and Hunting" },
          { code: "23", text: "Construction" },
          { code: "31-33", text: "Manufacturing" },
          { code: "44-45", text: "Retail Trade" },
          { code: "999999", text: "Invalid code" }, // Too long, should be filtered out
          { code: "1", text: "Too short" }, // Too short, should be filtered out
        ],
      };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/industries/")
        .reply(200, mockResponse);

      const naics = await getNaics();
      const expectedNaics = mockResponse.industries
        .filter((industry) => industry.code.length >= 2)
        .map((industry) => buildExpectedNaics(industry));

      expect(naics).toHaveLength(expectedNaics.length);
      expect(naics).toEqual(expect.arrayContaining(expectedNaics));
    });

    it("should handle hyphenated ranges correctly", async () => {
      const mockResponse: NaicsResponseType = {
        industries: [
          { code: "31-33", text: "Manufacturing" },
          { code: "44-45", text: "Retail Trade" },
        ],
      };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/industries/")
        .reply(200, mockResponse);

      const naics = await getNaics();

      const expectedNaics = mockResponse.industries.map((industry) =>
        buildExpectedNaics(industry)
      );

      expect(naics).toHaveLength(expectedNaics.length);
      expect(naics).toEqual(expect.arrayContaining(expectedNaics));

      for (const industry of mockResponse.industries) {
        const entry = naics.find((n) => n.naics_code === industry.code);
        expect(entry?.parent_code).toBeNull();
      }
    });

    it("should calculate parent codes for hierarchical NAICS codes", async () => {
      const mockResponse: NaicsResponseType = {
        industries: [
          { code: "11", text: "Agriculture" },
          { code: "111", text: "Crop Production" },
          { code: "1111", text: "Oilseed and Grain Farming" },
          { code: "11111", text: "Soybean Farming" },
          { code: "111110", text: "Soybean Farming" },
        ],
      };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/industries/")
        .reply(200, mockResponse);

      const naics = await getNaics();

      expect(naics).toHaveLength(5);

      // Check level 2 has no parent
      const level2 = naics.find(
        (n) => n.naics_code === mockResponse.industries[0].code
      );
      expect(level2?.parent_code).toBeNull();

      // Check level 3 parent is level 2 code
      const level3 = naics.find(
        (n) => n.naics_code === mockResponse.industries[1].code
      );
      expect(level3?.parent_code).toBe(mockResponse.industries[0].code);

      // Check level 4 parent is level 3 code
      const level4 = naics.find(
        (n) => n.naics_code === mockResponse.industries[2].code
      );
      expect(level4?.parent_code).toBe(mockResponse.industries[1].code);

      // Check level 5 parent is level 4 code
      const level5 = naics.find(
        (n) => n.naics_code === mockResponse.industries[3].code
      );
      expect(level5?.parent_code).toBe(mockResponse.industries[2].code);

      // Check level 6 parent is level 5 code
      const level6 = naics.find(
        (n) => n.naics_code === mockResponse.industries[4].code
      );
      expect(level6?.parent_code).toBe(mockResponse.industries[3].code);
    });

    it("should filter out NAICS codes with invalid lengths", async () => {
      const mockResponse: NaicsResponseType = {
        industries: [
          { code: "11", text: "Valid 2-digit" },
          { code: "111", text: "Valid 3-digit" },
          { code: "1111", text: "Valid 4-digit" },
          { code: "11111", text: "Valid 5-digit" },
          { code: "111110", text: "Valid 6-digit" },
          { code: "1", text: "Invalid 1-digit" },
          { code: "1111111", text: "Invalid 7-digit" },
        ],
      };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/industries/")
        .reply(200, mockResponse);

      const naics = await getNaics();

      const expectedCodes = mockResponse.industries
        .filter(
          (industry) => industry.code.length >= 2 && industry.code.length <= 6
        )
        .map((industry) => industry.code);
      expect(naics).toHaveLength(expectedCodes.length); // Only codes with length 2-6
      expect(naics.map((n) => n.naics_code)).toEqual(expectedCodes);
    });

    it("should throw a descriptive error for invalid code shapes", async () => {
      const mockResponse: NaicsResponseType = {
        industries: [
          { code: "1A", text: "Invalid format" },
          { code: "11", text: "Agriculture" },
        ],
      };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/industries/")
        .reply(200, mockResponse);

      const invalidCode = mockResponse.industries[0].code;
      await expect(getNaics()).rejects.toThrow(
        `Invalid NAICS code format: ${invalidCode}`
      );
    });

    it("should return empty array for empty API response", async () => {
      const mockResponse: NaicsResponseType = { industries: [] };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/industries/")
        .reply(200, mockResponse);

      const naics = await getNaics();

      expect(naics).toHaveLength(mockResponse.industries.length);
      expect(naics).toEqual([]);
    });

    it("should throw error when API request fails", async () => {
      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/industries/")
        .reply(500, { error: "Internal server error" });

      await expect(getNaics()).rejects.toThrow("NAICS codes API");
    });

    it("should throw error when response validation fails", async () => {
      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/industries/")
        .reply(200, { invalid: "data" });

      await expect(getNaics()).rejects.toThrow("NAICS codes API");
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

      const mockResponse: NaicsResponseType = {
        industries: [
          { code: "11", text: "Agriculture" },
          { code: "23", text: "Construction" },
        ],
      };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/industries/")
        .reply(200, mockResponse);

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
