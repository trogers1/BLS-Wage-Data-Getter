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
import { SocResponseType } from "../../schemas/schemas.ts";
import { TestDbManager } from "../../test-utils/testDBManager.ts";
import { getSocs, insertSocsIntoDb } from "./utils.ts";

describe("get_socs/utils", () => {
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

  describe("getSocs", () => {
    it("should fetch and parse SOC codes from BLS API", async () => {
      const mockResponse: SocResponseType = {
        occupations: [
          { code: "11-1011", text: "Chief Executives" },
          { code: "15-1252", text: "Software Developers" },
          { code: "29-1141", text: "Registered Nurses" },
          { code: "99-9999", text: "Invalid code" }, // Should be filtered out
        ],
      };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/occupations/")
        .reply(200, mockResponse);

      const socs = await getSocs();

      const expectedSocs = mockResponse.occupations.map((occupation) => ({
        soc_code: occupation.code,
        title: occupation.text,
      }));
      expect(socs).toHaveLength(expectedSocs.length); // All have valid SOC code format
      expect(socs).toEqual(expectedSocs);
    });

    it("should throw error when API request fails", async () => {
      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/occupations/")
        .reply(500, { error: "Internal server error" });

      await expect(getSocs()).rejects.toThrow("SOC codes API");
    });

    it("should throw error when response validation fails", async () => {
      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/occupations/")
        .reply(200, { invalid: "data" });

      await expect(getSocs()).rejects.toThrow("SOC codes API");
    });

    it("should filter out non-standard SOC codes", async () => {
      const mockResponse: SocResponseType = {
        occupations: [
          { code: "11-1011", text: "Chief Executives" },
          { code: "11-101", text: "Invalid format" }, // Too short
          { code: "11-10111", text: "Too long" }, // Too long
          { code: "AB-1011", text: "Non-numeric" }, // Non-numeric
          { code: "11-ABCD", text: "Non-numeric" }, // Non-numeric
        ],
      };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/occupations/")
        .reply(200, mockResponse);

      const socs = await getSocs();
      const expectedSocs = mockResponse.occupations
        .filter((occupation) => /^\d{2}-\d{4}$/.test(occupation.code))
        .map((occupation) => ({
          soc_code: occupation.code,
          title: occupation.text,
        }));
      expect(socs).toHaveLength(expectedSocs.length);
      expect(socs).toEqual(expectedSocs);
    });

    it("should return empty array for empty API response", async () => {
      const mockResponse: SocResponseType = { occupations: [] };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/occupations/")
        .reply(200, mockResponse);

      const socs = await getSocs();

      expect(socs).toHaveLength(mockResponse.occupations.length);
      expect(socs).toEqual([]);
    });
  });

  describe("insertSocsIntoDb", () => {
    it("should insert SOC codes into database", async () => {
      const db = await dbManager.getTestDb("insert-socs-test");

      const testSocs = [
        { soc_code: "11-1011", title: "Chief Executives" },
        { soc_code: "15-1252", title: "Software Developers" },
        { soc_code: "29-1141", title: "Registered Nurses" },
      ];

      await insertSocsIntoDb({ socs: testSocs, db });

      const inserted = await db.selectFrom("soc_codes").selectAll().execute();

      expect(inserted).toHaveLength(3);
      expect(inserted).toEqual(
        expect.arrayContaining(
          testSocs.map((soc) => expect.objectContaining(soc))
        )
      );
    });

    it("should handle duplicates with onConflict doNothing", async () => {
      const db = await dbManager.getTestDb("duplicate-socs-test");

      // Insert first batch
      const firstBatch = [
        { soc_code: "11-1011", title: "Chief Executives" },
        { soc_code: "15-1252", title: "Software Developers" },
      ];

      await insertSocsIntoDb({ socs: firstBatch, db });

      // Try to insert same SOC codes with different titles (should be ignored)
      const secondBatch = [
        { soc_code: "11-1011", title: "Different Title" }, // Should be ignored
        { soc_code: "29-1141", title: "Registered Nurses" }, // New
      ];

      await insertSocsIntoDb({ socs: secondBatch, db });

      const inserted = await db
        .selectFrom("soc_codes")
        .selectAll()
        .orderBy("soc_code")
        .execute();

      expect(inserted).toHaveLength(3);

      // Check that the original title was preserved
      const chiefExec = inserted.find(
        (s) => s.soc_code === firstBatch[0].soc_code
      );
      expect(chiefExec?.title).toBe(firstBatch[0].title);

      // Check that new SOC code was added
      expect(
        inserted.find((s) => s.soc_code === secondBatch[1].soc_code)
      ).toBeTruthy();
    });

    it("should insert SOC codes in a transaction", async () => {
      const db = await dbManager.getTestDb("transaction-socs-test");

      const testSocs = [
        { soc_code: "11-1011", title: "Chief Executives" },
        { soc_code: "15-1252", title: "Software Developers" },
      ];

      // Mock transaction to verify it's used
      const executeSpy = vi.spyOn(db, "transaction");

      await insertSocsIntoDb({ socs: testSocs, db });

      expect(executeSpy).toHaveBeenCalled();

      const inserted = await db.selectFrom("soc_codes").selectAll().execute();

      expect(inserted).toHaveLength(testSocs.length);
    });

    it("should work with deterministic random data", async () => {
      const db = await dbManager.createAndSeedTestDbDeterministic(
        "deterministic-socs-test",
        "test-seed-123"
      );

      // Use the deterministic seeder directly to get SOC codes
      const seeder = new (
        await import("../../test-utils/testDataSeeder/index.ts")
      ).TestDataSeeder(db);
      await seeder.seedSocCodesDeterministic("test-seed-456");

      const inserted = await db.selectFrom("soc_codes").selectAll().execute();

      expect(inserted.length).toBeGreaterThan(0);

      // All SOC codes should have valid format
      for (const soc of inserted) {
        expect(soc.soc_code).toMatch(/^\d{2}-\d{4}$/);
        expect(soc.title).toBeTruthy();
      }
    });
  });

  describe("integration", () => {
    it("should fetch from API and insert into database", async () => {
      const db = await dbManager.getTestDb("integration-test");

      const mockResponse: SocResponseType = {
        occupations: [
          { code: "11-1011", text: "Chief Executives" },
          { code: "15-1252", text: "Software Developers" },
        ],
      };

      nock(API_URL)
        .get("/publicAPI/v2/surveys/OEWS/occupations/")
        .reply(200, mockResponse);

      // Fetch SOC codes
      const socs = await getSocs();
      expect(socs).toHaveLength(mockResponse.occupations.length);

      // Insert into database
      await insertSocsIntoDb({ socs, db });

      // Verify insertion
      const inserted = await db.selectFrom("soc_codes").selectAll().execute();

      expect(inserted).toHaveLength(socs.length);
      expect(inserted).toEqual(
        expect.arrayContaining(socs.map((soc) => expect.objectContaining(soc)))
      );
    });
  });
});
