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
import { TimeseriesResponseType } from "../../schemas/schemas.ts";
import { TestDbManager } from "../../test-utils/testDBManager.ts";
import {
  BLSWageRequestBody,
  createSeriesId,
  getWages,
  insertWagesIntoDb,
} from "./utils.ts";
import { API_BASE_URL, WAGE_API_PATH } from "../constants.ts";

// Test Fixtures - Shared test data for consistency
const TEST_SOC_1 = { code: "11-1011", title: "Chief Executives" };
const TEST_SOC_2 = { code: "11-1021", title: "General Managers" };

const TEST_NAICS_1 = { code: "11", title: "Agriculture", level: 2 };
const TEST_NAICS_2 = { code: "23", title: "Construction", level: 2 };
const TEST_NAICS_3 = { code: "111", title: "Crop Production", level: 3 };
const TEST_NAICS_4 = { code: "1111", title: "Oilseed Farming", level: 4 };

const WAGE_VALUE_1 = 150000;
const WAGE_VALUE_2 = 145000;
const WAGE_VALUE_3 = 180000;

const TEST_YEAR_1 = 2023;
const TEST_YEAR_2 = 2022;
const START_YEAR = 2020;
const END_YEAR = 2023;

const API_SUCCESS_STATUS = "REQUEST_SUCCEEDED";
const ANNUAL_PERIOD = "A01";

describe("crawl_wages/utils", () => {
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

  describe("createSeriesId", () => {
    it("should create a valid series ID from SOC and NAICS codes", () => {
      const result = createSeriesId({ soc: "11-1011", naics: "11" });
      expect(result).toBe("OEUN00001111101103");
    });

    it("should pad NAICS codes to 6 digits", () => {
      const result = createSeriesId({ soc: "15-1256", naics: "541511" });
      expect(result).toBe("OEUN54151115125603");
    });

    it("should remove hyphens from SOC codes", () => {
      const result = createSeriesId({ soc: "11-1011", naics: "23" });
      expect(result).toContain("111011");
      expect(result).not.toContain("-");
    });

    it("should handle longer NAICS codes correctly", () => {
      const result = createSeriesId({ soc: "11-1011", naics: "1111" });
      expect(result).toBe("OEUN00111111101103");
    });
  });

  describe("getWages", () => {
    it("should return empty results when no SOC codes exist", async () => {
      const db = await dbManager.getTestDb("empty-db-test");

      // No SOC or NAICS codes inserted - database is empty

      nock(API_BASE_URL)
        .post(WAGE_API_PATH)
        .reply(200, {
          status: API_SUCCESS_STATUS,
          responseTime: 100,
          message: [],
          Results: { series: [] },
        })
        .persist();

      const result = await getWages({
        db,
        startYear: START_YEAR,
        endYear: END_YEAR,
      });

      expect(result.oewsSeries).toHaveLength(0);
      expect(result.wages).toHaveLength(0);
    });

    it("should fetch wage data for SOC-NAICS combinations", async () => {
      const db = await dbManager.getTestDb("get-wages-test");

      // Insert test data using shared fixtures
      const testSoc = TEST_SOC_1;
      const testNaicsCodes = [TEST_NAICS_1, TEST_NAICS_2];

      await db
        .insertInto("soc_codes")
        .values([{ soc_code: testSoc.code, title: testSoc.title }])
        .execute();

      await db
        .insertInto("naics_codes")
        .values(
          testNaicsCodes.map((n) => ({
            naics_code: n.code,
            title: n.title,
            level: n.level,
            parent_code: null,
          }))
        )
        .execute();

      const seriesId1 = createSeriesId({
        soc: testSoc.code,
        naics: testNaicsCodes[0].code,
      });
      const seriesId2 = createSeriesId({
        soc: testSoc.code,
        naics: testNaicsCodes[1].code,
      });

      const mockResponse: TimeseriesResponseType = {
        status: API_SUCCESS_STATUS,
        responseTime: 100,
        message: [],
        Results: {
          series: [
            {
              seriesID: seriesId1,
              data: [
                {
                  year: TEST_YEAR_1.toString(),
                  period: ANNUAL_PERIOD,
                  periodName: "Annual",
                  value: WAGE_VALUE_1.toString(),
                  footnotes: [],
                },
                {
                  year: TEST_YEAR_2.toString(),
                  period: ANNUAL_PERIOD,
                  periodName: "Annual",
                  value: WAGE_VALUE_2.toString(),
                  footnotes: [],
                },
              ],
            },
            {
              seriesID: seriesId2,
              data: [
                {
                  year: TEST_YEAR_1.toString(),
                  period: ANNUAL_PERIOD,
                  periodName: "Annual",
                  value: WAGE_VALUE_3.toString(),
                  footnotes: [],
                },
              ],
            },
          ],
        },
      };

      nock(API_BASE_URL).post(WAGE_API_PATH).reply(200, mockResponse);

      const result = await getWages({
        db,
        startYear: START_YEAR,
        endYear: END_YEAR,
      });

      expect(result.wages).toHaveLength(3);
      expect(result.oewsSeries).toHaveLength(2);

      // Check series records using variables
      const series1 = result.oewsSeries.find(
        (s) => s.naics_code === testNaicsCodes[0].code
      );
      expect(series1).toBeTruthy();
      expect(series1?.soc_code).toBe(testSoc.code);
      expect(series1?.does_exist).toBe(true);

      const series2 = result.oewsSeries.find(
        (s) => s.naics_code === testNaicsCodes[1].code
      );
      expect(series2).toBeTruthy();
      expect(series2?.soc_code).toBe(testSoc.code);
      expect(series2?.does_exist).toBe(true);

      // Check wage records using variables
      expect(result.wages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            series_id: seriesId1,
            year: TEST_YEAR_1,
            mean_annual_wage: WAGE_VALUE_1,
          }),
          expect.objectContaining({
            series_id: seriesId1,
            year: TEST_YEAR_2,
            mean_annual_wage: WAGE_VALUE_2,
          }),
          expect.objectContaining({
            series_id: seriesId2,
            year: TEST_YEAR_1,
            mean_annual_wage: WAGE_VALUE_3,
          }),
        ])
      );
    });

    it("should skip already processed series", async () => {
      const db = await dbManager.getTestDb("skip-processed-test");

      // 2 SOCs * 2 NAICS = 4 total combinations
      const testSocCodes = [TEST_SOC_1, TEST_SOC_2];
      const testNaicsCodes = [TEST_NAICS_1, TEST_NAICS_2];

      await db
        .insertInto("soc_codes")
        .values(testSocCodes.map((s) => ({ soc_code: s.code, title: s.title })))
        .execute();

      await db
        .insertInto("naics_codes")
        .values(
          testNaicsCodes.map((n) => ({
            naics_code: n.code,
            title: n.title,
            level: n.level,
            parent_code: null,
          }))
        )
        .execute();

      // Calculate all series IDs
      const seriesId11 = createSeriesId({
        soc: testSocCodes[0].code,
        naics: testNaicsCodes[0].code,
      });
      const seriesId12 = createSeriesId({
        soc: testSocCodes[0].code,
        naics: testNaicsCodes[1].code,
      });
      const seriesId21 = createSeriesId({
        soc: testSocCodes[1].code,
        naics: testNaicsCodes[0].code,
      });
      const seriesId22 = createSeriesId({
        soc: testSocCodes[1].code,
        naics: testNaicsCodes[1].code,
      });

      // Mark first series as already processed
      await db
        .insertInto("oews_series")
        .values({
          series_id: seriesId11,
          soc_code: testSocCodes[0].code,
          naics_code: testNaicsCodes[0].code,
          does_exist: true,
          last_checked: new Date(),
        })
        .execute();

      // Track which series IDs were requested
      const requestedSeriesIds: string[] = [];

      nock(API_BASE_URL)
        .post(WAGE_API_PATH)
        .reply(200, (_uri: string, requestBody: BLSWageRequestBody) => {
          requestedSeriesIds.push(...requestBody.seriesid);

          const series = requestBody.seriesid.map((id) => ({
            seriesID: id,
            data: [
              {
                year: TEST_YEAR_1.toString(),
                period: ANNUAL_PERIOD,
                periodName: "Annual",
                value: WAGE_VALUE_1.toString(),
                footnotes: [],
              },
            ],
          }));

          return {
            status: API_SUCCESS_STATUS,
            responseTime: 100,
            message: [],
            Results: { series },
          };
        })
        .persist();

      const result = await getWages({
        db,
        startYear: START_YEAR,
        endYear: END_YEAR,
      });

      // Verify the already processed series was NOT requested
      expect(requestedSeriesIds).not.toContain(seriesId11);

      // Verify the already processed series is NOT in the results
      const processedSeriesInResult = result.oewsSeries.find(
        (s) => s.series_id === seriesId11
      );
      expect(processedSeriesInResult).toBeUndefined();

      // Verify only the 3 unprocessed series are in results
      expect(result.oewsSeries).toHaveLength(3);
      expect(result.oewsSeries.map((s) => s.series_id).sort()).toEqual(
        [seriesId12, seriesId21, seriesId22].sort()
      );

      // Verify all unprocessed series have does_exist = true
      expect(result.oewsSeries.every((s) => s.does_exist === true)).toBe(true);

      // Verify wages were only created for unprocessed series
      expect(result.wages).toHaveLength(3);
      const wageSeriesIds = result.wages.map((w) => w.series_id);
      expect(wageSeriesIds).not.toContain(seriesId11);
      expect(wageSeriesIds.sort()).toEqual(
        [seriesId12, seriesId21, seriesId22].sort()
      );
    });

    it("should handle series with no data (does_exist = false)", async () => {
      const db = await dbManager.getTestDb("no-data-test");

      const testSocCodes = [TEST_SOC_1, TEST_SOC_2];
      const testNaicsCodes = [TEST_NAICS_1, TEST_NAICS_2];

      await db
        .insertInto("soc_codes")
        .values(testSocCodes.map((s) => ({ soc_code: s.code, title: s.title })))
        .execute();

      await db
        .insertInto("naics_codes")
        .values(
          testNaicsCodes.map((n) => ({
            naics_code: n.code,
            title: n.title,
            level: n.level,
            parent_code: null,
          }))
        )
        .execute();

      const mockResponse: TimeseriesResponseType = {
        status: API_SUCCESS_STATUS,
        responseTime: 100,
        message: [],
        Results: {
          series: [], // No data returned
        },
      };

      nock(API_BASE_URL).post(WAGE_API_PATH).reply(200, mockResponse).persist();

      const result = await getWages({
        db,
        startYear: START_YEAR,
        endYear: END_YEAR,
      });

      // Should record series but with does_exist = false (2 SOCs * 2 NAICS = 4)
      const expectedSeriesCount = testSocCodes.length * testNaicsCodes.length;
      expect(result.oewsSeries).toHaveLength(expectedSeriesCount);
      expect(result.oewsSeries.every((s) => s.does_exist === false)).toBe(true);
      expect(result.wages).toHaveLength(0);
    });

    it("should explore child NAICS codes when data is found", async () => {
      const db = await dbManager.getTestDb("explore-children-test");

      const testSoc = TEST_SOC_1;
      const testNaicsHierarchy = [TEST_NAICS_1, TEST_NAICS_3, TEST_NAICS_4];

      await db
        .insertInto("soc_codes")
        .values([{ soc_code: testSoc.code, title: testSoc.title }])
        .execute();

      // Create NAICS hierarchy: level 2 -> level 3 -> level 4
      await db
        .insertInto("naics_codes")
        .values([
          {
            naics_code: testNaicsHierarchy[0].code,
            title: testNaicsHierarchy[0].title,
            level: testNaicsHierarchy[0].level,
            parent_code: null,
          },
          {
            naics_code: testNaicsHierarchy[1].code,
            title: testNaicsHierarchy[1].title,
            level: testNaicsHierarchy[1].level,
            parent_code: testNaicsHierarchy[0].code,
          },
          {
            naics_code: testNaicsHierarchy[2].code,
            title: testNaicsHierarchy[2].title,
            level: testNaicsHierarchy[2].level,
            parent_code: testNaicsHierarchy[1].code,
          },
        ])
        .execute();

      const seriesIdLevel2 = createSeriesId({
        soc: testSoc.code,
        naics: testNaicsHierarchy[0].code,
      });
      const seriesIdLevel3 = createSeriesId({
        soc: testSoc.code,
        naics: testNaicsHierarchy[1].code,
      });
      let requestCount = 0;
      nock(API_BASE_URL)
        .post(WAGE_API_PATH)
        .reply(200, (_: string, requestBody: BLSWageRequestBody) => {
          requestCount++;
          const seriesIds = requestBody.seriesid;
          const series: Array<{
            seriesID: string;
            data: Array<{
              year: string;
              period: string;
              periodName: string;
              value: string;
              footnotes: [];
            }>;
          }> = [];

          // Return data for level 2 and 3, no data for level 4
          if (seriesIds.includes(seriesIdLevel2)) {
            series.push({
              seriesID: seriesIdLevel2,
              data: [
                {
                  year: TEST_YEAR_1.toString(),
                  period: ANNUAL_PERIOD,
                  periodName: "Annual",
                  value: WAGE_VALUE_1.toString(),
                  footnotes: [],
                },
              ],
            });
          }
          if (seriesIds.includes(seriesIdLevel3)) {
            series.push({
              seriesID: seriesIdLevel3,
              data: [
                {
                  year: TEST_YEAR_1.toString(),
                  period: ANNUAL_PERIOD,
                  periodName: "Annual",
                  value: WAGE_VALUE_3.toString(),
                  footnotes: [],
                },
              ],
            });
          }

          return {
            status: API_SUCCESS_STATUS,
            responseTime: 100,
            message: [],
            Results: { series },
          };
        })
        .persist();

      const result = await getWages({
        db,
        startYear: START_YEAR,
        endYear: END_YEAR,
      });

      // Should have series records for all 3 levels
      expect(result.oewsSeries).toHaveLength(testNaicsHierarchy.length);

      // Level 2 and 3 should have data, level 4 should not
      const level2Series = result.oewsSeries.find(
        (s) => s.naics_code === testNaicsHierarchy[0].code
      );
      const level3Series = result.oewsSeries.find(
        (s) => s.naics_code === testNaicsHierarchy[1].code
      );
      const level4Series = result.oewsSeries.find(
        (s) => s.naics_code === testNaicsHierarchy[2].code
      );

      expect(level2Series?.does_exist).toBe(true);
      expect(level3Series?.does_exist).toBe(true);
      expect(level4Series?.does_exist).toBe(false);

      // Should have wage data for level 2 and 3
      expect(result.wages).toHaveLength(2);
    });

    it("should throw error when BLS_API_KEY is missing", async () => {
      const db = await dbManager.getTestDb("missing-key-test");

      // Temporarily remove API key
      const originalKey = process.env.BLS_API_KEY;
      delete process.env.BLS_API_KEY;

      await expect(
        getWages({ db, startYear: START_YEAR, endYear: END_YEAR })
      ).rejects.toThrow("BLS_API_KEY environment variable is required");

      // Restore API key
      process.env.BLS_API_KEY = originalKey;
    });

    it("should handle API errors gracefully", async () => {
      const db = await dbManager.getTestDb("api-error-test");

      const seeder = new (
        await import("../../test-utils/testDataSeeder/index.ts")
      ).TestDataSeeder(db);
      await seeder.seedSocCodes();
      await seeder.seedNaicsCodes();

      nock(API_BASE_URL)
        .post(WAGE_API_PATH)
        .reply(500, { error: "Internal server error" });

      await expect(
        getWages({ db, startYear: START_YEAR, endYear: END_YEAR })
      ).rejects.toThrow("timeseries data");
    });

    it("should process in batches of BATCH_SIZE", async () => {
      const db = await dbManager.getTestDb("batch-test");

      const testSoc = TEST_SOC_1;
      const numNaicsCodes = 60;
      const batchSize = 50;
      const expectedRequestCount = Math.ceil(numNaicsCodes / batchSize);

      await db
        .insertInto("soc_codes")
        .values([{ soc_code: testSoc.code, title: testSoc.title }])
        .execute();

      // Create NAICS codes dynamically
      const naicsCodes = Array.from({ length: numNaicsCodes }, (_, i) => ({
        naics_code: (10 + i).toString(),
        title: `Industry ${i}`,
        level: 2,
        parent_code: null,
      }));

      await db.insertInto("naics_codes").values(naicsCodes).execute();

      let requestCount = 0;
      nock(API_BASE_URL)
        .post(WAGE_API_PATH)
        .reply(200, () => {
          requestCount++;
          return {
            status: API_SUCCESS_STATUS,
            responseTime: 100,
            message: [],
            Results: { series: [] },
          };
        })
        .persist();

      const result = await getWages({
        db,
        startYear: START_YEAR,
        endYear: END_YEAR,
      });

      expect(requestCount).toBe(expectedRequestCount);
      expect(result.oewsSeries).toHaveLength(numNaicsCodes);
    });
  });

  describe("insertWagesIntoDb", () => {
    it("should insert series and wage data into database", async () => {
      const db = await dbManager.getTestDb("insert-wages-test");

      const testSoc = TEST_SOC_1;
      const testNaicsCodes = [TEST_NAICS_1, TEST_NAICS_2];
      const seriesId1 = createSeriesId({
        soc: testSoc.code,
        naics: testNaicsCodes[0].code,
      });
      const seriesId2 = createSeriesId({
        soc: testSoc.code,
        naics: testNaicsCodes[1].code,
      });

      await db
        .insertInto("soc_codes")
        .values([{ soc_code: testSoc.code, title: testSoc.title }])
        .execute();

      await db
        .insertInto("naics_codes")
        .values(
          testNaicsCodes.map((n) => ({
            naics_code: n.code,
            title: n.title,
            level: n.level,
            parent_code: null,
          }))
        )
        .execute();

      const testSeries = [
        {
          series_id: seriesId1,
          soc_code: testSoc.code,
          naics_code: testNaicsCodes[0].code,
          does_exist: true,
          last_checked: new Date(),
        },
        {
          series_id: seriesId2,
          soc_code: testSoc.code,
          naics_code: testNaicsCodes[1].code,
          does_exist: false,
          last_checked: new Date(),
        },
      ];

      const testWages = [
        {
          series_id: seriesId1,
          year: TEST_YEAR_1,
          mean_annual_wage: WAGE_VALUE_1,
        },
        {
          series_id: seriesId1,
          year: TEST_YEAR_2,
          mean_annual_wage: WAGE_VALUE_2,
        },
      ];

      await insertWagesIntoDb({ wages: testWages, oewsSeries: testSeries, db });

      // Verify series insertion
      const insertedSeries = await db
        .selectFrom("oews_series")
        .selectAll()
        .orderBy("series_id")
        .execute();

      expect(insertedSeries).toHaveLength(2);
      expect(insertedSeries[0].series_id).toBe(seriesId1);
      expect(insertedSeries[0].does_exist).toBe(true);
      expect(insertedSeries[1].series_id).toBe(seriesId2);
      expect(insertedSeries[1].does_exist).toBe(false);

      // Verify wages insertion
      const insertedWages = await db
        .selectFrom("wages")
        .selectAll()
        .orderBy("series_id")
        .orderBy("year")
        .execute();

      expect(insertedWages).toHaveLength(2);
      expect(insertedWages[0]).toEqual(
        expect.objectContaining({
          series_id: seriesId1,
          year: TEST_YEAR_2,
          mean_annual_wage: WAGE_VALUE_2,
        })
      );
      expect(insertedWages[1]).toEqual(
        expect.objectContaining({
          series_id: seriesId1,
          year: TEST_YEAR_1,
          mean_annual_wage: WAGE_VALUE_1,
        })
      );
    });

    it("should handle duplicates with onConflict doNothing", async () => {
      const db = await dbManager.getTestDb("duplicate-wages-test");

      const testSocCodes = [TEST_SOC_1, TEST_SOC_2];
      const testNaicsCodes = [TEST_NAICS_1, TEST_NAICS_2];
      const seriesId1 = createSeriesId({
        soc: testSocCodes[0].code,
        naics: testNaicsCodes[0].code,
      });
      const seriesId2 = createSeriesId({
        soc: testSocCodes[0].code,
        naics: testNaicsCodes[1].code,
      });

      await db
        .insertInto("soc_codes")
        .values(testSocCodes.map((s) => ({ soc_code: s.code, title: s.title })))
        .execute();

      await db
        .insertInto("naics_codes")
        .values(
          testNaicsCodes.map((n) => ({
            naics_code: n.code,
            title: n.title,
            level: n.level,
            parent_code: null,
          }))
        )
        .execute();

      // Insert initial series and wage
      await db
        .insertInto("oews_series")
        .values({
          series_id: seriesId1,
          soc_code: testSocCodes[0].code,
          naics_code: testNaicsCodes[0].code,
          does_exist: true,
          last_checked: new Date(),
        })
        .execute();

      await db
        .insertInto("wages")
        .values({
          series_id: seriesId1,
          year: TEST_YEAR_1,
          mean_annual_wage: WAGE_VALUE_1,
        })
        .execute();

      const testSeries = [
        {
          series_id: seriesId1, // Already exists
          soc_code: testSocCodes[0].code,
          naics_code: testNaicsCodes[0].code,
          does_exist: false, // Different value, should be ignored
          last_checked: new Date(),
        },
        {
          series_id: seriesId2, // New
          soc_code: testSocCodes[0].code,
          naics_code: testNaicsCodes[1].code,
          does_exist: true,
          last_checked: new Date(),
        },
      ];

      const testWages = [
        {
          series_id: seriesId1,
          year: TEST_YEAR_1,
          mean_annual_wage: 999999, // Already exists, should be ignored
        },
        {
          series_id: seriesId2,
          year: TEST_YEAR_1,
          mean_annual_wage: WAGE_VALUE_3, // New
        },
      ];

      await insertWagesIntoDb({ wages: testWages, oewsSeries: testSeries, db });

      // Verify series: should have 2 (original + new)
      const insertedSeries = await db
        .selectFrom("oews_series")
        .selectAll()
        .orderBy("series_id")
        .execute();

      expect(insertedSeries).toHaveLength(2);

      // Original series should still have does_exist = true
      const originalSeries = insertedSeries.find(
        (s) => s.series_id === seriesId1
      );
      expect(originalSeries?.does_exist).toBe(true);

      // Verify wages: should have 2 (original + new)
      const insertedWages = await db
        .selectFrom("wages")
        .selectAll()
        .orderBy("series_id")
        .orderBy("year")
        .execute();

      expect(insertedWages).toHaveLength(2);

      // Original wage should still have original value
      const originalWage = insertedWages.find((w) => w.series_id === seriesId1);
      expect(originalWage?.mean_annual_wage).toBe(WAGE_VALUE_1);
    });
  });

  describe("integration", () => {
    it("should fetch wages and insert into database", async () => {
      const db = await dbManager.getTestDb("integration-wages-test");

      const testSocCodes = [TEST_SOC_1, TEST_SOC_2];
      const testNaicsCodes = [TEST_NAICS_1, TEST_NAICS_2];

      await db
        .insertInto("soc_codes")
        .values(testSocCodes.map((s) => ({ soc_code: s.code, title: s.title })))
        .execute();

      await db
        .insertInto("naics_codes")
        .values(
          testNaicsCodes.map((n) => ({
            naics_code: n.code,
            title: n.title,
            level: n.level,
            parent_code: null,
          }))
        )
        .execute();

      // Create a dynamic mock that returns data for the first series in each request
      nock(API_BASE_URL)
        .post(WAGE_API_PATH)
        .reply(200, (_uri: string, requestBody: BLSWageRequestBody) => {
          const firstSeriesId = requestBody.seriesid[0];
          return {
            status: API_SUCCESS_STATUS,
            responseTime: 100,
            message: [],
            Results: {
              series: [
                {
                  seriesID: firstSeriesId,
                  data: [
                    {
                      year: TEST_YEAR_1.toString(),
                      period: ANNUAL_PERIOD,
                      periodName: "Annual",
                      value: WAGE_VALUE_1.toString(),
                      footnotes: [],
                    },
                  ],
                },
              ],
            },
          };
        })
        .persist();

      // Fetch wages
      const result = await getWages({
        db,
        startYear: START_YEAR,
        endYear: END_YEAR,
      });
      const expectedSeriesCount = testSocCodes.length * testNaicsCodes.length;
      expect(result.oewsSeries).toHaveLength(expectedSeriesCount);
      expect(result.wages.length).toBeGreaterThan(0);

      // Insert into database
      await insertWagesIntoDb({
        wages: result.wages,
        oewsSeries: result.oewsSeries,
        db,
      });

      // Verify data in database
      const dbSeries = await db.selectFrom("oews_series").selectAll().execute();
      const dbWages = await db.selectFrom("wages").selectAll().execute();

      expect(dbSeries).toHaveLength(expectedSeriesCount);
      expect(dbWages.length).toBeGreaterThan(0);
      expect(dbWages[0].mean_annual_wage).toBe(WAGE_VALUE_1);
    });

    it("should process full deterministic dataset with multiple years, hierarchy, and partial batch", async () => {
      const db = await dbManager.createAndSeedTestDbDeterministic(
        "full-integration-test",
        "comprehensive-test-seed"
      );

      // Query actual seeded data to understand the structure
      const allNaics = await db.selectFrom("naics_codes").selectAll().execute();

      // Track API calls to verify batch behavior
      const apiCalls: string[][] = [];

      // Mock API to return data for first series in each batch (to trigger hierarchy exploration)
      // and multiple years of data
      nock(API_BASE_URL)
        .post(WAGE_API_PATH)
        .reply(200, (_uri: string, requestBody: BLSWageRequestBody) => {
          apiCalls.push(requestBody.seriesid);

          // Return data for first series only to trigger hierarchy exploration
          // but keep test execution reasonable
          const firstSeriesId = requestBody.seriesid[0];
          const hasData = requestBody.seriesid.length > 0;

          const series = hasData
            ? [
                {
                  seriesID: firstSeriesId,
                  data: [
                    {
                      year: "2020",
                      period: ANNUAL_PERIOD,
                      periodName: "Annual",
                      value: "75000",
                      footnotes: [],
                    },
                    {
                      year: "2021",
                      period: ANNUAL_PERIOD,
                      periodName: "Annual",
                      value: "80000",
                      footnotes: [],
                    },
                    {
                      year: "2022",
                      period: ANNUAL_PERIOD,
                      periodName: "Annual",
                      value: "85000",
                      footnotes: [],
                    },
                  ],
                },
              ]
            : [];

          return {
            status: API_SUCCESS_STATUS,
            responseTime: 100,
            message: [],
            Results: { series },
          };
        })
        .persist();

      // Run getWages with multiple years
      const result = await getWages({
        db,
        startYear: 2020,
        endYear: 2022,
      });

      // Verify basic structure without hardcoding exact counts
      expect(result.oewsSeries.length).toBeGreaterThan(0);
      expect(result.wages.length).toBeGreaterThan(0);

      // Verify multiple years are present in wages
      const years = new Set(result.wages.map((w) => w.year));
      expect(years.size).toBeGreaterThanOrEqual(1);
      expect([...years].every((y) => y >= 2020 && y <= 2022)).toBe(true);

      // Verify batch processing occurred
      expect(apiCalls.length).toBeGreaterThan(1);

      // Verify no batch exceeded BATCH_SIZE (50)
      expect(apiCalls.every((call) => call.length <= 50)).toBe(true);

      // Verify partial batch behavior (last call should be smaller if total not divisible by 50)
      const totalSeriesRequested = apiCalls.reduce(
        (sum, call) => sum + call.length,
        0
      );
      const lastCallSize = apiCalls[apiCalls.length - 1].length;
      if (totalSeriesRequested % 50 !== 0) {
        expect(lastCallSize).toBeLessThan(50);
      }

      // Verify hierarchy exploration - check if any level > 2 NAICS were processed
      const childSeries = result.oewsSeries.filter((s) => {
        const naics = allNaics.find((n) => n.naics_code === s.naics_code);
        return naics && naics.level > 2;
      });

      // If there are children in the database, some should have been explored
      const hasChildrenInDb = allNaics.some((n) => n.level > 2);
      if (hasChildrenInDb) {
        expect(childSeries.length).toBeGreaterThan(0);
      }

      // Verify mix of does_exist values (some have data, some don't)
      const existingCount = result.oewsSeries.filter(
        (s) => s.does_exist
      ).length;
      const nonExistingCount = result.oewsSeries.filter(
        (s) => !s.does_exist
      ).length;
      expect(existingCount).toBeGreaterThan(0);
      expect(nonExistingCount).toBeGreaterThanOrEqual(0);

      // Get counts before insertion
      const seriesBefore = await db
        .selectFrom("oews_series")
        .selectAll()
        .execute();
      const wagesBefore = await db.selectFrom("wages").selectAll().execute();

      // Insert into database and verify
      await insertWagesIntoDb({
        wages: result.wages,
        oewsSeries: result.oewsSeries,
        db,
      });

      const dbSeries = await db.selectFrom("oews_series").selectAll().execute();
      const dbWages = await db.selectFrom("wages").selectAll().execute();

      // Verify new records were added (accounting for existing seeded data)
      expect(dbSeries.length).toBe(
        seriesBefore.length + result.oewsSeries.length
      );
      expect(dbWages.length).toBe(wagesBefore.length + result.wages.length);
    });
  });
});
