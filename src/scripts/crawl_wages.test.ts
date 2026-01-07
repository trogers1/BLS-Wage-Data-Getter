import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import nock from "nock";
import { TimeseriesResponseType } from "../schemas";

// Mock environment variable
process.env.BLS_API_KEY = "test-api-key";

// Mock the database module with more complex behavior
const mockDb = {
  selectFrom: vi.fn().mockReturnThis(),
  select: vi.fn().mockReturnThis(),
  selectAll: vi.fn().mockReturnThis(),
  where: vi.fn().mockReturnThis(),
  execute: vi.fn(),
  executeTakeFirst: vi.fn(),
  insertInto: vi.fn().mockReturnThis(),
  values: vi.fn().mockReturnThis(),
  onConflict: vi.fn().mockReturnThis(),
  doNothing: vi.fn().mockReturnThis(),
  destroy: vi.fn().mockResolvedValue(undefined),
};

vi.mock("../db", () => ({
  db: mockDb,
}));

describe("crawl_wages.ts", () => {
  const API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/";

  beforeEach(() => {
    nock.cleanAll();
    vi.clearAllMocks();

    // Reset all mock implementations
    mockDb.selectFrom.mockClear();
    mockDb.select.mockClear();
    mockDb.selectAll.mockClear();
    mockDb.where.mockClear();
    mockDb.execute.mockReset();
    mockDb.executeTakeFirst.mockReset();
    mockDb.insertInto.mockClear();
    mockDb.values.mockClear();
    mockDb.onConflict.mockClear();
    mockDb.doNothing.mockClear();

    // Default mock implementations
    mockDb.selectFrom.mockImplementation(() => mockDb);
    mockDb.select.mockImplementation(() => mockDb);
    mockDb.selectAll.mockImplementation(() => mockDb);
    mockDb.where.mockImplementation(() => mockDb);
    mockDb.insertInto.mockImplementation(() => mockDb);
    mockDb.values.mockImplementation(() => mockDb);
    mockDb.onConflict.mockImplementation(() => mockDb);
    mockDb.doNothing.mockImplementation(() => mockDb);
  });

  afterEach(() => {
    nock.cleanAll();
  });

  it("should process SOC codes and crawl NAICS hierarchy in batches", async () => {
    // Mock SOC codes
    mockDb.execute.mockResolvedValueOnce([
      { soc_code: "11-1011" },
      { soc_code: "15-1252" },
    ]);

    // Mock initial NAICS codes (level 2)
    mockDb.execute.mockResolvedValueOnce([
      { naics_code: "11", title: "Agriculture", level: 2, parent_code: null },
      { naics_code: "23", title: "Construction", level: 2, parent_code: null },
    ]);

    // Mock series existence checks (none exist)
    mockDb.executeTakeFirst.mockResolvedValue(null);

    // Mock child NAICS codes for successful parents
    mockDb.execute.mockResolvedValueOnce([
      {
        naics_code: "111",
        title: "Crop Production",
        level: 3,
        parent_code: "11",
      },
      {
        naics_code: "112",
        title: "Animal Production",
        level: 3,
        parent_code: "11",
      },
    ]);

    // Mock batch API response
    const mockBatchResponse = {
      status: "REQUEST_SUCCEEDED",
      responseTime: 100,
      message: [],
      Results: {
        series: [
          {
            seriesID: "OEUN00000001100011101103",
            data: [
              {
                year: "2023",
                period: "A01",
                periodName: "Annual",
                value: "150000",
                footnotes: [],
              },
            ],
          },
          {
            seriesID: "OEUN00000002300011101103",
            data: [
              {
                year: "2023",
                period: "A01",
                periodName: "Annual",
                value: "120000",
                footnotes: [],
              },
            ],
          },
        ],
      },
    } satisfies TimeseriesResponseType;

    nock("https://api.bls.gov")
      .post("/publicAPI/v2/timeseries/data/")
      .reply(200, mockBatchResponse);

    // Import and run the module
    await import("./crawl_wages.ts");

    // Verify the API was called with batch of 2 series
    expect(nock.isDone()).toBe(true);

    // Verify database inserts for series
    expect(mockDb.insertInto).toHaveBeenCalledWith("oews_series");

    // Verify wage inserts for successful series
    expect(mockDb.insertInto).toHaveBeenCalledWith("wages");

    // Verify children were fetched for successful NAICS
    expect(mockDb.where).toHaveBeenCalledWith("parent_code", "=", "11");
  });

  it("should not crawl children if parent series doesn't exist", async () => {
    // Mock SOC codes
    mockDb.execute.mockResolvedValueOnce([{ soc_code: "11-1011" }]);

    // Mock initial NAICS codes
    mockDb.execute.mockResolvedValueOnce([
      { naics_code: "11", title: "Agriculture", level: 2, parent_code: null },
    ]);

    // Mock series existence checks
    mockDb.executeTakeFirst.mockResolvedValue(null);

    // Mock batch API response with no data (series doesn't exist)
    const mockBatchResponse = {
      status: "REQUEST_SUCCEEDED",
      responseTime: 100,
      message: ["No data available"],
      Results: {
        series: [],
      },
    } satisfies TimeseriesResponseType;

    nock("https://api.bls.gov")
      .post("/publicAPI/v2/timeseries/data/")
      .reply(200, mockBatchResponse);

    // Import and run
    await import("./crawl_wages.ts");

    // Verify API was called
    expect(nock.isDone()).toBe(true);

    // Should NOT fetch children since parent doesn't exist
    expect(mockDb.where).not.toHaveBeenCalledWith("parent_code", "=", "11");
  });

  it("should skip already processed series", async () => {
    // Mock SOC codes
    mockDb.execute.mockResolvedValueOnce([{ soc_code: "11-1011" }]);

    // Mock initial NAICS codes
    mockDb.execute.mockResolvedValueOnce([
      { naics_code: "11", title: "Agriculture", level: 2, parent_code: null },
    ]);

    // Mock series already exists
    mockDb.executeTakeFirst.mockResolvedValue({ exists: true });

    // Import and run
    await import("./crawl_wages.ts");

    // Should NOT call API for already processed series
    expect(nock.isDone()).toBe(false);
  });

  it("should handle API errors gracefully", async () => {
    // Mock SOC codes
    mockDb.execute.mockResolvedValueOnce([{ soc_code: "11-1011" }]);

    // Mock initial NAICS codes
    mockDb.execute.mockResolvedValueOnce([
      { naics_code: "11", title: "Agriculture", level: 2, parent_code: null },
    ]);

    // Mock series doesn't exist
    mockDb.executeTakeFirst.mockResolvedValue(null);

    // Mock API error
    nock("https://api.bls.gov")
      .post("/publicAPI/v2/timeseries/data/")
      .reply(500, { error: "Internal Server Error" });

    // Should throw validation error
    await expect(import("./crawl_wages.ts")).rejects.toThrow();
  });

  it("should respect BATCH_SIZE limit of 50", async () => {
    // Mock many SOC codes
    const manySocs = Array.from({ length: 3 }, (_, i) => ({
      soc_code: `11-10${(i + 11).toString().padStart(2, "0")}`,
    }));
    mockDb.execute.mockResolvedValueOnce(manySocs);

    // Mock many NAICS codes (more than batch size)
    const manyNaics = Array.from({ length: 60 }, (_, i) => ({
      naics_code: `${i + 10}`,
      title: `Industry ${i + 10}`,
      level: 2,
      parent_code: null,
    }));
    mockDb.execute.mockResolvedValueOnce(manyNaics.slice(0, 2)); // First SOC gets first 2
    mockDb.execute.mockResolvedValueOnce(manyNaics.slice(2, 52)); // First batch of 50
    mockDb.execute.mockResolvedValueOnce(manyNaics.slice(52, 60)); // Remaining 8

    // Mock series doesn't exist
    mockDb.executeTakeFirst.mockResolvedValue(null);

    // Mock successful responses
    const createMockResponse = (count: number) =>
      ({
        status: "REQUEST_SUCCEEDED",
        responseTime: 100,
        message: [],
        Results: {
          series: Array.from({ length: count }, (_, i) => ({
            seriesID: `OEUN0000000${i + 10}00011101103`,
            data: [
              {
                year: "2023",
                period: "A01",
                periodName: "Annual",
                value: "100000",
                footnotes: [],
              },
            ],
          })),
        },
      }) satisfies TimeseriesResponseType;

    // Expect multiple API calls due to batch size limit
    nock("https://api.bls.gov")
      .post("/publicAPI/v2/timeseries/data/")
      .times(2) // 2 batches: 50 + 8
      .reply(200, (uri, body) => {
        const data = JSON.parse(body as string);
        const count = data.seriesid.length;
        return createMockResponse(count);
      });

    await import("./crawl_wages.ts");

    // Should have made 2 API calls
    expect(nock.isDone()).toBe(true);
  });
});
