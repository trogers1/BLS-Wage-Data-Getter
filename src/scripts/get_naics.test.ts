import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import nock from "nock";
import { NaicsResponseType } from "../schemas";

// Mock the database module
vi.mock("../db", () => ({
  db: {
    insertInto: vi.fn().mockReturnThis(),
    values: vi.fn().mockReturnThis(),
    onConflict: vi.fn().mockReturnThis(),
    execute: vi.fn().mockResolvedValue({}),
    destroy: vi.fn().mockResolvedValue(undefined),
  },
}));

describe("get_naics.ts", () => {
  const API_URL = "https://api.bls.gov/publicAPI/v2/surveys/OEWS/industries/";

  beforeEach(() => {
    nock.cleanAll();
    vi.clearAllMocks();
  });

  afterEach(() => {
    nock.cleanAll();
  });

  it("should fetch and process NAICS codes with correct hierarchy", async () => {
    const mockResponse = {
      industries: [
        { code: "11", text: "Agriculture, Forestry, Fishing and Hunting" },
        { code: "111", text: "Crop Production" },
        { code: "1111", text: "Oilseed and Grain Farming" },
        { code: "11111", text: "Soybean Farming" },
        { code: "111110", text: "Soybean Farming" },
        { code: "1", text: "Invalid Level 1" }, // Should be filtered out (level < 2)
        { code: "1111111", text: "Invalid Level 7" }, // Should be filtered out (level > 6)
      ],
    } satisfies NaicsResponseType;

    nock("https://api.bls.gov")
      .get("/publicAPI/v2/surveys/OEWS/industries/")
      .reply(200, mockResponse);

    // Import the module
    const module = await import("./get_naics.ts");

    // Verify database was called for valid NAICS codes
    const { db } = await import("../db");

    // Should insert 5 valid codes (levels 2-6)
    expect(db.insertInto).toHaveBeenCalledTimes(5);
    expect(db.insertInto).toHaveBeenCalledWith("naics_codes");

    // Check parent_code assignments
    const calls = (db.insertInto as any).mock.calls;
    const insertedData = calls
      .map((call: any[]) => {
        const values = call[1] || call[0]?.values;
        return values
          ? {
              naics_code: values.naics_code,
              level: values.level,
              parent_code: values.parent_code,
            }
          : null;
      })
      .filter(Boolean);

    // Verify level 2 has no parent
    const level2 = insertedData.find((d: any) => d.naics_code === "11");
    expect(level2?.level).toBe(2);
    expect(level2?.parent_code).toBeNull();

    // Verify level 3 has correct parent
    const level3 = insertedData.find((d: any) => d.naics_code === "111");
    expect(level3?.level).toBe(3);
    expect(level3?.parent_code).toBe("11");

    // Verify level 4 has correct parent
    const level4 = insertedData.find((d: any) => d.naics_code === "1111");
    expect(level4?.level).toBe(4);
    expect(level4?.parent_code).toBe("111");

    // Verify level 5 has correct parent
    const level5 = insertedData.find((d: any) => d.naics_code === "11111");
    expect(level5?.level).toBe(5);
    expect(level5?.parent_code).toBe("1111");

    // Verify level 6 has correct parent
    const level6 = insertedData.find((d: any) => d.naics_code === "111110");
    expect(level6?.level).toBe(6);
    expect(level6?.parent_code).toBe("11111");
  });

  it("should handle API errors", async () => {
    nock("https://api.bls.gov")
      .get("/publicAPI/v2/surveys/OEWS/industries/")
      .reply(500, { error: "Internal Server Error" });

    await expect(import("./get_naics.ts")).rejects.toThrow();
  });

  it("should validate response schema", async () => {
    const invalidResponse = {
      industries: [
        { code: "11", text: "Agriculture", extra: "field" }, // extra field
      ],
    };

    nock("https://api.bls.gov")
      .get("/publicAPI/v2/surveys/OEWS/industries/")
      .reply(200, invalidResponse);

    await expect(import("./get_naics.ts")).rejects.toThrow();
  });
});
