import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import nock from "nock";
import { SocResponseType } from "../schemas";

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

describe("get_socs.ts", () => {
  const API_URL = "https://api.bls.gov/publicAPI/v2/surveys/OEWS/occupations/";

  beforeEach(() => {
    nock.cleanAll();
    vi.clearAllMocks();
  });

  afterEach(() => {
    nock.cleanAll();
  });

  it("should fetch and process SOC codes successfully", async () => {
    const mockResponse = {
      occupations: [
        { code: "11-1011", text: "Chief Executives" },
        { code: "11-1021", text: "General and Operations Managers" },
        { code: "15-1252", text: "Software Developers" },
        { code: "INVALID", text: "Invalid Code" }, // Should be filtered out
      ],
    } satisfies SocResponseType;

    nock("https://api.bls.gov")
      .get("/publicAPI/v2/surveys/OEWS/occupations/")
      .reply(200, mockResponse);

    // Import the module dynamically to avoid hoisting issues
    const module = await import("./get_socs.ts");

    // Since the script runs immediately on import, we need to check the mocks
    const { db } = await import("../db");

    // Verify database was called for valid SOC codes
    expect(db.insertInto).toHaveBeenCalledTimes(3); // Only 3 valid codes
    expect(db.insertInto).toHaveBeenCalledWith("soc_codes");

    // Check that invalid code was filtered out
    const calls = (db.insertInto as any).mock.calls;
    const insertedCodes = calls
      .map((call: any[]) => call[1]?.soc_code || call[0]?.values?.soc_code)
      .filter(Boolean);

    expect(insertedCodes).toContain("11-1011");
    expect(insertedCodes).toContain("11-1021");
    expect(insertedCodes).toContain("15-1252");
    expect(insertedCodes).not.toContain("INVALID");
  });

  it("should handle API errors gracefully", async () => {
    nock("https://api.bls.gov")
      .get("/publicAPI/v2/surveys/OEWS/occupations/")
      .reply(500, { error: "Internal Server Error" });

    // The script should throw when imported due to validation failure
    await expect(import("./get_socs.ts")).rejects.toThrow();
  });

  it("should validate response schema", async () => {
    const invalidResponse = {
      occupations: [
        { code: 12345, text: "Invalid" }, // code should be string
      ],
    };

    nock("https://api.bls.gov")
      .get("/publicAPI/v2/surveys/OEWS/occupations/")
      .reply(200, invalidResponse);

    // Should throw validation error
    await expect(import("./get_socs.ts")).rejects.toThrow();
  });
});
