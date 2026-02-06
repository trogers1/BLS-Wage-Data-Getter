import { describe, it, expect } from "vitest";
import { SocResponse, NaicsResponse } from "./schemas.ts";
import { validate, ValidationError } from "./validate.ts";

describe("TypeBox Validation", () => {
  describe("SOC Response Validation", () => {
    it("should validate valid SOC response", () => {
      const validSocData = {
        occupations: [
          { code: "11-1011", text: "Chief Executives" },
          { code: "11-1021", text: "General and Operations Managers" },
        ],
      };

      const validated = validate(SocResponse, validSocData, "SOC test");
      expect(validated.occupations).toHaveLength(2);
      expect(validated.occupations[0].code).toBe("11-1011");
      expect(validated.occupations[0].text).toBe("Chief Executives");
    });

    it("should reject invalid SOC response with wrong type", () => {
      const invalidSocData = {
        occupations: [
          { code: 12345, text: "Invalid" }, // code should be string
        ],
      };

      expect(() =>
        validate(SocResponse, invalidSocData, "Invalid SOC test")
      ).toThrow(ValidationError);
    });

    it("should reject SOC response with missing required field", () => {
      const invalidSocData = {
        occupations: [
          { code: "11-1011" }, // missing text field
        ],
      };

      expect(() => validate(SocResponse, invalidSocData)).toThrow(
        ValidationError
      );
    });
  });

  describe("NAICS Response Validation", () => {
    it("should validate valid NAICS response", () => {
      const validNaicsData = {
        industries: [
          { code: "11", text: "Agriculture, Forestry, Fishing and Hunting" },
          { code: "111", text: "Crop Production" },
        ],
      };

      const validated = validate(NaicsResponse, validNaicsData, "NAICS test");
      expect(validated.industries).toHaveLength(2);
      expect(validated.industries[0].code).toBe("11");
      expect(validated.industries[0].text).toBe(
        "Agriculture, Forestry, Fishing and Hunting"
      );
    });

    it("should reject NAICS response with extra fields", () => {
      const invalidNaicsData = {
        industries: [
          { code: "11", text: "Agriculture", extra: "field" }, // extra field
        ],
      };

      expect(() => validate(NaicsResponse, invalidNaicsData)).toThrow(
        ValidationError
      );
    });
  });

  describe("ValidationError", () => {
    it("should include error details in ValidationError", () => {
      const invalidData = {
        occupations: [{ code: 12345, text: "Invalid" }],
      };

      try {
        validate(SocResponse, invalidData);
        expect.fail("Should have thrown ValidationError");
      } catch (error) {
        expect(error).toBeInstanceOf(ValidationError);
        if (error instanceof ValidationError) {
          expect(error.errors).toBeDefined();
          expect(error.errors.length).toBeGreaterThan(0);
          expect(error.data).toBe(invalidData);
          expect(error.message).toContain("Validation failed");
        }
      }
    });
  });
});
