import { describe, it, expect } from "vitest";
import {
  resolveFieldThatMayBeAFunction,
  normalizeFuncPropertiesToValues,
} from "./overridableFieldHelpers.ts";

describe("overridableFieldHelpers", () => {
  describe("resolveFieldThatMayBeAFunction", () => {
    it("should return literal value unchanged", () => {
      const field = "literal string";
      const randomSeed = "test-seed";

      const result = resolveFieldThatMayBeAFunction({ field, randomSeed });

      expect(result).toBe("literal string");
    });

    it("should call function with seed and return its result", () => {
      const field = (seed: string) => `computed from ${seed}`;
      const randomSeed = "my-seed";

      const result = resolveFieldThatMayBeAFunction({ field, randomSeed });

      expect(result).toBe("computed from my-seed");
    });

    it("should work with numeric literals", () => {
      const field = 42;
      const randomSeed = "any";

      const result = resolveFieldThatMayBeAFunction({ field, randomSeed });

      expect(result).toBe(42);
    });

    it("should work with boolean literals", () => {
      const field = false;
      const randomSeed = "any";

      const result = resolveFieldThatMayBeAFunction({ field, randomSeed });

      expect(result).toBe(false);
    });

    it("should work with object literals", () => {
      const field = { nested: "value" };
      const randomSeed = "any";

      const result = resolveFieldThatMayBeAFunction({ field, randomSeed });

      expect(result).toEqual({ nested: "value" });
    });

    it("should call function that returns different values based on seed", () => {
      const field = (seed: string) => seed.length;
      const randomSeed1 = "short";
      const randomSeed2 = "very long seed";

      const result1 = resolveFieldThatMayBeAFunction({
        field,
        randomSeed: randomSeed1,
      });
      const result2 = resolveFieldThatMayBeAFunction({
        field,
        randomSeed: randomSeed2,
      });

      expect(result1).toBe(5);
      expect(result2).toBe(14);
    });
  });

  describe("normalizeFuncPropertiesToValues", () => {
    it("should convert all function properties to values", () => {
      const obj = {
        literal: "plain",
        computed: (seed: string) => `value from ${seed}`,
        number: 123,
        boolFunc: (seed: string) => seed.length > 5,
      };
      const randomSeed = "test-seed";

      const result = normalizeFuncPropertiesToValues({ obj, randomSeed });

      expect(result).toEqual({
        literal: "plain",
        computed: "value from test-seed",
        number: 123,
        boolFunc: true, // "test-seed".length = 9 > 5
      });
    });

    it("should handle empty object", () => {
      const obj = {};
      const randomSeed = "any";

      const result = normalizeFuncPropertiesToValues({ obj, randomSeed });

      expect(result).toEqual({});
    });

    it("should preserve undefined and null values", () => {
      const obj = {
        nil: null,
        undef: undefined,
        func: (seed: string) => seed.length,
      };
      const randomSeed = "seed";

      const result = normalizeFuncPropertiesToValues({ obj, randomSeed });

      expect(result).toEqual({
        nil: null,
        undef: undefined,
        func: 4, // "seed".length
      });
    });

    it("should work with nested object properties (shallow)", () => {
      const obj = {
        nested: { key: "value" },
        nestedFunc: (seed: string) => ({ computed: seed }),
      };
      const randomSeed = "test";

      const result = normalizeFuncPropertiesToValues({ obj, randomSeed });

      expect(result).toEqual({
        nested: { key: "value" },
        nestedFunc: { computed: "test" },
      });
    });
  });
});
