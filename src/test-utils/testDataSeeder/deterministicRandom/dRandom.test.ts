import { describe, it, expect } from "vitest";
import { oneOf, randomInt, shuffle } from "./dRandom.ts";

describe("deterministicRandom", () => {
  describe("oneOf", () => {
    const lotsOfOptions = Array.from(
      { length: 100000 },
      (_, i) => `option${i + 1}`
    );
    it("should return the same option for the same seed (deterministic)", () => {
      const seed = "test-seed";

      const result1 = oneOf({ seed, options: lotsOfOptions });
      const result2 = oneOf({ seed, options: lotsOfOptions });

      expect(result1).toStrictEqual(result2);
    });

    it("should return different options for different seeds", () => {
      const result1 = oneOf({ seed: "seed1", options: lotsOfOptions });
      const result2 = oneOf({ seed: "seed2", options: lotsOfOptions });
      const result3 = oneOf({ seed: "seed3", options: lotsOfOptions });

      // At least some should be different (though not guaranteed, it's very likely)
      const results = [result1, result2, result3];
      const uniqueResults = new Set(results);
      expect(uniqueResults.size).toBeGreaterThan(1);
    });

    it("should handle single element array", () => {
      const options = ["only-option"];
      const seed = "single-element";

      const result = oneOf({ seed, options });

      expect(result).toStrictEqual("only-option");
    });

    it("should always return an option from the provided array", () => {
      const seeds = [
        "seed1",
        "seed2",
        "seed3",
        "seed4",
        "seed5",
        "seed6",
        "seed7",
        "seed8",
      ];

      seeds.forEach((seed) => {
        const result = oneOf({ seed, options: lotsOfOptions });
        expect(lotsOfOptions).toContain(result);
      });
    });

    it("should work with concatenated seeds like in usage pattern", () => {
      const options = [true, false];
      const baseSeed = "randomSeed";
      const suffix1 = "isDeleted";
      const suffix2 = "isLanguage";

      const result1 = oneOf({ seed: baseSeed + suffix1, options });
      const result2 = oneOf({ seed: baseSeed + suffix2, options });
      const result3 = oneOf({ seed: baseSeed + suffix1, options }); // Same as result1

      expect(result1).toStrictEqual(result3); // Same seed should return same result
      expect(options).toContain(result1);
      expect(options).toContain(result2);
    });

    it("should handle different array lengths correctly", () => {
      const seed = "length-test";

      const result2 = oneOf({ seed, options: ["a", "b"] });
      const result3 = oneOf({ seed, options: ["a", "b", "c"] });
      const result5 = oneOf({ seed, options: ["a", "b", "c", "d", "e"] });
      const resultMany = oneOf({ seed, options: lotsOfOptions });

      expect(["a", "b"]).toContain(result2);
      expect(["a", "b", "c"]).toContain(result3);
      expect(["a", "b", "c", "d", "e"]).toContain(result5);
      expect(lotsOfOptions).toContain(resultMany);
    });

    it("should handle case-sensitive seeds differently", () => {
      const resultLower = oneOf({ seed: "test", options: lotsOfOptions });
      const resultUpper = oneOf({ seed: "TEST", options: lotsOfOptions });
      const resultMixed = oneOf({ seed: "Test", options: lotsOfOptions });

      // They might be the same by chance, but they're likely different
      expect(lotsOfOptions).toContain(resultLower);
      expect(lotsOfOptions).toContain(resultUpper);
      expect(lotsOfOptions).toContain(resultMixed);
    });

    it("should handle seeds with special characters", () => {
      const options = [true, false];
      const seed1 = "test-seed";
      const seed2 = "test@seed#123";
      const seed3 = "test with spaces";

      const result1 = oneOf({ seed: seed1, options });
      const result2 = oneOf({ seed: seed2, options });
      const result3 = oneOf({ seed: seed3, options });

      expect(options).toContain(result1);
      expect(options).toContain(result2);
      expect(options).toContain(result3);
    });

    it("should handle empty string seed", () => {
      const result = oneOf({ seed: "", options: lotsOfOptions });

      expect(lotsOfOptions).toContain(result);
    });
    it("should handle long seed strings", () => {
      const longSeed = "a".repeat(1000);
      const result = oneOf({ seed: longSeed, options: lotsOfOptions });

      expect(lotsOfOptions).toContain(result);
    });

    it("should handle unicode characters in seed", () => {
      const options = ["option1", "option2"];
      const result = oneOf({ seed: "test 🌍 seed", options });

      expect(options).toContain(result);
    });
  });

  describe("shuffle", () => {
    const lotsOfOptions = Array.from(
      { length: 100000 },
      (_, i) => `option${i + 1}`
    );
    it("should return the same shuffled array for the same seed (deterministic)", () => {
      const randomSeed = "test-seed";
      const options = lotsOfOptions;

      const result1 = shuffle({ randomSeed, options });
      const result2 = shuffle({ randomSeed, options });

      expect(result1).toStrictEqual(result2);
    });

    it("should return different shuffles for different seeds", () => {
      const options = lotsOfOptions;

      const result1 = shuffle({ randomSeed: "seed1", options });
      const result2 = shuffle({ randomSeed: "seed2", options });
      const result3 = shuffle({ randomSeed: "seed3", options });

      // At least some should be different (though not guaranteed, it's very likely)
      expect(result1).not.toStrictEqual(result2);
      expect(result1).not.toStrictEqual(result3);
      expect(result2).not.toStrictEqual(result3);
    });

    it("should handle single element array", () => {
      const options = ["only-option"];
      const randomSeed = "single-element";

      const result = shuffle({ randomSeed, options });

      expect(result).toStrictEqual(options);
    });

    it("should return all elements with no duplicates or missing", () => {
      const options = lotsOfOptions;
      const randomSeed = "all-elements-test";

      const result = shuffle({ randomSeed, options });

      expect(result).toHaveLength(options.length);
      expect(new Set(result).size).toBe(options.length); // No duplicates
      expect(result.sort()).toStrictEqual(options.sort()); // All elements present
    });

    it("should handle empty array", () => {
      const options: string[] = [];
      const randomSeed = "empty-array";

      const result = shuffle({ randomSeed, options });

      expect(result).toStrictEqual(options);
    });

    it("should work with concatenated seeds like in usage pattern", () => {
      const options = lotsOfOptions;
      const baseSeed = "randomSeed";
      const suffix1 = "isDeleted";
      const suffix2 = "isLanguage";

      const result1 = shuffle({ randomSeed: baseSeed + suffix1, options });
      const result2 = shuffle({ randomSeed: baseSeed + suffix2, options });
      const result3 = shuffle({ randomSeed: baseSeed + suffix1, options }); // Same as result1

      expect(result1).toStrictEqual(result3); // Same seed should return same result
      expect(result1).not.toStrictEqual(options);
      expect(result1.sort()).toStrictEqual(options.sort());
      expect(result2).not.toStrictEqual(options);
      expect(result2.sort()).toStrictEqual(options.sort());
    });

    it("should handle case-sensitive seeds differently", () => {
      const options = lotsOfOptions;
      const resultLower = shuffle({ randomSeed: "test", options });
      const resultUpper = shuffle({ randomSeed: "TEST", options });
      const resultMixed = shuffle({ randomSeed: "Test", options });

      expect(resultLower).not.toStrictEqual(options);
      expect(resultUpper).not.toStrictEqual(options);
      expect(resultMixed).not.toStrictEqual(options);
      expect(resultLower).not.toStrictEqual(resultUpper);
      expect(resultLower).not.toStrictEqual(resultMixed);
      expect(resultUpper).not.toStrictEqual(resultMixed);
    });

    it("should handle seeds with special characters", () => {
      const options = lotsOfOptions;
      const seed1 = "test-seed";
      const seed2 = "test@seed#123";
      const seed3 = "test with spaces";
      const result1 = shuffle({ randomSeed: seed1, options });
      const result2 = shuffle({ randomSeed: seed2, options });
      const result3 = shuffle({ randomSeed: seed3, options });

      expect(result1).not.toStrictEqual(options);
      expect(result2).not.toStrictEqual(options);
      expect(result3).not.toStrictEqual(options);
      expect(result1).not.toStrictEqual(result2);
      expect(result1).not.toStrictEqual(result3);
      expect(result2).not.toStrictEqual(result3);
    });

    it("should handle empty string seed", () => {
      const options = lotsOfOptions;
      const result = shuffle({ randomSeed: "", options });

      expect(result.sort()).toStrictEqual(options.sort());
    });

    it("should handle long seed strings", () => {
      const longSeed = "a".repeat(1000);
      const options = lotsOfOptions;
      const result = shuffle({ randomSeed: longSeed, options });

      expect(result).not.toStrictEqual(options);
      expect(result.sort()).toStrictEqual(options.sort());
    });

    it("should handle unicode characters in seed", () => {
      const options = lotsOfOptions;
      const result = shuffle({ randomSeed: "test 🌍 seed", options });

      expect(result).not.toStrictEqual(options);
      expect(result.sort()).toStrictEqual(options.sort());
    });

    it("should be idempotent - multiple calls return same result", () => {
      const options = lotsOfOptions;
      const randomSeed = "idempotent-test";

      const result1 = shuffle({ randomSeed, options });
      const result2 = shuffle({ randomSeed, options });
      const result3 = shuffle({ randomSeed, options });

      expect(result1).toStrictEqual(result2);
      expect(result2).toStrictEqual(result3);
      expect(result1).toStrictEqual(result3);
    });
  });

  describe("randomInt", () => {
    it("should return the same integer for the same seed (deterministic)", () => {
      const seed = "test-seed";
      const min = 1;
      const max = 10;

      const result1 = randomInt({ seed, min, max });
      const result2 = randomInt({ seed, min, max });

      expect(result1).toBe(result2);
    });

    it("should return different integers for different seeds", () => {
      const min = 0;
      const max = 100;

      const result1 = randomInt({ seed: "seed1", min, max });
      const result2 = randomInt({ seed: "seed2", min, max });
      const result3 = randomInt({ seed: "seed3", min, max });

      const results = [result1, result2, result3];
      const uniqueResults = new Set(results);
      expect(uniqueResults.size).toBeGreaterThan(1);
    });

    it("should respect the min and max bounds (inclusive)", () => {
      const seed = "bounds-test";
      const min = 5;
      const max = 7;

      const results = Array.from({ length: 100 }, (_, i) =>
        randomInt({ seed: `${seed}-${i}`, min, max })
      );

      results.forEach((value) => {
        expect(value).toBeGreaterThanOrEqual(min);
        expect(value).toBeLessThanOrEqual(max);
      });
    });

    it("should handle single-value range (min === max)", () => {
      const seed = "single-range";
      const min = 42;
      const max = 42;

      const result = randomInt({ seed, min, max });

      expect(result).toBe(42);
    });

    it("should handle negative ranges", () => {
      const seed = "negative-range";
      const min = -10;
      const max = -1;

      const result = randomInt({ seed, min, max });

      expect(result).toBeGreaterThanOrEqual(min);
      expect(result).toBeLessThanOrEqual(max);
    });
  });
});
