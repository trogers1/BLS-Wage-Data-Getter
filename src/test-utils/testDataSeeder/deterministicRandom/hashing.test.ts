import { describe, it, expect } from "vitest";
import { hashString } from "./hashing";

describe("hashString", () => {
  it("should return the same hash for the same input (deterministic)", () => {
    const input = "test-string";
    const hash1 = hashString(input);
    const hash2 = hashString(input);

    expect(hash1).toStrictEqual(hash2);
  });

  it("should return different hashes for different inputs", () => {
    const hash1 = hashString("test-string-1");
    const hash2 = hashString("test-string-2");

    expect(hash1).not.toStrictEqual(hash2);
  });

  it("should handle empty strings", () => {
    const hash = hashString("");
    expect(typeof hash).toStrictEqual("number");
    expect(Number.isInteger(hash)).toStrictEqual(true);
  });

  it("should handle single character strings", () => {
    const hash = hashString("a");
    expect(typeof hash).toStrictEqual("number");
    expect(Number.isInteger(hash)).toStrictEqual(true);
  });

  it("should handle long strings", () => {
    const longString = "a".repeat(1000);
    const hash = hashString(longString);
    expect(typeof hash).toStrictEqual("number");
    expect(Number.isInteger(hash)).toStrictEqual(true);
  });

  it("should handle strings with special characters", () => {
    const hash1 = hashString("hello-world!");
    const hash2 = hashString("hello@world#123");
    const hash3 = hashString("test with spaces");

    expect(typeof hash1).toStrictEqual("number");
    expect(typeof hash2).toStrictEqual("number");
    expect(typeof hash3).toStrictEqual("number");
    expect(hash1).not.toStrictEqual(hash2);
    expect(hash2).not.toStrictEqual(hash3);
  });

  it("should handle case-sensitive strings differently", () => {
    const hashLower = hashString("hello");
    const hashUpper = hashString("HELLO");
    const hashMixed = hashString("Hello");

    expect(hashLower).not.toStrictEqual(hashUpper);
    expect(hashLower).not.toStrictEqual(hashMixed);
    expect(hashUpper).not.toStrictEqual(hashMixed);
  });

  it("should produce a 32-bit integer", () => {
    const hash = hashString("test");
    // 32-bit signed integer range: -2,147,483,648 to 2,147,483,647
    expect(hash).toBeGreaterThanOrEqual(-2147483648);
    expect(hash).toBeLessThanOrEqual(2147483647);
  });

  it("should handle unicode characters", () => {
    const hash1 = hashString("hello 🌍");
    const hash2 = hashString("hello world");
    const hash3 = hashString("café");

    expect(typeof hash1).toStrictEqual("number");
    expect(typeof hash2).toStrictEqual("number");
    expect(typeof hash3).toStrictEqual("number");
    expect(hash1).not.toStrictEqual(hash2);
  });

  it("should produce consistent hashes across multiple calls", () => {
    const inputs = ["seed1", "seed2", "seed3", "seed4", "seed5"];
    const firstHashes = inputs.map((input) => hashString(input));
    const secondHashes = inputs.map((input) => hashString(input));

    expect(firstHashes).toEqual(secondHashes);
  });

  it("should handle strings with numbers", () => {
    const hash1 = hashString("test123");
    const hash2 = hashString("test456");
    const hash3 = hashString("123test");

    expect(hash1).not.toStrictEqual(hash2);
    expect(hash1).not.toStrictEqual(hash3);
    expect(hash2).not.toStrictEqual(hash3);
  });
});
