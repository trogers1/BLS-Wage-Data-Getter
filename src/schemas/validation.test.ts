import { describe, it, expect } from "vitest";
import { Type } from "@sinclair/typebox";
import { validate, ValidationError } from "./validate.ts";

describe("TypeBox Validation", () => {
  it("should validate a simple object", () => {
    const schema = Type.Object({ value: Type.Number() });
    const validated = validate(schema, { value: 3 });
    expect(validated.value).toBe(3);
  });

  it("should throw ValidationError for invalid data", () => {
    const schema = Type.Object({ value: Type.Number() });
    expect(() => validate(schema, { value: "nope" })).toThrow(ValidationError);
  });
});
