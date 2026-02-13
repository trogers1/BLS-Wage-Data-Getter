import { describe, it, expect } from "vitest";
import { parseSeriesLine, parseDataLine } from "./parsers.ts";

describe("bulk parsers", () => {
  it("should parse oe.series line", () => {
    // Real data sample from BLS oe.series file (tab-delimited)
    const line =
      "OEUM001018000000000000004     \tU\tM\t000000\t000000\t04\t48\t0010180\t00--01\tAnnual mean wage for All Occupations in All Industries in Abilene, TX\t2\t2024\tA01\t2024\tA01";

    const parsed = parseSeriesLine(line);

    expect(parsed.series_id).toBe("OEUM001018000000000000004");
    expect(parsed.industry_code).toBe("000000");
    expect(parsed.occupation_code).toBe("000000");
    expect(parsed.datatype_code).toBe("04");
    expect(parsed.area_code).toBe("0010180");
    expect(parsed.series_title).toContain("Annual mean wage");
    expect(parsed.begin_year).toBe(2024);
    expect(parsed.end_year).toBe(2024);
  });

  it("should parse oe.data line", () => {
    // Real data sample from BLS oe.data.0.Current file (tab-delimited)
    const parsed = parseDataLine(
      "OEUM001018000000000000009     \t2024\tA01\t      29.34\t"
    );

    expect(parsed.series_id).toBe("OEUM001018000000000000009");
    expect(parsed.year).toBe(2024);
    expect(parsed.period).toBe("A01");
    expect(parsed.value).toBe(29.34);
  });
});
