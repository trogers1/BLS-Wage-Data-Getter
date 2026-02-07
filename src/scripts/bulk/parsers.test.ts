import { describe, it, expect } from "vitest";
import { parseSeriesLine, parseDataLine } from "./parsers.ts";

const padRight = (value: string, length: number) => value.padEnd(length, " ");

describe("bulk parsers", () => {
  it("should parse oe.series line", () => {
    const line =
      padRight("OEUN0000000111101110103", 30) +
      "U" +
      "N" +
      padRight("111110", 6) +
      padRight("111011", 6) +
      padRight("15", 2) +
      padRight("00", 2) +
      padRight("0000000", 7) +
      padRight("000000", 6) +
      padRight("Mean annual wage for test", 40) +
      padRight("", 10) +
      padRight("2022", 4) +
      padRight("A01", 3) +
      padRight("2023", 4) +
      padRight("A01", 3);

    const parsed = parseSeriesLine(line);

    expect(parsed.series_id).toBe("OEUN0000000111101110103");
    expect(parsed.industry_code).toBe("111110");
    expect(parsed.occupation_code).toBe("111011");
    expect(parsed.datatype_code).toBe("15");
    expect(parsed.area_code).toBe("0000000");
    expect(parsed.series_title).toContain("Mean annual wage");
    expect(parsed.begin_year).toBe(2022);
    expect(parsed.end_year).toBe(2023);
  });

  it("should parse oe.data line", () => {
    const parsed = parseDataLine("OEUN0000000111101110103 2023 A01 85000");

    expect(parsed.series_id).toBe("OEUN0000000111101110103");
    expect(parsed.year).toBe(2023);
    expect(parsed.period).toBe("A01");
    expect(parsed.value).toBe(85000);
  });
});
