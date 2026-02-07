const PREFIX_FIELD_SPECS = [
  { name: "series_id", length: 30 },
  { name: "seasonal", length: 1 },
  { name: "areatype_code", length: 1 },
  { name: "industry_code", length: 6 },
  { name: "occupation_code", length: 6 },
  { name: "datatype_code", length: 2 },
  { name: "state_code", length: 2 },
  { name: "area_code", length: 7 },
  { name: "sector_code", length: 6 },
];

const SERIES_SUFFIX_LENGTH = 10 + 4 + 3 + 4 + 3;

export function parseSeriesLine(line: string) {
  let cursor = 0;
  const parsed: Record<string, string> = {};

  for (const spec of PREFIX_FIELD_SPECS) {
    parsed[spec.name] = line.slice(cursor, cursor + spec.length).trim();
    cursor += spec.length;
  }

  const suffixStart = Math.max(cursor, line.length - SERIES_SUFFIX_LENGTH);
  const seriesTitle = line.slice(cursor, suffixStart).trim();
  const suffix = line.slice(suffixStart);

  if (suffix.length < SERIES_SUFFIX_LENGTH) {
    throw new Error(`Invalid oe.series line length: ${line}`);
  }

  const footnoteCodes = suffix.slice(0, 10).trim();
  const beginYear = suffix.slice(10, 14).trim();
  const beginPeriod = suffix.slice(14, 17).trim();
  const endYear = suffix.slice(17, 21).trim();
  const endPeriod = suffix.slice(21, 24).trim();

  const beginYearValue = Number(beginYear);
  const endYearValue = Number(endYear);

  if (Number.isNaN(beginYearValue) || Number.isNaN(endYearValue)) {
    throw new Error(`Invalid year in oe.series line: ${line}`);
  }

  return {
    series_id: parsed.series_id,
    seasonal: parsed.seasonal,
    areatype_code: parsed.areatype_code,
    industry_code: parsed.industry_code,
    occupation_code: parsed.occupation_code,
    datatype_code: parsed.datatype_code,
    state_code: parsed.state_code,
    area_code: parsed.area_code,
    sector_code: parsed.sector_code,
    series_title: seriesTitle,
    footnote_codes: footnoteCodes.length > 0 ? footnoteCodes : null,
    begin_year: beginYearValue,
    begin_period: beginPeriod,
    end_year: endYearValue,
    end_period: endPeriod,
  };
}

export function parseDataLine(line: string) {
  const parts = line.trim().split(/\s+/);
  if (parts.length < 4) {
    throw new Error(`Invalid oe.data line: ${line}`);
  }

  const [seriesId, year, period, value, footnoteCodes] = parts;
  const parsedYear = Number(year);
  const parsedValue = Number(value);
  if (Number.isNaN(parsedYear) || Number.isNaN(parsedValue)) {
    throw new Error(`Invalid value in oe.data line: ${line}`);
  }

  return {
    series_id: seriesId,
    year: parsedYear,
    period,
    value: parsedValue,
    footnote_codes: footnoteCodes ? footnoteCodes.trim() : null,
  };
}
