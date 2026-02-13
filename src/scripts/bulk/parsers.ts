export function parseSeriesLine(line: string) {
  // oe.series is tab-delimited
  const parts = line.split("\t").map((p) => p.trim());

  if (parts.length < 11) {
    throw new Error(
      `Invalid oe.series line (expected 11+ tab-delimited fields): ${line}`
    );
  }

  const [
    series_id,
    seasonal,
    areatype_code,
    industry_code,
    occupation_code,
    datatype_code,
    state_code,
    area_code,
    sector_code,
    series_title,
    footnote_codes,
    begin_year_str,
    begin_period,
    end_year_str,
    end_period,
  ] = parts;

  const begin_year = Number(begin_year_str);
  const end_year = Number(end_year_str);

  if (Number.isNaN(begin_year) || Number.isNaN(end_year)) {
    throw new Error(`Invalid year in oe.series line: ${line}`);
  }

  return {
    series_id,
    seasonal,
    areatype_code,
    industry_code,
    occupation_code,
    datatype_code,
    state_code,
    area_code,
    sector_code,
    series_title,
    footnote_codes: footnote_codes?.length > 0 ? footnote_codes : null,
    begin_year,
    begin_period,
    end_year,
    end_period,
  };
}

export function parseDataLine(line: string) {
  // oe.data.0.Current is tab-delimited
  const parts = line.split("\t").map((p) => p.trim());

  if (parts.length < 4) {
    throw new Error(
      `Invalid oe.data line (expected 4+ tab-delimited fields): ${line}`
    );
  }

  const [series_id, year_str, period, value_str, footnote_codes] = parts;
  const year = Number(year_str);

  // Handle suppressed/missing values (marked with "-")
  const value = value_str === "-" ? null : Number(value_str);

  if (Number.isNaN(year)) {
    throw new Error(`Invalid year in oe.data line: ${line}`);
  }

  return {
    series_id,
    year,
    period,
    value,
    footnote_codes: footnote_codes?.length > 0 ? footnote_codes : null,
  };
}
