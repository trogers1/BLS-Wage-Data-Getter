/**
 * Analysis module for wage-meaningfulness correlation study
 * Uses jstat for all statistical calculations
 */
import { Kysely, sql } from "kysely";
import type { DB } from "../db/generated/db.d.ts";
import jStat from "jstat";

export interface AnalysisDataPoint {
  seriesId: string;
  occupationCode: string;
  occupationName: string;
  industryCode: string;
  industryName: string;
  areaCode: string;
  stateCode: string;
  areaName: string;
  year: number;
  wageValue: number;
  meaningfulnessScore: number | null;
}

export interface SummaryStats {
  n: number;
  mean: number;
  median: number;
  stdDev: number;
  min: number;
  max: number;
  q25: number;
  q75: number;
}

export interface AnalysisResult {
  totalRecords: number;
  recordsWithScores: number;
  pearson: {
    correlation: number;
    pValue: number;
    interpretation: string;
  };
  spearman: {
    correlation: number;
    pValue: number;
    interpretation: string;
  };
  wageStats: SummaryStats;
  meaningfulnessStats: SummaryStats;
}

export interface IndustryAnalysis {
  industryCode: string;
  industryName: string;
  recordCount: number;
  pearsonCorrelation: number;
  spearmanCorrelation: number;
  avgWage: number;
  avgMeaningfulness: number;
}

/**
 * Calculate summary statistics for an array using jstat
 */
function summaryStats(values: number[]): SummaryStats {
  if (values.length === 0) {
    return {
      n: 0,
      mean: 0,
      median: 0,
      stdDev: 0,
      min: 0,
      max: 0,
      q25: 0,
      q75: 0,
    };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const n = values.length;
  const quartiles = jStat.quartiles(values);

  return {
    n,
    mean: jStat.mean(values),
    median: quartiles[1],
    stdDev: n >= 2 ? jStat.stdev(values, true) : 0, // true = sample std dev
    min: jStat.min(values),
    max: jStat.max(values),
    q25: quartiles[0],
    q75: quartiles[2],
  };
}

/**
 * Calculate proper two-tailed p-value for correlation using t-distribution
 * t = r * sqrt((n-2)/(1-r^2))
 */
function correlationPValue(correlation: number, n: number): number {
  if (n < 3) return 1;

  const df = n - 2;
  const rSquared = correlation * correlation;
  const tStatistic =
    (correlation * Math.sqrt(df)) / Math.sqrt(Math.max(0.0001, 1 - rSquared));

  // Two-tailed p-value using jstat's studentt cdf
  const absT = Math.abs(tStatistic);
  const pValue = 2 * (1 - jStat.studentt.cdf(absT, df));

  return pValue;
}

/**
 * Fetch data for analysis by querying underlying tables
 * Filters for national-level data (state_code = '00') and selectable occupations/industries
 */
export async function fetchAnalysisData(
  db: Kysely<DB>
): Promise<AnalysisDataPoint[]> {
  // First, get the datatype codes for mean annual wage
  const allDatatypes = await db
    .selectFrom("oe_datatypes")
    .select(["datatype_code", "datatype_name"])
    .execute();

  const wageDatatypeCodes = allDatatypes
    .filter(
      (d) =>
        d.datatype_name.toLowerCase().includes("mean") &&
        d.datatype_name.toLowerCase().includes("annual") &&
        d.datatype_name.toLowerCase().includes("wage")
    )
    .map((d) => d.datatype_code);

  if (wageDatatypeCodes.length === 0) {
    return [];
  }

  // Get the latest year for each series
  const latestData = db
    .selectFrom("oe_data")
    .select(["series_id", sql<number>`max(year)`.as("year")])
    .where("period", "=", "A01")
    .groupBy("series_id")
    .as("latest");

  const result = await db
    .selectFrom("oe_series as s")
    .innerJoin(latestData, (join) =>
      join.onRef("latest.series_id", "=", "s.series_id")
    )
    .innerJoin("oe_data as d", (join) =>
      join
        .onRef("d.series_id", "=", "s.series_id")
        .onRef("d.year", "=", "latest.year")
        .on("d.period", "=", "A01")
    )
    .innerJoin(
      "oe_occupations as occ",
      "occ.occupation_code",
      "s.occupation_code"
    )
    .innerJoin("oe_industries as ind", "ind.industry_code", "s.industry_code")
    .innerJoin("oe_areas as area", (join) =>
      join
        .onRef("area.area_code", "=", "s.area_code")
        .onRef("area.state_code", "=", "s.state_code")
    )
    .leftJoin("meaningfulness_scores as ms", (join) =>
      join
        .onRef("ms.occupation_code", "=", "s.occupation_code")
        .onRef("ms.industry_code", "=", "s.industry_code")
    )
    .select([
      "s.series_id",
      "s.occupation_code",
      "occ.occupation_name",
      "s.industry_code",
      "ind.industry_name",
      "s.area_code",
      "s.state_code",
      "area.area_name",
      "d.year",
      "d.value",
      "ms.score",
    ])
    .where("s.state_code", "=", "00") // National-level data only
    .where("s.datatype_code", "in", wageDatatypeCodes)
    .where("occ.selectable", "=", true)
    .where("ind.selectable", "=", true)
    .where("ms.score", "is not", null)
    .execute();

  return result.map((row) => ({
    seriesId: row.series_id,
    occupationCode: row.occupation_code,
    occupationName: row.occupation_name,
    industryCode: row.industry_code,
    industryName: row.industry_name,
    areaCode: row.area_code,
    stateCode: row.state_code,
    areaName: row.area_name,
    year: row.year,
    wageValue: parseFloat(String(row.value)),
    meaningfulnessScore: row.score,
  }));
}

/**
 * Perform correlation analysis on wage vs meaningfulness data
 */
export function analyzeCorrelation(data: AnalysisDataPoint[]): AnalysisResult {
  const wages = data.map((d) => d.wageValue);
  const scores = data.map((d) => d.meaningfulnessScore!);

  const pearsonCorr = jStat.corrcoeff(wages, scores);
  const spearmanCorr = jStat.spearmancoeff(wages, scores);

  const n = data.length;

  return {
    totalRecords: data.length,
    recordsWithScores: data.filter((d) => d.meaningfulnessScore !== null)
      .length,
    pearson: {
      correlation: pearsonCorr,
      pValue: correlationPValue(pearsonCorr, n),
      interpretation: interpretCorrelation(pearsonCorr),
    },
    spearman: {
      correlation: spearmanCorr,
      pValue: correlationPValue(spearmanCorr, n),
      interpretation: interpretCorrelation(spearmanCorr),
    },
    wageStats: summaryStats(wages),
    meaningfulnessStats: summaryStats(scores),
  };
}

/**
 * Analyze correlation within each industry
 * This helps control for industry effects
 */
export function analyzeByIndustry(
  data: AnalysisDataPoint[]
): IndustryAnalysis[] {
  // Group data by industry
  const byIndustry = new Map<string, AnalysisDataPoint[]>();

  for (const point of data) {
    const existing = byIndustry.get(point.industryCode) || [];
    existing.push(point);
    byIndustry.set(point.industryCode, existing);
  }

  const results: IndustryAnalysis[] = [];

  byIndustry.forEach((points, industryCode) => {
    if (points.length < 3) return;

    const wages = points.map((p) => p.wageValue);
    const scores = points.map((p) => p.meaningfulnessScore!);

    results.push({
      industryCode,
      industryName: points[0].industryName,
      recordCount: points.length,
      pearsonCorrelation: jStat.corrcoeff(wages, scores),
      spearmanCorrelation: jStat.spearmancoeff(wages, scores),
      avgWage: jStat.mean(wages),
      avgMeaningfulness: jStat.mean(scores),
    });
  });

  // Sort by correlation strength
  return results.sort((a, b) =>
    Math.abs(b.pearsonCorrelation) > Math.abs(a.pearsonCorrelation) ? 1 : -1
  );
}

/**
 * Interpret correlation coefficient
 */
function interpretCorrelation(r: number): string {
  const abs = Math.abs(r);
  const direction = r > 0 ? "positive" : "negative";

  if (abs < 0.1) return "negligible";
  if (abs < 0.3) return `weak ${direction}`;
  if (abs < 0.5) return `moderate ${direction}`;
  if (abs < 0.7) return `strong ${direction}`;
  return `very strong ${direction}`;
}

/**
 * Format analysis results for display
 */
export function formatResults(result: AnalysisResult): string {
  const lines: string[] = [];

  lines.push("=".repeat(60));
  lines.push("WAGE vs MEANINGFULNESS CORRELATION ANALYSIS");
  lines.push("=".repeat(60));
  lines.push("");

  lines.push(`Total Records Analyzed: ${result.totalRecords}`);
  lines.push("");

  lines.push("WAGE STATISTICS");
  lines.push("-".repeat(40));
  lines.push(`  Mean: $${result.wageStats.mean.toFixed(2)}`);
  lines.push(`  Median: $${result.wageStats.median.toFixed(2)}`);
  lines.push(`  Std Dev: $${result.wageStats.stdDev.toFixed(2)}`);
  lines.push(
    `  Range: $${result.wageStats.min.toFixed(0)} - $${result.wageStats.max.toFixed(0)}`
  );
  lines.push("");

  lines.push("MEANINGFULNESS SCORE STATISTICS");
  lines.push("-".repeat(40));
  lines.push(`  Mean: ${result.meaningfulnessStats.mean.toFixed(2)}`);
  lines.push(`  Median: ${result.meaningfulnessStats.median.toFixed(2)}`);
  lines.push(`  Std Dev: ${result.meaningfulnessStats.stdDev.toFixed(2)}`);
  lines.push(
    `  Range: ${result.meaningfulnessStats.min.toFixed(1)} - ${result.meaningfulnessStats.max.toFixed(1)}`
  );
  lines.push("");

  lines.push("CORRELATION RESULTS");
  lines.push("-".repeat(40));
  lines.push("Pearson Correlation:");
  lines.push(`  Coefficient: ${result.pearson.correlation.toFixed(4)}`);
  lines.push(`  Interpretation: ${result.pearson.interpretation}`);
  lines.push(`  P-value: ${result.pearson.pValue.toFixed(4)}`);
  lines.push(
    `  Significance: ${result.pearson.pValue < 0.05 ? "Significant" : "Not significant"} (α=0.05)`
  );
  lines.push("");
  lines.push("Spearman Correlation:");
  lines.push(`  Coefficient: ${result.spearman.correlation.toFixed(4)}`);
  lines.push(`  Interpretation: ${result.spearman.interpretation}`);
  lines.push(`  P-value: ${result.spearman.pValue.toFixed(4)}`);
  lines.push(
    `  Significance: ${result.spearman.pValue < 0.05 ? "Significant" : "Not significant"} (α=0.05)`
  );
  lines.push("");

  lines.push("INTERPRETATION");
  lines.push("-".repeat(40));
  if (result.pearson.correlation > 0) {
    lines.push("Higher wages are associated with MORE meaningful jobs");
  } else if (result.pearson.correlation < 0) {
    lines.push("Higher wages are associated with LESS meaningful jobs");
  } else {
    lines.push("No clear relationship between wages and meaningfulness");
  }
  lines.push("");

  return lines.join("\n");
}

/**
 * Format industry-level results
 */
export function formatIndustryResults(results: IndustryAnalysis[]): string {
  const lines: string[] = [];

  lines.push("=".repeat(80));
  lines.push("INDUSTRY-LEVEL ANALYSIS");
  lines.push("=".repeat(80));
  lines.push("");
  lines.push("Top 10 Industries by Correlation Strength:");
  lines.push("");

  const top10 = results.slice(0, 10);
  for (const r of top10) {
    lines.push(`${r.industryName}`);
    lines.push(`  Records: ${r.recordCount}`);
    lines.push(`  Pearson: ${r.pearsonCorrelation.toFixed(4)}`);
    lines.push(`  Spearman: ${r.spearmanCorrelation.toFixed(4)}`);
    lines.push(`  Avg Wage: $${r.avgWage.toFixed(0)}`);
    lines.push(`  Avg Meaningfulness: ${r.avgMeaningfulness.toFixed(2)}`);
    lines.push("");
  }

  return lines.join("\n");
}
