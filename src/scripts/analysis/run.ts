/**
 * Script to run wage-meaningfulness correlation analysis
 */
import { getDbInstance } from "../../db/index.js";
import {
  fetchAnalysisData,
  analyzeCorrelation,
  analyzeByIndustry,
  formatResults,
  formatIndustryResults,
} from "../../analysis/correlation.js";

async function main() {
  console.log("Starting wage-meaningfulness correlation analysis...\n");

  const db = getDbInstance();

  try {
    // Fetch the data
    console.log("Fetching analysis data...");
    const data = await fetchAnalysisData(db);
    console.log(
      `Retrieved ${data.length} records with meaningfulness scores\n`
    );

    if (data.length === 0) {
      console.log("No data found. Make sure you have:");
      console.log("  1. Downloaded and ingested BLS bulk data");
      console.log("  2. Generated meaningfulness scores");
      process.exit(0);
    }

    // Perform overall correlation analysis
    console.log("Calculating correlations...");
    const result = analyzeCorrelation(data);

    // Display results
    console.log(formatResults(result));

    // Perform industry-level analysis
    console.log("\nAnalyzing by industry...");
    const industryResults = analyzeByIndustry(data);

    if (industryResults.length > 0) {
      console.log(formatIndustryResults(industryResults));
    } else {
      console.log(
        "Not enough data for industry-level analysis (need at least 3 records per industry)\n"
      );
    }

    // Summary
    console.log("=".repeat(60));
    console.log("SUMMARY");
    console.log("=".repeat(60));
    console.log(
      `\nResearch Question: Do higher wages correlate with less meaningful jobs?`
    );
    console.log(`\nAnswer: ${getSummaryAnswer(result)}`);
    console.log("");
  } catch (error) {
    console.error("Analysis failed:", error);
    process.exit(1);
  } finally {
    await db.destroy();
  }
}

function getSummaryAnswer(
  result: ReturnType<typeof analyzeCorrelation>
): string {
  const corr = result.pearson.correlation;
  const sig = result.pearson.pValue < 0.05;

  if (!sig) {
    return "No statistically significant relationship was found.";
  }

  if (corr > 0.3) {
    return "HIGHER wages are significantly associated with MORE meaningful jobs (contrary to the hypothesis).";
  } else if (corr > 0.1) {
    return "There is a weak positive relationship - higher wages are slightly associated with more meaningful jobs.";
  } else if (corr < -0.3) {
    return "LOWER wages are significantly associated with MORE meaningful jobs (supporting the hypothesis).";
  } else if (corr < -0.1) {
    return "There is a weak negative relationship - higher wages are slightly associated with less meaningful jobs.";
  } else {
    return "No clear relationship was detected.";
  }
}

main();
