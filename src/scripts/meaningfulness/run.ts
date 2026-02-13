import { getDbInstance } from "../../db/index.ts";
import * as readline from "readline";

const DEFAULT_MODEL = "gpt-4.1-mini";
const PROMPT_VERSION = "v2";

type MeaningfulnessScore = {
  score: number;
  reason: string;
};

function getEnv(name: string) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function normalizeJson(text: string) {
  const trimmed = text.trim();
  if (trimmed.startsWith("```")) {
    return trimmed
      .replace(/^```[a-zA-Z]*\n?/, "")
      .replace(/```$/, "")
      .trim();
  }
  return trimmed;
}

function validateScore(raw: MeaningfulnessScore) {
  if (!Number.isInteger(raw.score) || raw.score < 1 || raw.score > 5) {
    throw new Error(`Invalid score: ${raw.score}`);
  }
  if (!raw.reason || raw.reason.trim().length === 0) {
    throw new Error("Missing reason");
  }
}

async function callOpenAI({
  apiKey,
  model,
  occupationName,
  occupationDescription,
  industryName,
}: {
  apiKey: string;
  model: string;
  occupationName: string;
  occupationDescription: string | null;
  industryName: string;
}): Promise<MeaningfulnessScore> {
  const descriptionSection = occupationDescription
    ? `\nOccupation Description: ${occupationDescription}`
    : "";

  const prompt = `You are rating how meaningful a job is in a specific industry context.

Occupation: ${occupationName}${descriptionSection}
Industry: ${industryName}

Consider:
- How much the occupation contributes to society or others' wellbeing
- Whether the work has a clear purpose or impact
- How well the industry context enables meaningful work

Return a JSON object with:
- score: integer 1-5 (1 = least meaningful, 5 = most meaningful, should approximate a normal distribution)
- reason: 1-2 sentences explaining the score
`;

  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      messages: [
        { role: "system", content: "You are a careful analyst." },
        { role: "user", content: prompt },
      ],
      temperature: 0.2,
    }),
  });

  if (!res.ok) {
    throw new Error(`OpenAI request failed: ${res.status} ${await res.text()}`);
  }

  const json = (await res.json()) as {
    choices?: Array<{ message?: { content?: string } }>;
  };
  const content = json.choices?.[0]?.message?.content;
  if (!content) {
    throw new Error("OpenAI response missing content");
  }

  const parsed = JSON.parse(normalizeJson(content)) as MeaningfulnessScore;
  validateScore(parsed);
  return parsed;
}

function askToContinue(rl: readline.Interface): Promise<boolean> {
  return new Promise((resolve) => {
    rl.question("Continue with next batch? (y/n): ", (answer) => {
      resolve(
        answer.toLowerCase().trim() === "y" ||
          answer.toLowerCase().trim() === "yes"
      );
    });
  });
}

async function run() {
  const apiKey = getEnv("OPENAI_API_KEY");
  const model = process.env.OPENAI_MODEL ?? DEFAULT_MODEL;
  const batchLimit = Number(process.env.MEANINGFULNESS_BATCH_LIMIT ?? "50");

  if (!Number.isFinite(batchLimit) || batchLimit <= 0) {
    throw new Error(`Invalid MEANINGFULNESS_BATCH_LIMIT: ${batchLimit}`);
  }

  const db = getDbInstance();
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  try {
    // Get total count of unscored pairs
    const totalCountResult = await db
      .selectFrom("oe_series as s")
      .leftJoin("meaningfulness_scores as m", (join) =>
        join
          .onRef("m.occupation_code", "=", "s.occupation_code")
          .onRef("m.industry_code", "=", "s.industry_code")
          .on("m.prompt_version", "=", PROMPT_VERSION)
      )
      .select((eb) => eb.fn.count("s.series_id").as("count"))
      .where("m.id", "is", null)
      .executeTakeFirst();

    const totalUnscored = Number(totalCountResult?.count ?? 0);

    if (totalUnscored === 0) {
      console.log("No unscored occupation/industry pairs found.");
      return;
    }

    console.log(`\nTotal occupation/industry pairs to score: ${totalUnscored}`);
    console.log(`Batch size: ${batchLimit}`);
    console.log(
      `Estimated batches needed: ${Math.ceil(totalUnscored / batchLimit)}\n`
    );

    let scoredCount = 0;
    let shouldContinue = true;

    while (shouldContinue && scoredCount < totalUnscored) {
      // Get batch of unscored pairs
      const pairs = await db
        .selectFrom("oe_series as s")
        .innerJoin(
          "oe_occupations as o",
          "o.occupation_code",
          "s.occupation_code"
        )
        .innerJoin("oe_industries as i", "i.industry_code", "s.industry_code")
        .leftJoin("meaningfulness_scores as m", (join) =>
          join
            .onRef("m.occupation_code", "=", "s.occupation_code")
            .onRef("m.industry_code", "=", "s.industry_code")
            .on("m.prompt_version", "=", PROMPT_VERSION)
        )
        .select((eb) => [
          eb.ref("s.occupation_code").as("occupation_code"),
          eb.ref("s.industry_code").as("industry_code"),
          eb.ref("o.occupation_name").as("occupation_name"),
          eb.ref("o.occupation_description").as("occupation_description"),
          eb.ref("i.industry_name").as("industry_name"),
        ])
        .where("m.id", "is", null)
        .groupBy([
          "s.occupation_code",
          "s.industry_code",
          "o.occupation_name",
          "o.occupation_description",
          "i.industry_name",
        ])
        .limit(batchLimit)
        .execute();

      if (pairs.length === 0) {
        console.log("\nNo more unscored pairs found.");
        break;
      }

      // Process the batch
      for (const pair of pairs) {
        const result = await callOpenAI({
          apiKey,
          model,
          occupationName: pair.occupation_name,
          occupationDescription: pair.occupation_description,
          industryName: pair.industry_name,
        });

        await db
          .insertInto("meaningfulness_scores")
          .values({
            occupation_code: pair.occupation_code,
            industry_code: pair.industry_code,
            score: result.score,
            reason: result.reason,
            model,
            prompt_version: PROMPT_VERSION,
            source_inputs: {
              occupation: pair.occupation_name,
              occupation_description: pair.occupation_description,
              industry: pair.industry_name,
            },
          })
          .onConflict((oc) =>
            oc
              .columns(["occupation_code", "industry_code", "prompt_version"])
              .doNothing()
          )
          .execute();

        console.log(
          `Scored ${pair.occupation_code}/${pair.industry_code} -> ${result.score}`
        );
      }

      scoredCount += pairs.length;
      const remaining = totalUnscored - scoredCount;
      const percentComplete = ((scoredCount / totalUnscored) * 100).toFixed(1);

      console.log(`\n========== BATCH COMPLETE ==========`);
      console.log(`Scored this batch: ${pairs.length}`);
      console.log(
        `Total scored: ${scoredCount} / ${totalUnscored} (${percentComplete}%)`
      );
      console.log(`Remaining: ${remaining}`);
      console.log(`=====================================\n`);

      if (remaining > 0) {
        shouldContinue = await askToContinue(rl);
        if (!shouldContinue) {
          console.log("\nScoring paused. Run again later to continue.");
        }
      }
    }

    if (scoredCount >= totalUnscored) {
      console.log("\n✓ All occupation/industry pairs have been scored!");
    }
  } finally {
    rl.close();
    await db.destroy();
  }
}

await run();
