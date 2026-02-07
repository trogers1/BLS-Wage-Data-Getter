import { getDbInstance } from "../../db/index.ts";

const DEFAULT_MODEL = "gpt-4.1-mini";
const PROMPT_VERSION = "v1";

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
  industryName,
}: {
  apiKey: string;
  model: string;
  occupationName: string;
  industryName: string;
}): Promise<MeaningfulnessScore> {
  const prompt = `You are rating how meaningful a job is in a specific industry context.

Occupation: ${occupationName}
Industry: ${industryName}

Return a JSON object with:
- score: integer 1-5 (1 = least meaningful, 5 = most meaningful)
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

async function run() {
  const apiKey = getEnv("OPENAI_API_KEY");
  const model = process.env.OPENAI_MODEL ?? DEFAULT_MODEL;
  const batchLimit = Number(process.env.MEANINGFULNESS_BATCH_LIMIT ?? "50");

  if (!Number.isFinite(batchLimit) || batchLimit <= 0) {
    throw new Error(`Invalid MEANINGFULNESS_BATCH_LIMIT: ${batchLimit}`);
  }

  const db = getDbInstance();

  try {
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
        eb.ref("i.industry_name").as("industry_name"),
      ])
      .where("m.id", "is", null)
      .groupBy([
        "s.occupation_code",
        "s.industry_code",
        "o.occupation_name",
        "i.industry_name",
      ])
      .limit(batchLimit)
      .execute();

    if (pairs.length === 0) {
      console.log("No unscored occupation/industry pairs found.");
      return;
    }

    for (const pair of pairs) {
      const result = await callOpenAI({
        apiKey,
        model,
        occupationName: pair.occupation_name,
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
  } finally {
    await db.destroy();
  }
}

await run();
