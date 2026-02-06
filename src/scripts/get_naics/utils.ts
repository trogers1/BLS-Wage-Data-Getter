import { getDbInstance } from "../../db/index.ts";
import type { NaicsCodes } from "../../db/generated/db.d.ts";
import { validate } from "../../schemas/validate.ts";
import { IndustryRow } from "../../schemas/bulk.ts";
import { BLS_OE_BULK_BASE_URL } from "../constants.ts";
import { Type } from "@sinclair/typebox";
import { log } from "../../utils/logger.ts";

const INDUSTRY_HEADERS = [
  "industry_code",
  "industry_name",
  "display_level",
  "selectable",
  "sort_sequence",
];

function parseSelectable(value: string) {
  if (value === "T") {
    return true;
  }

  if (value === "F") {
    return false;
  }

  throw new Error(`Invalid selectable value: ${value}`);
}

function parseIndustryFile(text: string) {
  const lines = text
    .split("\n")
    .map((line) => line.replace(/\r$/, ""))
    .filter((line) => line.trim().length > 0);

  if (lines.length === 0) {
    throw new Error("NAICS industry file is empty");
  }

  const headerParts = lines[0].split("\t").map((part) => part.trim());
  const headerMatches =
    headerParts.length === INDUSTRY_HEADERS.length &&
    INDUSTRY_HEADERS.every((header, index) => headerParts[index] === header);

  if (!headerMatches) {
    throw new Error(`Unexpected industry headers: ${headerParts.join("\t")}`);
  }

  const rows = lines.slice(1).map((line) => {
    const parts = line.split("\t");
    if (parts.length < INDUSTRY_HEADERS.length) {
      throw new Error(`Invalid industry row: ${line}`);
    }

    const industryCode = parts[0]?.trim() ?? "";
    const industryName = parts[1]?.trim() ?? "";
    const displayLevel = Number(parts[2]?.trim() ?? "");
    const selectable = parts[3]?.trim() ?? "";
    const sortSequence = Number(parts[4]?.trim() ?? "");

    if (Number.isNaN(displayLevel) || Number.isNaN(sortSequence)) {
      throw new Error(`Invalid industry row: ${line}`);
    }

    return {
      industry_code: industryCode,
      industry_name: industryName,
      display_level: displayLevel,
      selectable: parseSelectable(selectable),
      sort_sequence: sortSequence,
    };
  });

  return validate(Type.Array(IndustryRow), rows, "oe.industry");
}

export async function getNaics(): Promise<NaicsCodes[]> {
  const res = await fetch(`${BLS_OE_BULK_BASE_URL}/oe.industry`);

  if (!res.ok) {
    throw new Error(
      `Failed getNaics request: ${res.status}\n${await res.text()}`
    );
  }

  const text = await res.text();
  const validated = parseIndustryFile(text);
  log(`NAICS industry rows: ${validated.length}`);

  const naics: NaicsCodes[] = [];
  for (const n of validated) {
    if (!/^\d{6}$/.test(n.industry_code)) {
      continue;
    }

    const level = n.display_level;
    if (level < 2 || level > 6) {
      continue;
    }

    naics.push({
      naics_code: n.industry_code,
      title: n.industry_name,
      level,
      parent_code: level > 2 ? n.industry_code.slice(0, level - 1) : null,
    });
  }
  return naics;
}

export async function insertNaicsIntoDb({
  naics,
  db,
}: {
  naics: NaicsCodes[];
  db: ReturnType<typeof getDbInstance>;
}): Promise<void> {
  await db.transaction().execute(async (trx) => {
    for (const n of naics) {
      await trx
        .insertInto("naics_codes")
        .values(n)
        .onConflict((oc) => oc.column("naics_code").doNothing())
        .execute();
    }
  });
  log("NAICS codes loaded");
}
