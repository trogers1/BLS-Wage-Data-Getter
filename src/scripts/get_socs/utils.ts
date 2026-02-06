import { SocResponse } from "../../schemas/schemas.ts";
import { validate } from "../../schemas/validate.ts";
import { getDbInstance } from "../../db/index.ts";
import type { SocCodes } from "../../db/generated/db.d.ts";
import { BLS_OE_BULK_BASE_URL } from "../constants.ts";
import { log } from "../../utils/logger.ts";

const OCCUPATION_HEADERS = [
  "occupation_code",
  "occupation_name",
  "display_level",
  "selectable",
  "sort_sequence",
];

function formatSocCode(code: string): string {
  if (/^\d{6}$/.test(code)) {
    return `${code.slice(0, 2)}-${code.slice(2)}`;
  }

  return code;
}

function parseOccupationFile(text: string) {
  const lines = text
    .split("\n")
    .map((line) => line.replace(/\r$/, ""))
    .filter((line) => line.trim().length > 0);

  if (lines.length === 0) {
    throw new Error("SOC occupation file is empty");
  }

  const headerParts = lines[0].split("\t").map((part) => part.trim());
  const headerMatches =
    headerParts.length === OCCUPATION_HEADERS.length &&
    OCCUPATION_HEADERS.every((header, index) => headerParts[index] === header);

  if (!headerMatches) {
    throw new Error(
      `Unexpected SOC occupation headers: ${headerParts.join("\t")}`
    );
  }

  const occupations = lines.slice(1).map((line) => {
    const parts = line.split("\t");

    if (parts.length < OCCUPATION_HEADERS.length) {
      throw new Error(`Invalid SOC occupation row: ${line}`);
    }

    const occupationCode = parts[0]?.trim() ?? "";
    const occupationName = parts[1]?.trim() ?? "";

    return {
      code: formatSocCode(occupationCode),
      text: occupationName,
    };
  });

  return validate(SocResponse, { occupations }, "SOC occupation file");
}

export async function getSocs(): Promise<SocCodes[]> {
  const res = await fetch(`${BLS_OE_BULK_BASE_URL}/oe.occupation`);

  if (!res.ok) {
    throw new Error(
      `Failed getSocs request: ${res.status}\n${await res.text()}`
    );
  }
  const text = await res.text();

  const validated = parseOccupationFile(text);

  log(`SOC occupation rows: ${validated.occupations.length}`);

  const socs: SocCodes[] = [];
  for (const o of validated.occupations) {
    if (/^\d{2}-\d{4}$/.test(o.code)) {
      socs.push({
        soc_code: o.code,
        title: o.text,
      });
    }
  }
  return socs;
}

export async function insertSocsIntoDb({
  socs,
  db,
}: {
  socs: SocCodes[];
  db: ReturnType<typeof getDbInstance>;
}): Promise<void> {
  await db.transaction().execute(async (trx) => {
    for (const soc of socs) {
      await trx
        .insertInto("soc_codes")
        .values(soc)
        .onConflict((oc) => oc.column("soc_code").doNothing())
        .execute();
    }
  });
  log("SOC codes loaded");
}
