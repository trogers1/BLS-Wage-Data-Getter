import { getDbInstance } from "../../db/index.ts";
import {
  AreaRow,
  AreatypeRow,
  DatatypeRow,
  FootnoteRow,
  IndustryRow,
  OccupationRow,
  ReleaseRow,
  SeasonalRow,
  SectorRow,
} from "../../schemas/bulk.ts";
import { validate } from "../../schemas/validate.ts";
import {
  BULK_FILES,
  readBulkFile,
  parseTabLine,
  toBoolean,
  toNumber,
} from "./utils.ts";
import { Type } from "@sinclair/typebox";

const TAB_HEADER = {
  "oe.occupation": [
    "occupation_code",
    "occupation_name",
    "occupation_description",
    "display_level",
    "selectable",
    "sort_sequence",
  ],
  "oe.industry": [
    "industry_code",
    "industry_name",
    "display_level",
    "selectable",
    "sort_sequence",
  ],
  "oe.area": ["state_code", "area_code", "areatype_code", "area_name"],
  "oe.areatype": ["areatype_code", "areatype_name"],
  "oe.datatype": ["datatype_code", "datatype_name"],
  "oe.sector": ["sector_code", "sector_name"],
  "oe.footnote": ["footnote_code", "footnote_text"],
  "oe.release": ["release_date", "description"],
  "oe.seasonal": ["seasonal_code", "seasonal_text"],
} satisfies Record<string, string[]>;

type MappingFileName = keyof typeof TAB_HEADER;

function assertHeader(fileName: MappingFileName, header: string[]) {
  const expected = TAB_HEADER[fileName];
  if (!expected) {
    throw new Error(`Unknown mapping file: ${fileName}`);
  }

  const matches =
    header.length === expected.length &&
    expected.every((value: string, index: number) => header[index] === value);

  if (!matches) {
    throw new Error(`Unexpected ${fileName} headers: ${header.join("\t")}`);
  }
}

function parseTabFile(text: string) {
  return text
    .split("\n")
    .map((line) => line.replace(/\r$/, ""))
    .filter((line) => line.trim().length > 0);
}

async function ingestOccupations(db: ReturnType<typeof getDbInstance>) {
  const lines = parseTabFile(await readBulkFile("oe.occupation"));
  if (lines.length === 0) {
    throw new Error("oe.occupation is empty");
  }
  assertHeader("oe.occupation", parseTabLine(lines[0]));

  const rows = lines.slice(1).map((line) => {
    const [code, name, description, level, selectable, sort] =
      parseTabLine(line);
    return {
      occupation_code: code,
      occupation_name: name,
      occupation_description: description?.length > 0 ? description : null,
      display_level: toNumber(level, "display_level"),
      selectable: toBoolean(selectable),
      sort_sequence: toNumber(sort, "sort_sequence"),
    };
  });

  const validated = validate(Type.Array(OccupationRow), rows, "oe.occupation");

  await db
    .insertInto("oe_occupations")
    .values(validated)
    .onConflict((oc) => oc.column("occupation_code").doNothing())
    .execute();
}

async function ingestIndustries(db: ReturnType<typeof getDbInstance>) {
  const lines = parseTabFile(await readBulkFile("oe.industry"));
  if (lines.length === 0) {
    throw new Error("oe.industry is empty");
  }
  assertHeader("oe.industry", parseTabLine(lines[0]));

  const rows = lines.slice(1).map((line) => {
    const [code, name, level, selectable, sort] = parseTabLine(line);
    return {
      industry_code: code,
      industry_name: name,
      display_level: toNumber(level, "display_level"),
      selectable: toBoolean(selectable),
      sort_sequence: toNumber(sort, "sort_sequence"),
    };
  });

  const validated = validate(Type.Array(IndustryRow), rows, "oe.industry");

  await db
    .insertInto("oe_industries")
    .values(validated)
    .onConflict((oc) => oc.column("industry_code").doNothing())
    .execute();
}

async function ingestAreas(db: ReturnType<typeof getDbInstance>) {
  const lines = parseTabFile(await readBulkFile("oe.area"));
  if (lines.length === 0) {
    throw new Error("oe.area is empty");
  }
  assertHeader("oe.area", parseTabLine(lines[0]));

  const rows = lines.slice(1).map((line) => {
    const [stateCode, areaCode, areaType, name] = parseTabLine(line);
    return {
      state_code: stateCode,
      area_code: areaCode,
      areatype_code: areaType,
      area_name: name,
    };
  });

  const validated = validate(Type.Array(AreaRow), rows, "oe.area");

  await db
    .insertInto("oe_areas")
    .values(validated)
    .onConflict((oc) => oc.columns(["state_code", "area_code"]).doNothing())
    .execute();
}

async function ingestAreatypes(db: ReturnType<typeof getDbInstance>) {
  const lines = parseTabFile(await readBulkFile("oe.areatype"));
  if (lines.length === 0) {
    throw new Error("oe.areatype is empty");
  }
  assertHeader("oe.areatype", parseTabLine(lines[0]));

  const rows = lines.slice(1).map((line) => {
    const [code, name] = parseTabLine(line);
    return { areatype_code: code, areatype_name: name };
  });

  const validated = validate(Type.Array(AreatypeRow), rows, "oe.areatype");

  await db
    .insertInto("oe_areatypes")
    .values(validated)
    .onConflict((oc) => oc.column("areatype_code").doNothing())
    .execute();
}

async function ingestDatatypes(db: ReturnType<typeof getDbInstance>) {
  const lines = parseTabFile(await readBulkFile("oe.datatype"));
  if (lines.length === 0) {
    throw new Error("oe.datatype is empty");
  }
  assertHeader("oe.datatype", parseTabLine(lines[0]));

  const rows = lines.slice(1).map((line) => {
    const [code, name] = parseTabLine(line);
    return { datatype_code: code, datatype_name: name };
  });

  const validated = validate(Type.Array(DatatypeRow), rows, "oe.datatype");

  await db
    .insertInto("oe_datatypes")
    .values(validated)
    .onConflict((oc) => oc.column("datatype_code").doNothing())
    .execute();
}

async function ingestSectors(db: ReturnType<typeof getDbInstance>) {
  const lines = parseTabFile(await readBulkFile("oe.sector"));
  if (lines.length === 0) {
    throw new Error("oe.sector is empty");
  }
  assertHeader("oe.sector", parseTabLine(lines[0]));

  const rows = lines.slice(1).map((line) => {
    const [code, name] = parseTabLine(line);
    return { sector_code: code, sector_name: name };
  });

  const validated = validate(Type.Array(SectorRow), rows, "oe.sector");

  await db
    .insertInto("oe_sectors")
    .values(validated)
    .onConflict((oc) => oc.column("sector_code").doNothing())
    .execute();
}

async function ingestFootnotes(db: ReturnType<typeof getDbInstance>) {
  const lines = parseTabFile(await readBulkFile("oe.footnote"));
  if (lines.length === 0) {
    throw new Error("oe.footnote is empty");
  }
  assertHeader("oe.footnote", parseTabLine(lines[0]));

  const rows = lines.slice(1).map((line) => {
    const [code, text] = parseTabLine(line);
    return { footnote_code: code, footnote_text: text };
  });

  const validated = validate(Type.Array(FootnoteRow), rows, "oe.footnote");

  await db
    .insertInto("oe_footnotes")
    .values(validated)
    .onConflict((oc) => oc.column("footnote_code").doNothing())
    .execute();
}

async function ingestReleases(db: ReturnType<typeof getDbInstance>) {
  const lines = parseTabFile(await readBulkFile("oe.release"));
  if (lines.length === 0) {
    throw new Error("oe.release is empty");
  }
  assertHeader("oe.release", parseTabLine(lines[0]));

  const rows = lines.slice(1).map((line) => {
    const [date, description] = parseTabLine(line);
    return { release_date: date, description };
  });

  const validated = validate(Type.Array(ReleaseRow), rows, "oe.release");

  await db
    .insertInto("oe_releases")
    .values(validated)
    .onConflict((oc) => oc.column("release_date").doNothing())
    .execute();
}

async function ingestSeasonal(db: ReturnType<typeof getDbInstance>) {
  const lines = parseTabFile(await readBulkFile("oe.seasonal"));
  if (lines.length === 0) {
    throw new Error("oe.seasonal is empty");
  }
  assertHeader("oe.seasonal", parseTabLine(lines[0]));

  const rows = lines.slice(1).map((line) => {
    const [code, text] = parseTabLine(line);
    return { seasonal_code: code, seasonal_text: text };
  });

  const validated = validate(Type.Array(SeasonalRow), rows, "oe.seasonal");

  await db
    .insertInto("oe_seasonal")
    .values(validated)
    .onConflict((oc) => oc.column("seasonal_code").doNothing())
    .execute();
}

export async function ingestMappingFiles() {
  const requiredFiles = BULK_FILES.filter(
    (file) => !file.startsWith("oe.data") && file !== "oe.series"
  );

  if (requiredFiles.length === 0) {
    throw new Error("No mapping files configured");
  }

  const db = getDbInstance();

  try {
    await ingestAreatypes(db);
    await ingestAreas(db);
    await ingestDatatypes(db);
    await ingestSectors(db);
    await ingestOccupations(db);
    await ingestIndustries(db);
    await ingestFootnotes(db);
    await ingestReleases(db);
    await ingestSeasonal(db);
  } finally {
    await db.destroy();
  }
}

await ingestMappingFiles();
