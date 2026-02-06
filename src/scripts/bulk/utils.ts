import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile } from "node:fs/promises";
import { pipeline } from "node:stream/promises";
import path from "node:path";
import { createInterface } from "node:readline";
import { BLS_OE_BULK_BASE_URL } from "../constants.ts";

export const BULK_FILES = [
  "oe.area",
  "oe.areatype",
  "oe.datatype",
  "oe.footnote",
  "oe.industry",
  "oe.occupation",
  "oe.release",
  "oe.seasonal",
  "oe.sector",
  "oe.series",
  "oe.data.0.Current",
];

export const BULK_DATA_DIR = path.resolve(process.cwd(), "data", "bulk", "oe");

export async function downloadBulkFile(fileName: string) {
  const res = await fetch(`${BLS_OE_BULK_BASE_URL}/${fileName}`);

  if (!res.ok || !res.body) {
    throw new Error(
      `Failed to download ${fileName}: ${res.status}\n${await res.text()}`
    );
  }

  await mkdir(BULK_DATA_DIR, { recursive: true });
  const filePath = path.join(BULK_DATA_DIR, fileName);
  await pipeline(res.body, createWriteStream(filePath));
  return filePath;
}

export async function downloadBulkFiles(files = BULK_FILES) {
  const downloaded: string[] = [];
  for (const fileName of files) {
    downloaded.push(await downloadBulkFile(fileName));
  }
  return downloaded;
}

export async function readBulkFile(fileName: string) {
  const filePath = path.join(BULK_DATA_DIR, fileName);
  return readFile(filePath, "utf-8");
}

export function getBulkFilePath(fileName: string) {
  return path.join(BULK_DATA_DIR, fileName);
}

export function createLineReader(filePath: string) {
  return createInterface({
    input: createReadStream(filePath),
    crlfDelay: Infinity,
  });
}

export function parseTabLine(line: string) {
  return line.split("\t").map((part) => part.trim());
}

export function toBoolean(value: string) {
  if (value === "T") {
    return true;
  }

  if (value === "F") {
    return false;
  }

  throw new Error(`Invalid boolean token: ${value}`);
}

export function toNumber(value: string, context: string) {
  const parsed = Number(value);
  if (Number.isNaN(parsed)) {
    throw new Error(`Invalid number for ${context}: ${value}`);
  }
  return parsed;
}
