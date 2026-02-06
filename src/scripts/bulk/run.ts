import { downloadBulkFiles } from "./utils.ts";
import { ingestMappingFiles } from "./ingest_mappings.ts";
import { ingestSeriesFile } from "./ingest_series.ts";
import { ingestDataFile } from "./ingest_data.ts";

await downloadBulkFiles();
await ingestMappingFiles();
await ingestSeriesFile();
await ingestDataFile();
