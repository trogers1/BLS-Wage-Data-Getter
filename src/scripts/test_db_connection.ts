import { getDbInstance } from "../db/index.ts";
import { log } from "../utils/logger.ts";

async function testConnection() {
  const db = getDbInstance();
  try {
    const result = await db
      .selectFrom("soc_codes")
      .selectAll()
      .limit(1)
      .execute();
    log("Database connection successful");
    log("Sample data:", result);
  } catch (error) {
    console.error("Database connection failed:", error);
  } finally {
    await db.destroy();
  }
}

testConnection();
