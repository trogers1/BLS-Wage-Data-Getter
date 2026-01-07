import { db } from "../db/index.ts";

async function testConnection() {
  try {
    const result = await db
      .selectFrom("soc_codes")
      .selectAll()
      .limit(1)
      .execute();
    console.log("Database connection successful");
    console.log("Sample data:", result);
  } catch (error) {
    console.error("Database connection failed:", error);
  } finally {
    await db.destroy();
  }
}

testConnection();
