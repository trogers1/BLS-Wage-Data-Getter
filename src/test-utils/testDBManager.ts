import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { Kysely, PostgresDialect } from "kysely";
import { Pool } from "pg";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { readFileSync } from "fs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export interface Database {
  soc_codes: {
    soc_code: string;
    title: string;
  };
  naics_codes: {
    naics_code: string;
    title: string;
    level: number;
    parent_code: string | null;
  };
  oews_series: {
    series_id: string;
    soc_code: string;
    naics_code: string;
    exists: boolean;
    last_checked: Date;
  };
  wages: {
    series_id: string;
    year: number;
    mean_annual_wage: number;
  };
}

export class TestDbManager {
  private container: PostgreSqlContainer | null = null;
  private db: Kysely<Database> | null = null;
  private connectionUrl: string | null = null;

  async start(): Promise<string> {
    this.container = await new PostgreSqlContainer("postgres:16")
      .withDatabase("testdb")
      .withUsername("testuser")
      .withPassword("testpass")
      .start();

    this.connectionUrl = this.container.getConnectionUri();

    // Create Kysely instance
    const pool = new Pool({
      connectionString: this.connectionUrl,
    });

    this.db = new Kysely<Database>({
      dialect: new PostgresDialect({ pool }),
    });

    // Run migrations
    await this.runMigrations();

    return this.connectionUrl;
  }

  async runMigrations(): Promise<void> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    // Read and execute the migration file
    const migrationPath = join(
      __dirname,
      "../db/migrations/20260106155742_init.ts"
    );
    const migrationModule = await import(migrationPath);

    await migrationModule.up(this.db);
  }

  getDb(): Kysely<Database> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }
    return this.db;
  }

  async seedTestData(): Promise<void> {
    const db = this.getDb();

    // Seed SOC codes
    await db
      .insertInto("soc_codes")
      .values([
        { soc_code: "11-1011", title: "Chief Executives" },
        { soc_code: "11-1021", title: "General and Operations Managers" },
        { soc_code: "15-1252", title: "Software Developers" },
      ])
      .execute();

    // Seed NAICS codes with hierarchy
    await db
      .insertInto("naics_codes")
      .values([
        { naics_code: "11", title: "Agriculture", level: 2, parent_code: null },
        {
          naics_code: "111",
          title: "Crop Production",
          level: 3,
          parent_code: "11",
        },
        {
          naics_code: "1111",
          title: "Oilseed Farming",
          level: 4,
          parent_code: "111",
        },
        {
          naics_code: "23",
          title: "Construction",
          level: 2,
          parent_code: null,
        },
        {
          naics_code: "236",
          title: "Construction of Buildings",
          level: 3,
          parent_code: "23",
        },
      ])
      .execute();

    // Seed some existing series
    await db
      .insertInto("oews_series")
      .values([
        {
          series_id: "OEUN00000001100011101103",
          soc_code: "11-1011",
          naics_code: "11",
          exists: true,
          last_checked: new Date(),
        },
        {
          series_id: "OEUN00000002300011101103",
          soc_code: "11-1011",
          naics_code: "23",
          exists: false,
          last_checked: new Date(),
        },
      ])
      .execute();

    // Seed some wages
    await db
      .insertInto("wages")
      .values([
        {
          series_id: "OEUN00000001100011101103",
          year: 2023,
          mean_annual_wage: 150000,
        },
      ])
      .execute();
  }

  async clearTestData(): Promise<void> {
    const db = this.getDb();

    await db.deleteFrom("wages").execute();
    await db.deleteFrom("oews_series").execute();
    await db.deleteFrom("naics_codes").execute();
    await db.deleteFrom("soc_codes").execute();
  }

  async stop(): Promise<void> {
    if (this.db) {
      await this.db.destroy();
      this.db = null;
    }

    if (this.container) {
      await this.container.stop();
      this.container = null;
    }

    this.connectionUrl = null;
  }

  async reset(): Promise<void> {
    await this.clearTestData();
    await this.seedTestData();
  }
}
