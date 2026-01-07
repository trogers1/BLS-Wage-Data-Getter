import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { Kysely, PostgresDialect } from "kysely";
import { Pool } from "pg";
import { DB } from "../db/generated/db";
import { migrateToLatest } from "../db/migrate";

export class TestDbManager {
  private container: StartedPostgreSqlContainer | null = null;
  private baseConnectionUrl: string | null = null;
  private testDbs: Map<string, Kysely<DB>> = new Map();

  async start(): Promise<string> {
    const container = await new PostgreSqlContainer("postgres:16")
      .withDatabase("postgres") // Use default database to create others
      .withUsername("testuser")
      .withPassword("testpass")
      .start();
    this.container = container;

    this.baseConnectionUrl = this.container.getConnectionUri();

    return this.baseConnectionUrl;
  }

  private async runMigrations(db: Kysely<DB>): Promise<void> {
    await migrateToLatest({ db });
  }

  private async createDatabase(dbName: string): Promise<void> {
    if (!this.baseConnectionUrl) {
      throw new Error("Container not started");
    }

    const adminPool = new Pool({
      connectionString: this.baseConnectionUrl,
    });

    try {
      await adminPool.query(`CREATE DATABASE "${dbName}"`);
    } finally {
      await adminPool.end();
    }
  }

  private async getConnectionUrlForDb(dbName: string): Promise<string> {
    if (!this.baseConnectionUrl) {
      throw new Error("Container not started");
    }

    // Replace the database name in the connection URL
    const url = new URL(this.baseConnectionUrl);
    url.pathname = `/${dbName}`;
    return url.toString();
  }

  async getTestDb(testId: string): Promise<Kysely<DB>> {
    if (this.testDbs.has(testId)) {
      return this.testDbs.get(testId)!;
    }

    const dbName = `testdb_${testId.replace(/[^a-zA-Z0-9_]/g, "_")}`;

    // Create the database
    await this.createDatabase(dbName);

    // Get connection URL for the new database
    const connectionUrl = await this.getConnectionUrlForDb(dbName);

    // Create Kysely instance
    const pool = new Pool({
      connectionString: connectionUrl,
    });

    const db = new Kysely<DB>({
      dialect: new PostgresDialect({ pool }),
    });

    // Run migrations on the new database
    await this.runMigrations(db);

    // Store the database instance
    this.testDbs.set(testId, db);

    return db;
  }

  async createAndSeedTestDb(testId: string): Promise<Kysely<DB>> {
    const db = await this.getTestDb(testId);
    await this.seedTestData(db);
    return db;
  }

  async seedTestData(db: Kysely<DB>): Promise<void> {
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

  async clearTestData(db: Kysely<DB>): Promise<void> {
    await db.deleteFrom("wages").execute();
    await db.deleteFrom("oews_series").execute();
    await db.deleteFrom("naics_codes").execute();
    await db.deleteFrom("soc_codes").execute();
  }

  async stop(): Promise<void> {
    // Clean up all test database connections
    for (const db of this.testDbs.values()) {
      await db.destroy();
    }
    this.testDbs.clear();

    if (this.container) {
      await this.container.stop();
      this.container = null;
    }

    this.baseConnectionUrl = null;
  }

  async resetTestDb(testId: string): Promise<void> {
    const db = await this.getTestDb(testId);
    await this.clearTestData(db);
    await this.seedTestData(db);
  }

  async cleanupTestDb(testId: string): Promise<void> {
    const db = this.testDbs.get(testId);
    if (db) {
      await db.destroy();
      this.testDbs.delete(testId);
    }
  }
}
