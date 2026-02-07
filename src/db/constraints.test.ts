import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { TestDbManager } from "../test-utils/testDBManager.ts";

describe("Database FK constraints", () => {
  let dbManager: TestDbManager;

  beforeAll(async () => {
    dbManager = new TestDbManager();
    await dbManager.start();
  });

  afterAll(async () => {
    await dbManager.stop();
  });

  it("should enforce oe_series foreign keys", async () => {
    const db = await dbManager.getTestDb("fk-test");

    await db
      .insertInto("oe_areatypes")
      .values({
        areatype_code: "N",
        areatype_name: "National",
      })
      .execute();
    await db
      .insertInto("oe_areas")
      .values({
        state_code: "00",
        area_code: "0000000",
        areatype_code: "N",
        area_name: "National",
      })
      .execute();
    await db
      .insertInto("oe_datatypes")
      .values({
        datatype_code: "15",
        datatype_name: "Mean Annual Wage",
      })
      .execute();
    await db
      .insertInto("oe_sectors")
      .values({
        sector_code: "000000",
        sector_name: "All industries",
      })
      .execute();
    await db
      .insertInto("oe_occupations")
      .values({
        occupation_code: "111011",
        occupation_name: "Chief Executives",
        display_level: 3,
        selectable: true,
        sort_sequence: 1,
      })
      .execute();
    await db
      .insertInto("oe_industries")
      .values({
        industry_code: "111110",
        industry_name: "Soybean Farming",
        display_level: 6,
        selectable: true,
        sort_sequence: 1,
      })
      .execute();

    await db
      .insertInto("oe_series")
      .values({
        series_id: "OEUN0000000111101110103",
        seasonal: "U",
        areatype_code: "N",
        industry_code: "111110",
        occupation_code: "111011",
        datatype_code: "15",
        state_code: "00",
        area_code: "0000000",
        sector_code: "000000",
        series_title: "Mean annual wage for test",
        footnote_codes: null,
        begin_year: 2022,
        begin_period: "A01",
        end_year: 2023,
        end_period: "A01",
      })
      .execute();

    await expect(
      db
        .insertInto("oe_series")
        .values({
          series_id: "OEUN00000001111099999903",
          seasonal: "U",
          areatype_code: "N",
          industry_code: "111110",
          occupation_code: "999999",
          datatype_code: "15",
          state_code: "00",
          area_code: "0000000",
          sector_code: "000000",
          series_title: "Invalid occupation",
          footnote_codes: null,
          begin_year: 2022,
          begin_period: "A01",
          end_year: 2023,
          end_period: "A01",
        })
        .execute()
    ).rejects.toThrow();
  });
});
