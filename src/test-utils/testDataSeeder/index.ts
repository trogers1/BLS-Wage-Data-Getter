import { Kysely } from "kysely";
import type { DB, NaicsCodes, SocCodes } from "../../db/generated/db.d.ts";
import {
  SOC_CODES,
  NAICS_CODES,
  SERIES_EXAMPLES,
  WAGE_EXAMPLES,
} from "./constants.ts";

export class TestDataSeeder {
  constructor(private db: Kysely<DB>) {}

  async seedAll(): Promise<void> {
    await this.seedSocCodes();
    await this.seedNaicsCodes();
    await this.seedOewsSeries();
    await this.seedWages();
  }

  async seedSocCodes(): Promise<void> {
    await this.db
      .insertInto("soc_codes")
      .values(SOC_CODES)
      .onConflict((oc) => oc.column("soc_code").doNothing())
      .execute();
  }

  async seedNaicsCodes(): Promise<void> {
    await this.db
      .insertInto("naics_codes")
      .values(NAICS_CODES)
      .onConflict((oc) => oc.column("naics_code").doNothing())
      .execute();
  }

  async seedOewsSeries(): Promise<void> {
    const seriesData = SERIES_EXAMPLES.flatMap((example) =>
      example.naics_codes.map((naicsCode) => ({
        series_id: this.generateSeriesId({
          socCode: example.soc_code,
          naicsCode: naicsCode,
        }),
        soc_code: example.soc_code,
        naics_code: naicsCode,
        exists: true,
        last_checked: new Date(),
      }))
    );

    await this.db
      .insertInto("oews_series")
      .values(seriesData)
      .onConflict((oc) => oc.column("series_id").doNothing())
      .execute();
  }

  async seedWages(): Promise<void> {
    const wageData = WAGE_EXAMPLES.map((example) => ({
      series_id: this.generateSeriesId({
        socCode: example.soc_code,
        naicsCode: example.naics_code,
      }),
      year: example.year,
      mean_annual_wage: example.mean_annual_wage,
    }));

    await this.db
      .insertInto("wages")
      .values(wageData)
      .onConflict((oc) => oc.doNothing())
      .execute();
  }

  async clearAll(): Promise<void> {
    await this.db.deleteFrom("wages").execute();
    await this.db.deleteFrom("oews_series").execute();
    await this.db.deleteFrom("naics_codes").execute();
    await this.db.deleteFrom("soc_codes").execute();
  }

  private generateSeriesId({
    socCode,
    naicsCode,
  }: {
    socCode: SocCodes["soc_code"];
    naicsCode: NaicsCodes["naics_code"];
  }): string {
    const socPart = socCode.replace("-", "");
    const naicsPart = naicsCode.replace("-", "").padStart(6, "0");
    return `OEUN${naicsPart}${socPart}03`;
  }
}
