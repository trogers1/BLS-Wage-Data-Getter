import { Kysely } from "kysely";
import type { DB, NaicsCodes, SocCodes } from "../../db/generated/db.d.ts";
import {
  SOC_CODES,
  NAICS_CODES,
  SERIES_EXAMPLES,
  WAGE_EXAMPLES,
} from "./constants.ts";
import { oneOf, randomInt, shuffle } from "./deterministicRandom/dRandom.ts";

export class TestDataSeeder {
  constructor(private db: Kysely<DB>) {}

  async seedAll(): Promise<void> {
    await this.seedSocCodes();
    await this.seedNaicsCodes();
    await this.seedOewsSeries();
    await this.seedWages();
  }

  async seedDeterministic(baseSeed: string): Promise<void> {
    await this.seedSocCodesDeterministic(baseSeed);
    await this.seedNaicsCodesDeterministic(baseSeed);
    await this.seedOewsSeriesDeterministic(baseSeed);
    await this.seedWagesDeterministic(baseSeed);
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
        does_exist: true,
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

  async seedSocCodesDeterministic(baseSeed: string): Promise<void> {
    const socTitles = [
      "Chief Executives",
      "General and Operations Managers",
      "Software Developers",
      "Registered Nurses",
      "Customer Service Representatives",
      "Heavy and Tractor-Trailer Truck Drivers",
      "Marketing Managers",
      "Financial Managers",
      "Accountants",
      "Web Developers",
      "Database Administrators",
      "Network Architects",
      "Physicians",
      "Lawyers",
      "Teachers",
    ];

    const socCodes: SocCodes[] = [];
    for (let i = 0; i < 20; i++) {
      const major = randomInt({
        seed: `${baseSeed}-soc-major-${i}`,
        min: 11,
        max: 53,
      });
      const minor = randomInt({
        seed: `${baseSeed}-soc-minor-${i}`,
        min: 0,
        max: 9999,
      });
      const socCode = `${major.toString().padStart(2, "0")}-${minor.toString().padStart(4, "0")}`;
      const title = oneOf({
        seed: `${baseSeed}-soc-title-${i}`,
        options: socTitles,
      });
      socCodes.push({ soc_code: socCode, title });
    }

    await this.db
      .insertInto("soc_codes")
      .values(socCodes)
      .onConflict((oc) => oc.column("soc_code").doNothing())
      .execute();
  }

  async seedNaicsCodesDeterministic(baseSeed: string): Promise<void> {
    const naicsTitles = [
      "Agriculture, Forestry, Fishing and Hunting",
      "Construction",
      "Manufacturing",
      "Retail Trade",
      "Information",
      "Finance and Insurance",
      "Professional, Scientific, and Technical Services",
      "Health Care and Social Assistance",
      "Accommodation and Food Services",
      "Crop Production",
      "Animal Production",
      "Construction of Buildings",
      "Specialty Trade Contractors",
      "Computer and Electronic Product Manufacturing",
      "Publishing Industries",
      "Data Processing, Hosting, and Related Services",
      "Professional, Scientific, and Technical Services",
      "Hospitals",
      "Food Services and Drinking Places",
      "Oilseed and Grain Farming",
    ];

    const naicsCodes: NaicsCodes[] = [];

    // Generate level 2 codes (2-digit)
    for (let i = 0; i < 10; i++) {
      const code = randomInt({
        seed: `${baseSeed}-naics-l2-${i}`,
        min: 11,
        max: 99,
      }).toString();
      const title = oneOf({
        seed: `${baseSeed}-naics-title-l2-${i}`,
        options: naicsTitles,
      });
      naicsCodes.push({
        naics_code: code,
        title,
        level: 2,
        parent_code: null,
      });
    }

    // Generate some hierarchical structure
    for (const parent of naicsCodes.filter((n) => n.level === 2)) {
      const childCount = randomInt({
        seed: `${baseSeed}-children-${parent.naics_code}`,
        min: 1,
        max: 3,
      });
      for (let j = 0; j < childCount; j++) {
        const childCode = `${parent.naics_code}${randomInt({ seed: `${baseSeed}-child-${parent.naics_code}-${j}`, min: 1, max: 9 })}`;
        const title = oneOf({
          seed: `${baseSeed}-naics-title-${childCode}`,
          options: naicsTitles,
        });
        naicsCodes.push({
          naics_code: childCode,
          title,
          level: childCode.length,
          parent_code: parent.naics_code,
        });
      }
    }

    await this.db
      .insertInto("naics_codes")
      .values(naicsCodes)
      .onConflict((oc) => oc.column("naics_code").doNothing())
      .execute();
  }

  async seedOewsSeriesDeterministic(baseSeed: string): Promise<void> {
    // Get existing SOC and NAICS codes from database
    const socs = await this.db.selectFrom("soc_codes").selectAll().execute();
    const naics = await this.db.selectFrom("naics_codes").selectAll().execute();

    const seriesData = [];
    for (const soc of socs) {
      // Pick 1-5 NAICS codes per SOC
      const naicsCount = randomInt({
        seed: `${baseSeed}-series-count-${soc.soc_code}`,
        min: 1,
        max: 5,
      });
      const shuffledNaics = shuffle({
        randomSeed: `${baseSeed}-shuffle-${soc.soc_code}`,
        options: naics,
      });
      const selectedNaics = shuffledNaics.slice(0, naicsCount);

      for (const naicsCode of selectedNaics) {
        const doesExist =
          randomInt({
            seed: `${baseSeed}-exists-${soc.soc_code}-${naicsCode.naics_code}`,
            min: 0,
            max: 1,
          }) === 1;
        seriesData.push({
          series_id: this.generateSeriesId({
            socCode: soc.soc_code,
            naicsCode: naicsCode.naics_code,
          }),
          soc_code: soc.soc_code,
          naics_code: naicsCode.naics_code,
          does_exist: doesExist,
          last_checked: new Date(),
        });
      }
    }

    await this.db
      .insertInto("oews_series")
      .values(seriesData)
      .onConflict((oc) => oc.column("series_id").doNothing())
      .execute();
  }

  async seedWagesDeterministic(baseSeed: string): Promise<void> {
    const series = await this.db
      .selectFrom("oews_series")
      .selectAll()
      .where("does_exist", "=", true)
      .execute();

    const wages = [];
    for (const s of series) {
      // Generate 1-3 years of wage data
      const yearCount = randomInt({
        seed: `${baseSeed}-yearcount-${s.series_id}`,
        min: 1,
        max: 3,
      });
      const startYear = randomInt({
        seed: `${baseSeed}-startyear-${s.series_id}`,
        min: 2020,
        max: 2023,
      });

      for (let i = 0; i < yearCount; i++) {
        const year = startYear + i;
        const meanWage = randomInt({
          seed: `${baseSeed}-wage-${s.series_id}-${year}`,
          min: 30000,
          max: 250000,
        });
        wages.push({
          series_id: s.series_id,
          year,
          mean_annual_wage: meanWage,
        });
      }
    }

    await this.db
      .insertInto("wages")
      .values(wages)
      .onConflict((oc) => oc.doNothing())
      .execute();
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
