import { Kysely, type Insertable } from "kysely";
import type {
  DB,
  MeaningfulnessScores,
  OeData,
  OeIndustries,
  OeOccupations,
  OeSeries,
} from "../../db";
import {
  OE_AREAS,
  OE_AREATYPES,
  OE_DATATYPES,
  OE_INDUSTRIES,
  OE_OCCUPATIONS,
  OE_SECTORS,
} from "./constants.ts";
import { randomInt } from "./deterministicRandom/dRandom.ts";
import { hashString } from "./deterministicRandom/hashing.ts";

export class TestDataSeeder {
  constructor(private db: Kysely<DB>) {}

  async seedAll(): Promise<void> {
    await this.seedOeAreatypes();
    await this.seedOeAreas();
    await this.seedOeDatatypes();
    await this.seedOeSectors();
    await this.seedOeOccupations();
    await this.seedOeIndustries();
  }

  async seedDeterministic(baseSeed: string): Promise<void> {
    await this.seedOeAreatypes();
    await this.seedOeAreas();
    await this.seedOeDatatypes();
    await this.seedOeSectors();
    const dataset = this.buildDeterministicDataset(baseSeed);

    await this.db
      .insertInto("oe_occupations")
      .values(dataset.occupations)
      .onConflict((oc) => oc.column("occupation_code").doNothing())
      .execute();

    await this.db
      .insertInto("oe_industries")
      .values(dataset.industries)
      .onConflict((oc) => oc.column("industry_code").doNothing())
      .execute();

    await this.db
      .insertInto("oe_series")
      .values(dataset.series)
      .onConflict((oc) => oc.column("series_id").doNothing())
      .execute();

    await this.db
      .insertInto("oe_data")
      .values(dataset.data)
      .onConflict((oc) =>
        oc.columns(["series_id", "year", "period"]).doNothing()
      )
      .execute();

    await this.db
      .insertInto("meaningfulness_scores")
      .values(dataset.meaningfulness)
      .onConflict((oc) =>
        oc
          .columns(["occupation_code", "industry_code", "prompt_version"])
          .doNothing()
      )
      .execute();
  }

  async seedOeOccupations(): Promise<void> {
    await this.db
      .insertInto("oe_occupations")
      .values(OE_OCCUPATIONS)
      .onConflict((oc) => oc.column("occupation_code").doNothing())
      .execute();
  }

  async seedOeIndustries(): Promise<void> {
    await this.db
      .insertInto("oe_industries")
      .values(OE_INDUSTRIES)
      .onConflict((oc) => oc.column("industry_code").doNothing())
      .execute();
  }

  async clearAll(): Promise<void> {
    await this.db.deleteFrom("meaningfulness_scores").execute();
    await this.db.deleteFrom("oe_data").execute();
    await this.db.deleteFrom("oe_series").execute();
    await this.db.deleteFrom("oe_industries").execute();
    await this.db.deleteFrom("oe_occupations").execute();
    await this.db.deleteFrom("oe_sectors").execute();
    await this.db.deleteFrom("oe_datatypes").execute();
    await this.db.deleteFrom("oe_areas").execute();
    await this.db.deleteFrom("oe_areatypes").execute();
  }

  async seedOeAreatypes(): Promise<void> {
    await this.db
      .insertInto("oe_areatypes")
      .values(OE_AREATYPES)
      .onConflict((oc) => oc.column("areatype_code").doNothing())
      .execute();
  }

  async seedOeAreas(): Promise<void> {
    await this.db
      .insertInto("oe_areas")
      .values(OE_AREAS)
      .onConflict((oc) => oc.columns(["state_code", "area_code"]).doNothing())
      .execute();
  }

  async seedOeDatatypes(): Promise<void> {
    await this.db
      .insertInto("oe_datatypes")
      .values(OE_DATATYPES)
      .onConflict((oc) => oc.column("datatype_code").doNothing())
      .execute();
  }

  async seedOeSectors(): Promise<void> {
    await this.db
      .insertInto("oe_sectors")
      .values(OE_SECTORS)
      .onConflict((oc) => oc.column("sector_code").doNothing())
      .execute();
  }

  private buildDeterministicDataset(baseSeed: string): {
    occupations: OeOccupations[];
    industries: OeIndustries[];
    series: OeSeries[];
    data: OeData[];
    meaningfulness: Insertable<MeaningfulnessScores>[];
  } {
    const seedHash = Math.abs(hashString(baseSeed));
    const seedToken = seedHash.toString(36);
    const occupationNames = [
      "Chief Executives",
      "Software Developers",
      "Registered Nurses",
      "Accountants",
      "Teachers",
      "Data Scientists",
      "Civil Engineers",
      "Financial Analysts",
    ];
    const industryNames = [
      "Soybean Farming",
      "Construction of Buildings",
      "Hospitals",
      "Software Publishers",
      "Retail Trade",
      "Manufacturing",
      "Professional Services",
      "Transportation and Warehousing",
    ];
    const occupationCount = 6;
    const industryCount = 6;
    const occupations: OeOccupations[] = [];
    const industries: OeIndustries[] = [];

    for (let i = 0; i < occupationCount; i++) {
      const major = 11 + ((seedHash + i * 3) % 42);
      const minor = 1000 + ((seedHash + i * 137) % 9000);
      occupations.push({
        occupation_code: `${major.toString().padStart(2, "0")}${minor
          .toString()
          .padStart(4, "0")}`,
        occupation_name:
          occupationNames[(seedHash + i) % occupationNames.length],
        occupation_description: null,
        display_level: 3,
        selectable: true,
        sort_sequence: i + 1,
      });
    }

    for (let i = 0; i < industryCount; i++) {
      const code = (100000 + ((seedHash + i * 271) % 900000))
        .toString()
        .padStart(6, "0");
      industries.push({
        industry_code: code,
        industry_name: industryNames[(seedHash + i * 2) % industryNames.length],
        display_level: 6,
        selectable: true,
        sort_sequence: i + 1,
      });
    }

    const datatypeMap: Record<string, string> = {
      "15": "Mean Annual Wage",
      "01": "Employment",
    };
    const datatypeCodes = ["15", "01"];
    const series: OeSeries[] = [];
    const data: OeData[] = [];
    const meaningfulness: Insertable<MeaningfulnessScores>[] = [];

    let seriesIndex = 0;
    let pairIndex = 0;
    for (const occupation of occupations) {
      for (const industry of industries) {
        if ((pairIndex + seedHash) % 2 === 0) {
          meaningfulness.push({
            occupation_code: occupation.occupation_code,
            industry_code: industry.industry_code,
            score: randomInt({
              seed: `${baseSeed}-meaning-${occupation.occupation_code}-${industry.industry_code}`,
              min: 10,
              max: 95,
            }),
            reason: `Deterministic score for ${occupation.occupation_name} in ${industry.industry_name}.`,
            model: "test-model",
            prompt_version: "v1",
            source_inputs: {
              occupation_code: occupation.occupation_code,
              occupation_name: occupation.occupation_name,
              industry_code: industry.industry_code,
              industry_name: industry.industry_name,
              base_seed: baseSeed,
            },
          });
        }

        pairIndex += 1;

        for (const datatypeCode of datatypeCodes) {
          const seriesId = `OE-${seedToken}-${datatypeCode}-${industry.industry_code}-${occupation.occupation_code}-${seriesIndex}`;
          const datatypeName = datatypeMap[datatypeCode] ?? "Unknown";
          series.push({
            series_id: seriesId,
            seasonal: "U",
            areatype_code: "N",
            industry_code: industry.industry_code,
            occupation_code: occupation.occupation_code,
            datatype_code: datatypeCode,
            state_code: "00",
            area_code: "0000000",
            sector_code: "000000",
            series_title: `${datatypeName} for ${occupation.occupation_name} in ${industry.industry_name}`,
            footnote_codes: null,
            begin_year: 2022,
            begin_period: "A01",
            end_year: 2023,
            end_period: "A01",
          });

          const baseValue = randomInt({
            seed: `${baseSeed}-value-${seriesId}`,
            min: 40000,
            max: 180000,
          });
          const step = randomInt({
            seed: `${baseSeed}-value-step-${seriesId}`,
            min: 500,
            max: 5000,
          });
          for (const year of [2022, 2023]) {
            data.push({
              series_id: seriesId,
              year,
              period: "A01",
              value: baseValue + (year - 2022) * step,
              footnote_codes: null,
            });
          }

          seriesIndex += 1;
        }
      }
    }

    return { occupations, industries, series, data, meaningfulness };
  }
}
