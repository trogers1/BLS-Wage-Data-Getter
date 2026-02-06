import { Kysely } from "kysely";
import type { DB, NaicsCodes, SocCodes } from "../../db/generated/db.d.ts";
import { SOC_CODES, NAICS_CODES } from "./constants.ts";
import { oneOf, randomInt } from "./deterministicRandom/dRandom.ts";

export class TestDataSeeder {
  constructor(private db: Kysely<DB>) {}

  async seedAll(): Promise<void> {
    await this.seedSocCodes();
    await this.seedNaicsCodes();
  }

  async seedDeterministic(baseSeed: string): Promise<void> {
    await this.seedSocCodesDeterministic(baseSeed);
    await this.seedNaicsCodesDeterministic(baseSeed);
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

  async clearAll(): Promise<void> {
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
}
