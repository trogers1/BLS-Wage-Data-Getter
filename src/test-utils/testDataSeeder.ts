import { Kysely } from "kysely";
import { DB } from "../db/generated/db";

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
      .values([
        { soc_code: "11-1011", title: "Chief Executives" },
        { soc_code: "11-1021", title: "General and Operations Managers" },
        { soc_code: "15-1252", title: "Software Developers" },
        { soc_code: "29-1141", title: "Registered Nurses" },
        { soc_code: "43-4051", title: "Customer Service Representatives" },
        {
          soc_code: "53-3032",
          title: "Heavy and Tractor-Trailer Truck Drivers",
        },
      ])
      .onConflict((oc) => oc.column("soc_code").doNothing())
      .execute();
  }

  async seedNaicsCodes(): Promise<void> {
    // Create a comprehensive NAICS hierarchy
    const naicsData = [
      // Level 2
      {
        naics_code: "11",
        title: "Agriculture, Forestry, Fishing and Hunting",
        level: 2,
        parent_code: null,
      },
      { naics_code: "23", title: "Construction", level: 2, parent_code: null },
      {
        naics_code: "31-33",
        title: "Manufacturing",
        level: 2,
        parent_code: null,
      },
      {
        naics_code: "44-45",
        title: "Retail Trade",
        level: 2,
        parent_code: null,
      },
      { naics_code: "51", title: "Information", level: 2, parent_code: null },
      {
        naics_code: "52",
        title: "Finance and Insurance",
        level: 2,
        parent_code: null,
      },
      {
        naics_code: "54",
        title: "Professional, Scientific, and Technical Services",
        level: 2,
        parent_code: null,
      },
      {
        naics_code: "62",
        title: "Health Care and Social Assistance",
        level: 2,
        parent_code: null,
      },
      {
        naics_code: "72",
        title: "Accommodation and Food Services",
        level: 2,
        parent_code: null,
      },

      // Level 3 examples
      {
        naics_code: "111",
        title: "Crop Production",
        level: 3,
        parent_code: "11",
      },
      {
        naics_code: "112",
        title: "Animal Production",
        level: 3,
        parent_code: "11",
      },
      {
        naics_code: "236",
        title: "Construction of Buildings",
        level: 3,
        parent_code: "23",
      },
      {
        naics_code: "238",
        title: "Specialty Trade Contractors",
        level: 3,
        parent_code: "23",
      },
      {
        naics_code: "334",
        title: "Computer and Electronic Product Manufacturing",
        level: 3,
        parent_code: "31-33",
      },
      {
        naics_code: "511",
        title: "Publishing Industries",
        level: 3,
        parent_code: "51",
      },
      {
        naics_code: "518",
        title: "Data Processing, Hosting, and Related Services",
        level: 3,
        parent_code: "51",
      },
      {
        naics_code: "541",
        title: "Professional, Scientific, and Technical Services",
        level: 3,
        parent_code: "54",
      },
      { naics_code: "622", title: "Hospitals", level: 3, parent_code: "62" },
      {
        naics_code: "722",
        title: "Food Services and Drinking Places",
        level: 3,
        parent_code: "72",
      },

      // Level 4 examples
      {
        naics_code: "1111",
        title: "Oilseed and Grain Farming",
        level: 4,
        parent_code: "111",
      },
      {
        naics_code: "1112",
        title: "Vegetable and Melon Farming",
        level: 4,
        parent_code: "111",
      },
      {
        naics_code: "2361",
        title: "Residential Building Construction",
        level: 4,
        parent_code: "236",
      },
      {
        naics_code: "2362",
        title: "Nonresidential Building Construction",
        level: 4,
        parent_code: "236",
      },
      {
        naics_code: "3341",
        title: "Computer and Peripheral Equipment Manufacturing",
        level: 4,
        parent_code: "334",
      },
      {
        naics_code: "3342",
        title: "Communications Equipment Manufacturing",
        level: 4,
        parent_code: "334",
      },
      {
        naics_code: "5112",
        title: "Software Publishers",
        level: 4,
        parent_code: "511",
      },
      {
        naics_code: "5415",
        title: "Computer Systems Design and Related Services",
        level: 4,
        parent_code: "541",
      },
      {
        naics_code: "6221",
        title: "General Medical and Surgical Hospitals",
        level: 4,
        parent_code: "622",
      },
      {
        naics_code: "7225",
        title: "Restaurants and Other Eating Places",
        level: 4,
        parent_code: "722",
      },

      // Level 5 examples
      {
        naics_code: "11111",
        title: "Soybean Farming",
        level: 5,
        parent_code: "1111",
      },
      {
        naics_code: "11112",
        title: "Oilseed (except Soybean) Farming",
        level: 5,
        parent_code: "1111",
      },
      {
        naics_code: "23611",
        title: "Residential Building Construction",
        level: 5,
        parent_code: "2361",
      },
      {
        naics_code: "23621",
        title: "Industrial Building Construction",
        level: 5,
        parent_code: "2362",
      },
      {
        naics_code: "33411",
        title: "Electronic Computer Manufacturing",
        level: 5,
        parent_code: "3341",
      },
      {
        naics_code: "51121",
        title: "Software Publishers",
        level: 5,
        parent_code: "5112",
      },
      {
        naics_code: "54151",
        title: "Computer Systems Design and Related Services",
        level: 5,
        parent_code: "5415",
      },
      {
        naics_code: "62211",
        title: "General Medical and Surgical Hospitals",
        level: 5,
        parent_code: "6221",
      },
      {
        naics_code: "72251",
        title: "Restaurants and Other Eating Places",
        level: 5,
        parent_code: "7225",
      },

      // Level 6 examples
      {
        naics_code: "111110",
        title: "Soybean Farming",
        level: 6,
        parent_code: "11111",
      },
      {
        naics_code: "111120",
        title: "Oilseed (except Soybean) Farming",
        level: 6,
        parent_code: "11112",
      },
      {
        naics_code: "236115",
        title: "New Single-Family Housing Construction",
        level: 6,
        parent_code: "23611",
      },
      {
        naics_code: "236116",
        title: "New Multifamily Housing Construction",
        level: 6,
        parent_code: "23611",
      },
      {
        naics_code: "334111",
        title: "Electronic Computer Manufacturing",
        level: 6,
        parent_code: "33411",
      },
      {
        naics_code: "511210",
        title: "Software Publishers",
        level: 6,
        parent_code: "51121",
      },
      {
        naics_code: "541511",
        title: "Custom Computer Programming Services",
        level: 6,
        parent_code: "54151",
      },
      {
        naics_code: "541512",
        title: "Computer Systems Design Services",
        level: 6,
        parent_code: "54151",
      },
      {
        naics_code: "622110",
        title: "General Medical and Surgical Hospitals",
        level: 6,
        parent_code: "62211",
      },
      {
        naics_code: "722511",
        title: "Full-Service Restaurants",
        level: 6,
        parent_code: "72251",
      },
    ];

    await this.db
      .insertInto("naics_codes")
      .values(naicsData)
      .onConflict((oc) => oc.column("naics_code").doNothing())
      .execute();
  }

  async seedOewsSeries(): Promise<void> {
    // Create series IDs for various SOC/NAICS combinations
    const seriesData = [
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
      {
        series_id: "OEUN0000000313300011101103",
        soc_code: "11-1011",
        naics_code: "31-33",
        exists: true,
        last_checked: new Date(),
      },
      {
        series_id: "OEUN00000005100011101103",
        soc_code: "15-1252",
        naics_code: "51",
        exists: true,
        last_checked: new Date(),
      },
      {
        series_id: "OEUN00000005400011101103",
        soc_code: "15-1252",
        naics_code: "54",
        exists: true,
        last_checked: new Date(),
      },
      {
        series_id: "OEUN00000006200011101103",
        soc_code: "29-1141",
        naics_code: "62",
        exists: true,
        last_checked: new Date(),
      },
      {
        series_id: "OEUN00000007200011101103",
        soc_code: "43-4051",
        naics_code: "72",
        exists: true,
        last_checked: new Date(),
      },
    ];

    await this.db
      .insertInto("oews_series")
      .values(seriesData)
      .onConflict((oc) => oc.column("series_id").doNothing())
      .execute();
  }

  async seedWages(): Promise<void> {
    const wageData = [
      {
        series_id: "OEUN00000001100011101103",
        year: 2023,
        mean_annual_wage: 150000,
      },
      {
        series_id: "OEUN00000001100011101103",
        year: 2022,
        mean_annual_wage: 145000,
      },
      {
        series_id: "OEUN0000000313300011101103",
        year: 2023,
        mean_annual_wage: 180000,
      },
      {
        series_id: "OEUN00000005100011101103",
        year: 2023,
        mean_annual_wage: 120000,
      },
      {
        series_id: "OEUN00000005400011101103",
        year: 2023,
        mean_annual_wage: 130000,
      },
      {
        series_id: "OEUN00000006200011101103",
        year: 2023,
        mean_annual_wage: 85000,
      },
      {
        series_id: "OEUN00000007200011101103",
        year: 2023,
        mean_annual_wage: 35000,
      },
    ];

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
}
