export const OE_OCCUPATIONS = [
  {
    occupation_code: "111011",
    occupation_name: "Chief Executives",
    occupation_description: null,
    display_level: 3,
    selectable: true,
    sort_sequence: 1,
  },
  {
    occupation_code: "151252",
    occupation_name: "Software Developers",
    occupation_description: null,
    display_level: 3,
    selectable: true,
    sort_sequence: 2,
  },
] as const;

export const OE_INDUSTRIES = [
  {
    industry_code: "111110",
    industry_name: "Soybean Farming",
    display_level: 6,
    selectable: true,
    sort_sequence: 1,
  },
  {
    industry_code: "622110",
    industry_name: "General Medical and Surgical Hospitals",
    display_level: 6,
    selectable: true,
    sort_sequence: 2,
  },
] as const;

export const OE_AREATYPES = [
  { areatype_code: "N", areatype_name: "National" },
] as const;

export const OE_AREAS = [
  {
    state_code: "00",
    area_code: "0000000",
    areatype_code: "N",
    area_name: "National",
  },
] as const;

export const OE_DATATYPES = [
  { datatype_code: "01", datatype_name: "Employment" },
  { datatype_code: "15", datatype_name: "Mean Annual Wage" },
] as const;

export const OE_SECTORS = [
  { sector_code: "000000", sector_name: "All industries" },
] as const;
