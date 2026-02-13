import { Type, type Static } from "@sinclair/typebox";

export const OccupationRow = Type.Object(
  {
    occupation_code: Type.String(),
    occupation_name: Type.String(),
    occupation_description: Type.Union([Type.String(), Type.Null()]),
    display_level: Type.Number(),
    selectable: Type.Boolean(),
    sort_sequence: Type.Number(),
  },
  { additionalProperties: false }
);

export const IndustryRow = Type.Object(
  {
    industry_code: Type.String(),
    industry_name: Type.String(),
    display_level: Type.Number(),
    selectable: Type.Boolean(),
    sort_sequence: Type.Number(),
  },
  { additionalProperties: false }
);

export const AreaRow = Type.Object(
  {
    state_code: Type.String(),
    area_code: Type.String(),
    areatype_code: Type.String(),
    area_name: Type.String(),
  },
  { additionalProperties: false }
);

export const AreatypeRow = Type.Object(
  {
    areatype_code: Type.String(),
    areatype_name: Type.String(),
  },
  { additionalProperties: false }
);

export const DatatypeRow = Type.Object(
  {
    datatype_code: Type.String(),
    datatype_name: Type.String(),
  },
  { additionalProperties: false }
);

export const SectorRow = Type.Object(
  {
    sector_code: Type.String(),
    sector_name: Type.String(),
  },
  { additionalProperties: false }
);

export const FootnoteRow = Type.Object(
  {
    footnote_code: Type.String(),
    footnote_text: Type.String(),
  },
  { additionalProperties: false }
);

export const ReleaseRow = Type.Object(
  {
    release_date: Type.String(),
    description: Type.String(),
  },
  { additionalProperties: false }
);

export const SeasonalRow = Type.Object(
  {
    seasonal_code: Type.String(),
    seasonal_text: Type.String(),
  },
  { additionalProperties: false }
);

export const SeriesRow = Type.Object(
  {
    series_id: Type.String(),
    seasonal: Type.String(),
    areatype_code: Type.String(),
    industry_code: Type.String(),
    occupation_code: Type.String(),
    datatype_code: Type.String(),
    state_code: Type.String(),
    area_code: Type.String(),
    sector_code: Type.String(),
    series_title: Type.String(),
    footnote_codes: Type.Union([Type.String(), Type.Null()]),
    begin_year: Type.Number(),
    begin_period: Type.String(),
    end_year: Type.Number(),
    end_period: Type.String(),
  },
  { additionalProperties: false }
);

export const DataRow = Type.Object(
  {
    series_id: Type.String(),
    year: Type.Number(),
    period: Type.String(),
    value: Type.Union([Type.Number(), Type.Null()]),
    footnote_codes: Type.Union([Type.String(), Type.Null()]),
  },
  { additionalProperties: false }
);

export type OccupationRowType = Static<typeof OccupationRow>;
export type IndustryRowType = Static<typeof IndustryRow>;
export type AreaRowType = Static<typeof AreaRow>;
export type AreatypeRowType = Static<typeof AreatypeRow>;
export type DatatypeRowType = Static<typeof DatatypeRow>;
export type SectorRowType = Static<typeof SectorRow>;
export type FootnoteRowType = Static<typeof FootnoteRow>;
export type ReleaseRowType = Static<typeof ReleaseRow>;
export type SeasonalRowType = Static<typeof SeasonalRow>;
export type SeriesRowType = Static<typeof SeriesRow>;
export type DataRowType = Static<typeof DataRow>;
