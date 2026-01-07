import { Type, type Static } from "@sinclair/typebox";

export const Occupation = Type.Object(
  {
    code: Type.String(),
    text: Type.String(),
  },
  { additionalProperties: false }
);

export const SocResponse = Type.Object(
  {
    occupations: Type.Array(Occupation),
  },
  { additionalProperties: false }
);

export const Industry = Type.Object(
  {
    code: Type.String(),
    text: Type.String(),
  },
  { additionalProperties: false }
);

export const NaicsResponse = Type.Object(
  {
    industries: Type.Array(Industry),
  },
  { additionalProperties: false }
);

export const TimeseriesDataPoint = Type.Object(
  {
    year: Type.String(),
    period: Type.String(),
    periodName: Type.String(),
    value: Type.String(),
    footnotes: Type.Array(
      Type.Object(
        {
          code: Type.String(),
          text: Type.String(),
        },
        { additionalProperties: false }
      )
    ),
  },
  { additionalProperties: false }
);

export const TimeseriesSeries = Type.Object(
  {
    seriesID: Type.String(),
    data: Type.Array(TimeseriesDataPoint),
  },
  { additionalProperties: false }
);

export const TimeseriesResults = Type.Object(
  {
    series: Type.Array(TimeseriesSeries),
  },
  { additionalProperties: false }
);

export const TimeseriesResponse = Type.Object(
  {
    status: Type.Union([
      Type.Literal("REQUEST_SUCCEEDED"),
      Type.Literal("REQUEST_FAILED"),
    ]),
    responseTime: Type.Number(),
    message: Type.Array(Type.String()),
    Results: TimeseriesResults,
  },
  { additionalProperties: false }
);

export type OccupationType = Static<typeof Occupation>;
export type SocResponseType = Static<typeof SocResponse>;
export type IndustryType = Static<typeof Industry>;
export type NaicsResponseType = Static<typeof NaicsResponse>;
export type TimeseriesDataPointType = Static<typeof TimeseriesDataPoint>;
export type TimeseriesSeriesType = Static<typeof TimeseriesSeries>;
export type TimeseriesResultsType = Static<typeof TimeseriesResults>;
export type TimeseriesResponseType = Static<typeof TimeseriesResponse>;
