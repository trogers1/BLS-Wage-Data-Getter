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

export type OccupationType = Static<typeof Occupation>;
export type SocResponseType = Static<typeof SocResponse>;
export type IndustryType = Static<typeof Industry>;
export type NaicsResponseType = Static<typeof NaicsResponse>;
