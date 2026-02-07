import { Kysely, sql } from "kysely";

export async function up(db: Kysely<any>) {
  await db.schema
    .createView("occupation_industry_meaningfulness")
    .orReplace()
    .as(
      sql`
        select
          s.series_id,
          s.occupation_code,
          o.occupation_name,
          s.industry_code,
          i.industry_name,
          s.datatype_code,
          d.datatype_name,
          s.area_code,
          s.state_code,
          a.area_name,
          x.year,
          x.period,
          x.value as wage_value,
          m.score as meaningfulness_score,
          m.reason as meaningfulness_reason,
          m.model as meaningfulness_model,
          m.prompt_version as meaningfulness_prompt_version,
          m.created_at as meaningfulness_scored_at
        from oe_series s
        join (
          select series_id, max(year) as year
          from oe_data
          where period = 'A01'
          group by series_id
        ) latest on latest.series_id = s.series_id
        join oe_data x
          on x.series_id = s.series_id
         and x.year = latest.year
         and x.period = 'A01'
        join oe_occupations o on o.occupation_code = s.occupation_code
        join oe_industries i on i.industry_code = s.industry_code
        join oe_datatypes d on d.datatype_code = s.datatype_code
        join oe_areas a on a.area_code = s.area_code and a.state_code = s.state_code
        left join meaningfulness_scores m
          on m.occupation_code = s.occupation_code
         and m.industry_code = s.industry_code
        where d.datatype_name ilike '%mean%annual%wage%'
      `
    )
    .execute();
}

export async function down(db: Kysely<any>) {
  await db.schema
    .dropView("occupation_industry_meaningfulness")
    .ifExists()
    .execute();
}
