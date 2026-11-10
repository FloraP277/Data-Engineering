-- Grain: one row per hospital per CMS release, linked to the hospital version
-- that was valid on that release date.
select
    s.facility_id,
    h.hospital_key,
    md5(concat_ws('|', s.state, s.county_parish, s.city_town, s.zip_code)) as location_key,
    to_char(s.snapshot_date, 'YYYYMMDD')::int                              as date_key,
    s.snapshot_date                                                         as release_date,
    s.overall_rating,
    s.overall_rating is not null                                            as rating_available
from {{ ref('stg_hospitals') }} s
left join {{ ref('dim_hospital') }} h
  on h.facility_id = s.facility_id
 and s.snapshot_date >= h.valid_from
 and s.snapshot_date <  h.valid_to
