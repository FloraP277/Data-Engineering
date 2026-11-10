-- One clean row per hospital per CMS release.
with src as (
    select * from {{ source('raw', 'hospital_general_information') }}
)
select
    facility_id,
    facility_name,
    city_town,
    upper(state)                                       as state,
    left(zip_code, 5)                                  as zip_code,
    county_parish,
    hospital_type,
    coalesce(hospital_ownership, 'Unknown')            as hospital_ownership,
    case lower(emergency_services) when 'yes' then true when 'no' then false end as has_emergency,
    case when hospital_overall_rating ~ '^[1-5]$'
         then hospital_overall_rating::int end         as overall_rating,   -- "Not Available" -> NULL
    snapshot_date
from src
where facility_id is not null
