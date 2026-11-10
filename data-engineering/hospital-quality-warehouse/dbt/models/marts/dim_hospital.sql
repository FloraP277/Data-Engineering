-- One row per version of each hospital (SCD Type 2).
select
    dbt_scd_id                                     as hospital_key,
    facility_id,
    facility_name,
    hospital_type,
    hospital_ownership,
    has_emergency,
    overall_rating,
    dbt_valid_from::date                           as valid_from,
    coalesce(dbt_valid_to::date, '9999-12-31')     as valid_to,
    dbt_valid_to is null                           as is_current
from {{ ref('hospitals_snapshot') }}
