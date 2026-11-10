{% snapshot hospitals_snapshot %}
{#
  SCD Type 2: a new version of a hospital is recorded whenever any check column
  changes. updated_at = the CMS release date, so validity starts on the release
  that showed the change, even when old releases are loaded later.
#}
{{ config(
    target_schema='snapshots',
    unique_key='facility_id',
    strategy='check',
    check_cols=['facility_name', 'hospital_type', 'hospital_ownership', 'overall_rating', 'has_emergency'],
    updated_at='snapshot_date',
    invalidate_hard_deletes=True
) }}
select facility_id, facility_name, hospital_type, hospital_ownership, overall_rating, has_emergency,
       state, city_town, county_parish, zip_code, snapshot_date::timestamp as snapshot_date
from {{ ref('stg_hospitals_release') }}
{% endsnapshot %}
