select distinct
    to_char(snapshot_date, 'YYYYMMDD')::int as date_key,
    snapshot_date                            as release_date,
    extract(year from snapshot_date)::int    as year,
    extract(quarter from snapshot_date)::int as quarter
from {{ ref('stg_hospitals') }}
