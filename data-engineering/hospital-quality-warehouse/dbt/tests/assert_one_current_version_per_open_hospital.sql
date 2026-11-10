-- A hospital in the latest release must have exactly one current version.
with latest as (
    select facility_id from {{ ref('stg_hospitals') }}
    where snapshot_date = (select max(snapshot_date) from {{ ref('stg_hospitals') }})
)
select l.facility_id, count(h.hospital_key) as current_versions
from latest l
left join {{ ref('dim_hospital') }} h on h.facility_id = l.facility_id and h.is_current
group by l.facility_id
having count(h.hospital_key) <> 1
