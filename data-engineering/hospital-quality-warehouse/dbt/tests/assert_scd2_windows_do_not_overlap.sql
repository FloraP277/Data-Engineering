-- Each hospital's versions must not overlap in time. Returns offending rows.
select a.facility_id, a.hospital_key, b.hospital_key as overlapping_key
from {{ ref('dim_hospital') }} a
join {{ ref('dim_hospital') }} b
  on a.facility_id = b.facility_id
 and a.hospital_key <> b.hospital_key
 and a.valid_from < b.valid_to
 and b.valid_from < a.valid_to
