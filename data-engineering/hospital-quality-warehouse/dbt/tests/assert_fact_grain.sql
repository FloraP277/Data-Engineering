-- One fact row per hospital per release.
select facility_id, release_date, count(*)
from {{ ref('fct_hospital_ratings') }}
group by facility_id, release_date
having count(*) > 1
