-- Business question: which states had the most hospitals gain or lose a star between releases?
with ordered as (
    select f.facility_id, l.state, f.release_date, f.overall_rating,
           lag(f.overall_rating) over (partition by f.facility_id order by f.release_date) as prev_rating,
           lag(f.release_date)   over (partition by f.facility_id order by f.release_date) as prev_release
    from {{ ref('fct_hospital_ratings') }} f
    join {{ ref('dim_location') }} l using (location_key)
)
select
    state,
    prev_release,
    release_date,
    count(*) filter (where overall_rating > prev_rating) as hospitals_gained_star,
    count(*) filter (where overall_rating < prev_rating) as hospitals_lost_star,
    count(*) filter (where overall_rating = prev_rating) as hospitals_unchanged,
    count(*)                                              as hospitals_compared
from ordered
where prev_rating is not null and overall_rating is not null
group by state, prev_release, release_date
order by release_date, hospitals_lost_star desc
