-- The single release being snapshotted: the snapshot_date var, or the latest release.
select *
from {{ ref('stg_hospitals') }}
where snapshot_date = {% if var('snapshot_date') %}'{{ var("snapshot_date") }}'::date
                      {% else %}(select max(snapshot_date) from {{ ref('stg_hospitals') }}){% endif %}
