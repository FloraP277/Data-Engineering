select distinct
    md5(concat_ws('|', state, county_parish, city_town, zip_code)) as location_key,
    state, county_parish, city_town, zip_code
from {{ ref('stg_hospitals') }}
