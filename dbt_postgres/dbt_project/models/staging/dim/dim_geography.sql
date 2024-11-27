{{ config(materialized='table') }}

with source_direct as (
    SELECT *
    FROM {{ source('public', 'country_data') }}
),
geo as (
    select 
        row_number() over() as id,
        dc.country_id,
        dc2.continents,
        dr.region,
        ds.subregion 
    from source_direct cd 
    left join {{ ref('dim_country') }} dc on cd.country_name = dc.country_name
    left join {{ ref('dim_continent') }} dc2 on cd.continents = dc2.continents 
    left join {{ ref('dim_region') }} dr on cd.region = dr.region 
    left join {{ ref('dim_subregion') }} ds on cd.subregion = ds.subregion 
)
select 
    geography_id, 
    country_id,
    coalesce(continents, 'unspecified') as continent,
    coalesce(region, 'unspecified') as region,
    coalesce(subregion, 'unspecified') as subregion
from geo
