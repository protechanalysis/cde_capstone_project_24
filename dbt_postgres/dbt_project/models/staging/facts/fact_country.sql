{{ config(materialized='table') }}

with source_direct as (
    SELECT *

    FROM {{ source('public', 'country_data') }}
),
fact as (
    SELECT
    country_id,
    area,
    population
    FROM source_direct sd
    join {{ ref('dim_country') }} c
        on sd.country_name = c.country_name
    where area is not null and population is not null
)

SELECT *
FROM fact
