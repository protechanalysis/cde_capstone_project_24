{{ config(materialized='table') }}

with source_direct as (
    SELECT *

    FROM {{ source('public', 'country_data') }}
),
region as (
    SELECT
    distinct region
    FROM source_direct
),
region_with_id as (
    SELECT
    ROW_NUMBER() OVER () as region_id,
    COALESCE(region, 'Unknown') region
    FROM region
)

SELECT *
FROM region_with_id