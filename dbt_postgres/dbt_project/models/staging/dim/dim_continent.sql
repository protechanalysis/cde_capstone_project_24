{{ config(materialized='table') }}

with source_direct as (
    SELECT *

    FROM {{ source('public', 'country_data') }}
),
continent as (
    SELECT
    distinct NULLIF(TRIM(continents), '') AS continents

    FROM source_direct
),
continent_with_id as (
    SELECT
    ROW_NUMBER() OVER () as continent_id,
    COALESCE(continents, 'Unspecified') continents
    FROM continent
)

SELECT *
FROM continent_with_id