{{ config(materialized='table') }}

with source_direct as (
    SELECT *

    FROM {{ source('public', 'country_data') }}
),
subregion as (
    SELECT
    distinct subregion
    FROM source_direct
),
subregion_with_id as (
    SELECT
    ROW_NUMBER() OVER () as subregion_id,
    COALESCE(subregion, 'Unknown') subregion
    FROM subregion
)

SELECT *
FROM subregion_with_id