{{ config(materialized='table') }}

with source_direct as (
    SELECT *
    FROM {{ source('public', 'country_data') }}
),

country_id_null as (
    SELECT
    ROW_NUMBER() OVER () AS country_id, -- Generate unique IDs
    country_name, 
    COALESCE(official_country_name, 'Unknown') official_country_name,
    COALESCE(common_native_name, 'Unknown') common_native_name, 
    COALESCE(capital, 'Unknown') capital
    FROM source_direct
),
remove_comma as (
    SELECT country_id, country_name, official_country_name,
    common_native_name, REPLACE(capital, '"', '') capital
    FROM country_id_null
)

SELECT *
FROM remove_comma