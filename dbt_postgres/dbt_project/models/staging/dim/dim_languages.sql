{{ config(materialized='table') }}

with source_direct as (
    SELECT *
    FROM {{ source('public', 'country_data') }}
),

languages as (
    SELECT
    ROW_NUMBER() OVER () AS languages_id, -- Generate unique IDs
    languages
    FROM source_direct
)

SELECT *
FROM languages