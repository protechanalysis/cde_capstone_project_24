{{ config(materialized='table') }}

with source_direct as (
    SELECT *
    FROM {{ source('public', 'country_data') }}
),

currency as (
    SELECT
    ROW_NUMBER() OVER () AS currency_id, -- Generate unique IDs
    currency_name, currency_code, currency_symbol
    FROM source_direct
)

SELECT *
FROM currency