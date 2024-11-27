-- This model creates the dim_country_languages table by extracting and joining
-- country-language data from the country_data table with the dim_languages table.

{{ config(
    materialized='table'  -- Change this to 'view' or 'incremental' as needed
) }}

with lan_cur as (
    -- Generate unique country IDs and unnest the languages
    select 
        row_number() over () as country_id,
        country_name,
        unnest(string_to_array(cd.languages, ',')) as language
    from {{ source('public', 'country_data') }} 
),

con as (
    -- Join the extracted languages with the languages dimension table
    select 
        c.country_id,
        ld.language
    from lan_cur as c
    join {{ ref('dim_languages') }} ld
        on c.language = ld.language
)

-- Final select statement to create the dim_country_languages table
select *
from con
