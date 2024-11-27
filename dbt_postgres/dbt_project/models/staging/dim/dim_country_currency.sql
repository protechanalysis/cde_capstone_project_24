{{ config(
    materialized='table' ) }}

with con_cur as (
    -- Generate unique country IDs and unnest the currency names
    select 
        row_number() over () as country_id,
        country_name,
        unnest(string_to_array(currency_name, ', ')) as currency_name 
    from {{ source('public', 'country_data') }} cd
),

cur as (
    -- Join the extracted currencies with the currency dimension table
    select 
        c.country_id,
        cd.currency_name
    from con_cur as c
    join {{ ref('dim_currency') }} cd
        on c.currency_name = cd.currency_name
)

-- Final select statement to output the results
select *
from cur
