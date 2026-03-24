{{ config(materialized='table') }}

-- Deduplicated Points of Contact at partner organizations

with source_data as (
    select * from {{ ref('stg_crm__pocs') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='id',
        order_by='updated_at desc',
       )
    }}
)

select
    id::text,
    partner_id::text,
    poc_name,
    poc_email,
    poc_contact,
    poc_designation,
    date_of_first_contact,
    removed,
    created_at,
    updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from deduplicated
