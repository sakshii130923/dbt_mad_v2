{{ config(materialized='table') }}

-- Deduplicated city master data linked to states

with source_data as (
    select * from {{ ref('stg_crm__cities') }}
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
    state_id::text,
    city_name,
    created_at,
    updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from deduplicated
