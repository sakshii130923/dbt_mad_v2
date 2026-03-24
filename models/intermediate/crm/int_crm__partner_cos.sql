{{ config(materialized='table') }}

-- Deduplicated partner to Community Organizer assignments

with source_data as (
    select * from {{ ref('stg_crm__partner_cos') }}
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
    co_id::text,
    created_at,
    updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from deduplicated
