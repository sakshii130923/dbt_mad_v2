{{ config(materialized='table') }}

-- Deduplicated MOU records between organization and partners

with source_data as (
    select * from {{ ref('stg_crm__mous') }}
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
    mou_url,
    mou_sign,
    mou_status,
    mou_start_date,
    mou_end_date,
    mou_sign_date,
    pending_mou_reason,
    confirmed_child_count,
    created_at,
    updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from deduplicated
