{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'mous') }}
)
select
    id,
    partner_id,
    mou_url,
    mou_sign,
    mou_status,
    mou_start_date,
    mou_end_date,
    mou_sign_date,
    pending_mou_reason,
    confirmed_child_count,
    "createdAt" as created_at,
    "updatedAt" as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from source
