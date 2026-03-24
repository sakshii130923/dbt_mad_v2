{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'partner_cos') }}
)
select
    id,
    partner_id,
    co_id,
    "createdAt" as created_at,
    "updatedAt" as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from source
