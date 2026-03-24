{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'poc_partners') }}
)
select
    id,
    poc_id,
    partner_id,
    "createdAt" as created_at,
    "updatedAt" as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from source
