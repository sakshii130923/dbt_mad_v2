{{ config(materialized='table') }}

-- State master data for geographic organization (no dedup needed, simple passthrough)

select
    id::text,
    state_name,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from {{ ref('stg_crm__states') }}
