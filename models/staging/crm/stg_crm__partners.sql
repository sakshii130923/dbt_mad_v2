{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'partners') }}
)
select
    id,
    partner_name,
    city_id,
    state_id,
    pincode,
    removed,
    interested,
    lead_source,
    school_type,
    partner_affiliation_type,
    address_line_1,
    address_line_2,
    total_child_count,
    low_income_resource,
    classes,
    created_by,
    "createdAt" as created_at,
    "updatedAt" as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from source
