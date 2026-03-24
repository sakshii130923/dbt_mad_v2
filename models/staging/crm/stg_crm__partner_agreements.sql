{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'partner_agreements') }}
)
select
    id,
    partner_id,
    current_status,
    conversion_stage,
    specific_doc_name,
    specific_doc_required,
    agreement_drop_date,
    non_conversion_reason,
    potential_child_count,
    expected_conversion_day,
    removed,
    "createdAt" as created_at,
    "updatedAt" as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from source
