{{ config(materialized='table') }}

-- Deduplicated partner agreement pipeline and conversion tracking

with source_data as (
    select * from {{ ref('stg_crm__partner_agreements') }}
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
    current_status,
    conversion_stage,
    specific_doc_name,
    specific_doc_required,
    agreement_drop_date,
    non_conversion_reason,
    potential_child_count,
    expected_conversion_day,
    removed,
    created_at,
    updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from deduplicated
