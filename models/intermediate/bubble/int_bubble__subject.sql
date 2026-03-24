{{ config(materialized='table') }}

-- Resolves UUID foreign keys for subject records
-- Flow: stg_bubble__subject → int_bubble__subject
-- Joins: program (UUID→program_id)

with program_map as (
    select _id as uuid, program_id
    from {{ ref('stg_bubble__program') }}
)

select
    raw.subject_id,
    raw.subject_name,
    raw.removed,
    program_map.program_id,
    raw.created_date,
    raw.modified_date,
    raw._airbyte_raw_id,
    raw._airbyte_extracted_at,
    raw._airbyte_meta
from {{ ref('stg_bubble__subject') }} raw
left join program_map on raw.program_id = program_map.uuid
