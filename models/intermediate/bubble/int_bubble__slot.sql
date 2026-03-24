{{ config(materialized='table') }}

-- Resolves UUID foreign keys for slot records
-- Flow: stg_bubble__slot → int_bubble__slot
-- Joins: partner (UUID→school_id)

with partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
)

select
    raw.slot_id,
    raw.slot_name,
    raw.academic_year,
    raw.day_of_week,
    raw.start_time,
    raw.end_time,
    raw.reccuring,
    partner_map.school_id,
    raw.removed,
    raw.created_date,
    raw.modified_date,
    raw._airbyte_raw_id,
    raw._airbyte_extracted_at,
    raw._airbyte_meta
from {{ ref('stg_bubble__slot') }} raw
left join partner_map on raw.school_id = partner_map.uuid
