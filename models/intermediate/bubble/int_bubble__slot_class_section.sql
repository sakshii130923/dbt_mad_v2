{{ config(materialized='table') }}

-- Resolves UUID foreign keys for slot_class_section records
-- Flow: stg_bubble__slot_class_section → int_bubble__slot_class_section
-- Joins: slot, class_section, class_section_subject (UUID→IDs)

with slot_map as (
    select _id as uuid, slot_id
    from {{ ref('stg_bubble__slot') }}
),
class_section_map as (
    select _id as uuid, class_section_id
    from {{ ref('stg_bubble__class_section') }}
),
class_section_subject_map as (
    select _id as uuid, class_section_subject_id
    from {{ ref('stg_bubble__class_section_subject') }}
)

select
    raw.slot_class_section_id,
    slot_map.slot_id,
    class_section_map.class_section_id,
    class_section_subject_map.class_section_subject_id,
    raw.academic_year,
    raw.removed,
    raw.is_active,
    raw.created_date,
    raw.modified_date,
    raw._airbyte_raw_id,
    raw._airbyte_extracted_at,
    raw._airbyte_meta
from {{ ref('stg_bubble__slot_class_section') }} raw
left join slot_map on raw.slot_id = slot_map.uuid
left join class_section_map on raw.class_section_id = class_section_map.uuid
left join class_section_subject_map on raw.class_section_subject_id = class_section_subject_map.uuid
