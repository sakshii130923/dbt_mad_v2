{{ config(materialized='table') }}

-- Resolves UUID foreign keys for slot_class_section_volunteer records
-- Flow: stg_bubble__slot_class_section_volunteer → int_bubble__slot_class_section_volunteer
-- Joins: slot_class_section (UUID→slot_class_section_id), user (UUID→volunteer_id)

with slot_class_section_map as (
    select _id as uuid, slot_class_section_id
    from {{ ref('stg_bubble__slot_class_section') }}
),
user_map as (
    select user_id as uuid, user_id_number as volunteer_id
    from {{ ref('stg_bubble__user') }}
)

select
    raw.slot_class_section_volunteer_id,
    slot_class_section_map.slot_class_section_id,
    user_map.volunteer_id,
    raw.academic_year,
    raw.removed,
    raw.created_date,
    raw.modified_date,
    raw._airbyte_raw_id,
    raw._airbyte_extracted_at,
    raw._airbyte_meta
from {{ ref('stg_bubble__slot_class_section_volunteer') }} raw
left join slot_class_section_map on raw.slot_class_section_id = slot_class_section_map.uuid
left join user_map on raw.volunteer_id = user_map.uuid
