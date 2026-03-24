{{ config(materialized='table') }}

-- Resolves UUID foreign keys for class_section_subject records
-- Flow: stg_bubble__class_section_subject → int_bubble__class_section_subject
-- Joins: class_section (UUID→class_section_id), subject (UUID→subject_id)

with class_section_map as (
    select _id as uuid, class_section_id
    from {{ ref('stg_bubble__class_section') }}
),
subject_map as (
    select _id as uuid, subject_id
    from {{ ref('stg_bubble__subject') }}
)

select
    raw.class_section_subject_id,
    raw.academic_year,
    class_section_map.class_section_id,
    subject_map.subject_id,
    raw.removed,
    raw.created_date,
    raw.modified_date,
    raw._airbyte_raw_id,
    raw._airbyte_extracted_at,
    raw._airbyte_meta
from {{ ref('stg_bubble__class_section_subject') }} raw
left join class_section_map on raw.class_section_id = class_section_map.uuid
left join subject_map on raw.subject_id = subject_map.uuid
