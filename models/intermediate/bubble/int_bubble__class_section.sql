{{ config(materialized='table') }}

-- Resolves UUID foreign keys for class_section records
-- Flow: stg_bubble__class_section → int_bubble__class_section
-- Joins: school_class (UUID→school_class_id), partner (UUID→school_id)

with school_class_map as (
    select _id as uuid, school_class_id
    from {{ ref('stg_bubble__school_class') }}
),
partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
)

select
    raw.class_section_id,
    raw.academic_year,
    raw.section_name,
    raw.removed,
    raw.is_active,
    school_class_map.school_class_id,
    partner_map.school_id,
    raw.created_date,
    raw.modified_date,
    raw._airbyte_raw_id,
    raw._airbyte_extracted_at,
    raw._airbyte_meta
from {{ ref('stg_bubble__class_section') }} raw
left join school_class_map on raw.school_class_id = school_class_map.uuid
left join partner_map on raw.school_id = partner_map.uuid
