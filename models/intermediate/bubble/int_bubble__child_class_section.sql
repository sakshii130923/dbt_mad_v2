{{ config(materialized='table') }}

-- Resolves UUID foreign keys for child_class_section records
-- Flow: stg_bubble__child_class_section → int_bubble__child_class_section
-- Joins: child (UUID→child_id), class_section (UUID→class_section_id)

with child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
class_section_map as (
    select _id as uuid, class_section_id
    from {{ ref('stg_bubble__class_section') }}
)

select
    raw.child_class_section_id,
    raw.academic_year,
    child_map.child_id,
    class_section_map.class_section_id,
    raw.removed as removed_boolean,
    raw.created_date,
    raw.modified_date,
    raw._airbyte_raw_id,
    raw._airbyte_extracted_at,
    raw._airbyte_meta
from {{ ref('stg_bubble__child_class_section') }} raw
left join child_map on raw.child_id = child_map.uuid
left join class_section_map on raw.class_section_id = class_section_map.uuid
