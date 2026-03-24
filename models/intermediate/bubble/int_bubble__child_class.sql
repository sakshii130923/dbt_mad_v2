{{ config(materialized='table') }}

-- Resolves UUID foreign keys for child_class records
-- Flow: stg_bubble__child_class → int_bubble__child_class
-- Joins: child (UUID→child_id), school_class (UUID→school_class_id)

with child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
school_class_map as (
    select _id as uuid, school_class_id
    from {{ ref('stg_bubble__school_class') }}
)

select
    raw.child_class_id,
    raw.academic_year,
    child_map.child_id,
    school_class_map.school_class_id,
    raw.removed as removed_boolean,
    raw.created_date,
    raw.modified_date,
    raw._airbyte_raw_id,
    raw._airbyte_extracted_at,
    raw._airbyte_meta
from {{ ref('stg_bubble__child_class') }} raw
left join child_map on raw.child_id = child_map.uuid
left join school_class_map on raw.school_class_id = school_class_map.uuid
