{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'slot_class_section') }}
)
select
    "_id",
    "volunteer_class_section_id_number" as slot_class_section_id,
    "slot_id_custom_slot" as slot_id,
    "class_section_id_custom_class_section" as class_section_id,
    "class_section_subject_id_custom_class_section_subject" as class_section_subject_id,
    "academic_year_text" as academic_year,
    "removed_boolean" as removed,
    "is_active_boolean" as is_active,
    "Created_Date" as created_date,
    "Modified_Date" as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
from source
