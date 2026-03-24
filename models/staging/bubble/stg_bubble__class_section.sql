{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'class_section') }}
)
select
    "_id",
    "class_section_id_number" as class_section_id,
    "academic_year_text" as academic_year,
    "section_name_text" as section_name,
    "removed_boolean" as removed,
    "is_active_boolean" as is_active,
    "school_class_id1_custom_school_class" as school_class_id,
    "school_id_custom_partner" as school_id,
    "Created_Date" as created_date,
    "Modified_Date" as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
from source
