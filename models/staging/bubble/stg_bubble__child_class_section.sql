{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'child_class_section') }}
)
select
    "child_class_section_id_number" as child_class_section_id,
    "academic_year_text" as academic_year,
    "child_id_custom_child" as child_id,
    "class_section_id_custom_class_section" as class_section_id,
    "removed_boolean" as removed,
    "Created_Date" as created_date,
    "Modified_Date" as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
from source
