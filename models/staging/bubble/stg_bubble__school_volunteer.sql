{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'school_volunteer') }}
)
select
    "school_volunteer_id_number" as school_volunteer_id,
    "academic_year_text" as academic_year,
    "school_id_custom_partner" as school_id,
    "volunteer_id_user" as volunteer_id,
    "removed_boolean" as removed,
    "Created_Date" as created_date,
    "Modified_Date" as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
from source
