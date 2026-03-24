{{ config(materialized='view') }}

with raw_batch_child as (
    select * from {{ source('bubble_raw', 'batch_child') }}
)
select
    "batch_child_id_number" as batch_child_id,
    "academic_year_text" as academic_year,
    "child_id_custom_child" as child_id,
    "school_id_custom_partner" as school_id,
    "removed_boolean" as removed,
    "Created_Date" as created_date,
    "Modified_Date" as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
from raw_batch_child
