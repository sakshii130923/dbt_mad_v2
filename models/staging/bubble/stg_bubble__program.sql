{{ config(materialized='view') }}

with raw_program as (
    select * from {{ source('bubble_raw', 'program') }}
)
select
    "_id",
    "program_id_number" as program_id,
    "program_name_text" as program_name,
    "Created_Date" as created_date,
    "Modified_Date" as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
from raw_program
