{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'cities') }}
)
select
    id,
    state_id,
    city_name,
    "createdAt" as created_at,
    "updatedAt" as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from source
