{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'pocs') }}
)
select
    id,
    partner_id,
    poc_name,
    poc_email,
    poc_contact,
    poc_designation,
    date_of_first_contact,
    removed,
    "createdAt" as created_at,
    "updatedAt" as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from source
