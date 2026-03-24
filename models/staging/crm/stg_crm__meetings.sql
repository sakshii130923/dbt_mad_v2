{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'meetings') }}
)
select
    id,
    partner_id,
    poc_id,
    user_id,
    meeting_date,
    follow_up_meeting_date,
    follow_up_meeting_scheduled,
    "createdAt" as created_at,
    "updatedAt" as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
from source
