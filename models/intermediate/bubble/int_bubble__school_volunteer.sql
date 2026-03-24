{{ config(materialized='table') }}

-- Resolves UUID foreign keys for school_volunteer records
-- Flow: stg_bubble__school_volunteer → int_bubble__school_volunteer
-- Joins: partner (UUID→school_id), user (UUID→volunteer_id)

with partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
user_map as (
    select user_id as uuid, user_id_number as volunteer_id
    from {{ ref('stg_bubble__user') }}
)

select
    raw.school_volunteer_id,
    raw.academic_year,
    partner_map.school_id,
    user_map.volunteer_id,
    raw.removed,
    raw.created_date,
    raw.modified_date,
    raw._airbyte_raw_id,
    raw._airbyte_extracted_at,
    raw._airbyte_meta
from {{ ref('stg_bubble__school_volunteer') }} raw
left join partner_map on raw.school_id = partner_map.uuid
left join user_map on raw.volunteer_id = user_map.uuid
