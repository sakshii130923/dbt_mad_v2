{{ config(materialized='table') }}

-- Resolves UUID foreign keys for child_removal_log records
-- Flow: stg_bubble__child_removal_log → int_bubble__child_removal_log
-- Joins: child (UUID→child_id), user (UUID→co_id), partner (UUID→school_id)

with child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
user_map as (
    select user_id as uuid, user_id_number as co_id
    from {{ ref('stg_bubble__user') }}
),
partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
)

select
    child_map.child_id,
    user_map.co_id,
    raw.child_removal_log_id,
    raw.other_details,
    raw.removal_reason,
    raw.removed,
    partner_map.school_id,
    raw.created_date,
    raw.modified_date
from {{ ref('stg_bubble__child_removal_log') }} raw
left join child_map on raw.child_id = child_map.uuid
left join user_map on raw.co_id = user_map.uuid
left join partner_map on raw.school_id = partner_map.uuid
