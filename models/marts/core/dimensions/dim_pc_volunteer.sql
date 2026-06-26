{{ config(materialized='table') }}

with volunteers as (
    select distinct
        volunteer_key,
        tagged_volunteer_id as volunteer_id
    from {{ ref('int_pc_volunteer_attendance') }}
),

user_data as (
    select
        "UserId" as user_id,
        "UserDisplayName" as volunteer_name,
        "Email" as volunteer_email,
        "UserRole" as volunteer_role
    from {{ ref('int_pc_user_data') }}
)

select
    v.volunteer_key,
    v.volunteer_id,
    u.volunteer_name,
    u.volunteer_email,
    u.volunteer_role
from volunteers v
left join user_data u on v.volunteer_id = u.user_id
