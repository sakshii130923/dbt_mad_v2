{{ config(materialized='table') }}

-- fct_school_volunteer: One row per volunteer assigned to one school
-- Source: int_bubble__school_volunteer

select
    school_volunteer_id,
    school_id,
    volunteer_id,
    academic_year,
    removed as is_removed,
    created_date,
    modified_date
from {{ ref('int_bubble__school_volunteer') }}
