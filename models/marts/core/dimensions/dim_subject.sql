{{ config(materialized='table') }}

-- dim_subject: One row per subject
-- Source: int_bubble__subject (resolves program UUID→ID)

select
    subject_id,
    subject_name,
    program_id,
    removed as is_removed,
    created_date,
    modified_date
from {{ ref('int_bubble__subject') }}
where removed = false
