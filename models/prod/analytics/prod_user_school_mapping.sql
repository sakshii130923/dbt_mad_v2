{{ config(materialized='table') }}

-- User School Chapter Mapping: CO-to-converted-partner mapping

WITH active_partners AS (
    SELECT partner_id AS school_id, partner_name AS school_name, co_user_id, co_name
    FROM {{ ref('int_crm__active_partners') }}
)

SELECT 
  school_id, school_name,
  co_user_id AS user_id,
  co_name AS user_name
FROM active_partners
ORDER BY school_name, co_name
