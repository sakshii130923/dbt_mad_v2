{{ config(materialized='table') }}

-- dim_crm_partner: One row per CRM partner/school
-- Flow: int_crm__partners + int_crm__cities + int_crm__states → dim_crm_partner

select
    p.id as crm_partner_id,
    p.partner_name,
    c.city_name as city,
    s.state_name as state,
    p.pincode,
    p.school_type,
    p.lead_source,
    p.interested,
    p.partner_affiliation_type,
    p.address_line_1,
    p.address_line_2,
    p.total_child_count,
    p.low_income_resource,
    p.classes,
    p.removed as is_removed,
    p.created_at,
    p.updated_at
from {{ ref('int_crm__partners') }} p
left join {{ ref('int_crm__cities') }} c on p.city_id = c.id
left join {{ ref('int_crm__states') }} s on p.state_id = s.id
