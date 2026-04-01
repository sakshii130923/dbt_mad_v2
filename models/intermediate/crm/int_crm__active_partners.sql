{{ config(materialized='table') }}

-- Active partners: CRM partners with latest converted agreement + latest CO assignment
-- Eliminates repetitive CTE patterns across multiple analytics models
-- Used by: prod_class_ops_data, prod_volunteer_recruitment, prod_school_data_comparison, etc.

with latest_partner_cos as (
    select
        partner_id,
        co_id as co_user_id
    from (
        select
            partner_id,
            co_id,
            row_number() over (
                partition by partner_id 
                order by updated_at desc, created_at desc, partner_co_id desc
            ) as rn
        from {{ ref('bridge_partner_co') }}
    ) ranked
    where rn = 1
),

latest_agreements as (
    select
        partner_id,
        conversion_stage,
        created_at
    from (
        select
            partner_id,
            conversion_stage,
            created_at,
            row_number() over (
                partition by partner_id 
                order by created_at desc, agreement_id desc
            ) as rn
        from {{ ref('fct_partner_agreements') }}
    ) ranked
    where rn = 1
),

bubble_users as (
    select user_id_number, user_display_name
    from (
        select user_id_number, user_display_name,
            row_number() over (
                partition by user_id_number 
                order by modified_date desc, created_date desc
            ) as rn
        from {{ ref('stg_bubble__user') }}
        where user_id_number is not null
    ) ranked
    where rn = 1
)

select 
    p.crm_partner_id as partner_id,
    p.partner_name,
    pco.co_user_id,
    bu.user_id_number as co_id_numeric,
    bu.user_display_name as co_name,
    la.conversion_stage,
    la.created_at as agreement_created_at
from {{ ref('dim_crm_partner') }} p
left join latest_partner_cos pco
    on p.crm_partner_id = pco.partner_id
join latest_agreements la
    on p.crm_partner_id = la.partner_id
    and la.conversion_stage = 'converted'
left join bubble_users bu
    on pco.co_user_id::integer = bu.user_id_number
where p.is_removed = false
