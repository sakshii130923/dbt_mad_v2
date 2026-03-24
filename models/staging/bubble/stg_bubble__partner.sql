{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'partner') }}
)
select
    "_id" as partner_id,
    "city_text" as city,
    "Created_By" as created_by,
    "co_id_user",
    "state_text" as state,
    "Created_Date" as created_date,
    "co_name_text" as co_name,
    "mou_url_text" as mou_url,
    "Modified_Date" as modified_date,
    "poc_name_text" as poc_name,
    "city_id_number" as city_id,
    "pincode_number" as pincode,
    "poc_email_text" as poc_email,
    "state_id_number" as state_id,
    "lead_source_text" as lead_source,
    "school_type_text" as school_type,
    "classes_list_text" as classes_list,
    "mou_end_date_date" as mou_end_date,
    "partner_name_text" as partner_name,
    "mou_sign_date_date" as mou_sign_date,
    "partner_id1_number" as partner_id1,
    "poc_contact_number" as poc_contact,
    "address_line_1_text" as address_line_1,
    "address_line_2_text" as address_line_2,
    "mou_start_date_date" as mou_start_date,
    "poc_designation_text" as poc_designation,
    "total_child_count_number" as total_child_count,
    "date_of_first_contact_date" as date_of_first_contact,
    "low_income_resource_boolean" as low_income_resource,
    "confirmed_child_count_number" as confirmed_child_count,
    "partner_affiliation_type_text" as partner_affiliation_type,
    "removed1_boolean" as removed,
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
from source
