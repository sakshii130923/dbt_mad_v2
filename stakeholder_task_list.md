# Task List: Dimensional Model Migration (Kimball Star Schema)

Based on the [Dimensional Model Proposal](../dbt_mad/DIMENSIONAL_MODEL_PROPOSAL.md), the following task list outlines the steps to build and migrate the analytical database structure within the `dbt_mad_new` directory. This document is intended for project tracking and stakeholder visibility, structured by source domain.

## Phase 1: Preparation & Architecture Setup
- [x] **Create New Folder Structure:**
  - Create `models/staging` for 1:1 source mirrors (views).
  - Create `models/marts/core` with subfolders for `dimensions/`, `facts/`, and `bridges/`.
- [x] **Configure Project Materializations:** Update `dbt_project.yml` to set `staging` as `views` and `marts` as `tables`.
- [ ] **(Optional)** Update `macros/generate_schema_name.sql` if custom schema routing is required.

## Phase 2: Bubble Source Migration
- **Staging Layer:**
  - [ ] **Bubble Integration:** Create ~22 `stg_` models + source/model YAML configs in `models/staging/bubble/`.
  - [ ] Migrate data freshness configurations for Bubble tables.
- **Intermediate Layer:**
  - [ ] *No structural changes required; preserve existing logic.*
- **Prod / Marts Layer:**
  - [ ] Build dimension: **`dim_bubble_partner`**
  - [ ] Build dimension: **`dim_child`** 
  - [ ] Build operational dimensions: **`dim_class_section`**, **`dim_subject`**, **`dim_slot`**
  - [ ] Build fact: **`fct_child_attendance`** (tracking granular child/session attendance)
  - [ ] Build fact: **`fct_school_volunteer`**
  - [ ] Build fact: **`fct_volunteer_slot_assignment`**
  - [ ] Build bridge: **`bridge_child_class_section`**
  - [ ] Build bridge: **`bridge_child_subject`**

## Phase 3: CRM Source Migration
- **Staging Layer:**
  - [ ] **CRM Integration:** Create ~10 `stg_` models + source/model YAML configs in `models/staging/crm/`.
  - [ ] Migrate data freshness configurations for CRM tables.
- **Intermediate Layer:**
  - [ ] *No structural changes required; preserve existing logic.*
- **Prod / Marts Layer:**
  - [ ] Build dimension: **`dim_crm_partner`**
  - [ ] Build dimension: **`dim_campaign`**
  - [ ] Build fact: **`fct_donations`** (consolidating donation and tip amounts)
  - [ ] Build fact: **`fct_meetings`** (CRM meeting tracking)

## Phase 4: Platform Commons (PC) Source Migration
- **Staging Layer:**
  - [ ] **Platform Commons Integration:** Create ~13 `stg_` models + source/model YAML configs in `models/staging/platform_commons/`.
  - [ ] Migrate data freshness configurations for PC tables.
- **Intermediate Layer:**
  - [ ] *No structural changes required; preserve existing logic.*
- **Prod / Marts Layer:**
  - [ ] Build dimension: **`dim_user`** (volunteers/staff deduplicated)
  - [ ] Build programmatic dimensions: **`dim_program`**, **`dim_chapter`**
  - [ ] Build dimension: **`dim_mou`**
  - [ ] Build fact: **`fct_applicant`** (UNION logic across 2023/2024/2025 apps)
  - [ ] Build fact: **`fct_events`**
  - [ ] Build fact: **`fct_credits`**
  - [ ] Build bridge: **`bridge_partner_co`**

## Phase 5: Common Shared Dimensions
- **Prod / Marts Layer:**
  - [ ] Build dimension: **`dim_date`** (using `dbt_utils.date_spine` to generate calendar base (2020-2030) + academic year logic).

## Phase 6: Testing & Documentation Coverage
- [ ] **Primary Key Checks:** Add `unique` and `not_null` tests strictly on all 23 new tables.
- [ ] **Foreign Key Checks:** Add `relationships` tests validating dimension linkage in Fact/Bridge tables.
- [ ] **Value Checks:** Add `accepted_values` on statuses (attendance/application) & expectations on financials.
- [ ] **Documentation:** Provide column-level descriptions for all dimension attributes.

## Phase 7: Validation & Production Readiness
- [ ] **Pipeline Execution:** Ensure `dbt run` and `dbt test` pass completely.
- [ ] **Data Validation:** Spot-check row counts across facts vs intermediate models.
- [ ] **Regression Testing:** Ensure all untouched components (47 intermediate models & 12 existing production analytics models) still compile and run properly.
- [ ] Stakeholder review and analytics transition sign-off.
