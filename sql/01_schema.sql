-- ============================================================================
-- 01_schema.sql — Hospital Patient Experience Dashboard
-- ----------------------------------------------------------------------------
-- Creates raw / core / marts tables and the ingest_log.
-- Idempotent: every CREATE uses IF NOT EXISTS, safe to re-run.
--
-- Run as hospital_app so that the tables are owned by the role the pipeline
-- connects with and the ALTER DEFAULT PRIVILEGES from 00_init.sql apply:
--
--   PGPASSWORD=$PGPASSWORD psql -U hospital_app -d hospital_dashboard \
--       -f sql/01_schema.sql
--
-- All columns in `raw.*` are TEXT to survive CMS API schema drift. Casting
-- and validation happen in the core/marts layer (Phase 4), not in DDL.
-- ============================================================================

\set ON_ERROR_STOP on

SET search_path = public;

-- ============================================================================
-- OPERATIONAL: raw.ingest_log
-- ----------------------------------------------------------------------------
-- One row per ingest run, written by the Python pipeline. Used to answer
-- "when did we last successfully refresh dataset X" and to diagnose failures.
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.ingest_log (
    id              bigserial PRIMARY KEY,
    dataset_id      text        NOT NULL,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    rows_inserted   integer,
    rows_updated    integer,
    status          text,
    error_message   text
);

COMMENT ON TABLE raw.ingest_log IS
    'Audit trail for CMS DKAN ingest runs. One row per (dataset_id, run). '
    'Written by src/pipeline; not populated by DDL. Status is free-text but '
    'the pipeline writes one of: running, ok, failed.';

CREATE INDEX IF NOT EXISTS ingest_log_dataset_started_idx
    ON raw.ingest_log (dataset_id, started_at DESC);


-- ============================================================================
-- raw.hospital_general_info  (CMS xubh-q36u, ~5,426 rows)
-- ----------------------------------------------------------------------------
-- One row per Medicare-certified hospital. Acts as the eventual source for
-- core.hospitals. PK is facility_id (CMS CCN). Quarterly full refresh: the
-- pipeline UPSERTs by facility_id and never appends history at this layer.
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.hospital_general_info (
    facility_id                                       text PRIMARY KEY,
    facility_name                                     text,
    address                                           text,
    citytown                                          text,
    state                                             text,
    zip_code                                          text,
    countyparish                                      text,
    telephone_number                                  text,
    hospital_type                                     text,
    hospital_ownership                                text,
    emergency_services                                text,
    meets_criteria_for_birthing_friendly_designation  text,
    hospital_overall_rating                           text,
    hospital_overall_rating_footnote                  text,
    mort_group_measure_count                          text,
    count_of_facility_mort_measures                   text,
    count_of_mort_measures_better                     text,
    count_of_mort_measures_no_different               text,
    count_of_mort_measures_worse                      text,
    mort_group_footnote                               text,
    safety_group_measure_count                        text,
    count_of_facility_safety_measures                 text,
    count_of_safety_measures_better                   text,
    count_of_safety_measures_no_different             text,
    count_of_safety_measures_worse                    text,
    safety_group_footnote                             text,
    readm_group_measure_count                         text,
    count_of_facility_readm_measures                  text,
    count_of_readm_measures_better                    text,
    count_of_readm_measures_no_different              text,
    count_of_readm_measures_worse                     text,
    readm_group_footnote                              text,
    pt_exp_group_measure_count                        text,
    count_of_facility_pt_exp_measures                 text,
    pt_exp_group_footnote                             text,
    te_group_measure_count                            text,
    count_of_facility_te_measures                     text,
    te_group_footnote                                 text,
    _ingested_at          timestamptz NOT NULL DEFAULT now(),
    _source_modified_at   timestamptz
);

COMMENT ON TABLE raw.hospital_general_info IS
    'CMS xubh-q36u, hospital-level dimension. Full quarterly refresh via '
    'INSERT ... ON CONFLICT (facility_id) DO UPDATE SET ... = EXCLUDED.*. '
    'All columns TEXT to absorb upstream schema drift; cast in core layer.';


-- ============================================================================
-- raw.hcahps  (CMS dgck-syfz, ~325,652 rows)
-- ----------------------------------------------------------------------------
-- LONG format: ~60 rows per hospital, one per granular HCAHPS measure ID
-- (e.g. H_COMP_1_A_P "Always", H_COMP_1_U_P "Usually", H_COMP_1_SN_P
-- "Sometimes/Never", H_COMP_1_LINEAR_SCORE, H_COMP_1_STAR_RATING). The core
-- layer will collapse these by dimension root.
--
-- Footnote columns are preserved verbatim (often comma-separated codes like
-- "1, 5") because they encode WHY a value is missing — keep them in raw,
-- decide what they mean in core.
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.hcahps (
    facility_id                              text NOT NULL,
    facility_name                            text,
    address                                  text,
    citytown                                 text,
    state                                    text,
    zip_code                                 text,
    countyparish                             text,
    telephone_number                         text,
    hcahps_measure_id                        text NOT NULL,
    hcahps_question                          text,
    hcahps_answer_description                text,
    patient_survey_star_rating               text,
    patient_survey_star_rating_footnote      text,
    hcahps_answer_percent                    text,
    hcahps_answer_percent_footnote           text,
    hcahps_linear_mean_value                 text,
    number_of_completed_surveys              text,
    number_of_completed_surveys_footnote     text,
    survey_response_rate_percent             text,
    survey_response_rate_percent_footnote    text,
    start_date                               text,
    end_date                                 text,
    _ingested_at          timestamptz NOT NULL DEFAULT now(),
    _source_modified_at   timestamptz,
    PRIMARY KEY (facility_id, hcahps_measure_id)
);

COMMENT ON TABLE raw.hcahps IS
    'CMS dgck-syfz, granular HCAHPS responses. Long format, PK is the '
    'natural key from the API. Quarterly full refresh: full UPSERT by '
    'natural key; rows that disappear upstream are NOT deleted automatically '
    '(handled by the pipeline if needed). Numeric fields stay TEXT here '
    'because "Not Applicable" is a literal value mixed in with numbers.';

CREATE INDEX IF NOT EXISTS hcahps_facility_idx ON raw.hcahps (facility_id);


-- ============================================================================
-- raw.unplanned_visits  (CMS 632h-zaca, ~67,046 rows)
-- ----------------------------------------------------------------------------
-- LONG format, ~12 rows per hospital. Covers 30-day readmission, EDAC
-- (excess days in acute care), and unplanned post-outpatient visits.
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.unplanned_visits (
    facility_id                  text NOT NULL,
    facility_name                text,
    address                      text,
    citytown                     text,
    state                        text,
    zip_code                     text,
    countyparish                 text,
    telephone_number             text,
    measure_id                   text NOT NULL,
    measure_name                 text,
    compared_to_national         text,
    denominator                  text,
    score                        text,
    lower_estimate               text,
    higher_estimate              text,
    number_of_patients           text,
    number_of_patients_returned  text,
    footnote                     text,
    start_date                   text,
    end_date                     text,
    _ingested_at          timestamptz NOT NULL DEFAULT now(),
    _source_modified_at   timestamptz,
    PRIMARY KEY (facility_id, measure_id)
);

COMMENT ON TABLE raw.unplanned_visits IS
    'CMS 632h-zaca. score column is risk-adjusted rate for some measures, '
    'count of EDAC days for others — context depends on measure_id. Do not '
    'try to unify here; cast in core.unplanned_visits.';

CREATE INDEX IF NOT EXISTS unplanned_visits_facility_idx
    ON raw.unplanned_visits (facility_id);


-- ============================================================================
-- core.hospitals
-- ----------------------------------------------------------------------------
-- Typed dimension table, one row per hospital. FK target for the long
-- core.* fact tables. Populated from raw.hospital_general_info in Phase 4.
-- ============================================================================

CREATE TABLE IF NOT EXISTS core.hospitals (
    facility_id                       text PRIMARY KEY,
    facility_name                     text,
    address                           text,
    city                              text,
    state                             text,
    zip_code                          text,
    county                            text,
    telephone_number                  text,
    hospital_type                     text,
    hospital_ownership                text,
    emergency_services                boolean,
    meets_birthing_friendly_criteria  boolean,
    overall_rating                    smallint,
    overall_rating_footnote           text,
    refreshed_at                      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE core.hospitals IS
    'Typed hospital dimension. overall_rating is NULL when raw value is '
    '"Not Available" or non-numeric. Footnote codes preserved as text so '
    'marts logic can reason about why a rating is missing.';


-- ============================================================================
-- core.patient_experience
-- ----------------------------------------------------------------------------
-- LONG by dimension root (e.g. H_COMP_1 "Communication with Nurses"), with
-- the Always/Usually/Sometimes-Never answer percentages pivoted into
-- top/middle/bottom box columns. ~10 dimensions × ~5,426 hospitals ≈ 54k rows.
--
-- IMPORTANT: hcahps_measure_id here is the DIMENSION ROOT (e.g. H_COMP_1),
-- not the granular per-answer ID stored in raw.hcahps (e.g. H_COMP_1_A_P).
-- The Phase 4 ELT strips the trailing answer-level suffix.
-- ============================================================================

CREATE TABLE IF NOT EXISTS core.patient_experience (
    facility_id           text NOT NULL REFERENCES core.hospitals (facility_id),
    hcahps_measure_id     text NOT NULL,
    hcahps_question_text  text,
    star_rating           numeric,
    top_box_pct           numeric,
    middle_box_pct        numeric,
    bottom_box_pct        numeric,
    completed_surveys     integer,
    response_rate_pct     numeric,
    footnote_codes        text,
    data_quality_flag     text,
    refreshed_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (facility_id, hcahps_measure_id)
);

COMMENT ON TABLE core.patient_experience IS
    'Long fact table: one row per (hospital, HCAHPS dimension root). '
    'star_rating is NULL when raw value is "Not Applicable". top/middle/'
    'bottom_box_pct are NULL when the answer was footnoted out. '
    'footnote_codes preserves the raw comma-separated codes from the source. '
    'data_quality_flag is set by Phase 4 ELT — expected values: '
    'ok, footnoted, not_applicable, missing.';

COMMENT ON COLUMN core.patient_experience.hcahps_measure_id IS
    'Dimension root only (e.g. H_COMP_1), with the _A_P / _U_P / _SN_P / '
    '_LINEAR_SCORE / _STAR_RATING suffix stripped during ELT.';


-- ============================================================================
-- core.unplanned_visits
-- ----------------------------------------------------------------------------
-- LONG, one row per (hospital, measure). Score semantics depend on measure
-- and are documented in the eventual Phase 4 measure dictionary.
-- ============================================================================

CREATE TABLE IF NOT EXISTS core.unplanned_visits (
    facility_id           text NOT NULL REFERENCES core.hospitals (facility_id),
    measure_id            text NOT NULL,
    measure_name          text,
    score                 numeric,
    denominator           integer,
    lower_estimate        numeric,
    higher_estimate       numeric,
    compared_to_national  text,
    footnote_codes        text,
    data_quality_flag     text,
    measurement_start     date,
    measurement_end       date,
    refreshed_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (facility_id, measure_id)
);

COMMENT ON TABLE core.unplanned_visits IS
    '30-day readmission and unplanned-visit rates, typed. score is NULL when '
    'the source value cannot be parsed (e.g. "Not Available"). data_quality_'
    'flag mirrors the convention used in core.patient_experience.';


-- ============================================================================
-- marts.patient_experience_index
-- ----------------------------------------------------------------------------
-- Headline composite score for the dashboard. n_dimensions_used lets the
-- dashboard grey out hospitals whose composite was built on thin data.
-- ============================================================================

CREATE TABLE IF NOT EXISTS marts.patient_experience_index (
    facility_id        text PRIMARY KEY REFERENCES core.hospitals (facility_id),
    composite_score    numeric,
    n_dimensions_used  integer,
    updated_at         timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE marts.patient_experience_index IS
    'Composite HCAHPS score per hospital. Built by Phase 4 marts job; this '
    'DDL only defines the contract. Looker Studio reads from here.';


-- ============================================================================
-- marts.state_rankings
-- ----------------------------------------------------------------------------
-- Per-state distribution of the composite score. One row per state.
-- ============================================================================

CREATE TABLE IF NOT EXISTS marts.state_rankings (
    state            text PRIMARY KEY,
    hospital_count   integer,
    median_score     numeric,
    p25_score        numeric,
    p75_score        numeric,
    p90_score        numeric,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE marts.state_rankings IS
    'Per-state quantile summary of marts.patient_experience_index. '
    'Refresh strategy: TRUNCATE + INSERT in the marts job, since states '
    'rarely appear or disappear and the table is tiny.';


-- ============================================================================
-- marts.top_bottom_performers
-- ----------------------------------------------------------------------------
-- Pre-ranked lists for the dashboard "leaderboard" tile. Direction is
-- 'top' or 'bottom'; key_dims is a JSON snapshot of the per-dimension
-- scores so the tile can show context without joining back to core.
-- ============================================================================

CREATE TABLE IF NOT EXISTS marts.top_bottom_performers (
    rank         integer NOT NULL,
    direction    text    NOT NULL,
    facility_id  text    NOT NULL REFERENCES core.hospitals (facility_id),
    score        numeric,
    key_dims     jsonb,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (rank, direction)
);

COMMENT ON TABLE marts.top_bottom_performers IS
    'Pre-ranked leaderboard for the dashboard. direction is the free-text '
    'tag the marts job writes — currently top or bottom. Refresh strategy: '
    'TRUNCATE + INSERT each marts run.';


-- ============================================================================
-- GRANTS
-- ----------------------------------------------------------------------------
-- Default privileges in 00_init.sql only fire when hospital_app creates the
-- table. These explicit grants make the migration safe to run as superuser
-- too, and ensure looker_reader can SELECT from marts immediately.
-- ============================================================================

GRANT USAGE  ON SCHEMA raw, core, marts TO hospital_app;
GRANT USAGE  ON SCHEMA marts            TO looker_reader;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA raw, core, marts TO hospital_app;

GRANT SELECT ON ALL TABLES IN SCHEMA marts TO looker_reader;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw TO hospital_app;
