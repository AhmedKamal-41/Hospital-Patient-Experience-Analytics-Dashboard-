-- ============================================================================
-- 02_seed_measure_dim.sql — HCAHPS dimension reference table
-- ----------------------------------------------------------------------------
-- Small, hand-curated lookup that maps the 10 standard HCAHPS composite
-- dimension roots to dashboard-friendly labels and a stable sort order.
-- Used by Looker Studio (and by the marts.top_bottom_performers key_dims
-- payload) to render dimension names without parsing them out of the long
-- hcahps_question column.
--
-- Run after 01_schema.sql:
--
--   PGPASSWORD=$PGPASSWORD psql -U hospital_app -d hospital_dashboard \
--       -f sql/02_seed_measure_dim.sql
--
-- Idempotent: ON CONFLICT DO UPDATE refreshes labels on re-run.
--
-- NOTE: the hcahps_measure_id values below are the published composite roots
-- as of the 2026-01-26 Care Compare release. Cross-check against the first
-- ingest of raw.hcahps before relying on the FK from core.patient_experience.
-- ============================================================================

\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS core.hcahps_measure_dim (
    hcahps_measure_id  text PRIMARY KEY,
    label              text NOT NULL,
    dimension_group    text NOT NULL,
    sort_order         integer NOT NULL
);

COMMENT ON TABLE core.hcahps_measure_dim IS
    'Reference table for HCAHPS composite dimensions. dimension_group is '
    'the dashboard-side category (Communication, Environment, Global, etc). '
    'sort_order is the canonical CMS display order — keep stable so '
    'dashboard widgets do not reshuffle quarter-to-quarter.';

INSERT INTO core.hcahps_measure_dim (hcahps_measure_id, label, dimension_group, sort_order) VALUES
    ('H_COMP_1',     'Communication with Nurses',           'Communication',      1),
    ('H_COMP_2',     'Communication with Doctors',          'Communication',      2),
    ('H_COMP_3',     'Responsiveness of Hospital Staff',    'Staff',              3),
    ('H_COMP_5',     'Communication About Medicines',       'Communication',      4),
    ('H_COMP_6',     'Discharge Information',               'Transitions',        5),
    ('H_COMP_7',     'Care Transition',                     'Transitions',        6),
    ('H_CLEAN_HSP',  'Cleanliness of Hospital Environment', 'Environment',        7),
    ('H_QUIET_HSP',  'Quietness of Hospital Environment',   'Environment',        8),
    ('H_HSP_RATING', 'Overall Hospital Rating',             'Global',             9),
    ('H_RECMND',     'Recommend the Hospital',              'Global',            10)
ON CONFLICT (hcahps_measure_id) DO UPDATE SET
    label           = EXCLUDED.label,
    dimension_group = EXCLUDED.dimension_group,
    sort_order      = EXCLUDED.sort_order;

GRANT SELECT ON core.hcahps_measure_dim TO looker_reader;
