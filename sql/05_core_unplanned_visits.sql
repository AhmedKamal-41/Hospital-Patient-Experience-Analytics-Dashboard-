-- ============================================================================
-- 05_core_unplanned_visits.sql — Phase 4 Step 6
-- ----------------------------------------------------------------------------
-- Type-cast pass on raw.unplanned_visits. Numeric score / denominator,
-- date parsing, footnote preserved as text, NULL-on-non-numeric.
-- Idempotent UPSERT by (facility_id, measure_id).
-- ============================================================================

\set ON_ERROR_STOP on

INSERT INTO core.unplanned_visits (
    facility_id, measure_id, measure_name,
    score, denominator, lower_estimate, higher_estimate,
    compared_to_national, footnote_codes, data_quality_flag,
    measurement_start, measurement_end, refreshed_at
)
SELECT
    facility_id,
    measure_id,
    NULLIF(measure_name, ''),
    CASE WHEN score ~ '^[0-9]+(\.[0-9]+)?$' THEN score::numeric END,
    CASE WHEN denominator ~ '^[0-9]+$' THEN denominator::integer END,
    CASE WHEN lower_estimate ~ '^[0-9]+(\.[0-9]+)?$' THEN lower_estimate::numeric END,
    CASE WHEN higher_estimate ~ '^[0-9]+(\.[0-9]+)?$' THEN higher_estimate::numeric END,
    NULLIF(compared_to_national, ''),
    NULLIF(footnote, ''),
    CASE
        WHEN score ~ '^[0-9]+(\.[0-9]+)?$'      THEN 'ok'
        WHEN score IN ('Not Applicable')         THEN 'not_applicable'
        WHEN NULLIF(footnote, '') IS NOT NULL    THEN 'footnoted'
        ELSE 'missing'
    END,
    CASE WHEN start_date ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(start_date, 'MM/DD/YYYY') END,
    CASE WHEN end_date   ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(end_date,   'MM/DD/YYYY') END,
    now()
FROM raw.unplanned_visits
ON CONFLICT (facility_id, measure_id) DO UPDATE SET
    measure_name         = EXCLUDED.measure_name,
    score                = EXCLUDED.score,
    denominator          = EXCLUDED.denominator,
    lower_estimate       = EXCLUDED.lower_estimate,
    higher_estimate      = EXCLUDED.higher_estimate,
    compared_to_national = EXCLUDED.compared_to_national,
    footnote_codes       = EXCLUDED.footnote_codes,
    data_quality_flag    = EXCLUDED.data_quality_flag,
    measurement_start    = EXCLUDED.measurement_start,
    measurement_end      = EXCLUDED.measurement_end,
    refreshed_at         = now();
