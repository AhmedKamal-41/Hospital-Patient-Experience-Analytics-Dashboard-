-- ============================================================================
-- 06_marts_patient_experience_index.sql — Phase 4 Step 7
-- ----------------------------------------------------------------------------
-- Composite HCAHPS score per hospital: equal-weight average of the 8
-- composite linear_score values, suppressed to NULL if fewer than 6 of 8
-- composites report.
--
-- Threshold reasoning: at least 6 of 8 composite dimensions reporting; below
-- that, the average is built from too few signals to trust at the
-- hospital level. 6/8 = 75% coverage. v1 design uses equal weights
-- (correlation-weighted is deferred — see Phase 4 spec).
--
-- Refresh: TRUNCATE + INSERT (cheap; one row per facility).
-- ============================================================================

\set ON_ERROR_STOP on

TRUNCATE marts.patient_experience_index;

WITH composites AS (
    SELECT facility_id, linear_score
    FROM core.patient_experience
    WHERE hcahps_measure_id IN (
        'H_COMP_1','H_COMP_2','H_COMP_5','H_COMP_6',
        'H_CLEAN','H_QUIET','H_HSP_RATING','H_RECMND'
    )
)
INSERT INTO marts.patient_experience_index (
    facility_id, composite_score, n_dimensions_used, updated_at
)
SELECT
    facility_id,
    CASE WHEN COUNT(linear_score) >= 6 THEN AVG(linear_score) END AS composite_score,
    COUNT(linear_score)::int AS n_dimensions_used,
    now()
FROM composites
GROUP BY facility_id;
