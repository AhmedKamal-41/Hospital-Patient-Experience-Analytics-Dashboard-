-- ============================================================================
-- profile_hcahps.sql — Phase 4 Step 1
-- ----------------------------------------------------------------------------
-- Run before writing any ELT code. Confirms the suffix taxonomy is complete
-- and reveals the structure the pivot in core.patient_experience must handle.
--
--   docker exec -e PGPASSWORD=apppass pg-hospital \
--     psql -U hospital_app -d hospital_dashboard -f /scripts/profile_hcahps.sql
-- ============================================================================

\echo '=== 1.1 Identify the bare "H" measure (and any other no-underscore IDs) ==='
SELECT DISTINCT hcahps_measure_id, hcahps_question
FROM raw.hcahps
WHERE hcahps_measure_id !~ '_'
   OR hcahps_measure_id ~ '^H_(LINEAR_SCORE|STAR_RATING)$'
ORDER BY 1;

\echo
\echo '=== 1.2 Full measure_id taxonomy (dimension_root x answer_type) ==='
SELECT
    regexp_replace(
        hcahps_measure_id,
        '_(A_P|U_P|SN_P|Y_P|N_P|DY|PY|DN|0_6|7_8|9_10|LINEAR_SCORE|STAR_RATING)$',
        ''
    ) AS dimension_root,
    CASE
        WHEN hcahps_measure_id ~ '_LINEAR_SCORE$' THEN 'linear_score'
        WHEN hcahps_measure_id ~ '_STAR_RATING$' THEN 'star_rating'
        WHEN hcahps_measure_id ~ '_(A_P|U_P|SN_P)$' THEN 'pct_ternary'
        WHEN hcahps_measure_id ~ '_(Y_P|N_P)$' THEN 'pct_binary'
        WHEN hcahps_measure_id ~ '_(DY|PY|DN)$' THEN 'pct_recommend'
        WHEN hcahps_measure_id ~ '_(0_6|7_8|9_10)$' THEN 'pct_rating_band'
        ELSE 'unknown'
    END AS answer_type,
    count(*) AS rows
FROM raw.hcahps
GROUP BY 1, 2
ORDER BY 1, 2;

\echo
\echo '=== 1.3 Suffix taxonomy gaps (any rows here means the spec is incomplete) ==='
SELECT DISTINCT hcahps_measure_id
FROM raw.hcahps
WHERE hcahps_measure_id !~ '_(A_P|U_P|SN_P|Y_P|N_P|DY|PY|DN|0_6|7_8|9_10|LINEAR_SCORE|STAR_RATING)$'
ORDER BY 1;

\echo
\echo '=== 1.4 hcahps_answer_percent value-type distribution per measure ==='
SELECT hcahps_measure_id,
       count(*) FILTER (WHERE hcahps_answer_percent ~ '^[0-9]+$') AS numeric_count,
       count(*) FILTER (WHERE hcahps_answer_percent = 'Not Applicable') AS not_applicable,
       count(*) FILTER (WHERE hcahps_answer_percent IS NULL OR hcahps_answer_percent = '') AS null_or_empty,
       count(*) FILTER (
           WHERE hcahps_answer_percent !~ '^[0-9]+$'
             AND hcahps_answer_percent NOT IN ('Not Applicable', '')
             AND hcahps_answer_percent IS NOT NULL
       ) AS other
FROM raw.hcahps
GROUP BY 1
ORDER BY 1
LIMIT 40;
