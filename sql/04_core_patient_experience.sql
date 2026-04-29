-- ============================================================================
-- 04_core_patient_experience.sql — Phase 4 Steps 3 + 5
-- ----------------------------------------------------------------------------
-- (a) Add the linear_score and kind columns the Phase 2 schema didn't
--     anticipate.
-- (b) Rebuild core.hcahps_measure_dim from real data, replacing the
--     hand-seeded rows from sql/02 (some were wrong for the 2026-01-26
--     release — see scripts/profile_hcahps.sql output).
-- (c) Pivot raw.hcahps from per-answer rows to per-dimension rows in
--     core.patient_experience.
-- ============================================================================

\set ON_ERROR_STOP on

-- ----------------------------------------------------------------------------
-- (a) Schema additions
-- ----------------------------------------------------------------------------
ALTER TABLE core.patient_experience
    ADD COLUMN IF NOT EXISTS linear_score numeric;

ALTER TABLE core.hcahps_measure_dim
    ADD COLUMN IF NOT EXISTS kind text;

COMMENT ON COLUMN core.patient_experience.linear_score IS
    '0–100 linear-mean HCAHPS score for the dimension. Only the 8 composites '
    'publish this; sub-question rows are NULL here.';

COMMENT ON COLUMN core.hcahps_measure_dim.kind IS
    'composite | sub_question | overall_summary. Composites feed '
    'marts.patient_experience_index; overall_summary is the bare-H row '
    '(H_STAR_RATING in raw).';

-- ----------------------------------------------------------------------------
-- (b) Rebuild core.hcahps_measure_dim from real data
-- ----------------------------------------------------------------------------
TRUNCATE core.hcahps_measure_dim;

WITH composites(root, label, dim_group, sort_order) AS (
    VALUES
        ('H_COMP_1',     'Communication with Nurses',           'Communication', 1),
        ('H_COMP_2',     'Communication with Doctors',          'Communication', 2),
        ('H_COMP_5',     'Communication About Medicines',       'Communication', 3),
        ('H_COMP_6',     'Discharge Information',               'Transitions',   4),
        ('H_CLEAN',      'Cleanliness of Hospital Environment', 'Environment',   5),
        ('H_QUIET',      'Quietness of Hospital Environment',   'Environment',   6),
        ('H_HSP_RATING', 'Overall Hospital Rating',             'Global',        7),
        ('H_RECMND',     'Would Recommend the Hospital',        'Global',        8)
),
roots AS (
    SELECT
        (regexp_match(
            hcahps_measure_id,
            '^(.+)_(A_P|U_P|SN_P|Y_P|N_P|DY|PY|DN|0_6|7_8|9_10|LINEAR_SCORE|STAR_RATING)$'
        ))[1] AS root,
        (array_agg(hcahps_question ORDER BY hcahps_measure_id))[1] AS sample_question
    FROM raw.hcahps
    GROUP BY 1
)
INSERT INTO core.hcahps_measure_dim (
    hcahps_measure_id, label, dimension_group, sort_order, kind
)
SELECT
    r.root,
    COALESCE(c.label, r.sample_question, r.root)  AS label,
    COALESCE(c.dim_group, 'Sub-questions')        AS dimension_group,
    COALESCE(c.sort_order, 99)                    AS sort_order,
    CASE
        WHEN c.root IS NOT NULL THEN 'composite'
        WHEN r.root = 'H'       THEN 'overall_summary'
        ELSE 'sub_question'
    END AS kind
FROM roots r
LEFT JOIN composites c ON c.root = r.root;


-- ----------------------------------------------------------------------------
-- (c) Pivot raw.hcahps → core.patient_experience
-- ----------------------------------------------------------------------------
WITH classified AS (
    SELECT
        h.facility_id,
        m[1] AS dimension_root,
        m[2] AS suffix,
        h.hcahps_question,
        h.patient_survey_star_rating AS star_raw,
        CASE WHEN m[2] IN ('A_P','U_P','SN_P','Y_P','N_P','DY','PY','DN','0_6','7_8','9_10')
              AND h.hcahps_answer_percent ~ '^[0-9]+$'
             THEN h.hcahps_answer_percent::numeric
        END AS pct_value,
        CASE WHEN m[2] = 'LINEAR_SCORE'
              AND h.hcahps_linear_mean_value ~ '^[0-9]+(\.[0-9]+)?$'
             THEN h.hcahps_linear_mean_value::numeric
        END AS linear_value,
        CASE WHEN m[2] = 'STAR_RATING'
              AND h.patient_survey_star_rating ~ '^[0-9]+$'
             THEN h.patient_survey_star_rating::numeric
        END AS star_value,
        CASE WHEN h.number_of_completed_surveys ~ '^[0-9]+$'
             THEN h.number_of_completed_surveys::integer
        END AS completed_v,
        CASE WHEN h.survey_response_rate_percent ~ '^[0-9]+(\.[0-9]+)?$'
             THEN h.survey_response_rate_percent::numeric
        END AS response_v,
        -- one combined non-empty footnote string per raw row
        NULLIF(
            array_to_string(
                array_remove(ARRAY[
                    NULLIF(h.hcahps_answer_percent_footnote, ''),
                    NULLIF(h.patient_survey_star_rating_footnote, ''),
                    NULLIF(h.number_of_completed_surveys_footnote, ''),
                    NULLIF(h.survey_response_rate_percent_footnote, '')
                ], NULL),
                ', '
            ),
            ''
        ) AS row_footnote
    FROM raw.hcahps h
    CROSS JOIN LATERAL (
        SELECT regexp_match(
            h.hcahps_measure_id,
            '^(.+)_(A_P|U_P|SN_P|Y_P|N_P|DY|PY|DN|0_6|7_8|9_10|LINEAR_SCORE|STAR_RATING)$'
        ) AS m
    ) r
    WHERE m IS NOT NULL  -- profiling confirmed this filter excludes 0 rows
)
INSERT INTO core.patient_experience (
    facility_id, hcahps_measure_id, hcahps_question_text,
    star_rating, linear_score,
    top_box_pct, middle_box_pct, bottom_box_pct,
    completed_surveys, response_rate_pct,
    footnote_codes, data_quality_flag, refreshed_at
)
SELECT
    facility_id,
    dimension_root,
    MAX(hcahps_question) AS hcahps_question_text,
    MAX(star_value)      AS star_rating,
    MAX(linear_value)    AS linear_score,
    MAX(pct_value) FILTER (WHERE suffix IN ('A_P','Y_P','DY','9_10'))      AS top_box_pct,
    MAX(pct_value) FILTER (WHERE suffix IN ('U_P','PY','7_8'))             AS middle_box_pct,
    MAX(pct_value) FILTER (WHERE suffix IN ('SN_P','N_P','DN','0_6'))      AS bottom_box_pct,
    MAX(completed_v)     AS completed_surveys,
    MAX(response_v)      AS response_rate_pct,
    string_agg(DISTINCT row_footnote, '; ' ORDER BY row_footnote) AS footnote_codes,
    CASE
        WHEN MAX(linear_value) IS NOT NULL                   THEN 'ok'
        WHEN bool_or(star_raw = 'Not Applicable')            THEN 'not_applicable'
        WHEN bool_or(row_footnote IS NOT NULL)               THEN 'footnoted'
        ELSE 'missing'
    END AS data_quality_flag,
    now()
FROM classified
GROUP BY facility_id, dimension_root
ON CONFLICT (facility_id, hcahps_measure_id) DO UPDATE SET
    hcahps_question_text = EXCLUDED.hcahps_question_text,
    star_rating          = EXCLUDED.star_rating,
    linear_score         = EXCLUDED.linear_score,
    top_box_pct          = EXCLUDED.top_box_pct,
    middle_box_pct       = EXCLUDED.middle_box_pct,
    bottom_box_pct       = EXCLUDED.bottom_box_pct,
    completed_surveys    = EXCLUDED.completed_surveys,
    response_rate_pct    = EXCLUDED.response_rate_pct,
    footnote_codes       = EXCLUDED.footnote_codes,
    data_quality_flag    = EXCLUDED.data_quality_flag,
    refreshed_at         = now();
