-- ============================================================================
-- 08_marts_top_bottom.sql — Phase 4 Step 9
-- ----------------------------------------------------------------------------
-- Top 10 + bottom 10 hospitals by composite_score. key_dims is a JSON
-- snapshot of the 8 composite linear_scores, so the dashboard can render
-- a per-dimension breakdown without joining back to core.
-- Refresh: TRUNCATE + INSERT.
-- ============================================================================

\set ON_ERROR_STOP on

TRUNCATE marts.top_bottom_performers;

WITH eligible AS (
    SELECT facility_id, composite_score
    FROM marts.patient_experience_index
    WHERE composite_score IS NOT NULL
),
ranked AS (
    SELECT
        facility_id,
        composite_score,
        ROW_NUMBER() OVER (ORDER BY composite_score DESC, facility_id) AS rk_top,
        ROW_NUMBER() OVER (ORDER BY composite_score ASC,  facility_id) AS rk_bottom
    FROM eligible
),
picked AS (
    SELECT facility_id, composite_score, rk_top AS rank, 'top'::text AS direction
    FROM ranked WHERE rk_top <= 10
    UNION ALL
    SELECT facility_id, composite_score, rk_bottom, 'bottom'
    FROM ranked WHERE rk_bottom <= 10
),
dims AS (
    SELECT
        p.rank, p.direction, p.facility_id, p.composite_score AS score,
        jsonb_object_agg(pe.hcahps_measure_id, pe.linear_score)
            FILTER (WHERE pe.linear_score IS NOT NULL) AS key_dims
    FROM picked p
    LEFT JOIN core.patient_experience pe
      ON pe.facility_id = p.facility_id
     AND pe.hcahps_measure_id IN (
         'H_COMP_1','H_COMP_2','H_COMP_5','H_COMP_6',
         'H_CLEAN','H_QUIET','H_HSP_RATING','H_RECMND'
     )
    GROUP BY p.rank, p.direction, p.facility_id, p.composite_score
)
INSERT INTO marts.top_bottom_performers (rank, direction, facility_id, score, key_dims, updated_at)
SELECT rank, direction, facility_id, score, key_dims, now() FROM dims;
