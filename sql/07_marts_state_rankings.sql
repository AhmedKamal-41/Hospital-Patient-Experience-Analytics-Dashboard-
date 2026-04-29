-- ============================================================================
-- 07_marts_state_rankings.sql — Phase 4 Step 8
-- ----------------------------------------------------------------------------
-- Per-state distribution summary of the composite score. Refresh: TRUNCATE
-- + INSERT (~50 rows; rebuild from scratch is cheaper than UPSERT).
-- ============================================================================

\set ON_ERROR_STOP on

TRUNCATE marts.state_rankings;

INSERT INTO marts.state_rankings (
    state, hospital_count, median_score, p25_score, p75_score, p90_score, updated_at
)
SELECT
    h.state,
    COUNT(idx.composite_score) FILTER (WHERE idx.composite_score IS NOT NULL)::int AS hospital_count,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY idx.composite_score) AS median_score,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY idx.composite_score) AS p25_score,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY idx.composite_score) AS p75_score,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY idx.composite_score) AS p90_score,
    now()
FROM core.hospitals h
JOIN marts.patient_experience_index idx USING (facility_id)
WHERE h.state IS NOT NULL
GROUP BY h.state
HAVING COUNT(idx.composite_score) FILTER (WHERE idx.composite_score IS NOT NULL) > 0;
