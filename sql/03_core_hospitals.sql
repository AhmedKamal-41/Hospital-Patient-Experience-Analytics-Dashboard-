-- ============================================================================
-- 03_core_hospitals.sql — Phase 4 Step 4
-- ----------------------------------------------------------------------------
-- Type-cast pass on raw.hospital_general_info into core.hospitals.
-- Idempotent UPSERT by facility_id. Rebuild strategy: rerun in place.
-- ============================================================================

\set ON_ERROR_STOP on

INSERT INTO core.hospitals (
    facility_id,
    facility_name,
    address,
    city,
    state,
    zip_code,
    county,
    telephone_number,
    hospital_type,
    hospital_ownership,
    emergency_services,
    meets_birthing_friendly_criteria,
    overall_rating,
    overall_rating_footnote,
    refreshed_at
)
SELECT
    facility_id,
    facility_name,
    NULLIF(address, ''),
    NULLIF(citytown, ''),
    NULLIF(state, ''),
    NULLIF(zip_code, ''),
    NULLIF(countyparish, ''),
    NULLIF(telephone_number, ''),
    NULLIF(hospital_type, ''),
    NULLIF(hospital_ownership, ''),
    CASE
        WHEN emergency_services = 'Yes' THEN TRUE
        WHEN emergency_services = 'No'  THEN FALSE
    END,
    CASE
        WHEN meets_criteria_for_birthing_friendly_designation = 'Y' THEN TRUE
        WHEN meets_criteria_for_birthing_friendly_designation = 'N' THEN FALSE
    END,
    CASE
        WHEN hospital_overall_rating ~ '^[0-5]$'
            THEN hospital_overall_rating::smallint
    END,
    NULLIF(hospital_overall_rating_footnote, ''),
    now()
FROM raw.hospital_general_info
ON CONFLICT (facility_id) DO UPDATE SET
    facility_name                    = EXCLUDED.facility_name,
    address                          = EXCLUDED.address,
    city                             = EXCLUDED.city,
    state                            = EXCLUDED.state,
    zip_code                         = EXCLUDED.zip_code,
    county                           = EXCLUDED.county,
    telephone_number                 = EXCLUDED.telephone_number,
    hospital_type                    = EXCLUDED.hospital_type,
    hospital_ownership               = EXCLUDED.hospital_ownership,
    emergency_services               = EXCLUDED.emergency_services,
    meets_birthing_friendly_criteria = EXCLUDED.meets_birthing_friendly_criteria,
    overall_rating                   = EXCLUDED.overall_rating,
    overall_rating_footnote          = EXCLUDED.overall_rating_footnote,
    refreshed_at                     = now();
