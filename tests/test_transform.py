"""End-to-end transform sanity tests.

These run against a populated database — they are integration tests, not
unit tests. The fixture loads ~30 synthetic hospitals across 3 states
into a fresh testcontainers Postgres, runs the full schema + transform
pipeline, and asserts on the resulting core/marts tables.

Skipped if Docker is not available.
"""

from __future__ import annotations

import os
import random
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = REPO_ROOT / "sql"


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(
            ["docker", "info"], check=True, capture_output=True, timeout=5
        )
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)

testcontainers_pg = pytest.importorskip("testcontainers.postgres")
PostgresContainer = testcontainers_pg.PostgresContainer  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Synthetic fixture
# ---------------------------------------------------------------------------
COMPOSITE_ROOTS = [
    "H_COMP_1", "H_COMP_2", "H_COMP_5", "H_COMP_6",
    "H_CLEAN", "H_QUIET", "H_HSP_RATING", "H_RECMND",
]


def _build_fixture(n_per_state: int = 10, seed: int = 17) -> dict:
    """Generate ~30 hospitals × 8 composites with correlation-by-construction."""
    rng = random.Random(seed)
    states = ["CA", "NY", "TX"]
    hospitals: list[dict] = []
    hcahps: list[dict] = []
    fid = 100000
    for state in states:
        for _ in range(n_per_state):
            fid += 1
            star = rng.randint(1, 5)
            hospitals.append({
                "facility_id": str(fid),
                "facility_name": f"Hospital {fid}",
                "state": state,
                "hospital_overall_rating": str(star),
                "emergency_services": rng.choice(["Yes", "No"]),
            })
            # Composite linear scores are 60 + 6*star + small noise.
            # Built-in correlation: overall_rating drives composite cleanly.
            for root in COMPOSITE_ROOTS:
                linear = 60 + 6 * star + rng.uniform(-2, 2)
                # Granular rows: one LINEAR_SCORE row per dim
                hcahps.append({
                    "facility_id": str(fid),
                    "hcahps_measure_id": f"{root}_LINEAR_SCORE",
                    "hcahps_question": f"{root} composite",
                    "hcahps_answer_percent": "Not Applicable",
                    "patient_survey_star_rating": "Not Applicable",
                    "hcahps_linear_mean_value": f"{linear:.1f}",
                    "number_of_completed_surveys": "300",
                    "survey_response_rate_percent": "20",
                })
                # And a STAR_RATING row
                hcahps.append({
                    "facility_id": str(fid),
                    "hcahps_measure_id": f"{root}_STAR_RATING",
                    "hcahps_question": f"{root} star",
                    "hcahps_answer_percent": "Not Applicable",
                    "patient_survey_star_rating": str(min(5, max(1, star))),
                    "hcahps_linear_mean_value": "Not Applicable",
                    "number_of_completed_surveys": "300",
                    "survey_response_rate_percent": "20",
                })
    return {"hospitals": hospitals, "hcahps": hcahps}


# ---------------------------------------------------------------------------
# Fixture: bootstrap DB, schema, raw load, transform
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def populated_db() -> Iterator[dict[str, str]]:
    import psycopg

    fixture = _build_fixture()
    container = PostgresContainer("postgres:16-alpine")
    container.start()
    try:
        super_dsn = (
            f"host={container.get_container_host_ip()} "
            f"port={container.get_exposed_port(5432)} "
            f"dbname={container.dbname} "
            f"user={container.username} "
            f"password={container.password}"
        )
        with psycopg.connect(super_dsn, autocommit=True) as c, c.cursor() as cur:
            cur.execute("CREATE ROLE hospital_app LOGIN PASSWORD 'apppass'")
            cur.execute("CREATE ROLE looker_reader LOGIN PASSWORD 'readpass'")
            cur.execute("CREATE DATABASE hospital_dashboard OWNER hospital_app")

        with psycopg.connect(
            super_dsn.replace(f"dbname={container.dbname}", "dbname=hospital_dashboard"),
            autocommit=True,
        ) as c, c.cursor() as cur:
            cur.execute("CREATE SCHEMA raw   AUTHORIZATION hospital_app")
            cur.execute("CREATE SCHEMA core  AUTHORIZATION hospital_app")
            cur.execute("CREATE SCHEMA marts AUTHORIZATION hospital_app")

        env = {
            "PGHOST": container.get_container_host_ip(),
            "PGPORT": str(container.get_exposed_port(5432)),
            "PGDATABASE": "hospital_dashboard",
            "PGUSER": "hospital_app",
            "PGPASSWORD": "apppass",
        }
        os.environ.update(env)

        from src.transform import _strip_psql_metacommands

        with psycopg.connect(
            host=env["PGHOST"], port=int(env["PGPORT"]),
            dbname=env["PGDATABASE"], user=env["PGUSER"], password=env["PGPASSWORD"],
            autocommit=True,
        ) as c, c.cursor() as cur:
            cur.execute(_strip_psql_metacommands((SQL_DIR / "01_schema.sql").read_text()))
            cur.execute(_strip_psql_metacommands((SQL_DIR / "02_seed_measure_dim.sql").read_text()))

            for h in fixture["hospitals"]:
                cur.execute(
                    """
                    INSERT INTO raw.hospital_general_info (
                        facility_id, facility_name, state,
                        hospital_overall_rating, emergency_services
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (h["facility_id"], h["facility_name"], h["state"],
                     h["hospital_overall_rating"], h["emergency_services"]),
                )

            for r in fixture["hcahps"]:
                cur.execute(
                    """
                    INSERT INTO raw.hcahps (
                        facility_id, hcahps_measure_id, hcahps_question,
                        hcahps_answer_percent, patient_survey_star_rating,
                        hcahps_linear_mean_value,
                        number_of_completed_surveys, survey_response_rate_percent
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (r["facility_id"], r["hcahps_measure_id"], r["hcahps_question"],
                     r["hcahps_answer_percent"], r["patient_survey_star_rating"],
                     r["hcahps_linear_mean_value"],
                     r["number_of_completed_surveys"], r["survey_response_rate_percent"]),
                )

        from src import transform
        transform.run_all()
        yield env
    finally:
        container.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def _connect(env):
    import psycopg
    return psycopg.connect(
        host=env["PGHOST"], port=int(env["PGPORT"]),
        dbname=env["PGDATABASE"], user=env["PGUSER"], password=env["PGPASSWORD"],
    )


def test_core_patient_experience_no_pk_collisions(populated_db) -> None:
    with _connect(populated_db) as c, c.cursor() as cur:
        cur.execute(
            "SELECT count(*) - count(DISTINCT (facility_id, hcahps_measure_id)) "
            "FROM core.patient_experience"
        )
        (dupes,) = cur.fetchone()
        assert dupes == 0


def test_core_patient_experience_score_bounds(populated_db) -> None:
    with _connect(populated_db) as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM core.patient_experience
            WHERE linear_score IS NOT NULL
              AND (linear_score < 0 OR linear_score > 100)
            """
        )
        (out_of_range,) = cur.fetchone()
        assert out_of_range == 0


def test_marts_state_rankings_completeness(populated_db) -> None:
    with _connect(populated_db) as c, c.cursor() as cur:
        cur.execute("SELECT state FROM marts.state_rankings ORDER BY state")
        states = [r[0] for r in cur.fetchall()]
        assert set(states) == {"CA", "NY", "TX"}


def test_marts_composite_score_correlates_with_overall_rating(
    populated_db,
) -> None:
    """Pearson r between composite and overall_rating.

    Threshold is 0.45, calibrated empirically against the live 2026-01-26
    Care Compare release: r=0.524 there. Synthetic fixture is rigged for
    much higher r (~0.95). 0.45 is the regression floor — anything lower
    means the composite has lost the relationship to the source signal.
    """
    with _connect(populated_db) as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT corr(idx.composite_score, raw_h.hospital_overall_rating::numeric)
            FROM marts.patient_experience_index idx
            JOIN raw.hospital_general_info raw_h USING (facility_id)
            WHERE idx.composite_score IS NOT NULL
              AND raw_h.hospital_overall_rating ~ '^[0-5]$'
            """
        )
        (r,) = cur.fetchone()
        assert r is not None and r > 0.45, f"Pearson r={r}"


def test_idempotent_rebuild(populated_db) -> None:
    """Running --all twice produces identical row counts and value samples."""
    from src import transform

    with _connect(populated_db) as c, c.cursor() as cur:
        cur.execute("SELECT count(*), sum(composite_score) FROM marts.patient_experience_index")
        before = cur.fetchone()

    transform.run_all()

    with _connect(populated_db) as c, c.cursor() as cur:
        cur.execute("SELECT count(*), sum(composite_score) FROM marts.patient_experience_index")
        after = cur.fetchone()

    assert after == before
