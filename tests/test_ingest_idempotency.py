"""End-to-end idempotency check.

Spins up Postgres in a container via testcontainers, runs the schema +
seed migrations, ingests against a stubbed CMSClient that yields a tiny
CSV, then runs ingest a second time. Asserts the row count is unchanged
and that the second run logs `rows_updated == N, rows_inserted == 0`.

Skipped if Docker is unavailable.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_INIT = REPO_ROOT / "sql" / "00_init.sql"
SQL_SCHEMA = REPO_ROOT / "sql" / "01_schema.sql"
SQL_SEED = REPO_ROOT / "sql" / "02_seed_measure_dim.sql"


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(
            ["docker", "info"],
            check=True,
            capture_output=True,
            timeout=5,
        )
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)

testcontainers = pytest.importorskip("testcontainers.postgres")
PostgresContainer = testcontainers.PostgresContainer  # type: ignore[attr-defined]


@pytest.fixture(scope="module")
def pg_container() -> Iterator[object]:
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture(scope="module")
def pg_env(pg_container) -> Iterator[dict[str, str]]:
    """Bootstrap DB + role + schemas, set PG* env vars, yield, then restore."""
    import psycopg

    super_dsn = (
        f"host={pg_container.get_container_host_ip()} "
        f"port={pg_container.get_exposed_port(5432)} "
        f"dbname={pg_container.dbname} "
        f"user={pg_container.username} "
        f"password={pg_container.password}"
    )

    with psycopg.connect(super_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE ROLE hospital_app LOGIN PASSWORD 'apppass'")
        cur.execute("CREATE ROLE looker_reader LOGIN PASSWORD 'readpass'")
        cur.execute("CREATE DATABASE hospital_dashboard OWNER hospital_app")

    app_dsn_super = super_dsn.replace(
        f"dbname={pg_container.dbname}", "dbname=hospital_dashboard"
    )
    with psycopg.connect(app_dsn_super, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA raw   AUTHORIZATION hospital_app")
        cur.execute("CREATE SCHEMA core  AUTHORIZATION hospital_app")
        cur.execute("CREATE SCHEMA marts AUTHORIZATION hospital_app")

    app_dsn = (
        f"host={pg_container.get_container_host_ip()} "
        f"port={pg_container.get_exposed_port(5432)} "
        "dbname=hospital_dashboard "
        "user=hospital_app password=apppass"
    )
    from src.transform import _strip_psql_metacommands

    with psycopg.connect(app_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_strip_psql_metacommands(SQL_SCHEMA.read_text()))
        cur.execute(_strip_psql_metacommands(SQL_SEED.read_text()))

    env = {
        "PGHOST": pg_container.get_container_host_ip(),
        "PGPORT": str(pg_container.get_exposed_port(5432)),
        "PGDATABASE": "hospital_dashboard",
        "PGUSER": "hospital_app",
        "PGPASSWORD": "apppass",
    }
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        yield env
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _stub_client_for_unplanned() -> MagicMock:
    """Pretend to be a CMSClient that returns 2 rows of unplanned_visits CSV."""
    csv_rows = [
        {
            "Facility ID": "010001",
            "Facility Name": "Test Hospital",
            "Address": "1 Main",
            "City/Town": "Dothan",
            "State": "AL",
            "ZIP Code": "36301",
            "County/Parish": "HOUSTON",
            "Telephone Number": "(334) 555-1212",
            "Measure ID": "READM_30_HOSP_WIDE",
            "Measure Name": "Rate of readmission",
            "Compared to National": "No Different",
            "Denominator": "1234",
            "Score": "15.2",
            "Lower Estimate": "14.0",
            "Higher Estimate": "16.5",
            "Number of Patients": "1234",
            "Number of Patients Returned": "188",
            "Footnote": "",
            "Start Date": "07/01/2023",
            "End Date": "06/30/2024",
        },
        {
            "Facility ID": "010002",
            "Facility Name": "Other Hospital",
            "Address": "2 Main",
            "City/Town": "Birmingham",
            "State": "AL",
            "ZIP Code": "35201",
            "County/Parish": "JEFFERSON",
            "Telephone Number": "(205) 555-1212",
            "Measure ID": "READM_30_HOSP_WIDE",
            "Measure Name": "Rate of readmission",
            "Compared to National": "Better",
            "Denominator": "5000",
            "Score": "12.1",
            "Lower Estimate": "11.0",
            "Higher Estimate": "13.0",
            "Number of Patients": "5000",
            "Number of Patients Returned": "605",
            "Footnote": "",
            "Start Date": "07/01/2023",
            "End Date": "06/30/2024",
        },
    ]

    client = MagicMock()
    client.resolve_download.return_value = ("https://cms.test/file.csv", None)
    client.stream_csv.return_value = iter(csv_rows)
    return client


def test_ingest_is_idempotent(pg_env, monkeypatch) -> None:
    from src import ingest

    client = _stub_client_for_unplanned()
    # First run
    result_a = ingest.ingest_one(
        "unplanned_visits", client=client, dry_run=False
    )
    assert result_a["rows_inserted"] == 2
    assert result_a["rows_updated"] == 0

    # Second run: stream_csv must yield again
    client.stream_csv.return_value = iter(_stub_client_for_unplanned().stream_csv.return_value)
    result_b = ingest.ingest_one(
        "unplanned_visits", client=client, dry_run=False
    )
    assert result_b["rows_inserted"] == 0
    assert result_b["rows_updated"] == 2

    # Final row count is still 2
    import psycopg

    with psycopg.connect(
        host=pg_env["PGHOST"],
        port=int(pg_env["PGPORT"]),
        dbname=pg_env["PGDATABASE"],
        user=pg_env["PGUSER"],
        password=pg_env["PGPASSWORD"],
    ) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.unplanned_visits")
        (count,) = cur.fetchone()
        assert count == 2

        cur.execute(
            "SELECT count(*) FROM raw.ingest_log "
            "WHERE dataset_id = '632h-zaca' AND status = 'success'"
        )
        (logs,) = cur.fetchone()
        assert logs == 2
