"""raw → core → marts ELT runner.

Each step is a single SQL file under sql/ that is idempotent. Re-running is
safe; the marts layer rebuilds from scratch each time, the core layer
UPSERTs in place.

CLI:
    python -m src.transform --layer core
    python -m src.transform --layer marts
    python -m src.transform --all
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

from src import db
from src.ingest import configure_logging

log = logging.getLogger("transform")

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = REPO_ROOT / "sql"

CORE_STEPS: list[tuple[str, str]] = [
    ("transform:core:hospitals",          "03_core_hospitals.sql"),
    ("transform:core:patient_experience", "04_core_patient_experience.sql"),
    ("transform:core:unplanned_visits",   "05_core_unplanned_visits.sql"),
]

MARTS_STEPS: list[tuple[str, str]] = [
    ("transform:marts:patient_experience_index", "06_marts_patient_experience_index.sql"),
    ("transform:marts:state_rankings",           "07_marts_state_rankings.sql"),
    ("transform:marts:top_bottom_performers",    "08_marts_top_bottom.sql"),
]


def _strip_psql_metacommands(sql_text: str) -> str:
    """psql client metacommands like `\\set ON_ERROR_STOP on` aren't valid SQL.

    psycopg already raises on error, so dropping them is harmless.
    """
    return "\n".join(
        line for line in sql_text.splitlines()
        if not line.lstrip().startswith("\\")
    )


def run_sql_file(conn, sql_path: Path) -> None:
    sql_text = _strip_psql_metacommands(sql_path.read_text())
    with conn.cursor() as cur:
        cur.execute(sql_text)


def run_step(conn, log_name: str, sql_filename: str) -> None:
    sql_path = SQL_DIR / sql_filename
    started = time.monotonic()
    log_id = db.start_ingest_log(conn, log_name)
    status = "running"
    error_message: str | None = None
    try:
        with db.transaction(conn):
            run_sql_file(conn, sql_path)
        status = "success"
    except Exception as exc:
        status = "failed"
        error_message = repr(exc)
        log.exception("transform_step_failed", extra={"step": log_name})
        raise
    finally:
        db.finish_ingest_log(
            conn,
            log_id,
            status=status,
            rows_inserted=None,
            rows_updated=None,
            error_message=error_message,
        )
        log.info(
            "transform_step_finished",
            extra={
                "step": log_name,
                "status": status,
                "sql_file": sql_filename,
                "runtime_sec": round(time.monotonic() - started, 2),
            },
        )


def run_layer(layer: str) -> None:
    steps = {"core": CORE_STEPS, "marts": MARTS_STEPS}[layer]
    conn = db.get_connection()
    try:
        for name, filename in steps:
            run_step(conn, name, filename)
    finally:
        conn.close()


def run_all() -> None:
    run_layer("core")
    run_layer("marts")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.transform",
        description="Run raw→core→marts ELT migrations.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--layer", choices=["core", "marts"])
    group.add_argument("--all", action="store_true")
    args = parser.parse_args(argv)

    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))

    try:
        if args.all:
            run_all()
        else:
            run_layer(args.layer)
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
