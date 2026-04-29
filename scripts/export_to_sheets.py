"""Push core/marts data into a Google Sheets workbook for Looker Studio.

Postgres → Sheets bridge: the Looker Studio dashboard reads from the
workbook, this script keeps the workbook fresh.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=path/to/sa.json \\
    PGHOST=localhost PGPORT=5433 PGDATABASE=hospital_dashboard \\
    PGUSER=looker_reader PGPASSWORD=... \\
    python scripts/export_to_sheets.py --workbook-id <gsheet_id>

The script is idempotent: each sheet is cleared and rewritten, never
appended. One row is written to raw.ingest_log per run (the looker_reader
role does not have INSERT on raw.ingest_log, so the audit row uses a
separate hospital_app connection — see ingest_log_dsn()).
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import time
from collections.abc import Iterable
from typing import Any

import psycopg

# Allow running as `python scripts/export_to_sheets.py` from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import db  # noqa: E402
from src.ingest import configure_logging  # noqa: E402

log = logging.getLogger("export")


# ---------------------------------------------------------------------------
# SQL queries — one per sheet
# ---------------------------------------------------------------------------
SQL_HOSPITALS = """
SELECT
    h.facility_id,
    h.facility_name AS name,
    h.state,
    h.hospital_type,
    h.hospital_ownership AS ownership,
    h.emergency_services,
    h.overall_rating,
    ROUND(idx.composite_score::numeric, 1) AS composite_score,
    idx.n_dimensions_used
FROM core.hospitals h
LEFT JOIN marts.patient_experience_index idx USING (facility_id)
ORDER BY h.facility_id
"""

SQL_PATIENT_EXPERIENCE = """
SELECT
    pe.facility_id,
    pe.hcahps_measure_id AS dimension_root,
    md.label AS dimension_label,
    md.kind,
    ROUND(pe.linear_score::numeric,    1) AS linear_score,
    ROUND(pe.top_box_pct::numeric,     1) AS top_box_pct,
    ROUND(pe.middle_box_pct::numeric,  1) AS middle_box_pct,
    ROUND(pe.bottom_box_pct::numeric,  1) AS bottom_box_pct,
    pe.star_rating,
    pe.completed_surveys,
    ROUND(pe.response_rate_pct::numeric, 1) AS response_rate_pct,
    pe.data_quality_flag
FROM core.patient_experience pe
JOIN core.hcahps_measure_dim md
    ON md.hcahps_measure_id = pe.hcahps_measure_id
WHERE md.kind = 'composite'
ORDER BY pe.facility_id, md.sort_order
"""

SQL_STATE_RANKINGS = """
SELECT
    state,
    hospital_count,
    ROUND(p25_score::numeric,    1) AS p25,
    ROUND(median_score::numeric, 1) AS median,
    ROUND(p75_score::numeric,    1) AS p75,
    ROUND(p90_score::numeric,    1) AS p90
FROM marts.state_rankings
ORDER BY median DESC NULLS LAST
"""

SQL_TOP_BOTTOM = """
SELECT
    tb.direction,
    tb.rank,
    tb.facility_id,
    h.facility_name AS name,
    h.state,
    ROUND(tb.score::numeric, 1) AS score
FROM marts.top_bottom_performers tb
JOIN core.hospitals h USING (facility_id)
ORDER BY tb.direction, tb.rank
"""

SQL_META = """
SELECT
    (SELECT count(*) FROM core.hospitals) AS total_hospitals,
    (SELECT count(*) FROM marts.patient_experience_index
       WHERE composite_score IS NOT NULL) AS total_scored,
    (SELECT count(*) FROM marts.patient_experience_index
       WHERE composite_score IS NULL)     AS total_suppressed
"""


# ---------------------------------------------------------------------------
# Sheet writer
# ---------------------------------------------------------------------------
class SheetsClient:
    def __init__(self, workbook_id: str, credentials_path: str) -> None:
        # Imported lazily so module loads even when gspread isn't installed
        # (e.g. running tests that don't touch this file).
        import gspread
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_file(
            credentials_path,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ],
        )
        self._gc = gspread.authorize(creds)
        self._wb = self._gc.open_by_key(workbook_id)
        self._gspread = gspread

    def write_sheet(
        self, name: str, headers: list[str], rows: list[list[Any]]
    ) -> int:
        """Replace `name` with [headers] + rows. Returns rows written."""
        try:
            ws = self._wb.worksheet(name)
        except self._gspread.WorksheetNotFound:
            ws = self._wb.add_worksheet(
                title=name, rows=max(len(rows) + 10, 10), cols=len(headers)
            )
        ws.clear()
        ws.update(values=[headers] + rows, range_name="A1")
        return len(rows)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def _fetch(conn: psycopg.Connection, sql: str) -> tuple[list[str], list[list[Any]]]:
    with conn.cursor() as cur:
        cur.execute(sql)
        headers = [d[0] for d in cur.description]
        rows = [list(_to_jsonable(v) for v in row) for row in cur.fetchall()]
    return headers, rows


def _to_jsonable(v: Any) -> Any:
    """Cast types Sheets won't accept (Decimal, datetime) to plain values."""
    if v is None:
        return ""
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
        # psycopg returns NUMERIC as decimal.Decimal — Sheets API needs floats
        return float(v)
    return v


# ---------------------------------------------------------------------------
# Audit log helpers — looker_reader can't write, so use hospital_app for log
# ---------------------------------------------------------------------------
def _ingest_log_conn() -> psycopg.Connection:
    """Open a hospital_app connection for the ingest_log row.

    Falls back to PG* env vars if PG_AUDIT_USER / PG_AUDIT_PASSWORD are unset.
    """
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ["PGDATABASE"],
        user=os.environ.get("PG_AUDIT_USER", os.environ["PGUSER"]),
        password=os.environ.get("PG_AUDIT_PASSWORD", os.environ["PGPASSWORD"]),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def export(workbook_id: str, credentials_path: str) -> dict[str, int]:
    started = time.monotonic()

    # Audit log via hospital_app (looker_reader has no INSERT on raw.*).
    audit = _ingest_log_conn()
    log_id = db.start_ingest_log(audit, "export:sheets")

    counts: dict[str, int] = {}
    status = "running"
    error_message: str | None = None

    try:
        sheets = SheetsClient(workbook_id, credentials_path)
        with db.get_connection() as conn:  # looker_reader
            # 1. hospitals (now with composite_score + n_dimensions_used)
            h_headers, h_rows = _fetch(conn, SQL_HOSPITALS)
            counts["hospitals"] = sheets.write_sheet("hospitals", h_headers, h_rows)
            log.info("sheet_written", extra={"sheet": "hospitals", "rows": counts["hospitals"]})

            # 2. patient_experience_dim_long (composites only)
            pe_headers, pe_rows = _fetch(conn, SQL_PATIENT_EXPERIENCE)
            counts["patient_experience_dim_long"] = sheets.write_sheet(
                "patient_experience_dim_long", pe_headers, pe_rows
            )
            log.info("sheet_written", extra={
                "sheet": "patient_experience_dim_long",
                "rows": counts["patient_experience_dim_long"],
            })

            # 3. state_rankings
            sr_headers, sr_rows = _fetch(conn, SQL_STATE_RANKINGS)
            counts["state_rankings"] = sheets.write_sheet(
                "state_rankings", sr_headers, sr_rows
            )
            log.info("sheet_written", extra={"sheet": "state_rankings", "rows": counts["state_rankings"]})

            # 4. top_bottom
            tb_headers, tb_rows = _fetch(conn, SQL_TOP_BOTTOM)
            counts["top_bottom"] = sheets.write_sheet("top_bottom", tb_headers, tb_rows)
            log.info("sheet_written", extra={"sheet": "top_bottom", "rows": counts["top_bottom"]})

            # 5. meta — one row, dashboard reads this for headline numbers
            m_headers, m_rows = _fetch(conn, SQL_META)
            now_utc = dt.datetime.now(dt.timezone.utc).isoformat()
            meta_headers = (
                ["last_refresh_utc"] + m_headers + ["dashboard_version"]
            )
            meta_row = [now_utc] + (m_rows[0] if m_rows else [0, 0, 0]) + ["v1"]
            sheets.write_sheet("meta", meta_headers, [meta_row])
            counts["meta"] = 1

        status = "success"
    except Exception as exc:
        status = "failed"
        error_message = repr(exc)
        log.exception("export_failed")
        raise
    finally:
        db.finish_ingest_log(
            audit,
            log_id,
            status=status,
            rows_inserted=sum(counts.values()) if counts else None,
            rows_updated=None,
            error_message=error_message,
        )
        audit.close()
        log.info(
            "export_finished",
            extra={
                "status": status,
                "runtime_sec": round(time.monotonic() - started, 2),
                "counts": counts,
            },
        )
    return counts


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python scripts/export_to_sheets.py",
        description="Export core/marts to a Google Sheets workbook.",
    )
    parser.add_argument(
        "--workbook-id",
        required=True,
        help="The Google Sheets file ID (from the URL).",
    )
    parser.add_argument(
        "--credentials",
        default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        help="Path to service-account JSON; defaults to "
             "$GOOGLE_APPLICATION_CREDENTIALS.",
    )
    args = parser.parse_args(list(argv) if argv else None)

    if not args.credentials:
        parser.error(
            "service-account credentials required: pass --credentials or "
            "set GOOGLE_APPLICATION_CREDENTIALS"
        )

    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    try:
        export(args.workbook_id, args.credentials)
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
