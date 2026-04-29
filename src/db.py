"""PostgreSQL helpers: connection from .env, batched COPY, upsert from temp.

Stays small on purpose. The pipeline-level orchestration lives in ingest.py.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg import sql

log = logging.getLogger(__name__)


def get_connection() -> psycopg.Connection:
    """Open a psycopg3 connection from PG* environment variables."""
    load_dotenv(override=False)
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        autocommit=False,
    )


@contextmanager
def transaction(conn: psycopg.Connection):
    """Outer-transaction wrapper that commits on success, rolls back on error."""
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def create_temp_like(
    conn: psycopg.Connection, target_table: str, temp_table: str
) -> None:
    """CREATE TEMP TABLE temp (LIKE target). Auto-dropped at txn end."""
    schema, table = _split_qualified(target_table)
    stmt = sql.SQL(
        "CREATE TEMP TABLE {temp} (LIKE {tgt} INCLUDING DEFAULTS) "
        "ON COMMIT DROP"
    ).format(
        temp=sql.Identifier(temp_table),
        tgt=sql.Identifier(schema, table),
    )
    with conn.cursor() as cur:
        cur.execute(stmt)


def copy_batches_to_temp(
    conn: psycopg.Connection,
    temp_table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    batch_size: int = 1000,
) -> int:
    """Stream `rows` into `temp_table` via psycopg COPY in batches.

    Returns the total row count copied. One COPY per batch keeps memory
    pressure flat and gives the pipeline a natural progress checkpoint.
    """
    total = 0
    batch: list[Sequence[Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            total += _copy_one_batch(conn, temp_table, columns, batch)
            batch.clear()
    if batch:
        total += _copy_one_batch(conn, temp_table, columns, batch)
    return total


def _copy_one_batch(
    conn: psycopg.Connection,
    temp_table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> int:
    copy_stmt = sql.SQL("COPY {tbl} ({cols}) FROM STDIN").format(
        tbl=sql.Identifier(temp_table),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )
    with conn.cursor() as cur, cur.copy(copy_stmt) as copy:
        for row in rows:
            copy.write_row(row)
    return len(rows)


def upsert_from_temp(
    conn: psycopg.Connection,
    target_table: str,
    temp_table: str,
    pk_columns: Sequence[str],
    data_columns: Sequence[str],
    source_modified_at: datetime | None,
) -> tuple[int, int]:
    """INSERT ... SELECT ... ON CONFLICT DO UPDATE from temp into target.

    Returns (rows_inserted, rows_updated). Uses the `xmax = 0` trick:
    after `INSERT ... ON CONFLICT DO UPDATE`, xmax is 0 only on freshly
    inserted rows.
    """
    schema, table = _split_qualified(target_table)
    non_pk = [c for c in data_columns if c not in pk_columns]

    insert_cols = list(data_columns) + ["_ingested_at", "_source_modified_at"]
    select_cols = list(data_columns) + ["now()", "%s"]

    set_clause = sql.SQL(", ").join(
        [
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c))
            for c in non_pk
        ]
        + [
            sql.SQL("_ingested_at = now()"),
            sql.SQL("_source_modified_at = EXCLUDED._source_modified_at"),
        ]
    )

    stmt = sql.SQL(
        """
        WITH upsert AS (
            INSERT INTO {tgt} ({insert_cols})
            SELECT {select_cols} FROM {tmp}
            ON CONFLICT ({pk}) DO UPDATE SET {set_clause}
            RETURNING (xmax = 0) AS inserted
        )
        SELECT
            COALESCE(SUM((inserted)::int), 0)::int AS rows_inserted,
            COALESCE(SUM((NOT inserted)::int), 0)::int AS rows_updated
        FROM upsert
        """
    ).format(
        tgt=sql.Identifier(schema, table),
        tmp=sql.Identifier(temp_table),
        insert_cols=sql.SQL(", ").join(sql.Identifier(c) for c in insert_cols),
        select_cols=sql.SQL(", ").join(
            sql.Identifier(c) if c not in ("now()", "%s") else sql.SQL(c)
            for c in select_cols
        ),
        pk=sql.SQL(", ").join(sql.Identifier(c) for c in pk_columns),
        set_clause=set_clause,
    )

    with conn.cursor() as cur:
        cur.execute(stmt, (source_modified_at,))
        row = cur.fetchone()
    return (int(row[0]), int(row[1])) if row else (0, 0)


# ------------------------------------------------------------------
# ingest_log helpers
# ------------------------------------------------------------------

def start_ingest_log(conn: psycopg.Connection, dataset_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.ingest_log (dataset_id, started_at, status)
            VALUES (%s, now(), 'running')
            RETURNING id
            """,
            (dataset_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("ingest_log INSERT returned no row")
        log_id = int(row[0])
    conn.commit()
    return log_id


def finish_ingest_log(
    conn: psycopg.Connection,
    log_id: int,
    status: str,
    rows_inserted: int | None = None,
    rows_updated: int | None = None,
    error_message: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raw.ingest_log
            SET finished_at = now(),
                status = %s,
                rows_inserted = %s,
                rows_updated = %s,
                error_message = %s
            WHERE id = %s
            """,
            (status, rows_inserted, rows_updated, error_message, log_id),
        )
    conn.commit()


def _split_qualified(name: str) -> tuple[str, str]:
    if "." not in name:
        raise ValueError(f"expected schema.table, got {name!r}")
    schema, table = name.split(".", 1)
    return schema, table
