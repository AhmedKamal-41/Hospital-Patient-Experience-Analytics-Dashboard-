"""Per-dataset ingest functions and CLI.

CLI:
    python -m src.ingest --dataset hcahps
    python -m src.ingest --all
    python -m src.ingest --dataset hcahps --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src import db
from src.cms_client import CMSClient
from src.column_maps import DATASETS, DatasetConfig

log = logging.getLogger("ingest")


# ----------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    _SKIP = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k not in self._SKIP and not k.startswith("_"):
                payload[k] = v
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(level)
    fmt = _JsonFormatter()
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(fmt)
    root.addHandler(stdout)
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    file_h = logging.FileHandler(log_dir / "ingest.log")
    file_h.setFormatter(fmt)
    root.addHandler(file_h)


# ----------------------------------------------------------------------
# CSV → row tuple iterator
# ----------------------------------------------------------------------

def _validate_headers(
    csv_headers: list[str], expected_map: dict[str, str], dataset_name: str
) -> None:
    csv_set = set(csv_headers)
    expected_set = set(expected_map.keys())
    missing = expected_set - csv_set
    if missing:
        raise RuntimeError(
            f"[{dataset_name}] CMS CSV is missing expected columns: "
            f"{sorted(missing)} — column_maps.py needs updating"
        )
    extra = csv_set - expected_set
    if extra:
        log.warning(
            "extra_csv_columns_ignored",
            extra={"dataset": dataset_name, "columns": sorted(extra)},
        )


def _rows_from_csv(
    client: CMSClient,
    download_url: str,
    column_map: dict[str, str],
    dataset_name: str,
) -> Iterator[tuple[Any, ...]]:
    db_columns = list(column_map.values())
    csv_columns = list(column_map.keys())
    headers_checked = False

    for raw_row in client.stream_csv(download_url):
        if not headers_checked:
            _validate_headers(list(raw_row.keys()), column_map, dataset_name)
            headers_checked = True
        # Empty strings → NULL so raw text values stay as ""<BLANK>"" only when
        # CMS literally sent a blank; csv.DictReader gives "" for empty cells.
        out: list[Any] = []
        for csv_col in csv_columns:
            v = raw_row.get(csv_col, "")
            out.append(v if v != "" else None)
        yield tuple(out)


# ----------------------------------------------------------------------
# Per-dataset ingest
# ----------------------------------------------------------------------

def ingest_one(
    dataset_name: str,
    *,
    client: CMSClient | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    if dataset_name not in DATASETS:
        raise KeyError(f"unknown dataset {dataset_name!r}")
    cfg: DatasetConfig = DATASETS[dataset_name]
    client = client or CMSClient()
    started = time.monotonic()

    download_url, source_modified = client.resolve_download(cfg["dataset_id"])
    log.info(
        "resolved_download_url",
        extra={
            "dataset": dataset_name,
            "dataset_id": cfg["dataset_id"],
            "url": download_url,
            "source_modified_at": str(source_modified),
        },
    )

    if dry_run:
        # Use the paginated JSON endpoint just to surface a row count without
        # downloading the CSV. One round trip, limit=1.
        meta_page = client._query_page(cfg["dataset_id"], limit=1, offset=0)
        total = int(meta_page.get("count", 0))
        log.info(
            "dry_run",
            extra={
                "dataset": dataset_name,
                "rows_to_fetch": total,
                "url": download_url,
            },
        )
        return {
            "dataset": dataset_name,
            "dry_run": True,
            "rows_to_fetch": total,
            "url": download_url,
        }

    conn = db.get_connection()
    log_id = db.start_ingest_log(conn, cfg["dataset_id"])
    rows_inserted = rows_updated = 0
    status = "running"
    error_message: str | None = None
    try:
        with db.transaction(conn):
            temp_table = f"_tmp_{dataset_name}"
            db.create_temp_like(conn, cfg["table"], temp_table)

            db_cols = list(cfg["column_map"].values())
            row_iter = _rows_from_csv(
                client, download_url, cfg["column_map"], dataset_name
            )
            copied = db.copy_batches_to_temp(
                conn, temp_table, db_cols, row_iter, batch_size=1000
            )
            log.info(
                "csv_copied_to_temp",
                extra={"dataset": dataset_name, "rows_copied": copied},
            )

            rows_inserted, rows_updated = db.upsert_from_temp(
                conn,
                cfg["table"],
                temp_table,
                pk_columns=cfg["pk_columns"],
                data_columns=db_cols,
                source_modified_at=source_modified,
            )
        status = "success"
    except Exception as exc:
        status = "failed"
        error_message = repr(exc)
        log.exception(
            "ingest_failed",
            extra={"dataset": dataset_name},
        )
        raise
    finally:
        db.finish_ingest_log(
            conn,
            log_id,
            status=status,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            error_message=error_message,
        )
        runtime = time.monotonic() - started
        log.info(
            "ingest_finished",
            extra={
                "dataset": dataset_name,
                "status": status,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "runtime_sec": round(runtime, 2),
            },
        )
        conn.close()

    return {
        "dataset": dataset_name,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
    }


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.ingest",
        description="Land CMS Care Compare datasets into raw.* tables.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--dataset",
        choices=sorted(DATASETS.keys()),
        help="single dataset to ingest",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="ingest every dataset in column_maps.DATASETS",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="resolve URL + report row count, do not write to the database",
    )
    args = parser.parse_args(argv)

    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))

    targets = sorted(DATASETS.keys()) if args.all else [args.dataset]
    failed = 0
    for name in targets:
        try:
            ingest_one(name, dry_run=args.dry_run)
        except Exception:
            failed += 1
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
