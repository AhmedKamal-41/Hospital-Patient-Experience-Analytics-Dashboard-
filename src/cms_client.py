"""HTTP client for the CMS Provider Data Catalog DKAN API.

This module knows nothing about PostgreSQL. It:
  - resolves dataset metadata (incl. the bulk CSV downloadURL)
  - streams the CSV row-by-row as dicts
  - exposes a paginated JSON fallback for completeness

Retries on 5xx and connection errors via tenacity. 4xx is fatal.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterator
from datetime import date, datetime, timezone
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

log = logging.getLogger(__name__)


class RetriableHTTPError(requests.HTTPError):
    """Raised for 5xx so tenacity retries; 4xx surfaces as plain HTTPError."""


def _raise_for_status(resp: requests.Response) -> None:
    if 500 <= resp.status_code < 600:
        raise RetriableHTTPError(
            f"{resp.status_code} from {resp.url}", response=resp
        )
    resp.raise_for_status()


_RETRY = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception_type(
        (RetriableHTTPError, requests.ConnectionError, requests.Timeout)
    ),
    reraise=True,
)


class CMSClient:
    def __init__(
        self,
        base_url: str = "https://data.cms.gov/provider-data/api/1",
        page_size: int = 5000,
        timeout: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "hospital-dashboard/0.1")

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------
    @_RETRY
    def get_metadata(self, dataset_id: str) -> dict[str, Any]:
        url = f"{self.base_url}/metastore/schemas/dataset/items/{dataset_id}"
        resp = self.session.get(url, timeout=self.timeout)
        _raise_for_status(resp)
        return resp.json()

    def resolve_download(self, dataset_id: str) -> tuple[str, datetime | None]:
        """Return (download_url, source_modified_at) for the first distribution.

        The downloadURL has a content-hash suffix that changes every refresh,
        so callers must NOT cache it across runs.
        """
        meta = self.get_metadata(dataset_id)
        distributions = meta.get("distribution") or []
        if not distributions:
            raise ValueError(f"no distribution on dataset {dataset_id}")
        download_url = distributions[0].get("downloadURL")
        if not download_url:
            raise ValueError(f"no downloadURL on dataset {dataset_id}")
        modified = _parse_modified(meta.get("modified"))
        return download_url, modified

    # ------------------------------------------------------------------
    # Bulk CSV (default ingest path)
    # ------------------------------------------------------------------
    @_RETRY
    def stream_csv(self, url: str) -> Iterator[dict[str, str]]:
        """Yield CSV rows as dicts. Streams over the wire; constant memory."""
        with self.session.get(url, timeout=self.timeout, stream=True) as resp:
            _raise_for_status(resp)
            resp.encoding = resp.encoding or "utf-8"
            line_iter = resp.iter_lines(decode_unicode=True)
            reader = csv.DictReader(line_iter)
            yield from reader

    # ------------------------------------------------------------------
    # Paginated JSON (fallback only — kept for future use)
    # ------------------------------------------------------------------
    @_RETRY
    def _query_page(
        self, dataset_id: str, limit: int, offset: int
    ) -> dict[str, Any]:
        url = f"{self.base_url}/datastore/query/{dataset_id}/0"
        resp = self.session.get(
            url,
            params={"limit": limit, "offset": offset},
            timeout=self.timeout,
        )
        _raise_for_status(resp)
        return resp.json()

    def query_all(self, dataset_id: str) -> Iterator[dict[str, Any]]:
        offset = 0
        first = self._query_page(dataset_id, self.page_size, offset)
        total = int(first.get("count", 0))
        log.info(
            "datastore_paginated_total",
            extra={"dataset_id": dataset_id, "total_rows": total},
        )
        for row in first.get("results", []):
            yield row
        offset += self.page_size
        while offset < total:
            page = self._query_page(dataset_id, self.page_size, offset)
            for row in page.get("results", []):
                yield row
            offset += self.page_size


def _parse_modified(value: str | None) -> datetime | None:
    if not value:
        return None
    # CMS publishes dates as YYYY-MM-DD with no time component.
    try:
        d = date.fromisoformat(value)
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            log.warning("cms_unparseable_modified", extra={"value": value})
            return None
