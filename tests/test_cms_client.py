"""Unit tests for src.cms_client. No network, no Postgres."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest
import requests

from src.cms_client import CMSClient, RetriableHTTPError


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make tenacity's exponential backoff finish instantly."""
    monkeypatch.setattr(time, "sleep", lambda *_a, **_k: None)


def _mock_response(
    *, status: int = 200, json_body: dict | None = None, text_body: str = ""
) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.ok = 200 <= status < 400
    resp.url = "https://example.test/x"
    resp.json.return_value = json_body or {}
    resp.text = text_body
    resp.encoding = "utf-8"

    def _raise() -> None:
        if status >= 400:
            raise requests.HTTPError(f"{status}", response=resp)

    resp.raise_for_status.side_effect = _raise
    resp.iter_lines.return_value = iter(text_body.splitlines())
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False
    return resp


def test_resolve_download_returns_first_distribution() -> None:
    session = MagicMock(spec=requests.Session)
    session.headers = {}
    session.get.return_value = _mock_response(
        json_body={
            "modified": "2026-01-26",
            "distribution": [
                {"downloadURL": "https://cms.test/data.csv"},
                {"downloadURL": "https://cms.test/other.csv"},
            ],
        }
    )
    client = CMSClient(session=session)
    url, modified = client.resolve_download("dgck-syfz")
    assert url == "https://cms.test/data.csv"
    assert modified is not None
    assert modified.year == 2026 and modified.month == 1 and modified.day == 26


def test_resolve_download_no_distribution_raises() -> None:
    session = MagicMock(spec=requests.Session)
    session.headers = {}
    session.get.return_value = _mock_response(
        json_body={"modified": "2026-01-26", "distribution": []}
    )
    client = CMSClient(session=session)
    with pytest.raises(ValueError, match="no distribution"):
        client.resolve_download("xyz")


def test_5xx_triggers_tenacity_retry_then_succeeds() -> None:
    session = MagicMock(spec=requests.Session)
    session.headers = {}
    session.get.side_effect = [
        _mock_response(status=502),
        _mock_response(status=502),
        _mock_response(
            status=200,
            json_body={
                "modified": "2026-01-26",
                "distribution": [{"downloadURL": "https://cms.test/x.csv"}],
            },
        ),
    ]
    client = CMSClient(session=session)
    meta = client.get_metadata("any")
    assert meta["distribution"][0]["downloadURL"] == "https://cms.test/x.csv"
    assert session.get.call_count == 3


def test_4xx_does_not_retry_and_raises() -> None:
    session = MagicMock(spec=requests.Session)
    session.headers = {}
    session.get.return_value = _mock_response(status=404)
    client = CMSClient(session=session)
    with pytest.raises(requests.HTTPError):
        client.get_metadata("missing")
    assert session.get.call_count == 1


def test_5xx_exhausts_retries_and_raises_retriable() -> None:
    session = MagicMock(spec=requests.Session)
    session.headers = {}
    session.get.return_value = _mock_response(status=503)
    client = CMSClient(session=session)
    with pytest.raises(RetriableHTTPError):
        client.get_metadata("flaky")
    assert session.get.call_count == 5  # stop_after_attempt(5)


def test_stream_csv_yields_dict_rows() -> None:
    session = MagicMock(spec=requests.Session)
    session.headers = {}
    csv_body = "Facility ID,State\n010001,AL\n010002,AL\n"
    resp = _mock_response(text_body=csv_body)
    session.get.return_value = resp

    client = CMSClient(session=session)
    rows = list(client.stream_csv("https://cms.test/x.csv"))
    assert rows == [
        {"Facility ID": "010001", "State": "AL"},
        {"Facility ID": "010002", "State": "AL"},
    ]
