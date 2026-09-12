"""Unit tests for the SGS client. No network: everything runs off fixtures."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
import requests

from include.bcb_client import BcbApiError, fetch_series, parse_payload

PAYLOAD = [
    {"data": "01/09/2026", "valor": "15.00"},
    {"data": "03/09/2026", "valor": "14.75"},
    {"data": "02/09/2026", "valor": "14.90"},
]


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text="", raise_json=False):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self._raise_json = raise_json

    def json(self):
        if self._raise_json:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    """Returns queued responses in order; records how many calls happened."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0
        self.last_params = None

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        self.last_params = params
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_parse_sorts_and_types():
    obs = parse_payload(PAYLOAD, 432, "selic_meta")
    assert [o.obs_date for o in obs] == [date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)]
    assert obs[0].value == Decimal("15.00")
    assert obs[0].series_name == "selic_meta"


def test_parse_skips_records_with_missing_fields():
    payload = PAYLOAD + [{"data": "04/09/2026"}, {"valor": "1.0"}, {}]
    assert len(parse_payload(payload, 432, "selic_meta")) == 3


def test_parse_raises_on_unparseable_date():
    with pytest.raises(ValueError, match="unparseable date"):
        parse_payload([{"data": "2026-09-01", "valor": "1"}], 432, "x")


def test_parse_accepts_comma_decimal():
    obs = parse_payload([{"data": "01/09/2026", "valor": "3,45"}], 1, "usd_brl_ptax")
    assert obs[0].value == Decimal("3.45")


def test_empty_window_is_not_an_error():
    """Weekends, holidays and monthly series on a daily grain all land here."""
    session = FakeSession([FakeResponse(200, [])])
    assert fetch_series(1, "usd_brl_ptax", date(2026, 9, 5), date(2026, 9, 6), session=session) == []


def test_retries_on_5xx_then_succeeds():
    session = FakeSession([
        FakeResponse(503),
        FakeResponse(500),
        FakeResponse(200, PAYLOAD),
    ])
    obs = fetch_series(432, "selic_meta", date(2026, 9, 1), date(2026, 9, 3),
                       session=session, sleep=lambda _: None)
    assert len(obs) == 3
    assert session.calls == 3


def test_retries_on_429():
    session = FakeSession([FakeResponse(429), FakeResponse(200, PAYLOAD)])
    fetch_series(432, "s", date(2026, 9, 1), date(2026, 9, 3), session=session, sleep=lambda _: None)
    assert session.calls == 2


def test_retries_on_network_error():
    session = FakeSession([requests.ConnectionError("boom"), FakeResponse(200, [])])
    fetch_series(432, "s", date(2026, 9, 1), date(2026, 9, 1), session=session, sleep=lambda _: None)
    assert session.calls == 2


def test_non_json_200_is_retried():
    session = FakeSession([FakeResponse(200, raise_json=True), FakeResponse(200, PAYLOAD)])
    obs = fetch_series(432, "s", date(2026, 9, 1), date(2026, 9, 3),
                       session=session, sleep=lambda _: None)
    assert len(obs) == 3


def test_4xx_fails_fast_without_retrying():
    session = FakeSession([FakeResponse(404, text="no such series")])
    with pytest.raises(BcbApiError, match="404"):
        fetch_series(999999, "nope", date(2026, 9, 1), date(2026, 9, 3),
                     session=session, sleep=lambda _: None)
    assert session.calls == 1  # did not waste retries on a permanent error


def test_gives_up_after_max_attempts():
    session = FakeSession([FakeResponse(503)] * 5)
    with pytest.raises(BcbApiError, match="failed after 5 attempts"):
        fetch_series(432, "s", date(2026, 9, 1), date(2026, 9, 3),
                     session=session, max_attempts=5, sleep=lambda _: None)


def test_inverted_window_is_rejected():
    with pytest.raises(ValueError, match="after end"):
        fetch_series(432, "s", date(2026, 9, 10), date(2026, 9, 1))


def test_dates_are_sent_in_bcb_format():
    session = FakeSession([FakeResponse(200, [])])
    fetch_series(432, "s", date(2026, 9, 1), date(2026, 9, 3), session=session)
    assert session.last_params["dataInicial"] == "01/09/2026"
    assert session.last_params["dataFinal"] == "03/09/2026"
