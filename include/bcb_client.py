"""Client for the BCB SGS (Sistema Gerenciador de Series Temporais) API.

Endpoint shape:
    GET https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados
        ?formato=json&dataInicial=DD/MM/YYYY&dataFinal=DD/MM/YYYY

Response shape:
    [{"data": "01/09/2026", "valor": "15.00"}, ...]

Quirks this module handles, because they are the actual work:
  * dates are DD/MM/YYYY, values are strings with a '.' decimal separator;
  * an empty window comes back two different ways: `[]` with HTTP 200, and
    bare HTTP 404. Weekends, holidays and monthly series queried on a daily
    grain hit both. Neither is a failure;
  * the API rate-limits aggressively and answers 429 without Retry-After;
  * occasional 5xx under load.

Network errors and 5xx/429 are retried with exponential backoff; 4xx other
than 429 and 404 fail fast, because retrying a malformed request never helps.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# SGS answers 404 - not an empty 200 - when a series has no observation inside
# the requested window. Monthly series queried on a daily grain hit this on
# most days. It is indistinguishable from "this series code does not exist",
# so the code is validated once at config level rather than per request, and
# here a 404 is read as "nothing in this window".
EMPTY_STATUS = {404}


class BcbApiError(RuntimeError):
    """Raised when the API cannot be read after exhausting retries."""


@dataclass(frozen=True)
class Observation:
    series_code: int
    series_name: str
    obs_date: date
    value: Decimal

    def as_row(self) -> dict:
        return {
            "series_code": self.series_code,
            "series_name": self.series_name,
            "obs_date": self.obs_date.isoformat(),
            "value": str(self.value),
        }


def _fmt(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def parse_payload(payload: list[dict], series_code: int, series_name: str) -> list[Observation]:
    """Turn the raw API payload into typed observations.

    Kept pure and separate from the HTTP call so it can be unit tested against
    fixtures without touching the network.
    """
    out: list[Observation] = []
    for i, item in enumerate(payload):
        raw_date = (item or {}).get("data")
        raw_value = (item or {}).get("valor")
        if raw_date is None or raw_value in (None, ""):
            log.warning("series %s: skipping record %d with missing field: %r", series_code, i, item)
            continue
        try:
            obs_date = datetime.strptime(raw_date, "%d/%m/%Y").date()
        except (ValueError, TypeError) as exc:
            raise ValueError(f"series {series_code}: unparseable date {raw_date!r}") from exc
        try:
            value = Decimal(str(raw_value).strip().replace(",", "."))
        except (InvalidOperation, AttributeError) as exc:
            raise ValueError(f"series {series_code}: unparseable value {raw_value!r}") from exc
        out.append(Observation(series_code, series_name, obs_date, value))

    out.sort(key=lambda o: o.obs_date)
    return out


def fetch_series(
    code: int,
    name: str,
    start: date,
    end: date,
    *,
    session: requests.Session | None = None,
    timeout: int = 30,
    max_attempts: int = 5,
    base_backoff: float = 1.5,
    sleep=time.sleep,
) -> list[Observation]:
    """Fetch one series for a closed [start, end] window.

    An empty list is a valid, expected result - never an error.
    """
    if start > end:
        raise ValueError(f"start {start} is after end {end}")

    sess = session or requests.Session()
    url = BASE_URL.format(code=code)
    params = {"formato": "json", "dataInicial": _fmt(start), "dataFinal": _fmt(end)}

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = sess.get(url, params=params, timeout=timeout)
        except requests.RequestException as exc:
            last_error = exc
            log.warning("series %s attempt %d/%d: network error: %s", code, attempt, max_attempts, exc)
        else:
            if resp.status_code == 200:
                # The API answers 200 with an HTML error page in some failure
                # modes, so a JSON decode failure is treated as retryable.
                try:
                    payload = resp.json()
                except ValueError as exc:
                    last_error = exc
                    log.warning("series %s attempt %d/%d: non-JSON 200 response", code, attempt, max_attempts)
                else:
                    if not isinstance(payload, list):
                        raise BcbApiError(f"series {code}: expected a list, got {type(payload).__name__}")
                    log.info("series %s: %d records for %s..%s", code, len(payload), start, end)
                    return parse_payload(payload, code, name)
            elif resp.status_code in EMPTY_STATUS:
                log.info("series %s: no observation in %s..%s (HTTP 404)", code, start, end)
                return []
            elif resp.status_code in RETRYABLE_STATUS:
                last_error = BcbApiError(f"HTTP {resp.status_code}")
                log.warning("series %s attempt %d/%d: HTTP %s", code, attempt, max_attempts, resp.status_code)
            else:
                # 4xx that is not 429: the request itself is wrong. Fail now.
                raise BcbApiError(
                    f"series {code}: HTTP {resp.status_code} for {start}..{end} - {resp.text[:200]}"
                )

        if attempt < max_attempts:
            delay = base_backoff ** attempt + random.uniform(0, 0.5)  # jitter
            sleep(delay)

    raise BcbApiError(f"series {code}: failed after {max_attempts} attempts") from last_error
