"""Data quality assertions.

These run as their own Airflow task, on purpose: a quality failure should show
up as a distinct red square in the Grid view, not as a silent branch inside the
load task. When this task is red you know the data arrived and was wrong -
which is a different incident from the load itself failing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


class DataQualityError(AssertionError):
    """Raised when loaded data violates an expectation."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


def check_rows(
    rows: list[dict],
    *,
    series_name: str,
    frequency: str,
    min_value: float,
    max_value: float,
    interval_start: date,
    interval_end: date,
) -> list[CheckResult]:
    """Return one CheckResult per expectation. Pure - easy to unit test."""
    results: list[CheckResult] = []

    # 1. Emptiness is only an error for daily series on a business day.
    #    Monthly series are legitimately empty on most days, and so are
    #    weekends and holidays. Encoding that distinction is the point.
    is_weekend = interval_start.weekday() >= 5
    expect_rows = frequency == "daily" and not is_weekend
    results.append(
        CheckResult(
            "non_empty_when_expected",
            passed=bool(rows) or not expect_rows,
            detail=f"{len(rows)} rows, expect_rows={expect_rows}",
        )
    )

    # 2. No duplicate observation dates within the series.
    dates = [r["obs_date"] for r in rows]
    dupes = {d for d in dates if dates.count(d) > 1}
    results.append(
        CheckResult("no_duplicate_dates", passed=not dupes, detail=f"duplicates: {sorted(dupes)}")
    )

    # 3. Every observation falls inside the interval this run owns.
    outside = [
        d for d in dates
        if not (interval_start.isoformat() <= d < interval_end.isoformat())
    ]
    results.append(
        CheckResult("dates_within_interval", passed=not outside, detail=f"outside: {outside[:5]}")
    )

    # 4. Values parse and sit inside sane bounds - catches scale/parsing bugs
    #    (a rate read as 1500 instead of 15.00), not real market moves.
    bad = []
    for r in rows:
        try:
            v = Decimal(str(r["value"]))
        except Exception:
            bad.append((r["obs_date"], r["value"], "unparseable"))
            continue
        if not (Decimal(str(min_value)) <= v <= Decimal(str(max_value))):
            bad.append((r["obs_date"], str(v), "out_of_range"))
    results.append(
        CheckResult("values_in_range", passed=not bad, detail=f"offenders: {bad[:5]}")
    )

    # 5. No nulls in key columns.
    missing = [r for r in rows if not r.get("series_name") or not r.get("obs_date")]
    results.append(
        CheckResult("no_null_keys", passed=not missing, detail=f"{len(missing)} rows with null keys")
    )

    return results


def assert_quality(results: list[CheckResult], series_name: str) -> None:
    failed = [r for r in results if not r.passed]
    if failed:
        lines = "\n".join(f"  - {r.name}: {r.detail}" for r in failed)
        raise DataQualityError(f"{series_name}: {len(failed)} check(s) failed\n{lines}")
