"""Tests for the quality rules and the atomic landing-zone write."""

from __future__ import annotations

import json
from datetime import date

import pytest

from include.quality import DataQualityError, assert_quality, check_rows
from include.storage import raw_path, read_rows, write_atomic

MON = date(2026, 9, 7)   # Monday
TUE = date(2026, 9, 8)
SAT = date(2026, 9, 5)   # Saturday
SUN = date(2026, 9, 6)


def rows(*pairs):
    return [
        {"series_code": 1, "series_name": "usd_brl_ptax", "obs_date": d, "value": v}
        for d, v in pairs
    ]


def run(rows_, *, frequency="daily", start=MON, end=TUE, lo=0.5, hi=50.0):
    return check_rows(
        rows_, series_name="usd_brl_ptax", frequency=frequency,
        min_value=lo, max_value=hi, interval_start=start, interval_end=end,
    )


def failed(results):
    return {r.name for r in results if not r.passed}


def test_happy_path_passes_everything():
    assert failed(run(rows(("2026-09-07", "5.42")))) == set()


def test_empty_on_a_business_day_fails():
    assert "non_empty_when_expected" in failed(run([]))


def test_empty_on_a_weekend_is_fine():
    """The rule that makes this project not-a-tutorial: absence is expected."""
    assert failed(run([], start=SAT, end=SUN)) == set()


def test_empty_monthly_series_on_a_daily_grain_is_fine():
    assert failed(run([], frequency="monthly")) == set()


def test_duplicate_dates_fail():
    assert "no_duplicate_dates" in failed(run(rows(("2026-09-07", "5.4"), ("2026-09-07", "5.5"))))


def test_row_outside_the_interval_fails():
    """Catches an off-by-one in the API window - the bug this design invites."""
    assert "dates_within_interval" in failed(run(rows(("2026-09-08", "5.4"))))


def test_interval_end_is_exclusive():
    assert "dates_within_interval" in failed(
        run(rows(("2026-09-08", "5.4")), start=MON, end=TUE)
    )


def test_out_of_range_value_fails():
    """A rate parsed as 542 instead of 5.42 is a scale bug, not a market move."""
    assert "values_in_range" in failed(run(rows(("2026-09-07", "542"))))


def test_unparseable_value_fails():
    assert "values_in_range" in failed(run(rows(("2026-09-07", "n/a"))))


def test_assert_quality_raises_with_detail():
    with pytest.raises(DataQualityError, match="no_duplicate_dates"):
        assert_quality(run(rows(("2026-09-07", "5.4"), ("2026-09-07", "5.5"))), "usd_brl_ptax")


# ---------- storage ----------

def test_raw_path_is_hive_partitioned(tmp_path):
    p = raw_path("selic_meta", MON, root=tmp_path)
    assert p.parent.name == "dt=2026-09-07"
    assert p.parent.parent.name == "selic_meta"


def test_write_is_atomic_and_roundtrips(tmp_path):
    p = raw_path("selic_meta", MON, root=tmp_path)
    data = rows(("2026-09-07", "15.00"))
    write_atomic(p, data)
    assert read_rows(p) == data
    # no temp files left behind
    assert [f.name for f in p.parent.iterdir()] == ["data.json"]


def test_rewriting_the_same_interval_overwrites(tmp_path):
    """Deterministic paths are half of the idempotency story."""
    p = raw_path("selic_meta", MON, root=tmp_path)
    write_atomic(p, rows(("2026-09-07", "15.00")))
    write_atomic(p, rows(("2026-09-07", "14.75")))
    assert json.loads(p.read_text())[0]["value"] == "14.75"
    assert len(list(p.parent.iterdir())) == 1
