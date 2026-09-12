"""Warehouse loads.

Idempotency has two halves:
  * DELETE by `interval_start` - each run reclaims exactly the rows it
    wrote before, whatever date the source assigned them;
  * INSERT ... ON CONFLICT DO UPDATE - a row that another run already
    claimed is re-pointed at this one instead of raising on the primary
    key. Needed because the BCB returns a monthly observation, dated the
    1st, for more than one daily window.
Together they make any interval replayable, in any order, any number of
times.

The DELETE half is what handles *retractions*: the BCB revises published
series, and a row that disappears from the source window is removed rather
than left behind as a stale orphan. A bare upsert would never notice.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import date

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger(__name__)

BRONZE_TABLE = "bronze.bcb_series"


def dsn() -> str:
    """Warehouse DSN. Deliberately NOT the Airflow metadata database."""
    return os.environ["WAREHOUSE_DSN"]


@contextmanager
def connection(dsn_str: str | None = None):
    conn = psycopg.connect(dsn_str or dsn(), row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def load_interval(
    rows: list[dict],
    series_name: str,
    interval_start: date,
    interval_end: date,
    *,
    dsn_str: str | None = None,
) -> int:
    """Replace this run's slice of one series. Returns rows written.

    See the module docstring for why idempotency needs both the DELETE and the
    ON CONFLICT.
    """
    with connection(dsn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                # Delete by the interval that OWNS the rows, not by observation
                # date. A monthly series is dated by the source on the 1st of
                # the reference month, so a run whose window is the 3rd still
                # inserts a row dated the 1st - deleting by obs_date would never
                # reclaim it and the re-insert would collide with the PK.
                f"DELETE FROM {BRONZE_TABLE} "
                "WHERE series_name = %s AND interval_start = %s",
                (series_name, interval_start),
            )
            deleted = cur.rowcount

            if rows:
                cur.executemany(
                    f"INSERT INTO {BRONZE_TABLE} "
                    "(series_code, series_name, obs_date, value, interval_start, loaded_at) "
                    "VALUES (%s, %s, %s, %s, %s, now()) "
                    # A monthly observation is handed back by more than one
                    # daily window, so two different runs can legitimately
                    # claim the same (series, date). The later run takes
                    # ownership instead of colliding with the primary key.
                    "ON CONFLICT (series_name, obs_date) DO UPDATE SET "
                    "value = EXCLUDED.value, "
                    "interval_start = EXCLUDED.interval_start, "
                    "loaded_at = now()",
                    [
                        (
                            r["series_code"],
                            r["series_name"],
                            r["obs_date"],
                            r["value"],
                            interval_start,
                        )
                        for r in rows
                    ],
                )
        conn.commit()

    log.info(
        "%s [%s, %s): deleted %d, inserted %d",
        series_name, interval_start, interval_end, deleted, len(rows),
    )
    return len(rows)