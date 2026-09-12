"""Warehouse loads.

The whole idempotency story lives in `load_interval`: every run DELETEs the
window it owns and re-INSERTs it, inside one transaction. Re-running any
interval - by backfill, by clearing a task, by a retry after a partial failure -
converges to the same rows. No upsert key gymnastics, no duplicates.

The alternative (INSERT ... ON CONFLICT) is fine too, but delete-insert also
removes rows that the source has *retracted*, which the BCB does revise.
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
    """Replace [interval_start, interval_end) for one series. Returns rows written.

    Note the half-open interval: Airflow's data_interval_end is exclusive, so
    using `>= start AND < end` means adjacent runs can never delete each
    other's rows.
    """
    with connection(dsn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {BRONZE_TABLE} "
                "WHERE series_name = %s AND obs_date >= %s AND obs_date < %s",
                (series_name, interval_start, interval_end),
            )
            deleted = cur.rowcount

            if rows:
                cur.executemany(
                    f"INSERT INTO {BRONZE_TABLE} "
                    "(series_code, series_name, obs_date, value, interval_start, loaded_at) "
                    "VALUES (%s, %s, %s, %s, %s, now())",
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


def count_interval(
    series_name: str, interval_start: date, interval_end: date, *, dsn_str: str | None = None
) -> int:
    with connection(dsn_str) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) AS n FROM {BRONZE_TABLE} "
            "WHERE series_name = %s AND obs_date >= %s AND obs_date < %s",
            (series_name, interval_start, interval_end),
        )
        return cur.fetchone()["n"]


def run_sql_file(path: str, *, dsn_str: str | None = None) -> None:
    with open(path, encoding="utf-8") as fh:
        sql = fh.read()
    with connection(dsn_str) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    log.info("executed %s", path)
