"""Ingest BCB SGS time series into a Postgres lakehouse-style warehouse.

Design notes (the long version lives in README.md):

* The DAG processes exactly ONE data interval per run. Nothing in here calls
  `datetime.now()` - every boundary comes from `data_interval_start` /
  `data_interval_end`. That is what makes backfill and re-runs correct.
* Each series is its own mapped task instance (`.expand`), so one failing
  series does not block the others and retries are scoped to the series that
  actually failed.
* Load is delete-insert over the interval, so re-running converges.
* Quality checks are a separate task per series: a red square there means
  "data arrived and is wrong", which is a different incident from a failed load.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pendulum
from airflow.sdk import Asset, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

from include.config import all_series_as_dicts
from include.quality import assert_quality, check_rows
from include.bcb_client import fetch_series
from include.storage import raw_path, read_rows, write_atomic
from include.warehouse import load_interval

log = logging.getLogger(__name__)

BRONZE = Asset("warehouse://bronze/bcb_series")

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=20),
}


@dag(
    dag_id="bcb_series_ingest",
    description="BCB SGS series -> raw landing -> bronze (idempotent, backfillable)",
    start_date=pendulum.datetime(2026, 8, 3, tz="America/Sao_Paulo"),  # ~6 semanas de catchup
    # Airflow 3 turns a bare cron string into a CronTriggerTimetable, whose
    # "data interval" has zero width (start == end == run time). This DAG is
    # interval-scoped, so the interval timetable is declared explicitly rather
    # than relying on the global `create_cron_data_intervals` setting.
    schedule=CronDataIntervalTimetable("0 6 * * 1-5", timezone="America/Sao_Paulo"),
    catchup=True,                 # Airflow 3 defaults this to False - be explicit
    max_active_runs=3,            # bounded parallelism when backfilling
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["bcb", "ingestion", "bronze"],
)
def bcb_series_ingest():

    @task
    def list_series() -> list[dict]:
        """Fan-out source. Adding a series to include/config.py is enough."""
        series = all_series_as_dicts()
        log.info("mapping over %d series", len(series))
        return series

    @task(
        pool="bcb_api",  # shared slot budget: respects the rate limit
        max_active_tis_per_dag=2,  # ... even during a wide backfill
        retries=5,
    )
    def fetch(series: dict, **context) -> dict:
        """Fetch ONE series for the interval this run owns, land it atomically."""
        if context.get("data_interval_start") is None:
            raise ValueError(
                "This DAG is interval-scoped: every run must own a data interval. "
                "Trigger it with `airflow dags trigger <dag> --logical-date <ts>` "
                "or via backfill."
            )
        start: date = context["data_interval_start"].date()
        end: date = context["data_interval_end"].date()

        # The API window is inclusive on both ends; the Airflow interval is
        # half-open. Subtract a day so we never pull a neighbour's rows.
        api_end = end - timedelta(days=1)

        observations = fetch_series(
            code=series["code"], name=series["name"], start=start, end=api_end
        )
        rows = [o.as_row() for o in observations]

        path = raw_path(series["name"], start)
        write_atomic(path, rows)
        return {"series": series, "path": str(path), "row_count": len(rows)}

    @task(outlets=[BRONZE])
    def load_bronze(fetched: dict, **context) -> dict:
        """Replace the interval in bronze. Re-running yields the same rows."""
        start: date = context["data_interval_start"].date()
        end: date = context["data_interval_end"].date()

        rows = read_rows(fetched["path"])
        written = load_interval(
            rows=rows,
            series_name=fetched["series"]["name"],
            interval_start=start,
            interval_end=end,
        )
        return {**fetched, "rows": rows, "written": written}

    @task
    def quality_check(loaded: dict, **context) -> None:
        """Fails loudly and separately when the data is wrong."""
        series = loaded["series"]
        results = check_rows(
            loaded["rows"],
            series_name=series["name"],
            frequency=series["frequency"],
            min_value=series["min_value"],
            max_value=series["max_value"],
            interval_start=context["data_interval_start"].date(),
            interval_end=context["data_interval_end"].date(),
        )
        for r in results:
            log.info("%s | %-26s | %s | %s", series["name"], r.name, "PASS" if r.passed else "FAIL", r.detail)
        assert_quality(results, series["name"])

    quality_check.expand(loaded=load_bronze.expand(fetched=fetch.expand(series=list_series())))


bcb_series_ingest()
