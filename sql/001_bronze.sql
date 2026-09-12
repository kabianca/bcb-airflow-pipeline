-- Bronze: raw observations, one row per (series, date), replaced per interval.
-- Idempotency comes from the DELETE+INSERT in include/warehouse.py, but the
-- unique constraint makes a double-insert bug fail loudly instead of silently
-- duplicating.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.bcb_series (
    series_code     integer        NOT NULL,
    series_name     text           NOT NULL,
    obs_date        date           NOT NULL,
    value           numeric(18, 6) NOT NULL,
    interval_start  date           NOT NULL,   -- which run owns this row
    loaded_at       timestamptz    NOT NULL DEFAULT now(),
    CONSTRAINT bcb_series_pk PRIMARY KEY (series_name, obs_date)
);

CREATE INDEX IF NOT EXISTS bcb_series_interval_idx
    ON bronze.bcb_series (series_name, interval_start);

COMMENT ON TABLE bronze.bcb_series IS
    'Raw BCB SGS observations. Replaced per Airflow data interval; safe to re-run.';
