-- Silver: the designed extension point.
--
-- This file ships as a VIEW rather than a materialised table on purpose: it
-- makes the project complete as-is (you can query silver today) while leaving
-- an obvious seam. The natural next step is to turn these into dbt models with
-- tests, or into an incremental table loaded by a second DAG triggered by the
-- BRONZE asset. See "Where this grows" in README.md.

CREATE SCHEMA IF NOT EXISTS silver;

-- Calendar-complete daily series: forward-fills weekends and holidays so
-- downstream joins do not silently drop days. This is the kind of decision
-- that belongs in silver, never in bronze - bronze stays faithful to source.
CREATE OR REPLACE VIEW silver.daily_series AS
WITH bounds AS (
    SELECT series_name, min(obs_date) AS from_date, max(obs_date) AS to_date
    FROM bronze.bcb_series
    GROUP BY series_name
),
calendar AS (
    SELECT b.series_name, gs::date AS obs_date
    FROM bounds b
    CROSS JOIN LATERAL generate_series(b.from_date, b.to_date, interval '1 day') gs
)
SELECT
    c.series_name,
    c.obs_date,
    s.value AS value_raw,
    -- last non-null value, i.e. carry the last published quote forward
    (array_agg(s.value) FILTER (WHERE s.value IS NOT NULL)
        OVER (PARTITION BY c.series_name ORDER BY c.obs_date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))[
        array_length(array_agg(s.value) FILTER (WHERE s.value IS NOT NULL)
            OVER (PARTITION BY c.series_name ORDER BY c.obs_date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1)
    ] AS value_filled,
    (s.value IS NULL) AS is_carried
FROM calendar c
LEFT JOIN bronze.bcb_series s
       ON s.series_name = c.series_name AND s.obs_date = c.obs_date;

COMMENT ON VIEW silver.daily_series IS
    'Calendar-complete view over bronze, with last-value-carried-forward.';
