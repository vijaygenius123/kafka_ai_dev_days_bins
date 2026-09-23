CREATE MATERIALIZED TABLE district_forecast AS
SELECT
  district_id,
  ts,
  current_fill               AS current_fill_pct,
  forecast[1].forecast_value AS forecast_fill_pct,
  forecast[1].upper_bound    AS upper_bound_pct
FROM (
  SELECT
    district_id,
    window_end AS ts,
    current_fill,
    ML_FORECAST(
      CAST(current_fill AS DOUBLE),
      window_end,
      JSON_OBJECT('minTrainingSize' VALUE 10, 'horizon' VALUE 5)
    ) OVER (
      PARTITION BY district_id
      ORDER BY window_time
    ) AS forecast
  FROM (
    SELECT
      district_id,
      window_end,
      window_time,
      AVG(fill_pct) AS current_fill
    FROM TABLE(
      TUMBLE(TABLE bin_events_enriched, DESCRIPTOR(`$rowtime`), INTERVAL '10' SECONDS)
    )
    GROUP BY district_id, window_start, window_end, window_time
  )
)
WHERE CARDINALITY(forecast) >= 1;
