CREATE MATERIALIZED TABLE dispatch_list AS
SELECT
  COALESCE(district_id, '') AS district_id,
  current_fill_pct,
  forecast_fill_pct,
  upper_bound_pct,
  CASE
    WHEN upper_bound_pct > 85 THEN 'DISPATCH'
    WHEN upper_bound_pct > 70 THEN 'WATCH'
    ELSE 'OK'
  END AS action
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY district_id ORDER BY `$rowtime` DESC) AS row_num
  FROM district_forecast
)
WHERE row_num = 1;
