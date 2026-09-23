CREATE MATERIALIZED TABLE bin_events_enriched AS
SELECT
  b.bin_id,
  b.fill_pct,
  b.battery_pct,
  k.district_id,
  k.route_id,
  k.capacity_litres
FROM bin_telemetry b
JOIN bins_keyed FOR SYSTEM_TIME AS OF b.`$rowtime` AS k
  ON b.bin_id = k.bin_id;
