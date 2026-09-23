CREATE MATERIALIZED TABLE weather_latest (
  site_id STRING NOT NULL,
  temperature_c DOUBLE,
  precipitation_mm DOUBLE,
  wind_kph DOUBLE,
  PRIMARY KEY (site_id) NOT ENFORCED
) AS
SELECT
  'CITY' AS site_id,
  `current`.`temperature_2m` AS temperature_c,
  `current`.`precipitation`  AS precipitation_mm,
  `current`.`wind_speed_10m` AS wind_kph
FROM weather_obs;
