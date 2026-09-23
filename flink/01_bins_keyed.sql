CREATE MATERIALIZED TABLE bins_keyed (
  bin_id STRING NOT NULL,
  district_id STRING,
  route_id STRING,
  capacity_litres INT,
  PRIMARY KEY (bin_id) NOT ENFORCED
) AS
SELECT
  COALESCE(bin_id, '') AS bin_id,
  district_id,
  route_id,
  capacity_litres
FROM bin_registry;
