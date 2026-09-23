# BinCast

Forecasting bin collection demand from IoT fill sensors and pedestrian footfall,
built on Confluent Cloud.

## The problem

Councils collect street bins on a fixed timetable. That timetable is wrong in
two directions at the same time. Trucks visit half empty bins in quiet
districts, which wastes fuel, crew hours and mileage. Bins on a busy high street
overflow for hours before their scheduled slot, which generates complaints and
litter.

Fill level sensors on their own do not solve this. Knowing a bin is 80 percent
full right now is already too late to re-route a truck that has left the depot.

## What this does

BinCast streams bin fill telemetry and footfall counts into Confluent Cloud,
joins them against the council's bin registry, and uses Flink's `ML_FORECAST` to
predict fill levels forward over a service horizon. Districts predicted to cross
the overflow threshold are flagged for collection before they actually overflow.

The lead time between the forecast crossing the line and the bins actually
filling is the point of the system.

## Architecture

```
Python producers (Avro, Schema Registry)
  bin_registry    ---> bins_keyed ------\
  bin_telemetry   ------------------------> bin_events_enriched
  footfall_counts                                   |
                                                    v
HTTP Source connector                        district_forecast
  weather_obs     ---> weather_latest               |
                                                    v
                                              dispatch_list
```

## How it is built

**Ingest.** Three Python producers write Avro to Kafka. A fully managed HTTP
Source connector polls the Open-Meteo API every 60 seconds into `weather_obs`.

**Processing.** Five Flink SQL statements in `flink/`. A temporal join enriches
each telemetry reading with its district, route and capacity. A tumbling window
builds a per district time series. `ML_FORECAST` predicts each district
independently. A deduplication step turns the latest forecast into an action.

**Schemas.** Every topic carries a registered Avro schema. This is not optional:
Flink builds table columns from Schema Registry, and a topic without a schema
appears as a single opaque `BYTES` column that no statement can read.

Footfall is stored as anonymous counts per district with no device or person
identifiers, so no personal data enters the pipeline.

## Layout

```
producers/    data generators and Kafka helpers
  config.py          env loading, client config, topic names
  kafka_io.py        producer, Avro serializer, send helper
  sim.py             districts, bins, footfall curve, fill model
  create_topics.py   one off, Confluent Cloud does not auto create topics
  produce_registry.py
  produce_bins.py
  produce_footfall.py
  schemas/           Avro schema files
flink/        SQL statements, numbered in run order, see flink/README.md
terraform/    compute pool and connector, so they can be torn down cheaply
api/          optional FastAPI service, reads the Flink output topics
web/          optional single page dashboard
docs/         build guide
```

## Setup

Requires Python 3.12 and uv.

```bash
uv sync
```

Create `.env` from `.env.example` and fill in six values from Confluent Cloud.
Note that the cluster and Schema Registry use different API key pairs.

```
BOOTSTRAP_SERVERS=
KAFKA_API_KEY=
KAFKA_API_SECRET=
SCHEMA_REGISTRY_URL=
SR_API_KEY=
SR_API_SECRET=
```

Check both sets of credentials before going further:

```bash
uv run python -m producers.test_connection
```

## Running

Run everything from the project root. Running from inside `producers/` breaks
both the imports and the `.env` lookup.

```bash
# once
uv run python -m producers.create_topics
uv run python -m producers.produce_registry

# leave running
uv run python -m producers.produce_bins
uv run python -m producers.produce_footfall
```

Then run the Flink statements in order. See `flink/README.md`.

The optional dashboard:

```bash
uv run uvicorn api.main:app --port 8000
```

## The simulation

Six districts with different footfall profiles, from a station forecourt at 2.2x
to a docks area at 0.4x. Twenty four bins, four per district, with capacities of
120, 240 and 360 litres, split across two collection routes.

One wall clock second is one simulated minute, so a full day cycle takes 24
minutes. Footfall follows three peaks across the day, at roughly 08:30, 13:00
and 17:30. Bin fill accumulates from footfall and resets when a bin passes 95
percent, which stands in for a collection.

`sim.spike("D2")` fills one district's bins immediately. Use it to drive a
district into the DISPATCH state without waiting for the footfall curve to
climb, which is the quickest way to exercise the forecast path end to end.

## Verifying it works

| Check | Expected |
|---|---|
| `uv run python -m producers.test_connection` | Kafka and Schema Registry both report OK |
| Topics, `bin_registry`, Schema tab | An Avro schema is registered |
| `SELECT * FROM bins_keyed;` | 24 bins |
| `SELECT * FROM bin_events_enriched;` | `district_id` is not null on every row |
| `SELECT * FROM district_forecast;` | Rows appear after about 100 seconds |
| `SELECT * FROM dispatch_list;` | 6 rows, one per district |

If `bin_telemetry` has a schema registered but Flink shows the column as
`BYTES`, the producer is not using `AvroSerializer`. Nothing downstream will
work until that is fixed.

## Stopping

The compute pool and the connector bill continuously whether or not anything is
consuming the output. Stop the pool and pause the connector when the project is
idle.

```bash
cd terraform
terraform destroy \
  -target=confluent_flink_compute_pool.main \
  -target=confluent_connector.weather
```

Source topics, schemas and the cluster are left in place, so a later
`terraform apply` brings the pipeline back without reloading data. The
materialized tables go with the pool, so re-run the statements in `flink/`
afterwards. `ML_FORECAST` starts cold again and needs about 100 seconds before
it emits.
