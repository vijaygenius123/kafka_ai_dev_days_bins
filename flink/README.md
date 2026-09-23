# Flink statements

Five statements that turn raw bin telemetry into a dispatch list. Run them in
numbered order. Each file holds one statement, so paste the whole file.

## Before you start

`produce_bins.py` has to be running, and it has to stay running while you work.
Flink advances event time from the timestamps on arriving records. If the
producer stops, watermarks stop, and every window downstream freezes without
reporting an error.

Set the Workspace context once at the top of the session and leave it alone:

```sql
USE CATALOG `<environment name>`;
USE `<cluster name>`;
```

The dropdowns and the `USE` statements both set this and they can drift apart.
Pinning it with `USE` avoids creating a table in one catalog and then failing to
find it from another.

## Run order

| File | What it does | Check afterwards |
|---|---|---|
| `00_teardown.sql` | Drops all five tables, reverse dependency order | `SHOW TABLES;` lists only source topics |
| `01_bins_keyed.sql` | Keys the registry into a lookup table | `SELECT * FROM bins_keyed;` gives 24 bins |
| `02_weather_latest.sql` | Reads the HTTP Source connector topic | `SELECT * FROM weather_latest;` gives 1 row |
| `03_enriched.sql` | Joins telemetry to the registry | `district_id` is not null |
| `04_forecast.sql` | Runs ML_FORECAST per district | Empty for about 100 seconds, then rows |
| `05_dispatch.sql` | Latest window per district, plus an action | 6 rows, one per district |

## What each statement does

### 01_bins_keyed

`bin_registry` arrives as an append only stream. A temporal join needs a
versioned table with a primary key, so this converts the stream into one row per
`bin_id` that updates in place.

`COALESCE(bin_id, '')` looks pointless because the producer never sends a null.
It is there because Flink infers the column as nullable, and a primary key
column cannot be nullable. COALESCE with a non null literal changes the inferred
type to NOT NULL.

Running `produce_registry.py` more than once is safe. The table is keyed, so
duplicates collapse. A `SELECT` may show more than 24 rows because it streams
the changelog, showing inserts and then updates. The table itself still holds 24.

### 02_weather_latest

Gives the HTTP Source connector a consumer. Without this statement nothing
reads `weather_obs`, so the connector writes to a topic that no part of the
pipeline uses.

The connector writes a nested Avro record, so the fields come out as
`current.temperature_2m` and so on. `current` needs backticks.

`site_id` is a constant. Weather covers one location, so there is no natural key,
but the table needs a primary key to act as a lookup.

### 03_enriched

The join. Every telemetry reading picks up `district_id`, `route_id` and
`capacity_litres` from the registry as of that record's event time.

If `district_id` comes back null on every row, the registry landed after
telemetry started. Re-run `produce_registry.py`, drop this table, and create it
again.

### 04_forecast

The forecast, and the part that matters most.

The inner query tumbles enriched events into 10 second windows and averages
`fill_pct` per district. `ML_FORECAST` then predicts each district separately
through `PARTITION BY district_id`. It returns an array of forecast points, so
`forecast[1]` takes the next window.

Six districts means six partitions, which keeps training fast and CFU use low.
Forecasting per bin instead would mean 24 partitions and a longer wait.

Reading the output: `current_fill_pct` is what just happened,
`forecast_fill_pct` is the prediction for the next window, and `upper_bound_pct`
is the top of the confidence range. A forecast above current means that district
is filling faster than it is being served.

### 05_dispatch

Deduplicates to the latest window per district with `ROW_NUMBER() = 1`, then
maps the forecast to an action. Above 85 is DISPATCH, above 70 is WATCH,
otherwise OK.

The threshold applies to `upper_bound_pct` rather than `forecast_fill_pct` on
purpose. Acting on the top of the confidence range means dispatching when
overflow is plausible, not waiting until it is likely.

## Things that went wrong while building this

These are all real errors from this build, with what fixed them.

**`DROP TABLE` says the table does not exist, but `CREATE` says it already does.**
These are materialized tables. `DROP TABLE` looks for a plain table and does not
match. Use `DROP MATERIALIZED TABLE`.

**`Invalid primary key 'PK_district_id'. Column 'district_id' is nullable.`**
Flink infers an upsert key from the `ROW_NUMBER() = 1` pattern and that key
cannot be nullable. Wrap the column in `COALESCE(district_id, '')`.

**`Currently the join key in Temporal Table Join can not be empty.`**
A temporal join needs an equality between a probe side column and the versioned
table's key. `ON w.site_id = 'CITY'` has no probe side column in it, so there is
no join key. Either carry a matching column on the probe side, or keep the two
chains separate, which is what this setup does.

**`SQL parse failed. Encountered "b" at line 3, column 102.`**
A newline was lost during copy and paste, so `capacity_litres` and `FROM` became
one identifier. Copy from these files rather than from a chat window or a
rendered document.

**`district_forecast` stays empty.**
Normal for about 100 seconds. `ML_FORECAST` emits nothing until
`minTrainingSize` windows exist for each partition key. Ten windows at ten
seconds each is 100 seconds. If it is still empty after three minutes, check
that `produce_bins.py` is running and that `bin_events_enriched` is gaining rows.

**The pipeline stalls with no error.**
Check Flink then Statements. Failed or stopped statements still hold CFUs, so a
small pool fills up and new statements queue instead of running. Delete dead
statements, then create the tables again.

## Cost and lifecycle

Every materialized table is a job that runs continuously and bills CFUs whether
or not anything reads its output. Five statements sit comfortably on a 10 CFU
pool, but stop the pool when the project is idle.

Stopping the pool drops the materialized tables with it. The source topics and
their schemas survive, so bringing the pipeline back means re-running `01`
through `05` and waiting out the `ML_FORECAST` warm up again. Nothing needs
reloading.

To change a statement, drop its table first. You cannot create over an existing
one, and editing in place is not supported.
