# data-hunter

Experiments for iteratively exploring data

## attributor.py

Anomaly attribution for DuckDB metric data. Given a metric and an anomalous time window, breaks down which dimension values (and combinations) are responsible for the spike or dip.

### Usage

```
uv run attributor.py --metric <column> --time-dimension <day|week|month> --anomaly-date <YYYY-MM-DD> [options]
```

### Arguments

| Argument | Required | Description |
|---|---|---|
| `--metric` | yes | Metric column to analyze (e.g. `widget_views`) |
| `--time-dimension` | yes | Aggregation granularity: `day`, `week`, or `month` |
| `--anomaly-date` | yes | Start of the anomalous period |
| `--anomaly-end` | no | End of anomalous period, inclusive. Defaults to `--anomaly-date` |
| `--backend` | no | Storage backend: `duckdb` or `databricks` (default: `duckdb`) |
| `--table` | no | Table name (default: `records`) |
| `--db` | no | Path to DuckDB file (default: `data.db`) |
| `--host` | no* | Databricks server hostname |
| `--http-path` | no* | Databricks warehouse HTTP path |
| `--catalog` | no | Databricks catalog (optional) |
| `--schema` | no | Databricks schema (optional) |
| `--date-field` | no | Name of the date/timestamp column (default: `session_date`) |
| `--top-n` | no | Results to show per combination level (default: 3) |
| `--combo-depth` | no | Max number of dimensions to combine (default: all) |

\* Required when `--backend databricks`

### Examples

Single day (DuckDB):
```
uv run attributor.py --metric widget_views --time-dimension day --anomaly-date 2026-01-30
```

Date range:
```
uv run attributor.py --metric widget_views --time-dimension day \
  --anomaly-date 2026-01-30 --anomaly-end 2026-01-31
```

Pairs only:
```
uv run attributor.py --metric widget_views --time-dimension day \
  --anomaly-date 2026-01-30 --combo-depth 2
```

Custom date column name:
```
uv run attributor.py --metric widget_views --time-dimension day \
  --anomaly-date 2026-01-30 --date-field event_date
```

Databricks SQL warehouse (browser OAuth triggered on first run):
```
uv run attributor.py --backend databricks \
  --metric widget_views --time-dimension day --anomaly-date 2026-01-30 \
  --host <workspace>.azuredatabricks.net \
  --http-path /sql/1.0/warehouses/<id> \
  --catalog my_catalog --schema my_schema --table records
```

### How it works

1. **Single-dimension attribution** — for each dimension, computes each segment's contribution to the total anomaly delta vs a scaled baseline.
2. **Combination attribution** — drills into 2-way, 3-way, ... N-way combinations to pinpoint intersecting segments with the largest contribution.

Contribution is defined as:

```
contribution(segment) = actual(segment) - expected(segment)
expected(segment)     = mean daily value in baseline × number of anomaly days
```

Dimensions are auto-detected from the schema (non-metric, non-date columns).

### Data format

Expects a table named `records` (or the value of `--table`) containing at least one numeric metric column, one date column (default: `session_date`, configurable via `--date-field`), and any number of string dimension columns. Works with both DuckDB files and Databricks SQL warehouses.
