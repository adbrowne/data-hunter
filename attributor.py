# /// script
# dependencies = [
#   "duckdb",
#   "pandas",
#   "rich",
#   "databricks-sql-connector",
#   "databricks-sdk",
# ]
# ///

import argparse
import sys
from itertools import combinations

import duckdb
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich import box

console = Console()

NUMERIC_TYPES = {"INTEGER", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "INT", "LONG", "REAL"}
DATE_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMP_NTZ"}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Attribute a metric anomaly to dimension values."
    )
    parser.add_argument("--metric", required=True, help="Metric column to analyze")
    parser.add_argument(
        "--time-dimension",
        required=True,
        choices=["day", "week", "month"],
        help="Time granularity",
    )
    parser.add_argument(
        "--anomaly-date",
        required=True,
        help="Start of the anomalous period (e.g. 2026-01-30)",
    )
    parser.add_argument(
        "--anomaly-end",
        default=None,
        help="End of the anomalous period, inclusive (e.g. 2026-01-31). Defaults to --anomaly-date.",
    )
    parser.add_argument("--db", default="data.db", help="Path to DuckDB file")
    parser.add_argument(
        "--backend",
        default="duckdb",
        choices=["duckdb", "databricks"],
        help="Storage backend (default: duckdb)",
    )
    parser.add_argument(
        "--table",
        default="records",
        help="Table name (default: records)",
    )
    parser.add_argument("--host", default=None, help="Databricks server hostname")
    parser.add_argument("--http-path", default=None, help="Databricks warehouse HTTP path")
    parser.add_argument("--catalog", default=None, help="Databricks catalog (optional)")
    parser.add_argument("--schema", default=None, help="Databricks schema (optional)")
    parser.add_argument(
        "--top-n",
        type=int,
        default=3,
        help="Top N results to show per combination level (default: 3)",
    )
    parser.add_argument(
        "--combo-depth",
        type=int,
        default=None,
        help="Maximum number of dimensions to combine (default: all dimensions)",
    )
    return parser.parse_args()


def _load_duckdb(db_path: str, table: str, metric: str, time_dim: str) -> tuple[pd.DataFrame, list[str]]:
    trunc_expr = {
        "day": "session_date",
        "week": "DATE_TRUNC('week', session_date)",
        "month": "DATE_TRUNC('month', session_date)",
    }[time_dim]

    con = duckdb.connect(db_path, read_only=True)

    # Validate metric column exists
    cols = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
    if metric not in cols:
        console.print(
            f"[bold red]Error:[/] metric column '[yellow]{metric}[/]' not found in table.\n"
            f"Available columns: {', '.join(cols)}"
        )
        sys.exit(1)

    # Auto-detect dimension columns (non-metric, non-date, non-numeric)
    type_map = {r[0]: r[1] for r in con.execute(f"DESCRIBE {table}").fetchall()}
    dimensions = [
        c
        for c, t in type_map.items()
        if c != metric
        and t.upper() not in NUMERIC_TYPES
        and t.upper() not in DATE_TYPES
    ]

    df = con.execute(
        f"SELECT {trunc_expr} AS period, {', '.join(dimensions)}, SUM({metric}) AS metric_value "
        f"FROM {table} "
        f"GROUP BY {trunc_expr}, {', '.join(dimensions)}"
    ).df()
    con.close()

    df["period"] = pd.to_datetime(df["period"]).dt.date
    return df, dimensions


def _load_databricks(args, metric: str, time_dim: str) -> tuple[pd.DataFrame, list[str]]:
    from databricks.sdk import WorkspaceClient
    import databricks.sql

    trunc_expr = {
        "day": "session_date",
        "week": "DATE_TRUNC('week', session_date)",
        "month": "DATE_TRUNC('month', session_date)",
    }[time_dim]

    # Fully-qualify table name if catalog/schema provided
    if args.catalog and args.schema:
        full_table = f"{args.catalog}.{args.schema}.{args.table}"
        info_schema_filter = (
            f"table_catalog = '{args.catalog}' AND table_schema = '{args.schema}' "
            f"AND table_name = '{args.table}'"
        )
    elif args.schema:
        full_table = f"{args.schema}.{args.table}"
        info_schema_filter = (
            f"table_schema = '{args.schema}' AND table_name = '{args.table}'"
        )
    else:
        full_table = args.table
        info_schema_filter = f"table_name = '{args.table}'"

    # Authenticate via browser OAuth — SDK triggers browser flow on first run
    w = WorkspaceClient(host=f"https://{args.host}")
    # Access config to trigger auth; authenticate() returns headers dict
    w.config.authenticate()

    conn = databricks.sql.connect(
        server_hostname=args.host,
        http_path=args.http_path,
        credentials_provider=lambda: w.config.authenticate,
    )
    cursor = conn.cursor()

    # Schema introspection via information_schema
    cursor.execute(
        f"SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE {info_schema_filter}"
    )
    rows = cursor.fetchall()
    if not rows:
        console.print(
            f"[bold red]Error:[/] no columns found for table '[yellow]{full_table}[/]'.\n"
            f"Check --catalog, --schema, and --table arguments."
        )
        conn.close()
        sys.exit(1)

    type_map = {r[0]: r[1] for r in rows}
    cols = list(type_map.keys())

    if metric not in cols:
        console.print(
            f"[bold red]Error:[/] metric column '[yellow]{metric}[/]' not found in table.\n"
            f"Available columns: {', '.join(cols)}"
        )
        conn.close()
        sys.exit(1)

    dimensions = [
        c
        for c, t in type_map.items()
        if c != metric
        and t.upper() not in NUMERIC_TYPES
        and t.upper() not in DATE_TYPES
    ]

    cursor.execute(
        f"SELECT {trunc_expr} AS period, {', '.join(dimensions)}, SUM({metric}) AS metric_value "
        f"FROM {full_table} "
        f"GROUP BY {trunc_expr}, {', '.join(dimensions)}"
    )
    desc = cursor.description
    col_names = [d[0] for d in desc]
    df = pd.DataFrame(cursor.fetchall(), columns=col_names)
    conn.close()

    df["period"] = pd.to_datetime(df["period"]).dt.date
    return df, dimensions


def load_data(args, metric: str, time_dim: str) -> tuple[pd.DataFrame, list[str]]:
    if args.backend == "duckdb":
        return _load_duckdb(args.db, args.table, metric, time_dim)
    else:
        return _load_databricks(args, metric, time_dim)


def contribution_to_change(
    df: pd.DataFrame,
    dimension: str,
    anomaly_periods: set,
    n_anomaly_days: int,
    baseline_avg_total: float,
    actual_total: float,
) -> pd.DataFrame:
    """Compute per-segment contribution to the total anomaly delta.

    Baseline is scaled by n_anomaly_days so the comparison is over the same
    number of days as the anomaly window.
    """
    anomaly_delta = actual_total - baseline_avg_total

    actual = (
        df[df["period"].isin(anomaly_periods)]
        .groupby(dimension)["metric_value"]
        .sum()
        .rename("actual")
    )
    # mean daily value × anomaly window length = expected total over window
    baseline_avg = (
        df[~df["period"].isin(anomaly_periods)]
        .groupby(dimension)["metric_value"]
        .mean()
        .mul(n_anomaly_days)
        .rename("baseline_avg")
    )

    result = pd.concat([actual, baseline_avg], axis=1).fillna(0)
    result["contribution"] = result["actual"] - result["baseline_avg"]
    result["pct_of_anomaly"] = (
        result["contribution"] / anomaly_delta * 100 if anomaly_delta != 0 else 0
    )
    return result.sort_values("contribution", ascending=False)


def combo_contribution(
    df: pd.DataFrame,
    dims: list[str],
    anomaly_periods: set,
    n_anomaly_days: int,
    baseline_avg_total: float,
    actual_total: float,
    top_n: int,
    max_depth: int | None = None,
) -> dict[int, list[tuple]]:
    """Drill into combinations of dimensions up to max_depth.

    Returns a dict keyed by combination size (2, 3, ...) where each value is
    a list of (label, contribution, pct) tuples sorted by abs(contribution).
    """
    anomaly_delta = actual_total - baseline_avg_total
    depth = min(max_depth, len(dims)) if max_depth is not None else len(dims)
    results: dict[int, list[tuple]] = {}

    for r in range(2, depth + 1):
        level_results = []
        for dim_combo in combinations(dims, r):
            actual_grp = (
                df[df["period"].isin(anomaly_periods)]
                .groupby(list(dim_combo))["metric_value"]
                .sum()
                .rename("actual")
            )
            baseline_grp = (
                df[~df["period"].isin(anomaly_periods)]
                .groupby(list(dim_combo))["metric_value"]
                .mean()
                .mul(n_anomaly_days)
                .rename("baseline_avg")
            )
            combo = pd.concat([actual_grp, baseline_grp], axis=1).fillna(0)
            combo["contribution"] = combo["actual"] - combo["baseline_avg"]
            combo["pct"] = (
                combo["contribution"] / anomaly_delta * 100 if anomaly_delta != 0 else 0
            )
            top = combo.nlargest(top_n, "contribution")
            for idx, row in top.iterrows():
                label = " + ".join(
                    f"{d}=[bold]{v}[/]"
                    for d, v in zip(dim_combo, (idx if isinstance(idx, tuple) else (idx,)))
                )
                level_results.append((label, row["contribution"], row["pct"]))

        results[r] = sorted(level_results, key=lambda x: abs(x[1]), reverse=True)

    return results


def print_summary(metric, period_label, actual_total, baseline_avg_total, n_days):
    delta = actual_total - baseline_avg_total
    pct_change = delta / baseline_avg_total * 100 if baseline_avg_total else float("inf")
    sign = "+" if delta >= 0 else ""
    days_note = f" ({n_days} days)" if n_days > 1 else ""

    console.print()
    console.rule("[bold cyan]Anomaly Summary[/]")
    console.print(
        f"  Metric:            [bold]{metric}[/]\n"
        f"  Period:            [bold]{period_label}[/]{days_note}\n"
        f"  Actual total:      [bold]{actual_total:,.1f}[/]\n"
        f"  Expected (scaled): {baseline_avg_total:,.1f}\n"
        f"  Delta:             [bold {'red' if delta > 0 else 'green'}]{sign}{delta:,.1f} ({sign}{pct_change:.0f}%)[/]"
    )
    console.print()


def print_dimension_table(dim: str, result: pd.DataFrame):
    table = Table(
        title=f"[bold]{dim}[/]",
        box=box.SIMPLE_HEAD,
        show_header=True,
        header_style="bold blue",
    )
    table.add_column("Value", style="cyan")
    table.add_column("Actual", justify="right")
    table.add_column("Baseline avg", justify="right")
    table.add_column("Contribution", justify="right")
    table.add_column("% of anomaly", justify="right")

    for val, row in result.iterrows():
        c = row["contribution"]
        p = row["pct_of_anomaly"]
        c_str = f"{'+'if c>=0 else ''}{c:,.1f}"
        p_str = f"{'+'if p>=0 else ''}{p:.1f}%"
        color = "red" if c > 0 else "green"
        table.add_row(
            str(val),
            f"{row['actual']:,.1f}",
            f"{row['baseline_avg']:,.1f}",
            f"[{color}]{c_str}[/]",
            f"[{color}]{p_str}[/]",
        )

    console.print(table)


def print_combo_table(combos_by_depth: dict[int, list[tuple]], top_n: int):
    ordinal = {2: "2-way", 3: "3-way", 4: "4-way", 5: "5-way"}
    for depth, combos in sorted(combos_by_depth.items()):
        label = ordinal.get(depth, f"{depth}-way")
        console.rule(f"[bold cyan]{label} Combination Attribution[/]")
        table = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold blue")
        table.add_column("Segment", style="cyan")
        table.add_column("Contribution", justify="right")
        table.add_column("% of anomaly", justify="right")

        for seg_label, contrib, pct in combos[:top_n]:
            c_str = f"{'+'if contrib>=0 else ''}{contrib:,.1f}"
            p_str = f"{'+'if pct>=0 else ''}{pct:.1f}%"
            color = "red" if contrib > 0 else "green"
            table.add_row(
                seg_label,
                f"[{color}]{c_str}[/]",
                f"[{color}]{p_str}[/]",
            )

        console.print(table)


def main():
    args = parse_args()

    # Validate backend-specific required args
    if args.backend == "databricks":
        missing = []
        if not args.host:
            missing.append("--host")
        if not args.http_path:
            missing.append("--http-path")
        if missing:
            console.print(
                f"[bold red]Error:[/] {' and '.join(missing)} required for databricks backend"
            )
            sys.exit(1)

    df, dimensions = load_data(args, args.metric, args.time_dimension)

    # Build the set of anomaly periods
    anomaly_start = pd.to_datetime(args.anomaly_date).date()
    anomaly_end = pd.to_datetime(args.anomaly_end).date() if args.anomaly_end else anomaly_start

    if anomaly_end < anomaly_start:
        console.print("[bold red]Error:[/] --anomaly-end must be >= --anomaly-date")
        sys.exit(1)

    all_periods = set(df["period"].unique())
    # Periods in the anomaly window that actually exist in the data
    anomaly_periods = {
        p for p in all_periods
        if anomaly_start <= p <= anomaly_end
    }

    missing = set(
        pd.date_range(anomaly_start, anomaly_end, freq="D").date
    ) - all_periods
    if not anomaly_periods:
        console.print(
            f"[bold red]Error:[/] no data found between [yellow]{anomaly_start}[/] and [yellow]{anomaly_end}[/].\n"
            f"Available periods: {sorted(all_periods)}"
        )
        sys.exit(1)
    if missing:
        console.print(f"[yellow]Warning:[/] some dates in range have no data: {sorted(missing)}")

    n_anomaly_days = len(anomaly_periods)
    period_label = (
        str(anomaly_start)
        if anomaly_start == anomaly_end
        else f"{anomaly_start} → {anomaly_end}"
    )

    actual_total = df[df["period"].isin(anomaly_periods)]["metric_value"].sum()
    # Scale baseline: mean daily total × number of days in anomaly window
    baseline_avg_total = (
        df[~df["period"].isin(anomaly_periods)]
        .groupby("period")["metric_value"]
        .sum()
        .mean()
        * n_anomaly_days
    )

    print_summary(args.metric, period_label, actual_total, baseline_avg_total, n_anomaly_days)

    console.rule("[bold cyan]Single-Dimension Attribution[/]")
    console.print()

    for dim in dimensions:
        result = contribution_to_change(
            df, dim, anomaly_periods, n_anomaly_days, baseline_avg_total, actual_total
        )
        print_dimension_table(dim, result)

    if len(dimensions) >= 2:
        console.print()
        combos_by_depth = combo_contribution(
            df, dimensions, anomaly_periods, n_anomaly_days,
            baseline_avg_total, actual_total, args.top_n, args.combo_depth,
        )
        if combos_by_depth:
            print_combo_table(combos_by_depth, args.top_n)

    console.print()


if __name__ == "__main__":
    main()
