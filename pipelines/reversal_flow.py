from prefect import task, flow
import polars as pl
import datetime as dt
from clients import get_clickhouse_client
from variables import IC


@task
def get_stock_returns(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    stock_returns_arrow = clickhouse_client.query_arrow(
        f"""SELECT * FROM stock_returns WHERE date BETWEEN '{start}' AND '{end}'"""
    )

    return pl.from_arrow(stock_returns_arrow)


@task
def get_idio_vol(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    stock_returns_arrow = clickhouse_client.query_arrow(
        f"""SELECT * FROM idio_vol WHERE date BETWEEN '{start}' AND '{end}'"""
    )

    return pl.from_arrow(stock_returns_arrow)


@task
def calculate_signals(stock_returns: pl.DataFrame) -> pl.DataFrame:
    return (
        stock_returns.sort("ticker", "date")
        .select(
            "ticker",
            "date",
            pl.lit("reversal").alias("signal"),
            pl.col("return")
            .log1p()
            .rolling_sum(21)
            .mul(-1)
            .over("ticker")
            .alias("value"),
        )
        .drop_nulls()
        .sort("ticker", "date")
    )


@task
def calculate_scores(signals: pl.DataFrame, signal_name: str) -> pl.DataFrame:
    return signals.select(
        "ticker",
        "date",
        pl.lit(signal_name).alias("signal"),
        pl.col("value")
        .sub(pl.col("value").mean())
        .truediv(pl.col("value").std())
        .alias("score"),
    )


@task
def calculate_alphas(
    scores: pl.DataFrame, idio_vol: pl.DataFrame, signal_name: str
) -> pl.DataFrame:
    return (
        scores.join(other=idio_vol, on=["ticker", "date"], how="left")
        .drop_nulls()
        .select(
            "ticker",
            "date",
            pl.lit(signal_name).alias("signal"),
            pl.lit(IC).mul(pl.col("score")).mul(pl.col("idio_vol")).alias("alpha"),
        )
        .sort("ticker", "date")
    )


@task
def upload_and_merge_signals(signals: pl.DataFrame) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()
    table_name = "signals"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticker String,
            date String,
            signal String,
            value Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, date, signal)
        """
    )

    # Insert
    clickhouse_client.insert_df_arrow(table_name, signals)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@task
def upload_and_merge_scores(scores: pl.DataFrame) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()
    table_name = "scores"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticker String,
            date String,
            signal String,
            score Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, date, signal)
        """
    )

    # Insert
    clickhouse_client.insert_df_arrow(table_name, scores)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@task
def upload_and_merge_alphas(alphas: pl.DataFrame) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()
    table_name = "alphas"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticker String,
            date String,
            signal String,
            alpha Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, date, signal)
        """
    )

    # Insert
    clickhouse_client.insert_df_arrow(table_name, alphas)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@flow
def reversal_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)
    signal_name = "reversal"

    stock_returns = get_stock_returns(start, end)
    idio_vol = get_idio_vol(start, end)

    signals = calculate_signals(stock_returns)
    scores = calculate_scores(signals, signal_name)
    alphas = calculate_alphas(scores, idio_vol, signal_name)

    upload_and_merge_signals(signals)
    upload_and_merge_scores(scores)
    upload_and_merge_alphas(alphas)


@task
def get_trading_date_range(window: int) -> dt.date:
    clickhouse_client = get_clickhouse_client()
    date_range_arrow = clickhouse_client.query_arrow(
        f"SELECT date FROM calendar ORDER BY date DESC LIMIT {window}"
    )
    return pl.from_arrow(date_range_arrow).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )


@flow
def reversal_daily_flow():
    date_range = get_trading_date_range(window=21)

    start = date_range["date"].min()
    end = date_range["date"].max()

    yesterday = dt.date.today() - dt.timedelta(days=1)

    # Only get new data if yesterday was the last market date
    if end != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", end)
        print("Yesterday:", yesterday)
        return

    signal_name = "reversal"

    stock_returns = get_stock_returns(start, end)
    idio_vol = get_idio_vol(start, end)

    signals = calculate_signals(stock_returns).filter(pl.col("date").eq(str(end)))
    scores = calculate_scores(signals, signal_name).filter(pl.col("date").eq(str(end)))
    alphas = calculate_alphas(scores, idio_vol, signal_name).filter(
        pl.col("date").eq(str(end))
    )

    if not (len(signals) > 0 and len(scores) > 0 and len(alphas) > 0):
        raise ValueError("No values found!")

    upload_and_merge_signals(signals)
    upload_and_merge_scores(scores)
    upload_and_merge_alphas(alphas)
