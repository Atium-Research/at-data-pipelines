from clients import get_clickhouse_client
import datetime as dt
import polars as pl


def get_last_market_date() -> dt.date:
    clickhouse_client = get_clickhouse_client()
    last_market_date = clickhouse_client.query("SELECT MAX(date) FROM calendar")
    return dt.datetime.strptime(last_market_date.result_rows[0][0], "%Y-%m-%d").date()


def get_trading_date_range(window: int) -> dt.date:
    clickhouse_client = get_clickhouse_client()
    date_range_arrow = clickhouse_client.query_arrow(
        f"SELECT date FROM calendar ORDER BY date DESC LIMIT {window}"
    )
    return pl.from_arrow(date_range_arrow).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )
