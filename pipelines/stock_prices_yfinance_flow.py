from clients import get_clickhouse_client
import datetime as dt
import pandas as pd
import polars as pl
from prefect import flow, task, get_run_logger
from pipelines.stock_prices_flow import get_tickers
from variables import TIME_ZONE
import yfinance as yf


@task
def get_stock_prices_yfinance(
    tickers: list[str], start: dt.datetime, end: dt.datetime
) -> pl.DataFrame:
    """
    Fetch historical stock prices from yfinance.
    Returns data in the same schema as Alpaca, with vwap and trade_count as null.
    """
    stock_prices_list = []
    logger = get_run_logger()
    for ticker in tickers:
        try:
            # yfinance expects dates without timezone, so convert to naive datetime
            start_naive = start.replace(tzinfo=None)
            end_naive = end.replace(tzinfo=None)

            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(start=start_naive, end=end_naive, interval="1d")

            if hist.empty:
                continue

            hist_df = pl.from_pandas(hist.reset_index())

            date_col = None
            for col in ["Date", "Datetime"]:
                if col in hist_df.columns:
                    date_col = col
                    break

            if date_col is None:
                continue

            # Rename columns to match schema
            stock_prices_clean = hist_df.select(
                pl.lit(ticker).alias("ticker"),
                pl.col(date_col).dt.date().cast(pl.String).alias("date"),
                pl.col("Open").alias("open"),
                pl.col("High").alias("high"),
                pl.col("Low").alias("low"),
                pl.col("Close").alias("close"),
                pl.col("Volume").cast(pl.Float64).alias("volume"),
            )

            stock_prices_list.append(stock_prices_clean)

        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}")
            continue

    if not stock_prices_list:
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "date": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
            }
        )

    return pl.concat(stock_prices_list).sort("date", "ticker")


@task
def get_stock_prices_yfinance_batches(
    tickers: list[str], start: dt.datetime, end: dt.datetime
) -> pl.DataFrame:
    """
    Fetch stock prices in batches by year.
    TODO: refactor to use yf.download()
    """
    years = range(start.year, end.year + 1)
    stock_prices_list = []

    for year in years:
        year_start = max(dt.datetime(year, 1, 1, 0, 0, 0, tzinfo=TIME_ZONE), start)
        year_end = min(dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=TIME_ZONE), end)

        stock_prices = get_stock_prices_yfinance(tickers, year_start, year_end)

        if not stock_prices.is_empty():
            stock_prices_list.append(stock_prices)

    if not stock_prices_list:
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "date": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "trade_count": pl.Float64,
                "vwap": pl.Float64,
            }
        )

    return pl.concat(stock_prices_list).sort("date", "ticker")


@flow
def stock_prices_yfinance_backfill_flow():
    """
    Grabbing yfinance data from 2000 to present day.
    """
    start = dt.datetime(2000, 1, 1, tzinfo=TIME_ZONE)
    end = dt.datetime.now(TIME_ZONE)

    tickers = get_tickers()
    stock_prices_df = get_stock_prices_yfinance_batches(tickers, start, end)
    """
    TODO: send to clickhouse client
    """
