from clients import get_alpaca_trading_client, get_bear_lake_client
from alpaca.trading.requests import GetPortfolioHistoryRequest
import datetime as dt
from zoneinfo import ZoneInfo
from rich import print
import polars as pl
import bear_lake as bl
from utils import get_last_market_date
from prefect import task, flow
from variables import TIME_ZONE


@task
def get_portfolio_history_by_date(date_: dt.date) -> pl.DataFrame:
    ext_open = dt.time(4, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    ext_close = dt.time(20, 0, 0, tzinfo=ZoneInfo("America/New_York"))

    start = dt.datetime.combine(date_, ext_open)
    end = dt.datetime.combine(date_, ext_close)

    alpaca_client = get_alpaca_trading_client()

    history_filter = GetPortfolioHistoryRequest(
        timeframe="1Min",  # Can only get 7 days of history.
        start=start,
        end=end,
        intraday_reporting="extended_hours",  # market_hours: 9:30am to 4pm ET. extended_hours: 4am to 8pm ET
        pnl_reset="per_day",
    )

    portfolio_history = alpaca_client.get_portfolio_history(history_filter)

    return pl.DataFrame(
        {
            "timestamp": portfolio_history.timestamp,
            "daily_cumulative_return": portfolio_history.profit_loss_pct,
            "daily_values": portfolio_history.base_value,
        }
    ).with_columns(
        pl.from_epoch("timestamp").dt.convert_time_zone("UTC"),
        pl.col("daily_values").mul(pl.col("daily_cumulative_return").add(1)),
    )


@task
def get_market_dates(start: dt.date, end: dt.date) -> list[dt.date]:
    bear_lake_client = get_bear_lake_client()

    return (
        bear_lake_client.query(
            bl.table("calendar").filter(pl.col("date").is_between(start, end))
        )["date"]
        .sort()
        .to_list()
    )


@task
def get_portfolio_history(start: dt.date, end: dt.date):
    market_dates = get_market_dates(start, end)

    portfolio_history_list = []
    for market_date in market_dates:
        portfolio_history_list.append(get_portfolio_history_by_date(market_date))

    return pl.concat(portfolio_history_list)


@task
def upload_and_merge_portfolio_history(portfolio_history: pl.DataFrame):
    bear_lake_client = get_bear_lake_client()
    table_name = "portfolio_history"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "timestamp": pl.Datetime,
            "daily_cumulative_return": pl.Float64,
            "daily_values": pl.Float64,
        },
        partition_keys=None,
        primary_keys=["timestamp"],
        mode="skip",
    )

    # Insert into table
    bear_lake_client.insert(name=table_name, data=portfolio_history, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@flow
def portfolio_history_backfill_flow():
    start = dt.date(2026, 1, 2)
    end = (dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)).date()

    portfolio_history = get_portfolio_history(start, end)

    upload_and_merge_portfolio_history(portfolio_history)


@flow
def portfolio_history_daily_flow():
    last_market_date = get_last_market_date()
    yesterday = (dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)).date()

    # Only get new data if yesterday was the last market date
    if last_market_date != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", last_market_date)
        print("Yesterday:", yesterday)
        return

    portfolio_history = get_portfolio_history(last_market_date, last_market_date)

    upload_and_merge_portfolio_history(portfolio_history)
