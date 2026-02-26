from benchmark_flow import benchmark_backfill_flow, benchmark_daily_flow
from calendar_flow import calendar_backfill_flow
from etf_prices_flow import etf_prices_backfill_flow, etf_prices_daily_flow
from prefect import flow, serve
from prefect.schedules import Cron
from returns_flow import returns_backfill_flow
from stock_prices_flow import (stock_prices_backfill_flow,
                               stock_prices_daily_flow)
from trading_flow import trading_daily_flow
from universe_flow import universe_backfill_flow


@flow
def daily_flow():
    calendar_backfill_flow()
    universe_backfill_flow()  # Depends on calendar
    stock_prices_daily_flow()  # Depends on universe
    etf_prices_daily_flow()  # Depends on calendar
    returns_backfill_flow()  # Depends on stock_prices and etf_prices
    benchmark_daily_flow()  # Depends on stock_returns


@flow
def backfill_flow():
    calendar_backfill_flow()
    universe_backfill_flow()  # Depends on calendar
    stock_prices_backfill_flow()  # Depends on universe
    etf_prices_backfill_flow()  # Depends on calendar
    returns_backfill_flow()  # Depends on stock_prices and etf_prices
    benchmark_backfill_flow()  # Depends on stock_returns


if __name__ == "__main__":
    serve(
        daily_flow.to_deployment(
            name="daily-flow", schedule=Cron("0 2 * * *", timezone="America/Denver")
        ),
        trading_daily_flow.to_deployment(
            name="trading-daily-flow",
            schedule=Cron("30 7 * * *", timezone="America/Denver"),
        ),
        backfill_flow.to_deployment(name="backfill-flow"),
    )
