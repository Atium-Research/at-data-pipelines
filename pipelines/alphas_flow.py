import datetime as dt

import polars as pl
from clients import get_bear_lake_client
from configs import load_alpha_configs
from prefect import flow, task
from signals import REGISTRY
from utils import (get_idio_vol, get_stock_returns, get_trading_date_range,
                   get_universe)
from atium.signals import compute_alphas, compute_scores


@task
def calculate_signals(stock_returns: pl.DataFrame, config: dict) -> pl.DataFrame:
    compute = REGISTRY[config["signal"]]
    expr = compute(**config["parameters"])
    return (
        stock_returns
        .with_columns(expr.over("ticker").alias("signal"))
        .select("date", "ticker", "signal")
    )


@task
def calculate_scores(signals: pl.DataFrame) -> pl.DataFrame:
    return compute_scores(signals)


@task
def calculate_alphas(
    universe: pl.DataFrame, scores: pl.DataFrame, idio_vol: pl.DataFrame
) -> pl.DataFrame:
    return compute_alphas(universe=universe, scores=scores, idio_vol=idio_vol)


@task
def upload_and_merge_signals(signals: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "signals"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "name": pl.String,
            "value": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["ticker", "date", "name"],
        mode="skip",
    )

    # Insert
    bear_lake_client.insert(name=table_name, data=signals, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@task
def upload_and_merge_scores(scores: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "scores"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "name": pl.String,
            "score": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["ticker", "date", "name"],
        mode="skip",
    )

    # Insert
    bear_lake_client.insert(name=table_name, data=scores, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@task
def upload_and_merge_alphas(alphas: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "alphas"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "name": pl.String,
            "alpha": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["ticker", "date", "name"],
        mode="skip",
    )

    # Insert
    bear_lake_client.insert(name=table_name, data=alphas, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


def run_configs(
    configs: list[dict],
    stock_returns: pl.DataFrame,
    universe: pl.DataFrame,
    idio_vol: pl.DataFrame,
    filter_date: dt.date | None = None,
):
    for config in configs:
        name = config["name"]

        signals = calculate_signals(stock_returns, config)
        scores = calculate_scores(signals)
        alphas = calculate_alphas(universe, scores, idio_vol)

        if filter_date is not None:
            signals = signals.filter(pl.col("date").eq(filter_date))
            scores = scores.filter(pl.col("date").eq(filter_date))
            alphas = alphas.filter(pl.col("date").eq(filter_date))

            if not (len(signals) > 0 and len(scores) > 0 and len(alphas) > 0):
                raise ValueError(f"No values found for config '{name}'!")

        # Format signals for upload
        signals = (
            signals
            .rename({"signal": "value"})
            .with_columns(
                pl.lit(name).alias("name"),
                pl.col("date").dt.year().alias("year"),
            )
            .select("ticker", "date", "year", "name", "value")
        )

        # Format scores for upload
        scores = scores.with_columns(
            pl.lit(name).alias("name"),
            pl.col("date").dt.year().alias("year"),
        ).select("ticker", "date", "year", "name", "score")

        # Format alphas for upload
        alphas = alphas.with_columns(
            pl.lit(name).alias("name"),
            pl.col("date").dt.year().alias("year"),
        ).select("ticker", "date", "year", "name", "alpha")

        upload_and_merge_signals(signals)
        upload_and_merge_scores(scores)
        upload_and_merge_alphas(alphas)


@flow
def alphas_backfill_flow():
    configs = load_alpha_configs()

    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)

    stock_returns = get_stock_returns(start, end)
    universe = get_universe(start, end)
    idio_vol = get_idio_vol(start, end)

    run_configs(configs, stock_returns, universe, idio_vol)


@flow
def alphas_daily_flow():
    configs = load_alpha_configs()

    max_window = max(
        sum(config["parameters"].values()) for config in configs
    )
    date_range = get_trading_date_range(window=max_window * 2)

    start = date_range["date"].min()
    end = date_range["date"].max()

    yesterday = dt.date.today() - dt.timedelta(days=1)

    # Only get new data if yesterday was the last market date
    if end != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", end)
        print("Yesterday:", yesterday)
        return

    stock_returns = get_stock_returns(start, end)
    universe = get_universe(start, end)
    idio_vol = get_idio_vol(start, end)

    run_configs(configs, stock_returns, universe, idio_vol, filter_date=end)

if __name__ == '__main__':
    alphas_backfill_flow()